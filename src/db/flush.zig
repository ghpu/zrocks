//! flush.zig — memtable -> L0 SST flush (M6.1).
//!
//! `flushMemTable` writes an immutable MemTable's entries into a new on-disk
//! L0 SSTable and records the new file in the VersionSet via a VersionEdit.
//! This is the path that makes committed data actually leave RAM and become
//! durable in the LSM tree (until M6.2 it only ever produces L0 files).
//!
//! The SST stores INTERNAL keys (user_key ++ trailer) ordered by the
//! InternalKeyComparator — exactly the order the MemTable iterator already
//! yields — so the table is built with a copy of the DB options whose
//! comparator is the IKC.  The block builder learned this comparator in the
//! M6.1 prerequisite fix, so multiple versions of the same user key (which
//! differ only in their trailer) land in the same data block correctly.
//!
//! Standalone test note (Zig 0.16): `../...` imports only resolve inside the
//! `src`-rooted module:
//!   printf 'test { _ = @import("db/flush.zig"); }' > src/_verify.zig \
//!     && zig test src/_verify.zig && rm src/_verify.zig

const std = @import("std");

const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const comparator = @import("../util/comparator.zig");
const internal_key = @import("../format/internal_key.zig");
const memtable_mod = @import("../memtable/memtable.zig");
const table_builder_mod = @import("../format/table_builder.zig");
const bloom = @import("../format/bloom.zig");

const version_set = @import("../version/version_set.zig");
const version_edit = @import("../version/version_edit.zig");
const filename = @import("../version/filename.zig");
const coding = @import("../util/coding.zig");

const MemTable = memtable_mod.MemTable;

/// Encode `user_key ++ fixed64(packSequenceAndType(seq, t))` (caller frees).
fn encodeInternalKey(gpa: std.mem.Allocator, user_key: []const u8, seq: u64, t: internal_key.ValueType) ![]u8 {
    const out = try gpa.alloc(u8, user_key.len + 8);
    @memcpy(out[0..user_key.len], user_key);
    coding.encodeFixed64(out[user_key.len..][0..8], internal_key.packSequenceAndType(seq, t));
    return out;
}

/// Bloom bits/key used for flushed SSTs.  Must match the TableCache reader,
/// which opens tables with `BloomFilterPolicy.init(10)`.
const kFilterBitsPerKey: usize = 10;

/// Write every entry of `imm` into a fresh L0 SSTable and register it with
/// `versions` via a VersionEdit.
///
/// Steps (mirrors LevelDB's `DBImpl::WriteLevel0Table` + `BuildTable`):
///   1. Allocate a fresh file number and create `<dbname>/<number>.sst`.
///   2. Build the table with the InternalKeyComparator over the memtable's
///      internal-key iterator, tracking the smallest (first) and largest (last)
///      internal keys.
///   3. logAndApply a VersionEdit: addFile(L0, ...) plus the rotated log number
///      and the new last sequence, so recovery picks up the SST and stops
///      replaying the flushed log.
///
/// `new_log_number` is the WAL number that becomes the active log after this
/// flush (the caller rotated the WAL before calling).  `last_sequence` is the
/// DB's last sequence at flush time.  An empty memtable is a no-op (LevelDB
/// likewise skips writing an empty table).
pub fn flushMemTable(
    gpa: std.mem.Allocator,
    e: env.Env,
    dbname: []const u8,
    options: options_mod.Options,
    ikc: comparator.Comparator,
    versions: *version_set.VersionSet,
    imm: *MemTable,
    new_log_number: u64,
    last_sequence: u64,
) !void {
    const file_number = versions.newFileNumber();

    const path = try filename.tableFileName(gpa, dbname, file_number);
    defer gpa.free(path);

    // Build the SST with the InternalKeyComparator (SSTs store internal keys).
    var build_opts = options;
    build_opts.comparator = ikc;
    const policy = bloom.BloomFilterPolicy.init(kFilterBitsPerKey);

    // smallest/largest are gpa-owned here; addFile later dupes them into
    // edit-owned memory, so we always free our copies on exit (defer covers the
    // success path too, not just errors).
    var smallest: ?[]u8 = null;
    var largest: ?[]u8 = null;
    defer {
        if (smallest) |s| gpa.free(s);
        if (largest) |l| gpa.free(l);
    }

    var num_entries: u64 = 0;
    var file_size: u64 = 0;

    {
        var wf = try e.newWritableFile(gpa, path);
        errdefer wf.close() catch {};

        var tb = try table_builder_mod.TableBuilder.init(gpa, build_opts, wf, policy);
        defer tb.deinit();

        var it = MemTable.Iterator.init(imm);
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            const ikey = it.internalKey();
            try tb.add(ikey, it.value());
            if (smallest == null) smallest = try gpa.dupe(u8, ikey);
            // largest is the last key seen; replace each step.
            if (largest) |l| gpa.free(l);
            largest = try gpa.dupe(u8, ikey);
            num_entries += 1;
        }

        // M7.5: carry the memtable's range tombstones into the SST's range-del
        // meta block, and widen the file's key range to cover them (so reads /
        // compaction inputs over the tombstone span pick up this file).  The
        // smallest/largest are INTERNAL keys; we synthesize them from the
        // tombstone begin/end at this memtable's sequence range.  A tombstone's
        // begin extends `smallest`; its end (exclusive) extends `largest`.
        for (imm.range_tombstones.tombstones.items) |t| {
            try tb.addRangeTombstone(t.begin, t.end, t.seq);
            // Synthesize internal keys for range widening (type/seq only matter
            // for the IKC user-key comparison done by overlap/get).
            const b_ik = try encodeInternalKey(gpa, t.begin, t.seq, .range_deletion);
            defer gpa.free(b_ik);
            const e_ik = try encodeInternalKey(gpa, t.end, t.seq, .range_deletion);
            defer gpa.free(e_ik);
            if (smallest == null or ikc.compare(b_ik, smallest.?) == .lt) {
                if (smallest) |s| gpa.free(s);
                smallest = try gpa.dupe(u8, b_ik);
            }
            if (largest == null or ikc.compare(e_ik, largest.?) == .gt) {
                if (largest) |l| gpa.free(l);
                largest = try gpa.dupe(u8, e_ik);
            }
            // The SST is non-empty if it carries any tombstone, even without
            // point entries, so it must be registered below.
            if (num_entries == 0) num_entries = 1;
        }

        try tb.finish();
        file_size = tb.fileSize();
        try wf.close();
    }

    var edit = version_edit.VersionEdit.init();
    defer edit.deinit(gpa);

    if (num_entries == 0) {
        // An empty memtable produces no L0 file; drop the (empty) table.  In
        // practice flush is only triggered when the memtable is non-empty, but
        // stay defensive so the log rotation below still happens.
        e.deleteFile(path) catch {};
    } else {
        // addFile dupes the key bytes into edit-owned memory.
        try edit.addFile(gpa, 0, file_number, file_size, smallest.?, largest.?);
        // D3a-M1 range-tombstone guard: record whether this SST carries any
        // range tombstones so `DB.get`/`DB.newIterator` can skip building the
        // aggregator on the fast path (no tombstones anywhere).
        edit.setLastFileHasRangeTombstones(!imm.range_tombstones.isEmpty());
    }

    // Rotate to the new active log and advance the last sequence so a later
    // recovery replays only the new (active) WAL — the old log's data is now in
    // this SST.
    edit.setLogNumber(new_log_number);
    edit.setLastSequence(last_sequence);
    try versions.logAndApply(&edit);
}
