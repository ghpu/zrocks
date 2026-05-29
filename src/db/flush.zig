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

/// The result of building an L0 SST from an immutable memtable (D2a-2): the
/// metadata a later, foreground `commitFlush` needs to register the file in the
/// VersionSet.  `smallest`/`largest` are gpa-owned INTERNAL keys (the caller
/// frees them — `commitFlush` does, or `deinit` on an error path).  When
/// `num_entries == 0` the SST was empty and dropped (no file on disk).
pub const BuildResult = struct {
    file_number: u64,
    file_size: u64 = 0,
    smallest: ?[]u8 = null,
    largest: ?[]u8 = null,
    num_entries: u64 = 0,
    has_range_tombstones: bool = false,

    /// Free the gpa-owned key copies (use on an error path that abandons the
    /// build before `commitFlush` consumes them).
    pub fn deinit(self: *BuildResult, gpa: std.mem.Allocator) void {
        if (self.smallest) |s| gpa.free(s);
        if (self.largest) |l| gpa.free(l);
        self.smallest = null;
        self.largest = null;
    }
};

/// Build a fresh L0 SST from `imm` and return its metadata WITHOUT touching the
/// VersionSet (D2a-2).  This is the heavy (encode + filesystem-write) phase of a
/// flush and is what the background flush worker runs concurrently; the caller
/// later calls `commitFlush` on the foreground to register the file.
///
/// `file_number` is pre-allocated by the caller (under the write mutex) so the
/// worker never touches the shared VersionSet's file-number counter.  An empty
/// memtable with no range tombstones writes no file (`num_entries == 0`); the
/// caller treats that as a no-op flush.
pub fn buildMemTableSST(
    gpa: std.mem.Allocator,
    e: env.Env,
    dbname: []const u8,
    options: options_mod.Options,
    ikc: comparator.Comparator,
    imm: *MemTable,
    file_number: u64,
) !BuildResult {
    const path = try filename.tableFileName(gpa, dbname, file_number);
    defer gpa.free(path);

    var build_opts = options;
    build_opts.comparator = ikc;
    build_opts.compression = options.compressionForLevel(0);
    const policy = bloom.BloomFilterPolicy.init(kFilterBitsPerKey);

    var result: BuildResult = .{ .file_number = file_number };
    errdefer result.deinit(gpa);

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
            if (result.smallest == null) result.smallest = try gpa.dupe(u8, ikey);
            if (result.largest) |l| gpa.free(l);
            result.largest = try gpa.dupe(u8, ikey);
            result.num_entries += 1;
        }

        for (imm.range_tombstones.tombstones.items) |t| {
            try tb.addRangeTombstone(t.begin, t.end, t.seq);
            const b_ik = try encodeInternalKey(gpa, t.begin, t.seq, .range_deletion);
            defer gpa.free(b_ik);
            const e_ik = try encodeInternalKey(gpa, t.end, t.seq, .range_deletion);
            defer gpa.free(e_ik);
            if (result.smallest == null or ikc.compare(b_ik, result.smallest.?) == .lt) {
                if (result.smallest) |s| gpa.free(s);
                result.smallest = try gpa.dupe(u8, b_ik);
            }
            if (result.largest == null or ikc.compare(e_ik, result.largest.?) == .gt) {
                if (result.largest) |l| gpa.free(l);
                result.largest = try gpa.dupe(u8, e_ik);
            }
            if (result.num_entries == 0) result.num_entries = 1;
        }

        try tb.finish();
        result.file_size = tb.fileSize();
        try wf.close();
    }

    result.has_range_tombstones = !imm.range_tombstones.isEmpty();

    if (result.num_entries == 0) {
        // Empty memtable: drop the (empty) file so the directory stays clean.
        e.deleteFile(path) catch {};
        result.deinit(gpa);
    }

    return result;
}

/// Register the SST built by `buildMemTableSST` in the VersionSet via a
/// VersionEdit (D2a-2): the foreground, serialized phase of a flush.  Consumes
/// (frees) `result`'s key copies.  `new_log_number`/`last_sequence` rotate the
/// active log + advance the sequence exactly as the legacy synchronous flush
/// did.  An empty build (`num_entries == 0`) still records the log rotation.
pub fn commitFlush(
    gpa: std.mem.Allocator,
    versions: *version_set.VersionSet,
    result: *BuildResult,
    new_log_number: u64,
    last_sequence: u64,
) !void {
    defer result.deinit(gpa);

    var edit = version_edit.VersionEdit.init();
    defer edit.deinit(gpa);

    if (result.num_entries != 0) {
        try edit.addFile(gpa, 0, result.file_number, result.file_size, result.smallest.?, result.largest.?);
        edit.setLastFileHasRangeTombstones(result.has_range_tombstones);
    }

    edit.setLogNumber(new_log_number);
    edit.setLastSequence(last_sequence);
    try versions.logAndApply(&edit);
}

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
    // D2a-2: the synchronous flush is now the build phase (the heavy SST write)
    // followed immediately by the foreground commit phase.  The background flush
    // worker (db.zig) runs `buildMemTableSST` on a concurrent fiber and calls
    // `commitFlush` on the foreground; this wrapper keeps the original
    // single-call semantics for the standalone flush tests + any direct caller.
    const file_number = versions.newFileNumber();
    var result = try buildMemTableSST(gpa, e, dbname, options, ikc, imm, file_number);
    // commitFlush consumes (frees) `result`.
    try commitFlush(gpa, versions, &result, new_log_number, last_sequence);
}
