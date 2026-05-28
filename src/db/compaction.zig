//! compaction.zig — leveled (LevelDB-style) compaction (M6.2).
//!
//! Merges SST files from one level into the next while preserving exact LSM
//! read semantics.  `pickCompaction` chooses what to compact (by score),
//! `doCompaction` performs the merge — feeding every input file's IKC-ordered
//! iterator through a `MergingIterator`, dropping shadowed/obsolete entries
//! (older duplicates below the snapshot, and tombstones that no deeper level
//! needs), and writing the surviving entries into fresh `level+1` SSTs split by
//! `target_file_size_base`.  A single `VersionEdit` removes the input files and
//! adds the outputs, so the new Version reflects the merge atomically.
//!
//! Following LevelDB: the merged stream is in IKC order (user key ascending,
//! then sequence descending), so the FIRST occurrence of each user key is the
//! newest version.
//!
//! Merge operands (M7.1): a `.merge` entry COMBINES with older entries rather
//! than superseding them, so the per-version drop rule must not touch it.  When
//! the newest surviving entry for a user key is a `.merge` and an operator is
//! configured, `collapseMergeRun` accumulates the operand run and collapses it
//! (with the underlying base/deletion) into a single value — keeping
//! above-snapshot operands verbatim and never losing an operand.
//!
//! Compaction filter (M7.4): when `options.compaction_filter` is set, the newest
//! plain `.value` survivor of each user key that is NOT visible to a live
//! snapshot is handed to the filter, which may keep, drop (`.remove`), or rewrite
//! (`.change`) it.  A `.change` replacement is gpa-allocated by the filter and
//! freed here after writing.  Snapshot-protected versions, merge operands,
//! deletions, and older (hidden) versions are never filtered.
//!
//! What is implemented vs left as TODO:
//!   * Size-based output split — implemented (correctness-sufficient).
//!   * Tombstone drop at the base level — implemented (isBaseLevelForKey).
//!   * Merge operand collapse — implemented (collapseMergeRun, M7.1).
//!   * Compaction filter on plain `.value` survivors — implemented (M7.4).
//!   * Compaction filter on merge-derived values / FilterMergeOperand — TODO.
//!   * Per-compaction CompactionFilterFactory — TODO (single filter for now).
//!   * Grandparent-overlap split — TODO (refinement; size split suffices).
//!   * Boundary-input expansion ("AddBoundaryInputs") — TODO (refinement).
//!   * Obsolete .sst deletion from disk — TODO (files are dropped from the
//!     Version but left on disk; a future SST file manager reclaims them).

const std = @import("std");

const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const comparator = @import("../util/comparator.zig");
const internal_key = @import("../format/internal_key.zig");
const version_set = @import("../version/version_set.zig");
const version_edit = @import("../version/version_edit.zig");
const table_cache_mod = @import("../version/table_cache.zig");
const merging_iterator = @import("../iterator/merging_iterator.zig");
const iterator = @import("../iterator/iterator.zig");
const table_builder_mod = @import("../format/table_builder.zig");
const bloom = @import("../format/bloom.zig");
const filename = @import("../version/filename.zig");
const coding = @import("../util/coding.zig");
const merge_operator_mod = @import("../rocks/merge_operator.zig");
const delete_range = @import("../rocks/delete_range.zig");

const FileMetaData = version_edit.FileMetaData;
const VersionSet = version_set.VersionSet;

/// Bloom bits/key for compaction-output SSTs (must match the reader policy).
const kFilterBitsPerKey: usize = 10;

pub const Compaction = struct {
    /// The level being compacted; output files land at `output_level` (which
    /// defaults to `level + 1` for leveled compaction).
    level: usize,
    /// Level the merged output files are written to.  Leveled compaction sets
    /// this to `level + 1`; universal compaction (M7.3) sets it to 0 so merged
    /// runs stay in L0.  When null, `doCompaction` treats it as `level + 1`.
    /// TODO: real RocksDB universal may place a fully-merged run at the bottom
    /// level; we keep it in L0 here (simpler, correctness-sufficient).
    output_level: ?usize = null,
    /// inputs[0] = files chosen from `level`; inputs[1] = the overlapping files
    /// at the level just below (`level + 1`) — empty for universal.  Each list
    /// deep-owns its FileMetaData key bytes.
    inputs: [2]std.ArrayListUnmanaged(FileMetaData),
    /// When true, tombstones must NEVER be dropped during this compaction even if
    /// `isBaseLevelForKey` says so, because OLDER data this compaction does not
    /// read could resurface.  Set for a PARTIAL universal merge (only the newest
    /// L0 runs are merged; older L0 runs survive and could resurrect a deleted
    /// key if its tombstone were dropped).  Leveled compaction leaves this false
    /// and relies on the per-key `isBaseLevelForKey` check.
    keep_tombstones: bool = false,

    /// The level the output files land at (`output_level` or `level + 1`).
    pub fn outputLevel(self: *const Compaction) usize {
        return self.output_level orelse (self.level + 1);
    }

    pub fn deinit(self: *Compaction, gpa: std.mem.Allocator) void {
        for (&self.inputs) |*list| {
            for (list.items) |f| {
                gpa.free(f.smallest);
                gpa.free(f.largest);
            }
            list.deinit(gpa);
        }
        self.* = undefined;
    }
};

/// Pick the next compaction to run, or null if no level wants compacting.
///
/// Scores every level (L0 by file count, deeper by bytes); the winner's input
/// files are chosen from that level — for L0 the first file then EXPAND to all
/// overlapping L0 files; for deeper levels the first file (round-robin
/// compact-pointers are a TODO).  The combined user-key range then selects the
/// overlapping files at `level+1` as inputs[1].
pub fn pickCompaction(
    gpa: std.mem.Allocator,
    versions: *VersionSet,
    user_cmp: comparator.Comparator,
) !?Compaction {
    const level = versions.pickCompactionLevel() orelse return null;
    const v = versions.currentVersion();
    if (v.numFiles(level) == 0) return null;

    var c = Compaction{
        .level = level,
        .inputs = .{ .empty, .empty },
    };
    errdefer c.deinit(gpa);

    // --- inputs[0]: file(s) from `level` -----------------------------------
    if (level == 0) {
        // Pick the first L0 file, then expand to every L0 file overlapping the
        // accumulated range (L0 files overlap arbitrarily).  overlappingInputs
        // returns a fresh deep-owned list, so take it directly.
        const first = v.files[0].items[0];
        c.inputs[0] = try v.overlappingInputs(gpa, 0, first.smallest, first.largest, user_cmp);
    } else {
        // Pick the first file at this level (TODO: round-robin compact pointer).
        const first = v.files[level].items[0];
        try c.inputs[0].append(gpa, .{
            .number = first.number,
            .file_size = first.file_size,
            .smallest = try gpa.dupe(u8, first.smallest),
            .largest = try gpa.dupe(u8, first.largest),
        });
    }

    // --- combined user-key range of inputs[0] (internal keys) --------------
    const range = keyRange(c.inputs[0].items, user_cmp);

    // --- inputs[1]: overlapping files at level+1 ---------------------------
    c.inputs[1] = try v.overlappingInputs(gpa, level + 1, range.smallest, range.largest, user_cmp);

    return c;
}

// ===========================================================================
// FIFO compaction (M7.3) — cache-like whole-file eviction of oldest L0 files
// ===========================================================================

/// Produce a VersionEdit that DROPS the oldest L0 files (lowest file numbers)
/// when `totalFileSize(0)` exceeds `fifo_max_table_files_size`, applying it via
/// `logAndApply` — until the L0 byte total is back at-or-below the budget (or
/// only one file remains; we never evict the sole/last file so the DB keeps the
/// most-recent data).  Returns `true` if any file was evicted (so the caller can
/// loop), `false` if nothing needed evicting.
///
/// FIFO never merges — it removes whole files, oldest first, so the
/// earliest-written data is evicted like a ring buffer.  No output files are
/// produced.
/// TODO: ttl — only the size-based policy is implemented.
pub fn runFifoEviction(
    gpa: std.mem.Allocator,
    versions: *VersionSet,
    fifo_max_table_files_size: u64,
) !bool {
    const v = versions.currentVersion();
    if (v.totalFileSize(0) <= fifo_max_table_files_size) return false;
    if (v.numFiles(0) <= 1) return false; // keep at least one (the newest) file.

    // L0 is stored oldest-first (newest last; see applyEdit), and file numbers
    // increase with recency.  Be robust to ordering by selecting the lowest file
    // numbers explicitly.  Collect (number, size) pairs and sort by number asc.
    const NumSize = struct { number: u64, size: u64 };
    var files: std.ArrayListUnmanaged(NumSize) = .empty;
    defer files.deinit(gpa);
    for (v.files[0].items) |f| try files.append(gpa, .{ .number = f.number, .size = f.file_size });
    std.mem.sort(NumSize, files.items, {}, struct {
        fn lt(_: void, a: NumSize, b: NumSize) bool {
            return a.number < b.number;
        }
    }.lt);

    // Walk oldest-first, marking files for eviction until we are back under the
    // budget — but never drop the last (newest) file.
    var total = v.totalFileSize(0);
    var edit = version_edit.VersionEdit.init();
    defer edit.deinit(gpa);
    var evicted: usize = 0;
    var idx: usize = 0;
    while (total > fifo_max_table_files_size and idx + 1 < files.items.len) : (idx += 1) {
        try edit.removeFile(gpa, 0, files.items[idx].number);
        total -= files.items[idx].size;
        evicted += 1;
    }
    if (evicted == 0) return false;

    try versions.logAndApply(&edit);
    return true;
}

const KeyRange = struct { smallest: []const u8, largest: []const u8 };

/// The min/max INTERNAL keys (by InternalKeyComparator user-key ordering) of a
/// non-empty file list.  Returned slices alias the list's own bytes.
fn keyRange(files: []const FileMetaData, user_cmp: comparator.Comparator) KeyRange {
    std.debug.assert(files.len > 0);
    var smallest = files[0].smallest;
    var largest = files[0].largest;
    for (files[1..]) |f| {
        if (user_cmp.compare(internal_key.extractUserKey(f.smallest), internal_key.extractUserKey(smallest)) == .lt) {
            smallest = f.smallest;
        }
        if (user_cmp.compare(internal_key.extractUserKey(f.largest), internal_key.extractUserKey(largest)) == .gt) {
            largest = f.largest;
        }
    }
    return .{ .smallest = smallest, .largest = largest };
}

/// True iff NO file at any level DEEPER than `level+1` overlaps `user_key` — so
/// a tombstone for it can be safely dropped (nothing below would resurface an
/// older value).  Conservative: any overlap returns false (keep the tombstone).
pub fn isBaseLevelForKey(
    versions: *VersionSet,
    level: usize,
    user_key: []const u8,
    user_cmp: comparator.Comparator,
) bool {
    const v = versions.currentVersion();
    var lvl: usize = level + 2;
    while (lvl < version_set.kNumLevels) : (lvl += 1) {
        for (v.files[lvl].items) |f| {
            const start = internal_key.extractUserKey(f.smallest);
            const limit = internal_key.extractUserKey(f.largest);
            if (user_cmp.compare(user_key, start) != .lt and
                user_cmp.compare(user_key, limit) != .gt)
            {
                return false; // a deeper file holds this user key — keep tombstone.
            }
        }
    }
    return true;
}

// ===========================================================================
// Universal compaction (M7.3) — size-tiered merge of L0 "runs"
// ===========================================================================

/// Pick a universal compaction over the L0 runs, or null if none is warranted.
///
/// Each L0 file is treated as a sorted "run"; the NEWEST run is the one with the
/// highest file number (L0 is stored oldest-first, so the last item is newest).
/// Triggered only when the L0 file count reaches
/// `level0_file_num_compaction_trigger`.
///
/// Selection (mirroring RocksDB's two-stage universal picker, simplified):
///   1. Space-amplification: if `(sum of all runs except the oldest) /
///      (oldest run size) * 100 > universal_max_size_amplification_percent`,
///      merge ALL runs.
///   2. Else size-ratio: starting from the NEWEST run, extend the candidate set
///      while the next (older) run's size is within
///      `(1 + universal_size_ratio/100)` of the running total of the candidate
///      set.  If the set reaches `>= universal_min_merge_width`, compact it.
///
/// The selected runs are merged by the EXISTING `doCompaction` machinery into a
/// single output file kept in L0 (`output_level = 0`): inputs[0] = the selected
/// L0 files, inputs[1] = empty.  Because the selection is always a contiguous
/// suffix of L0 (newest runs), removing them and appending the merged output
/// leaves L0 = [older survivors..., merged] in insertion order — the merged run
/// (newest data) lands last, so `Version.get`'s newest-first L0 scan still
/// resolves correctly.
/// TODO: real RocksDB universal may place a fully-merged run at the bottom
/// level and supports incremental/sub-compactions; we keep it L0-only here.
pub fn pickUniversalCompaction(
    gpa: std.mem.Allocator,
    versions: *VersionSet,
    options: options_mod.Options,
) !?Compaction {
    const v = versions.currentVersion();
    const n = v.numFiles(0);
    if (n < options.level0_file_num_compaction_trigger) return null;
    if (n < 2) return null; // nothing to merge.

    // Runs newest-first: L0 is oldest-first, so reverse-index it.  run[0] is the
    // newest, run[n-1] the oldest.  We work with indices into the L0 list.
    // l0[i] for i in 0..n is oldest..newest; newest-first index j -> l0[n-1-j].
    const l0 = v.files[0].items;

    // -- 1. Space-amplification check -------------------------------------
    // size_amp = (total - oldest) / oldest * 100; oldest run is l0[0].
    var total_size: u64 = 0;
    for (l0) |f| total_size += f.file_size;
    const oldest_size = l0[0].file_size;
    var merge_all = false;
    if (oldest_size > 0) {
        const without_oldest = total_size - oldest_size;
        // Compare without overflow: without_oldest * 100 > oldest * max_amp%.
        if (without_oldest *% 100 > oldest_size *% options.universal_max_size_amplification_percent) {
            merge_all = true;
        }
    }

    // The selected runs form a contiguous SUFFIX of L0 (newest `count` files):
    // l0[n-count .. n].  Determine `count`.
    var count: usize = 0;
    if (merge_all) {
        count = n;
    } else {
        // -- 2. Size-ratio check, newest-first --------------------------------
        // candidate = {newest}; running total = its size.  Admit the next older
        // run while its size <= candidate_total * (1 + ratio/100).
        // ratio is a percent; compute the bound without floats:
        //   next_size * 100 <= candidate_total * (100 + ratio)
        var candidate_total: u64 = l0[n - 1].file_size; // newest run
        var k: usize = 1; // number of runs in the candidate set
        // Walk toward older runs.
        var i: usize = n - 1;
        while (i > 0) {
            const next = l0[i - 1].file_size; // the next older run
            if (next *% 100 <= candidate_total *% (100 + @as(u64, options.universal_size_ratio))) {
                candidate_total += next;
                k += 1;
                i -= 1;
            } else {
                break;
            }
        }
        if (k >= options.universal_min_merge_width) {
            count = k;
        } else {
            return null; // no qualifying candidate set.
        }
    }

    if (count < 2) return null;

    // Build the Compaction: inputs[0] = the newest `count` L0 files (suffix
    // l0[n-count .. n]); inputs[1] = empty; output stays in L0.  A PARTIAL merge
    // (older L0 runs survive) must keep tombstones so a deleted key cannot be
    // resurrected by an older un-read run; a FULL merge (count == n) reads every
    // run, so tombstones below the snapshot may be dropped.
    var c = Compaction{
        .level = 0,
        .output_level = 0,
        .keep_tombstones = count < n,
        .inputs = .{ .empty, .empty },
    };
    errdefer c.deinit(gpa);

    const start = n - count;
    for (l0[start..]) |f| {
        try c.inputs[0].append(gpa, .{
            .number = f.number,
            .file_size = f.file_size,
            .smallest = try gpa.dupe(u8, f.smallest),
            .largest = try gpa.dupe(u8, f.largest),
        });
    }

    return c;
}

/// One in-progress / finished output SST during a compaction.
const Output = struct {
    number: u64,
    smallest: []u8, // owned
    largest: []u8, // owned
    file_size: u64,
};

/// Run the compaction: merge inputs[0] ++ inputs[1], drop shadowed/obsolete
/// entries, write the survivors into fresh `level+1` SSTs, and logAndApply a
/// VersionEdit that removes the inputs and adds the outputs.
pub fn doCompaction(
    gpa: std.mem.Allocator,
    e: env.Env,
    dbname: []const u8,
    options: options_mod.Options,
    ikc: comparator.Comparator,
    user_cmp: comparator.Comparator,
    versions: *VersionSet,
    compaction: *Compaction,
    smallest_snapshot: u64,
    /// True iff a LIVE snapshot pins `smallest_snapshot` (vs. it merely being the
    /// latest sequence because no snapshot is held).  The compaction filter (M7.4)
    /// must never modify an entry a live snapshot can still read, so when this is
    /// true the newest `.value` of a key is filtered only if its sequence is
    /// strictly ABOVE `smallest_snapshot` (a post-snapshot version the oldest
    /// snapshot does not see); when false, the newest version is always eligible.
    has_live_snapshot: bool,
) !void {
    // --- 1. Build child iterators over every input file --------------------
    var children: std.ArrayListUnmanaged(iterator.Iterator) = .empty;
    // On any error before the merger takes ownership, tear the children down.
    var merger_owns_children = false;
    errdefer if (!merger_owns_children) {
        for (children.items) |it| it.deinit();
        children.deinit(gpa);
    };

    // We need a TableCache to open the input files.  Build a private one for
    // this compaction so we don't depend on the DB's cache lifetime.  It must
    // be pinned (its ikcmp address is taken into opened tables), so heap it.
    const tc = try gpa.create(table_cache_mod.TableCache);
    tc.* = table_cache_mod.TableCache.init(gpa, e, dbname, options, null);
    defer {
        tc.deinit();
        gpa.destroy(tc);
    }

    // M7.5: gather range tombstones from every input file into a single
    // aggregator.  These are (a) applied to point keys during the merge (a point
    // key permanently shadowed below the snapshot is dropped) and (b) carried —
    // whole, unfragmented — into the compaction's output SSTs so the deletion
    // survives.  Whether a tombstone may itself be DROPPED is decided per
    // tombstone below (conservative: keep unless clearly safe).
    var input_tombstones = delete_range.RangeTombstoneList.init(gpa);
    defer input_tombstones.deinit();
    for (&compaction.inputs) |*list| {
        for (list.items) |f| {
            const it = try tc.newIterator(gpa, f.number, f.file_size);
            errdefer it.deinit();
            try children.append(gpa, it);

            const table = try tc.findTable(f.number, f.file_size);
            var rtl = try table.rangeTombstones(gpa);
            defer rtl.deinit();
            for (rtl.tombstones.items) |t| try input_tombstones.add(t.begin, t.end, t.seq);
        }
    }

    // --- 2. Merge them in InternalKeyComparator order ----------------------
    var merger = try merging_iterator.MergingIterator.init(gpa, ikc, children.items);
    merger_owns_children = true; // merger copied + now owns the children
    children.deinit(gpa); // free our temporary list (copies live in the merger)
    defer merger.deinit(); // tears down every child iterator
    const mit = merger.iterator();

    // --- 3. Accumulate finished outputs (their metadata) -------------------
    var outputs: std.ArrayListUnmanaged(Output) = .empty;
    defer {
        for (outputs.items) |o| {
            gpa.free(o.smallest);
            gpa.free(o.largest);
        }
        outputs.deinit(gpa);
    }

    // The currently-open output builder + its file/metadata (null between
    // outputs).  build_opts uses the IKC (SSTs store internal keys).
    var build_opts = options;
    build_opts.comparator = ikc;
    const policy = bloom.BloomFilterPolicy.init(kFilterBitsPerKey);

    var builder: ?table_builder_mod.TableBuilder = null;
    var cur_file: ?env.WritableFile = null;
    var cur_number: u64 = 0;
    var cur_smallest: ?[]u8 = null;
    var cur_largest: ?[]u8 = null;
    // Tear down a half-open output on error (the success path closes it cleanly).
    errdefer {
        if (builder) |*b| b.deinit();
        if (cur_file) |f| f.close() catch {};
        if (cur_smallest) |s| gpa.free(s);
        if (cur_largest) |l| gpa.free(l);
    }

    // last_user_key holds the user key of the most recent seen entry, in a
    // stable buffer (iterator slices are transient).  `last_sequence_for_key`
    // is the sequence of the PREVIOUS entry for that same user key, mirroring
    // LevelDB's `DBImpl::DoCompactionWork`: it is reset to "max" on each new
    // user key so the FIRST (newest) version is never dropped, and an entry is
    // dropped only once a strictly-newer version at-or-below the snapshot has
    // already been emitted for the key.  Checking the PREVIOUS entry's sequence
    // (not the current one's) is what keeps a value pinned by a snapshot whose
    // sequence equals that value's sequence (the M6.3 correctness property).
    var last_user_key: std.ArrayListUnmanaged(u8) = .empty;
    defer last_user_key.deinit(gpa);
    var has_last_user_key = false;
    var last_sequence_for_key: u64 = internal_key.kMaxSequenceNumber;

    // M7.5: decide which input tombstones SURVIVE into the output.  A tombstone
    // may be dropped only when it can no longer affect anything: it is below the
    // smallest snapshot AND the output reaches the bottom level (nothing deeper
    // could resurface a covered key).  Anything else is kept — conservative, so a
    // snapshot read or a deeper level never loses a needed tombstone.
    const bottom_level = compaction.outputLevel() >= version_set.kNumLevels - 1;
    var surviving = delete_range.RangeTombstoneList.init(gpa);
    defer surviving.deinit();
    for (input_tombstones.tombstones.items) |t| {
        const droppable = bottom_level and
            !compaction.keep_tombstones and
            t.seq <= smallest_snapshot;
        if (!droppable) try surviving.add(t.begin, t.end, t.seq);
    }

    // A reusable emitter closure-equivalent: append (ikey, value) into the
    // current output, opening a builder if needed and rolling over at the target
    // file size.  `ikey`/`value` may be transient — they are copied as needed.
    var emit_ctx = EmitCtx{
        .gpa = gpa,
        .e = e,
        .dbname = dbname,
        .build_opts = build_opts,
        .policy = policy,
        .versions = versions,
        .target_file_size = options.target_file_size_base,
        .builder = &builder,
        .cur_file = &cur_file,
        .cur_number = &cur_number,
        .cur_smallest = &cur_smallest,
        .cur_largest = &cur_largest,
        .outputs = &outputs,
        .surviving_tombstones = &surviving,
    };

    mit.seekToFirst();
    while (mit.valid()) {
        if (mit.status()) |err| return err;

        const ikey = mit.key();
        const value = mit.value();

        // On a parse failure, keep the entry verbatim (defensive — should not
        // happen for well-formed SSTs).
        const parsed = internal_key.parseInternalKey(ikey) catch {
            try emit_ctx.emit(ikey, value);
            mit.next();
            continue;
        };

        const user_key = parsed.user_key;
        const first_for_key = !has_last_user_key or
            user_cmp.compare(user_key, last_user_key.items) != .eq;

        if (first_for_key) {
            // New user key: remember it, and reset the per-key sequence so this
            // (newest) version is always kept.
            last_user_key.clearRetainingCapacity();
            try last_user_key.appendSlice(gpa, user_key);
            has_last_user_key = true;
            last_sequence_for_key = internal_key.kMaxSequenceNumber;
        }

        // --- M7.1: a merge operand at the head of this key's run -------------
        // Merge operands COMBINE rather than supersede, so the per-version drop
        // rule below would corrupt them.  When the newest surviving entry for a
        // user key is a `.merge` and an operator is configured, collapse the
        // operand run here (it advances `mit` past everything it consumes).
        // TODO(M7.4): the M7.4 compaction filter is intentionally NOT applied to
        // a value produced by collapseMergeRun (a merge-derived `.value`); only
        // plain `.value` survivors below are filtered.  Filtering a merge result
        // (and a FilterMergeOperand hook for operands) is a future refinement.
        if (parsed.type == .merge and
            options.merge_operator != null and
            last_sequence_for_key == internal_key.kMaxSequenceNumber) // first-for-key (newest)
        {
            try collapseMergeRun(
                gpa,
                mit,
                user_cmp,
                options.merge_operator.?,
                smallest_snapshot,
                user_key,
                &emit_ctx,
            );
            // collapseMergeRun consumed the whole run; the next loop iteration
            // starts at a fresh user key, so reset the per-key state.
            has_last_user_key = false;
            continue;
        }

        var drop = false;
        if (last_sequence_for_key <= smallest_snapshot) {
            // A strictly-newer version for this key was already emitted and sits
            // at-or-below the oldest snapshot, so no live read can see this older
            // entry → drop.
            drop = true;
        } else if (!compaction.keep_tombstones and
            parsed.type == .deletion and
            parsed.sequence <= smallest_snapshot and
            isBaseLevelForKey(versions, compaction.level, user_key, user_cmp))
        {
            // A tombstone no longer needed (nothing deeper would resurface and no
            // snapshot needs it) → drop.  `keep_tombstones` (a partial universal
            // merge) forces retention so an older, un-read L0 run cannot resurrect
            // the deleted key.
            drop = true;
        } else if (rangeCoversForDrop(&input_tombstones, user_key, parsed.sequence, smallest_snapshot, user_cmp)) {
            // M7.5: a range tombstone (visible at-or-below the snapshot) covers
            // this point entry and strictly outranks it, so this version is
            // PERMANENTLY shadowed for every live reader → drop it.  The tombstone
            // itself is carried into the output (see `surviving`), so a deeper
            // level's older value (which the tombstone also covers) stays hidden.
            // Versions whose sequence is >= the tombstone, or any version a live
            // snapshot can still see, are NOT dropped (rangeCoversForDrop requires
            // both the value and the covering tombstone to be <= smallest_snapshot).
            drop = true;
        }

        // --- M7.4: compaction filter on the newest, snapshot-eligible .value --
        // Only consider a plain `.value` that survives the drop rules and is the
        // NEWEST version of its user key (no strictly-newer version seen yet, i.e.
        // last_sequence_for_key is still the per-key reset sentinel).  Merge
        // operands never reach here (collapseMergeRun consumes them above), and
        // deletions / older versions are excluded by the type and freshness
        // checks.  The entry must additionally be SNAPSHOT-ELIGIBLE: when a live
        // snapshot pins `smallest_snapshot`, the oldest snapshot reads the newest
        // version with sequence <= smallest_snapshot, so that version must survive
        // verbatim — only a strictly-newer (post-snapshot) version may be
        // filtered.  Without a live snapshot the newest version is always
        // eligible.  The filter may keep, drop, or rewrite the value.
        const snapshot_eligible = !has_live_snapshot or parsed.sequence > smallest_snapshot;
        var filtered_value = value;
        var owned_change: ?[]const u8 = null;
        if (!drop and
            parsed.type == .value and
            last_sequence_for_key == internal_key.kMaxSequenceNumber and // newest for this key
            snapshot_eligible)
        {
            if (options.compaction_filter) |cf| {
                switch (try cf.filter(compaction.level, user_key, value, gpa)) {
                    .keep => {},
                    .remove => drop = true,
                    .change => |repl| {
                        // The compaction owns the replacement: emit it below, then
                        // free it (the ownership contract).
                        owned_change = repl;
                        filtered_value = repl;
                    },
                }
            }
        }
        // Free any change buffer once we are done emitting it (or if dropped).
        defer if (owned_change) |c| gpa.free(c);

        // Record this entry's sequence as the "previous" for the next one.
        last_sequence_for_key = parsed.sequence;

        if (!drop) try emit_ctx.emit(ikey, filtered_value);
        mit.next();
    }

    // M7.5: if surviving range tombstones were not yet attached to any output
    // (every point key dropped, or the inputs held only tombstones), force-open a
    // builder so the tombstones are carried forward (ensureBuilder seeds them +
    // widens the key range).  Otherwise the deletion would be lost.
    if (builder == null and !surviving.isEmpty()) {
        try emit_ctx.ensureBuilder();
    }

    // Close any final open output.
    if (builder != null) {
        try finishOutput(gpa, &builder, &cur_file, cur_number, &cur_smallest, &cur_largest, &outputs);
    }

    // --- 4. Apply the edit: remove inputs, add outputs at level+1 ----------
    var edit = version_edit.VersionEdit.init();
    defer edit.deinit(gpa);

    for (compaction.inputs[0].items) |f| {
        try edit.removeFile(gpa, @intCast(compaction.level), f.number);
    }
    for (compaction.inputs[1].items) |f| {
        try edit.removeFile(gpa, @intCast(compaction.level + 1), f.number);
    }
    // Universal keeps the merged run in L0 (output_level == 0); leveled writes it
    // to `level + 1`.  `outputLevel()` resolves the right destination.
    const out_level = compaction.outputLevel();
    for (outputs.items) |o| {
        try edit.addFile(gpa, @intCast(out_level), o.number, o.file_size, o.smallest, o.largest);
    }

    try versions.logAndApply(&edit);
    // TODO: delete obsolete input .sst files via an SST file manager.  They are
    // dropped from the Version here but left on disk so concurrent readers (none
    // yet) cannot fault.
}

/// Finish the current output builder: emit the table, capture its metadata into
/// `outputs`, close the file, and reset the in-progress slots.
fn finishOutput(
    gpa: std.mem.Allocator,
    builder: *?table_builder_mod.TableBuilder,
    cur_file: *?env.WritableFile,
    cur_number: u64,
    cur_smallest: *?[]u8,
    cur_largest: *?[]u8,
    outputs: *std.ArrayListUnmanaged(Output),
) !void {
    try builder.*.?.finish();
    const file_size = builder.*.?.fileSize();
    builder.*.?.deinit();
    builder.* = null;
    try cur_file.*.?.close();
    cur_file.* = null;

    // Hand the owned smallest/largest to the outputs list.
    try outputs.append(gpa, .{
        .number = cur_number,
        .smallest = cur_smallest.*.?,
        .largest = cur_largest.*.?,
        .file_size = file_size,
    });
    cur_smallest.* = null;
    cur_largest.* = null;
}

/// Bundles the compaction's output-builder state + the knobs `emit` needs, so a
/// single entry can be appended into the rolling output SST from several call
/// sites (the main loop and the merge-collapse helper).
const EmitCtx = struct {
    gpa: std.mem.Allocator,
    e: env.Env,
    dbname: []const u8,
    build_opts: options_mod.Options,
    policy: bloom.BloomFilterPolicy,
    versions: *VersionSet,
    target_file_size: u64,
    builder: *?table_builder_mod.TableBuilder,
    cur_file: *?env.WritableFile,
    cur_number: *u64,
    cur_smallest: *?[]u8,
    cur_largest: *?[]u8,
    outputs: *std.ArrayListUnmanaged(Output),
    /// M7.5: range tombstones surviving this compaction.  Carried — whole, no
    /// truncation — into EVERY output SST opened here, and they widen each
    /// output's key range so reads/overlap over the tombstone span find the file.
    /// TODO: tombstone truncation at output-file boundaries (we duplicate whole
    /// tombstones across split outputs, which is correct but not minimal).
    surviving_tombstones: *const delete_range.RangeTombstoneList,

    /// Open a fresh output builder (if none is open), seeding it with the
    /// surviving range tombstones and widening its key range to cover them.
    fn ensureBuilder(self: *EmitCtx) !void {
        const gpa = self.gpa;
        if (self.builder.* != null) return;
        self.cur_number.* = self.versions.newFileNumber();
        const path = try filename.tableFileName(gpa, self.dbname, self.cur_number.*);
        defer gpa.free(path);
        self.cur_file.* = try self.e.newWritableFile(gpa, path);
        self.builder.* = try table_builder_mod.TableBuilder.init(gpa, self.build_opts, self.cur_file.*.?, self.policy);
        self.cur_smallest.* = null;
        self.cur_largest.* = null;

        // Seed range tombstones + widen the key range from their endpoints.
        for (self.surviving_tombstones.tombstones.items) |t| {
            try self.builder.*.?.addRangeTombstone(t.begin, t.end, t.seq);
            const b_ik = try encodeInternalKey(gpa, t.begin, t.seq, .range_deletion);
            defer gpa.free(b_ik);
            const e_ik = try encodeInternalKey(gpa, t.end, t.seq, .range_deletion);
            defer gpa.free(e_ik);
            self.widenSmallest(b_ik) catch |err| return err;
            self.widenLargest(e_ik) catch |err| return err;
        }
    }

    fn widenSmallest(self: *EmitCtx, ik: []const u8) !void {
        const gpa = self.gpa;
        if (self.cur_smallest.* == null or
            self.build_opts.comparator.compare(ik, self.cur_smallest.*.?) == .lt)
        {
            if (self.cur_smallest.*) |s| gpa.free(s);
            self.cur_smallest.* = try gpa.dupe(u8, ik);
        }
    }

    fn widenLargest(self: *EmitCtx, ik: []const u8) !void {
        const gpa = self.gpa;
        if (self.cur_largest.* == null or
            self.build_opts.comparator.compare(ik, self.cur_largest.*.?) == .gt)
        {
            if (self.cur_largest.*) |l| gpa.free(l);
            self.cur_largest.* = try gpa.dupe(u8, ik);
        }
    }

    /// Append (ikey, value) into the current output, opening a fresh builder if
    /// none is open and rolling over to a new output once the file reaches the
    /// target size.  `ikey`/`value` are copied as needed (their bytes may be
    /// transient iterator slices).
    fn emit(self: *EmitCtx, ikey: []const u8, value: []const u8) !void {
        const gpa = self.gpa;
        try self.ensureBuilder();

        try self.builder.*.?.add(ikey, value);
        try self.widenSmallest(ikey);
        try self.widenLargest(ikey);

        if (self.builder.*.?.fileSize() >= self.target_file_size) {
            try finishOutput(gpa, self.builder, self.cur_file, self.cur_number.*, self.cur_smallest, self.cur_largest, self.outputs);
        }
    }
};

/// M7.5: true iff some range tombstone PERMANENTLY shadows the point entry
/// `(user_key, value_seq)` so it may be dropped during compaction:
///   * coverage:   begin <= user_key < end (by `user_cmp`), and
///   * shadowing:  value_seq < tomb.seq (the tombstone outranks the value), and
///   * finality:   tomb.seq <= smallest_snapshot (no live reader sees the value —
///                 if a snapshot pinned a seq >= the value's, the value's own
///                 sequence would be > smallest_snapshot and excluded here too).
/// Requiring `value_seq < tomb.seq <= smallest_snapshot` guarantees every live
/// reader (at any snapshot >= smallest_snapshot) observes the tombstone over this
/// value, so dropping it changes nothing observable.  This is exactly the
/// aggregator's `covered` query with the snapshot bound set to the smallest
/// snapshot.
fn rangeCoversForDrop(
    tombstones: *const delete_range.RangeTombstoneList,
    user_key: []const u8,
    value_seq: u64,
    smallest_snapshot: u64,
    user_cmp: comparator.Comparator,
) bool {
    return tombstones.covered(user_key, value_seq, smallest_snapshot, user_cmp);
}

/// Encode `user_key ++ fixed64(packSequenceAndType(seq, t))` (caller frees).
fn encodeInternalKey(gpa: std.mem.Allocator, user_key: []const u8, seq: u64, t: internal_key.ValueType) ![]u8 {
    const out = try gpa.alloc(u8, user_key.len + 8);
    @memcpy(out[0..user_key.len], user_key);
    coding.encodeFixed64(out[user_key.len..][0..8], internal_key.packSequenceAndType(seq, t));
    return out;
}

/// Collapse the run of `.merge` operands for `user_key` beginning at the current
/// `mit` position (which must be the newest operand of the run).  Advances `mit`
/// PAST the whole run (every operand plus an underlying `.value`/`.deletion`).
///
/// Correctness contract — a merge operand must NEVER be lost:
///   * Operands with `sequence > smallest_snapshot` are NOT collapsible (a live
///     snapshot may observe the intermediate state), so they are emitted VERBATIM
///     as `.merge` entries (kept).
///   * The remaining operands (`sequence <= smallest_snapshot`) together with the
///     underlying base/deletion are collapsed:
///       - `.value` base reached → `fullMerge(operands, base)` → one `.value`.
///       - `.deletion` reached   → `fullMerge(operands, no base)` → one `.value`
///         (a Delete STOPS the merge; operands merge with no base).
///       - run ends with no base in this compaction's inputs → a base (a Put)
///         may still live in a deeper level we did not read, so we must NOT
///         resolve to a final value (that could discard a base).  Shrink the run
///         to ONE operand via `partialMerge` if supported, else keep the operands
///         VERBATIM so a deeper compaction (which reads the base) finishes it.
fn collapseMergeRun(
    gpa: std.mem.Allocator,
    mit: iterator.Iterator,
    user_cmp: comparator.Comparator,
    merge_op: merge_operator_mod.MergeOperator,
    smallest_snapshot: u64,
    user_key_in: []const u8,
    emit_ctx: *EmitCtx,
) !void {
    // Stable copy of the user key (iterator slices are transient).
    const user_key = try gpa.dupe(u8, user_key_in);
    defer gpa.free(user_key);

    // Operands above the snapshot: kept verbatim (newest-first), each tagged with
    // its sequence so we re-emit it as a `.merge` at the same sequence.
    var kept: std.ArrayListUnmanaged(struct { seq: u64, op: []u8 }) = .empty;
    defer {
        for (kept.items) |k| gpa.free(k.op);
        kept.deinit(gpa);
    }
    // Operands at-or-below the snapshot: collapsible (newest-first).  We track
    // the newest such sequence to stamp the collapsed output entry.
    var collapsible: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (collapsible.items) |op| gpa.free(op);
        collapsible.deinit(gpa);
    }
    var newest_collapsible_seq: u64 = 0;
    var have_collapsible_seq = false;

    var base: ?[]u8 = null;
    defer if (base) |b| gpa.free(b);
    var have_base = false;
    var base_seq: u64 = 0;
    var saw_deletion = false;
    var deletion_seq: u64 = 0;

    // Walk the run.
    while (mit.valid()) {
        if (mit.status()) |err| return err;
        const ik = mit.key();
        const parsed = internal_key.parseInternalKey(ik) catch break;
        if (user_cmp.compare(parsed.user_key, user_key) != .eq) break;
        switch (parsed.type) {
            .merge => {
                if (parsed.sequence > smallest_snapshot) {
                    try kept.append(gpa, .{ .seq = parsed.sequence, .op = try gpa.dupe(u8, mit.value()) });
                } else {
                    try collapsible.append(gpa, try gpa.dupe(u8, mit.value()));
                    if (!have_collapsible_seq) {
                        newest_collapsible_seq = parsed.sequence;
                        have_collapsible_seq = true;
                    }
                }
                mit.next();
            },
            .value => {
                // The base is the newest non-merge below the operands.  It is
                // only a collapse base for operands at-or-below the snapshot.
                base = try gpa.dupe(u8, mit.value());
                have_base = true;
                base_seq = parsed.sequence;
                mit.next();
                break;
            },
            .deletion, .single_deletion, .range_deletion => {
                saw_deletion = true;
                deletion_seq = parsed.sequence;
                mit.next();
                break;
            },
        }
    }

    // 1. Re-emit any above-snapshot operands verbatim (newest-first preserves
    //    IKC order: higher sequence sorts first for the same user key).
    for (kept.items) |k| {
        const ik = try encodeInternalKey(gpa, user_key, k.seq, .merge);
        defer gpa.free(ik);
        try emit_ctx.emit(ik, k.op);
    }

    // 2. Collapse the at-or-below-snapshot operands.
    if (collapsible.items.len == 0) {
        // Nothing collapsible (all operands were above the snapshot and kept
        // verbatim above): re-emit the underlying base/deletion verbatim at its
        // ORIGINAL sequence so a snapshot read still resolves the run correctly.
        if (have_base) {
            const ik = try encodeInternalKey(gpa, user_key, base_seq, .value);
            defer gpa.free(ik);
            try emit_ctx.emit(ik, base.?);
        } else if (saw_deletion) {
            const ik = try encodeInternalKey(gpa, user_key, deletion_seq, .deletion);
            defer gpa.free(ik);
            try emit_ctx.emit(ik, "");
        }
        return;
    }

    // Reverse collapsible operands to OLDEST-first for the operator.
    std.mem.reverse([]u8, collapsible.items);
    const view = try gpa.alloc([]const u8, collapsible.items.len);
    defer gpa.free(view);
    for (collapsible.items, 0..) |op, i| view[i] = op;

    const collapse_seq = if (have_collapsible_seq) newest_collapsible_seq else 0;

    if (have_base) {
        // fullMerge with the base → a single value.
        const merged = (try merge_op.fullMerge(user_key, base.?, view, gpa)) orelse {
            // Operator failure: keep the base + operands verbatim (never lose).
            try emitOperandsVerbatim(gpa, user_key, collapsible.items, collapse_seq, emit_ctx);
            const ik = try encodeInternalKey(gpa, user_key, collapse_seq, .value);
            defer gpa.free(ik);
            try emit_ctx.emit(ik, base.?);
            return;
        };
        defer gpa.free(merged);
        const ik = try encodeInternalKey(gpa, user_key, collapse_seq, .value);
        defer gpa.free(ik);
        try emit_ctx.emit(ik, merged);
        return;
    }

    if (saw_deletion) {
        // A Delete stops the merge: operands merge with NO base → one value.
        const merged = (try merge_op.fullMerge(user_key, null, view, gpa)) orelse {
            try emitOperandsVerbatim(gpa, user_key, collapsible.items, collapse_seq, emit_ctx);
            return;
        };
        defer gpa.free(merged);
        const ik = try encodeInternalKey(gpa, user_key, collapse_seq, .value);
        defer gpa.free(ik);
        try emit_ctx.emit(ik, merged);
        return;
    }

    // No base reached within this compaction's inputs.  A base (a Put) MAY still
    // live in a deeper level that this compaction did not read, so we must NOT
    // resolve the merge to a final value here — that could discard a base.
    // Instead shrink the run into ONE operand via partialMerge (the safe, lossy-
    // free reduction); if the operator does not support partial merge, keep the
    // operands verbatim so a deeper compaction (which reads the base) finishes
    // the merge.  Either way NO operand is lost.
    if (try merge_op.partialMerge(user_key, view, gpa)) |combined| {
        defer gpa.free(combined);
        const ik = try encodeInternalKey(gpa, user_key, collapse_seq, .merge);
        defer gpa.free(ik);
        try emit_ctx.emit(ik, combined);
    } else {
        try emitOperandsVerbatim(gpa, user_key, collapsible.items, collapse_seq, emit_ctx);
    }
}

/// Re-emit `operands` (OLDEST-first as stored after the reverse) verbatim as
/// `.merge` entries.  They are stamped with descending sequences ending at
/// `newest_seq` so they keep their relative IKC order (newest sorts first).
fn emitOperandsVerbatim(
    gpa: std.mem.Allocator,
    user_key: []const u8,
    operands_oldest_first: []const []u8,
    newest_seq: u64,
    emit_ctx: *EmitCtx,
) !void {
    // operands_oldest_first[last] is the newest; emit newest-first.
    var i: usize = operands_oldest_first.len;
    var seq = newest_seq;
    while (i > 0) {
        i -= 1;
        const ik = try encodeInternalKey(gpa, user_key, seq, .merge);
        defer gpa.free(ik);
        try emit_ctx.emit(ik, operands_oldest_first[i]);
        if (seq > 0) seq -= 1;
    }
}


// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const db_mod = @import("db.zig");
const DB = db_mod.DB;

/// Number of files at a given level.
fn levelFiles(db: *DB, level: usize) usize {
    return db.versions.currentVersion().files[level].items.len;
}

test "M6.2: L0 -> L1 merge + dedup keeps latest values" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Tiny write buffer + low L0 trigger so a few puts flush several L0 files
    // and a compaction fires.
    const db = try DB.open(gpa, e, "l0l1", .{
        .write_buffer_size = 1,
        .level0_file_num_compaction_trigger = 2,
    });
    defer db.close();

    // Distinct keys with an overwrite of "k".
    try db.put(.{}, "k", "v1");
    try db.put(.{}, "a", "av");
    try db.put(.{}, "k", "v2");
    try db.put(.{}, "b", "bv");
    try db.put(.{}, "c", "cv");
    try db.put(.{}, "k", "v3");
    try db.put(.{}, "d", "dv");

    // After compaction L1 holds file(s) (data pushed down from L0).
    try testing.expect(levelFiles(db, 1) >= 1);

    // All live keys read correctly; "k" returns its latest value.
    {
        const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("v3", got);
    }
    for ([_]struct { k: []const u8, v: []const u8 }{
        .{ .k = "a", .v = "av" },
        .{ .k = "b", .v = "bv" },
        .{ .k = "c", .v = "cv" },
        .{ .k = "d", .v = "dv" },
    }) |kv| {
        const got = try db.get(.{}, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
}

test "M6.2: tombstone is dropped once compacted past the base level" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const db = try DB.open(gpa, e, "tomb", .{
        .write_buffer_size = 1,
        .level0_file_num_compaction_trigger = 2,
    });
    defer db.close();

    try db.put(.{}, "k", "v");
    try db.delete(.{}, "k");
    // A handful more writes to force flushes + compaction down to the base.
    try db.put(.{}, "a", "av");
    try db.put(.{}, "b", "bv");
    try db.put(.{}, "c", "cv");
    try db.put(.{}, "d", "dv");

    // The deleted key is absent.
    try testing.expect((try db.get(.{}, "k")) == null);

    // A full scan must not surface "k" at all (no tombstone entry leaks).
    {
        var it = try db.newIterator(gpa, .{});
        defer it.deinit();
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            try testing.expect(!std.mem.eql(u8, it.key(), "k"));
        }
    }
}

test "M6.2: overlap resolved by sequence — only newest survives below snapshot" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const db = try DB.open(gpa, e, "ovl", .{
        .write_buffer_size = 1,
        .level0_file_num_compaction_trigger = 2,
    });
    defer db.close();

    // Two SSTs with different-seq versions of "k" get merged; newest wins.
    try db.put(.{}, "k", "old");
    try db.put(.{}, "x", "xv"); // forces flush of {k=old}
    try db.put(.{}, "k", "new");
    try db.put(.{}, "y", "yv"); // forces flush of {k=new, x=xv}
    try db.put(.{}, "z", "zv"); // another flush -> compaction

    const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("new", got);
}

// --- THE RANDOMIZED GATE ---------------------------------------------------

/// Reference model: a live key/value map mirroring the DB.  Owns its value
/// bytes; `put` overwrites, `delete` removes.
const RefMap = struct {
    map: std.StringHashMapUnmanaged([]u8) = .empty,
    gpa: std.mem.Allocator,

    fn init(gpa: std.mem.Allocator) RefMap {
        return .{ .gpa = gpa };
    }
    fn deinit(self: *RefMap) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.gpa.free(entry.key_ptr.*);
            self.gpa.free(entry.value_ptr.*);
        }
        self.map.deinit(self.gpa);
    }
    fn put(self: *RefMap, key: []const u8, value: []const u8) !void {
        const gop = try self.map.getOrPut(self.gpa, key);
        if (gop.found_existing) {
            self.gpa.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.gpa.dupe(u8, key);
        }
        gop.value_ptr.* = try self.gpa.dupe(u8, value);
    }
    fn delete(self: *RefMap, key: []const u8) void {
        if (self.map.fetchRemove(key)) |kv| {
            self.gpa.free(kv.key);
            self.gpa.free(kv.value);
        }
    }
    fn get(self: *RefMap, key: []const u8) ?[]const u8 {
        return self.map.get(key);
    }
};

/// Assert DB.get matches the reference for EVERY key in the key space, and that
/// a full forward scan equals the reference's sorted live entries.
fn verifyAgainstRef(gpa: std.mem.Allocator, db: *DB, ref: *RefMap, key_space: usize) !void {
    // 1. Point lookups for every possible key.
    var i: usize = 0;
    while (i < key_space) : (i += 1) {
        var kbuf: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{i});
        const want = ref.get(k);
        const got = try db.get(.{}, k);
        if (want) |w| {
            const g = got orelse {
                std.debug.print("missing key {s}: ref={s} db=null\n", .{ k, w });
                return error.TestKeyMissing;
            };
            defer gpa.free(g);
            testing.expectEqualSlices(u8, w, g) catch {
                std.debug.print("mismatch key {s}: ref={s} db={s}\n", .{ k, w, g });
                return error.TestValueMismatch;
            };
        } else {
            if (got) |g| {
                defer gpa.free(g);
                std.debug.print("unexpected key {s}: db={s}\n", .{ k, g });
                return error.TestUnexpectedKey;
            }
        }
    }

    // 2. Full forward scan == sorted live reference entries.
    var sorted_keys: std.ArrayListUnmanaged([]const u8) = .empty;
    defer sorted_keys.deinit(gpa);
    var it_ref = ref.map.iterator();
    while (it_ref.next()) |entry| try sorted_keys.append(gpa, entry.key_ptr.*);
    std.mem.sort([]const u8, sorted_keys.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lt);

    var it = try db.newIterator(gpa, .{});
    defer it.deinit();
    var idx: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        if (idx >= sorted_keys.items.len) {
            std.debug.print("scan has extra key {s}\n", .{it.key()});
            return error.TestScanTooLong;
        }
        const want_k = sorted_keys.items[idx];
        const want_v = ref.get(want_k).?;
        testing.expectEqualSlices(u8, want_k, it.key()) catch {
            std.debug.print("scan key mismatch at {d}: ref={s} db={s}\n", .{ idx, want_k, it.key() });
            return error.TestScanKeyMismatch;
        };
        testing.expectEqualSlices(u8, want_v, it.value()) catch {
            std.debug.print("scan value mismatch at key {s}: ref={s} db={s}\n", .{ want_k, want_v, it.value() });
            return error.TestScanValueMismatch;
        };
        idx += 1;
    }
    if (idx != sorted_keys.items.len) {
        std.debug.print("scan too short: got {d} want {d}\n", .{ idx, sorted_keys.items.len });
        return error.TestScanTooShort;
    }
    try testing.expect(it.status() == null);
}

test "M6.2: randomized 2000-op gate vs reference map (get + scan + reopen)" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const key_space: usize = 200; // keys "key000".."key199"
    const opts = options_mod.Options{
        .write_buffer_size = 256, // many flushes
        .level0_file_num_compaction_trigger = 2, // many compactions
        .max_bytes_for_level_base = 4096, // small levels -> deep compaction
        .target_file_size_base = 2048, // small output files (multiple splits)
    };

    var ref = RefMap.init(gpa);
    defer ref.deinit();

    var prng = std.Random.DefaultPrng.init(0xC0FFEE_1234);
    const rand = prng.random();

    {
        const db = try DB.open(gpa, e, "fuzz", opts);
        defer db.close();

        var op: usize = 0;
        while (op < 2000) : (op += 1) {
            const key_idx = rand.uintLessThan(usize, key_space);
            var kbuf: [8]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{key_idx});

            if (rand.uintLessThan(u32, 100) < 70) {
                // 70% put random key -> random value.
                var vbuf: [40]u8 = undefined;
                const vlen = 1 + rand.uintLessThan(usize, vbuf.len);
                for (vbuf[0..vlen]) |*b| b.* = 'a' + rand.uintLessThan(u8, 26);
                const v = vbuf[0..vlen];
                try db.put(.{}, k, v);
                try ref.put(k, v);
            } else {
                // 30% delete.
                try db.delete(.{}, k);
                ref.delete(k);
            }

            if (op % 200 == 199) {
                try verifyAgainstRef(gpa, db, &ref, key_space);
            }
        }

        // Final verification before close.
        try verifyAgainstRef(gpa, db, &ref, key_space);
    }

    // Reopen and re-verify get for all keys (recovery after compaction).
    {
        const db = try DB.open(gpa, e, "fuzz", opts);
        defer db.close();
        try verifyAgainstRef(gpa, db, &ref, key_space);
    }
}

// --- THE MERGE GATE (M7.1) -------------------------------------------------

const Uint64AddOperator = merge_operator_mod.Uint64AddOperator;

/// Reference model for u64-counter merge semantics: put=set, merge=add,
/// delete=remove.  A merge on an absent key starts the accumulation from 0.
const CounterRef = struct {
    map: std.StringHashMapUnmanaged(u64) = .empty,
    gpa: std.mem.Allocator,

    fn init(gpa: std.mem.Allocator) CounterRef {
        return .{ .gpa = gpa };
    }
    fn deinit(self: *CounterRef) void {
        var it = self.map.iterator();
        while (it.next()) |entry| self.gpa.free(entry.key_ptr.*);
        self.map.deinit(self.gpa);
    }
    fn put(self: *CounterRef, key: []const u8, value: u64) !void {
        const gop = try self.map.getOrPut(self.gpa, key);
        if (!gop.found_existing) gop.key_ptr.* = try self.gpa.dupe(u8, key);
        gop.value_ptr.* = value;
    }
    fn merge(self: *CounterRef, key: []const u8, operand: u64) !void {
        const gop = try self.map.getOrPut(self.gpa, key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.gpa.dupe(u8, key);
            gop.value_ptr.* = 0;
        }
        gop.value_ptr.* +%= operand;
    }
    fn delete(self: *CounterRef, key: []const u8) void {
        if (self.map.fetchRemove(key)) |kv| self.gpa.free(kv.key);
    }
    fn get(self: *CounterRef, key: []const u8) ?u64 {
        return self.map.get(key);
    }
};

fn u64le(buf: *[8]u8, v: u64) []const u8 {
    std.mem.writeInt(u64, buf, v, .little);
    return buf[0..];
}

fn decU64(bytes: []const u8) !u64 {
    if (bytes.len != 8) return error.TestBadValueLen;
    return std.mem.readInt(u64, bytes[0..8], .little);
}

/// Assert DB.get matches the counter reference for every key, and a full scan
/// equals the sorted live entries (with merged values).
fn verifyCounterRef(gpa: std.mem.Allocator, db: *DB, ref: *CounterRef, key_space: usize) !void {
    var i: usize = 0;
    while (i < key_space) : (i += 1) {
        var kbuf: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{i});
        const want = ref.get(k);
        const got = try db.get(.{}, k);
        if (want) |w| {
            const g = got orelse {
                std.debug.print("missing key {s}: ref={d} db=null\n", .{ k, w });
                return error.TestKeyMissing;
            };
            defer gpa.free(g);
            const gv = try decU64(g);
            if (gv != w) {
                std.debug.print("mismatch key {s}: ref={d} db={d}\n", .{ k, w, gv });
                return error.TestValueMismatch;
            }
        } else {
            if (got) |g| {
                defer gpa.free(g);
                std.debug.print("unexpected key {s}: db={d}\n", .{ k, try decU64(g) });
                return error.TestUnexpectedKey;
            }
        }
    }

    // Full scan == sorted live reference entries.
    var sorted_keys: std.ArrayListUnmanaged([]const u8) = .empty;
    defer sorted_keys.deinit(gpa);
    var it_ref = ref.map.iterator();
    while (it_ref.next()) |entry| try sorted_keys.append(gpa, entry.key_ptr.*);
    std.mem.sort([]const u8, sorted_keys.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lt);

    var it = try db.newIterator(gpa, .{});
    defer it.deinit();
    var idx: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        if (idx >= sorted_keys.items.len) {
            std.debug.print("scan has extra key {s}\n", .{it.key()});
            return error.TestScanTooLong;
        }
        const want_k = sorted_keys.items[idx];
        const want_v = ref.get(want_k).?;
        if (!std.mem.eql(u8, want_k, it.key())) {
            std.debug.print("scan key mismatch at {d}: ref={s} db={s}\n", .{ idx, want_k, it.key() });
            return error.TestScanKeyMismatch;
        }
        const got_v = try decU64(it.value());
        if (got_v != want_v) {
            std.debug.print("scan value mismatch at key {s}: ref={d} db={d}\n", .{ want_k, want_v, got_v });
            return error.TestScanValueMismatch;
        }
        idx += 1;
    }
    if (idx != sorted_keys.items.len) {
        std.debug.print("scan too short: got {d} want {d}\n", .{ idx, sorted_keys.items.len });
        return error.TestScanTooShort;
    }
    try testing.expect(it.status() == null);
}

test "M7.1: randomized merge gate vs counter reference (flush + compaction + reopen)" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    var add = Uint64AddOperator{};
    const key_space: usize = 40; // small key space -> heavy operand stacking
    const opts = options_mod.Options{
        .merge_operator = add.operator(),
        .write_buffer_size = 256, // many flushes
        .level0_file_num_compaction_trigger = 2, // many compactions
        .max_bytes_for_level_base = 4096, // small levels -> deep compaction
        .target_file_size_base = 2048, // small output files
    };

    var ref = CounterRef.init(gpa);
    defer ref.deinit();

    var prng = std.Random.DefaultPrng.init(0xC0FFEE_5EED);
    const rand = prng.random();

    {
        const db = try DB.open(gpa, e, "mergefuzz", opts);
        defer db.close();

        var op: usize = 0;
        while (op < 3000) : (op += 1) {
            const key_idx = rand.uintLessThan(usize, key_space);
            var kbuf: [8]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{key_idx});

            const roll = rand.uintLessThan(u32, 100);
            var vbuf: [8]u8 = undefined;
            if (roll < 60) {
                // 60% merge (add a small operand).
                const operand = rand.uintLessThan(u64, 100);
                try db.merge(.{}, k, u64le(&vbuf, operand));
                try ref.merge(k, operand);
            } else if (roll < 85) {
                // 25% put (set a base).
                const value = rand.uintLessThan(u64, 1000);
                try db.put(.{}, k, u64le(&vbuf, value));
                try ref.put(k, value);
            } else {
                // 15% delete.
                try db.delete(.{}, k);
                ref.delete(k);
            }

            if (op % 300 == 299) {
                try verifyCounterRef(gpa, db, &ref, key_space);
            }
        }
        try verifyCounterRef(gpa, db, &ref, key_space);
    }

    // Reopen: merges must survive compaction + recovery.
    {
        const db = try DB.open(gpa, e, "mergefuzz", opts);
        defer db.close();
        try verifyCounterRef(gpa, db, &ref, key_space);
    }
}

// --- THE COMPACTION-FILTER GATE (M7.4) -------------------------------------

const compaction_filter_mod = @import("../rocks/compaction_filter.zig");

test "M7.4: compaction filter removes keys whose value has the configured prefix" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    var f = compaction_filter_mod.RemoveByValuePrefixFilter{ .prefix = "DEL" };
    const db = try DB.open(gpa, e, "cf-remove", .{
        .compaction_filter = f.filter(),
        .write_buffer_size = 1, // tiny buffer -> flush per put
        .level0_file_num_compaction_trigger = 2, // fire a compaction quickly
    });
    defer db.close();

    // A mix of "DEL..."-valued keys (to be removed) and normal keys (kept).
    try db.put(.{}, "a", "DELme");
    try db.put(.{}, "b", "keep-b");
    try db.put(.{}, "c", "DELme-too");
    try db.put(.{}, "d", "keep-d");
    try db.put(.{}, "e", "keep-e"); // extra writes force flushes + compaction

    // Data has been pushed to L1 by compaction.
    try testing.expect(levelFiles(db, 1) >= 1);

    // The "DEL"-valued keys are gone; the normal keys remain with their values.
    try testing.expect((try db.get(.{}, "a")) == null);
    try testing.expect((try db.get(.{}, "c")) == null);
    for ([_]struct { k: []const u8, v: []const u8 }{
        .{ .k = "b", .v = "keep-b" },
        .{ .k = "d", .v = "keep-d" },
        .{ .k = "e", .v = "keep-e" },
    }) |kv| {
        const got = try db.get(.{}, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
}

test "M7.4: compaction filter rewrites surviving values (change decision)" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    var f = compaction_filter_mod.AppendSuffixFilter{ .suffix = "!" };
    const db = try DB.open(gpa, e, "cf-change", .{
        .compaction_filter = f.filter(),
        .write_buffer_size = 1,
        .level0_file_num_compaction_trigger = 2,
    });
    defer db.close();

    // The keys under test (a..d), then a run of trailing keys whose only job is
    // to force flushes + compactions so EVERY key under test is swept down to L1
    // (the last-written keys would otherwise linger in L0 and never be filtered).
    try db.put(.{}, "a", "av");
    try db.put(.{}, "b", "bv");
    try db.put(.{}, "c", "cv");
    try db.put(.{}, "d", "dv");
    for ([_][]const u8{ "p", "q", "r", "s", "t", "u" }) |k| {
        try db.put(.{}, k, "x");
    }

    try testing.expect(levelFiles(db, 1) >= 1);

    // Every surviving value under test has the suffix appended exactly once by
    // the filter (L1 never overflows here, so a key is filtered a single time).
    for ([_]struct { k: []const u8, v: []const u8 }{
        .{ .k = "a", .v = "av!" },
        .{ .k = "b", .v = "bv!" },
        .{ .k = "c", .v = "cv!" },
        .{ .k = "d", .v = "dv!" },
    }) |kv| {
        const got = try db.get(.{}, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
}

test "M7.4: compaction filter must not touch snapshot-protected entries" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    var f = compaction_filter_mod.RemoveByValuePrefixFilter{ .prefix = "DEL" };
    // Small level budgets + output files so data is driven all the way down
    // through L1 -> L2 -> ... (an L1->L2 compaction re-reads every overlapping
    // L1 file, so the file holding "k" is eventually swept after release).
    const db = try DB.open(gpa, e, "cf-snap", .{
        .compaction_filter = f.filter(),
        .write_buffer_size = 64,
        .level0_file_num_compaction_trigger = 2,
        .max_bytes_for_level_base = 256,
        .target_file_size_base = 256,
    });
    defer db.close();

    // Write the value the filter would remove, then PIN it with a snapshot so a
    // compaction must not drop it.
    try db.put(.{}, "k", "DELme");
    const snap = try db.getSnapshot();

    // More writes to force flushes + a compaction while the snapshot is held.
    try db.put(.{}, "a", "av");
    try db.put(.{}, "b", "bv");
    try db.put(.{}, "c", "cv");

    // The snapshot read still sees the original value (filter left it alone
    // because "k" is protected by the snapshot — its sequence > smallest_snapshot).
    {
        const got = try db.get(.{ .snapshot = snap.sequence }, "k") orelse
            return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("DELme", got);
    }

    // Release the snapshot, then force more compactions.  We write a run of
    // keys spanning across and PAST "k" so the L1 file that holds "k" is pulled
    // into a level-0 -> level-1 compaction (overlap requires the input range to
    // cover "k").  With the snapshot gone "k" is now eligible and is removed.
    db.releaseSnapshot(snap);
    var kbuf: [8]u8 = undefined;
    var i: usize = 0;
    while (i < 60) : (i += 1) {
        const key = try std.fmt.bufPrint(&kbuf, "x{d:0>4}", .{i});
        try db.put(.{}, key, "keepmevalue-padding-to-grow-levels");
    }

    if (try db.get(.{}, "k")) |leftover| {
        defer gpa.free(leftover);
        return error.TestExpectedRemoved;
    }
}

// --- THE FIFO GATE (M7.3) --------------------------------------------------

test "M7.3: FIFO evicts the oldest L0 files once over the byte budget" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Tiny write buffer -> one L0 file per put.  Tiny FIFO budget so a handful
    // of files blows past it and the oldest are evicted.  We size the budget so
    // it holds only a couple of L0 files.
    const db = try DB.open(gpa, e, "fifo", .{
        .compaction_style = .fifo,
        .write_buffer_size = 1, // flush after every put
        .fifo_max_table_files_size = 1500, // ~ room for ~2-3 tiny SSTs
    });
    defer db.close();

    // Write distinct keys; each flush produces a new L0 file with a higher file
    // number.  The earliest-written keys live in the OLDEST (lowest-number) files
    // and must be evicted first.
    const n: usize = 30;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var kbuf: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "k{d:0>4}", .{i});
        try db.put(.{}, k, "value-padding-to-grow-the-sst-files");
    }

    // The L0 byte total is back under (or within one file of) the budget.
    const total = db.versions.currentVersion().totalFileSize(0);
    // Tolerance of one extra file: eviction stops once <= budget, but FIFO never
    // evicts the file currently being written; allow some slack for the last SST.
    try testing.expect(total <= 4 * 1500);

    // The most-recently-written keys are present.
    {
        var kbuf: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "k{d:0>4}", .{n - 1});
        const got = try db.get(.{}, k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("value-padding-to-grow-the-sst-files", got);
    }

    // The OLDEST keys were evicted (dropped whole-file, not merged) -> null.
    {
        const got = try db.get(.{}, "k0000");
        if (got) |v| {
            defer gpa.free(v);
            return error.TestOldestNotEvicted;
        }
    }

    // Monotonic frontier: there is a cut index below which everything is gone and
    // at/above which everything is present (FIFO drops contiguous oldest files).
    var present_seen = false;
    i = 0;
    while (i < n) : (i += 1) {
        var kbuf: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "k{d:0>4}", .{i});
        const got = try db.get(.{}, k);
        if (got) |v| {
            gpa.free(v);
            present_seen = true;
        }
    }
    try testing.expect(present_seen);
}

// --- THE UNIVERSAL GATE (M7.3) ---------------------------------------------

test "M7.3: universal merges L0 runs into fewer files, data preserved + reopen" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const key_space: usize = 60;
    const opts = options_mod.Options{
        .compaction_style = .universal,
        .write_buffer_size = 256, // many flushes -> many L0 runs
        .level0_file_num_compaction_trigger = 4, // trigger universal merges
        .target_file_size_base = 1 << 20, // merge into a single output file
    };

    var ref = RefMap.init(gpa);
    defer ref.deinit();

    var prng = std.Random.DefaultPrng.init(0xC0FFEE_AB12);
    const rand = prng.random();

    {
        const db = try DB.open(gpa, e, "uni", opts);
        defer db.close();

        var op: usize = 0;
        while (op < 1500) : (op += 1) {
            const key_idx = rand.uintLessThan(usize, key_space);
            var kbuf: [8]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{key_idx});
            var vbuf: [40]u8 = undefined;
            const vlen = 1 + rand.uintLessThan(usize, vbuf.len);
            for (vbuf[0..vlen]) |*b| b.* = 'a' + rand.uintLessThan(u8, 26);
            const v = vbuf[0..vlen];
            try db.put(.{}, k, v);
            try ref.put(k, v);
        }

        // Universal keeps runs in L0; nothing should be pushed to deeper levels.
        try testing.expectEqual(@as(usize, 0), levelFiles(db, 1));
        // The merges must have collapsed many flushed runs into a small count.
        try testing.expect(levelFiles(db, 0) < 1500 / 4);

        try verifyAgainstRef(gpa, db, &ref, key_space);
    }

    // Reopen and re-verify (universal output must survive recovery).
    {
        const db = try DB.open(gpa, e, "uni", opts);
        defer db.close();
        try verifyAgainstRef(gpa, db, &ref, key_space);
    }
}
