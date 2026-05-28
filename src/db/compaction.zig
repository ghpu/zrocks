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
//! What is implemented vs left as TODO:
//!   * Size-based output split — implemented (correctness-sufficient).
//!   * Tombstone drop at the base level — implemented (isBaseLevelForKey).
//!   * Merge operand collapse — implemented (collapseMergeRun, M7.1).
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

const FileMetaData = version_edit.FileMetaData;
const VersionSet = version_set.VersionSet;

/// Bloom bits/key for compaction-output SSTs (must match the reader policy).
const kFilterBitsPerKey: usize = 10;

pub const Compaction = struct {
    /// The level being compacted; output files land at `level + 1`.
    level: usize,
    /// inputs[0] = files chosen from `level`; inputs[1] = the overlapping files
    /// at `level + 1`.  Each list deep-owns its FileMetaData key bytes.
    inputs: [2]std.ArrayListUnmanaged(FileMetaData),

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

    for (&compaction.inputs) |*list| {
        for (list.items) |f| {
            const it = try tc.newIterator(gpa, f.number, f.file_size);
            errdefer it.deinit();
            try children.append(gpa, it);
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
        } else if (parsed.type == .deletion and
            parsed.sequence <= smallest_snapshot and
            isBaseLevelForKey(versions, compaction.level, user_key, user_cmp))
        {
            // A tombstone no longer needed (nothing deeper would resurface and no
            // snapshot needs it) → drop.
            drop = true;
        }

        // Record this entry's sequence as the "previous" for the next one.
        last_sequence_for_key = parsed.sequence;

        if (!drop) try emit_ctx.emit(ikey, value);
        mit.next();
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
    for (outputs.items) |o| {
        try edit.addFile(gpa, @intCast(compaction.level + 1), o.number, o.file_size, o.smallest, o.largest);
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

    /// Append (ikey, value) into the current output, opening a fresh builder if
    /// none is open and rolling over to a new output once the file reaches the
    /// target size.  `ikey`/`value` are copied as needed (their bytes may be
    /// transient iterator slices).
    fn emit(self: *EmitCtx, ikey: []const u8, value: []const u8) !void {
        const gpa = self.gpa;
        if (self.builder.* == null) {
            self.cur_number.* = self.versions.newFileNumber();
            const path = try filename.tableFileName(gpa, self.dbname, self.cur_number.*);
            defer gpa.free(path);
            self.cur_file.* = try self.e.newWritableFile(gpa, path);
            self.builder.* = try table_builder_mod.TableBuilder.init(gpa, self.build_opts, self.cur_file.*.?, self.policy);
            self.cur_smallest.* = null;
            self.cur_largest.* = null;
        }

        try self.builder.*.?.add(ikey, value);
        if (self.cur_smallest.* == null) self.cur_smallest.* = try gpa.dupe(u8, ikey);
        if (self.cur_largest.*) |l| gpa.free(l);
        self.cur_largest.* = try gpa.dupe(u8, ikey);

        if (self.builder.*.?.fileSize() >= self.target_file_size) {
            try finishOutput(gpa, self.builder, self.cur_file, self.cur_number.*, self.cur_smallest, self.cur_largest, self.outputs);
        }
    }
};

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

    try db.put(.{}, "a", "av");
    try db.put(.{}, "b", "bv");
    try db.put(.{}, "c", "cv");
    try db.put(.{}, "d", "dv");

    try testing.expect(levelFiles(db, 1) >= 1);

    // Every surviving value has the suffix appended by the filter.
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
    const db = try DB.open(gpa, e, "cf-snap", .{
        .compaction_filter = f.filter(),
        .write_buffer_size = 1,
        .level0_file_num_compaction_trigger = 2,
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

    // Release the snapshot, then force another compaction; now "k" is eligible
    // and the filter removes it.
    db.releaseSnapshot(snap);
    try db.put(.{}, "d", "dv");
    try db.put(.{}, "f", "fv");
    try db.put(.{}, "g", "gv");
    try db.put(.{}, "h", "hv");

    try testing.expect((try db.get(.{}, "k")) == null);
}
