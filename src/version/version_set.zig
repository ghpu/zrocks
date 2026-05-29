//! version_set.zig — Version / VersionSet / MANIFEST (LevelDB-style metadata).
//!
//! A `Version` is an immutable-by-convention snapshot of which SST files live
//! at which level.  A `VersionSet` owns the live (current) Version and a
//! MANIFEST log: every `logAndApply` builds a new current Version and appends
//! the encoded `VersionEdit` to the MANIFEST so the layout can be reconstructed
//! on `recover`.  A `CURRENT` pointer file names the live MANIFEST.
//!
//! Ownership model
//! ---------------
//! Each `Version` DEEP-OWNS the byte slices (smallest / largest internal keys)
//! of every `FileMetaData` it holds; `Version.deinit` frees them.  When
//! `logAndApply` carries a file forward from the previous Version, or adds one
//! from a `VersionEdit`, the bytes are duped into the new Version so the two
//! Versions never alias.  This is intentionally simple — files are tiny
//! metadata records.
//! TODO(perf): share FileMetaData via refcount across Versions.

const std = @import("std");
const version_edit = @import("version_edit.zig");
const log_writer = @import("../format/log_writer.zig");
const log_reader = @import("../format/log_reader.zig");
const env = @import("../env/env.zig");
const comparator = @import("../util/comparator.zig");
const coding = @import("../util/coding.zig");
const options = @import("../options.zig");
const filename = @import("filename.zig");
const internal_key = @import("../format/internal_key.zig");
const iterator = @import("../iterator/iterator.zig");
const table_cache = @import("table_cache.zig");
const merge_operator = @import("../rocks/merge_operator.zig");

const FileMetaData = version_edit.FileMetaData;
const VersionEdit = version_edit.VersionEdit;
const TableCache = table_cache.TableCache;

pub const kNumLevels = 7;

/// Result of a Version point lookup: a value (caller-owned bytes), a tombstone,
/// or — via the `?GetResult` return — "not present in any file".
pub const GetResult = union(enum) {
    found: []const u8,
    deleted,
};

/// Operand-accumulating point-lookup context (the LSM "GetContext").
///
/// A single `GetContext` is threaded through a newest-first probe of the
/// Version's files (and, in the DB layer, the memtables ahead of them).  Each
/// source feeds its newest-visible run of entries for the user key into the
/// context via `addEntry`; the context gathers `.merge` operands (newest-first)
/// until it meets a terminating base (`.value` → `found_value`, the put becomes
/// the merge base) or a deletion (`.deletion`/`.single_deletion` →
/// `found_deleted`, the base is empty).  Once `isTerminal()` is true no later
/// (older) source can change the answer, so the probe stops.
///
/// This replaces the previous one-shot `probeFile` (which discarded `.merge`
/// entries) AND the DB layer's separate full-Version re-scan: a merge read now
/// accumulates operands across files in a single forward pass.
pub const GetContext = struct {
    pub const State = enum { not_found, found_value, found_deleted };

    gpa: std.mem.Allocator,
    user_cmp: comparator.Comparator,
    merge_op: ?merge_operator.MergeOperator,

    state: State = .not_found,
    /// The merge base (a `.value` put), owned; null until a base is reached.
    base: ?[]u8 = null,
    /// Accumulated `.merge` operands, NEWEST-first; each entry owned by `gpa`.
    operands: std.ArrayListUnmanaged([]u8) = .empty,

    pub fn init(
        gpa: std.mem.Allocator,
        user_cmp: comparator.Comparator,
        merge_op: ?merge_operator.MergeOperator,
    ) GetContext {
        return .{ .gpa = gpa, .user_cmp = user_cmp, .merge_op = merge_op };
    }

    pub fn deinit(self: *GetContext) void {
        if (self.base) |b| self.gpa.free(b);
        self.base = null;
        for (self.operands.items) |op| self.gpa.free(op);
        self.operands.deinit(self.gpa);
        self.* = undefined;
    }

    /// True once a base value or a deletion has been reached: no older source
    /// can change the merged result, so the probe may stop.
    pub fn isTerminal(self: *const GetContext) bool {
        return self.state != .not_found;
    }

    /// Feed one entry (already known to match the user key and be visible at the
    /// snapshot).  Returns `true` when the context becomes terminal (the caller
    /// then stops feeding further entries from this and later sources).
    /// `val` is copied (the context owns its copies).
    fn addEntry(self: *GetContext, t: internal_key.ValueType, val: []const u8) !bool {
        switch (t) {
            .merge => {
                try self.operands.append(self.gpa, try self.gpa.dupe(u8, val));
                return false; // keep gathering older entries for a base
            },
            .value => {
                self.base = try self.gpa.dupe(u8, val);
                self.state = .found_value;
                return true;
            },
            .deletion, .single_deletion, .range_deletion => {
                self.state = .found_deleted;
                return true;
            },
        }
    }

    /// Resolve the accumulated state into a final `GetResult` (or null when the
    /// key is absent / a bare tombstone / fullMerge fails with no base).
    ///
    /// With operands present the merge operator combines them (OLDEST-first)
    /// over the optional base.  The returned `.found` bytes are gpa-allocated and
    /// owned by the CALLER.  After this call the context still owns `base`/the
    /// operand copies (freed by `deinit`); the returned value is a fresh copy.
    /// `user_key` is forwarded to `fullMerge` (some operators key off it).
    pub fn finish(self: *GetContext, user_key: []const u8) !?GetResult {
        if (self.operands.items.len == 0) {
            return switch (self.state) {
                .not_found => null,
                .found_value => .{ .found = try self.gpa.dupe(u8, self.base.?) },
                .found_deleted => .deleted,
            };
        }

        // Operands present → require a merge operator to combine them.
        const merge_op = self.merge_op orelse return error.MergeOperatorNotConfigured;

        // Operands were gathered newest-first; the operator wants OLDEST-first.
        const view = try self.gpa.alloc([]const u8, self.operands.items.len);
        defer self.gpa.free(view);
        const n = self.operands.items.len;
        for (self.operands.items, 0..) |op, i| view[n - 1 - i] = op;

        const existing: ?[]const u8 = if (self.state == .found_value) self.base.? else null;
        const merged = (try merge_op.fullMerge(user_key, existing, view, self.gpa)) orelse {
            // Operator declined: fall back to the base value, or not-found.
            return switch (self.state) {
                .found_value => .{ .found = try self.gpa.dupe(u8, self.base.?) },
                else => null,
            };
        };
        return .{ .found = merged };
    }
};

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

/// An immutable snapshot of the per-level SST file layout.  Owns the byte
/// slices of every FileMetaData it holds.
pub const Version = struct {
    files: [kNumLevels]std.ArrayListUnmanaged(FileMetaData),

    /// An empty Version (no files at any level).
    pub fn initEmpty() Version {
        return .{ .files = [_]std.ArrayListUnmanaged(FileMetaData){.empty} ** kNumLevels };
    }

    /// Free every owned FileMetaData byte slice and the per-level lists.
    pub fn deinit(self: *Version, gpa: std.mem.Allocator) void {
        for (&self.files) |*level| {
            for (level.items) |f| {
                gpa.free(f.smallest);
                gpa.free(f.largest);
            }
            level.deinit(gpa);
        }
    }

    /// Append a deep copy of `meta` to `level` (the Version takes ownership of
    /// the duped key bytes).  On error nothing is added and no bytes leak.
    fn addFileOwned(self: *Version, gpa: std.mem.Allocator, level: usize, meta: FileMetaData) !void {
        const s = try gpa.dupe(u8, meta.smallest);
        errdefer gpa.free(s);
        const l = try gpa.dupe(u8, meta.largest);
        errdefer gpa.free(l);
        try self.files[level].append(gpa, .{
            .number = meta.number,
            .file_size = meta.file_size,
            .smallest = s,
            .largest = l,
            .smallest_seqno = meta.smallest_seqno,
            .largest_seqno = meta.largest_seqno,
            .has_range_tombstones = meta.has_range_tombstones,
        });
    }

    /// Whether `[file.smallest, file.largest]` (by USER key) covers `user_key`.
    fn fileCovers(user_cmp: comparator.Comparator, f: FileMetaData, user_key: []const u8) bool {
        const smallest_uk = internal_key.extractUserKey(f.smallest);
        const largest_uk = internal_key.extractUserKey(f.largest);
        return user_cmp.compare(user_key, smallest_uk) != .lt and
            user_cmp.compare(user_key, largest_uk) != .gt;
    }

    /// Probe a single SST file for `user_key` at `lookup_ikey` (an internal key
    /// `user_key ++ trailer(sequence, seek)`), feeding every entry for the key
    /// that is visible at `sequence` into `ctx` (NEWEST-first within the file).
    ///
    /// A per-file FORWARD-ACCUMULATING scan (the LSM "GetContext" loop): after
    /// the seek lands at/before the newest qualifying entry, the loop walks
    /// forward over the consecutive same-user-key entries, handing each to
    /// `ctx.addEntry` — so a run of `.merge` operands is gathered across files
    /// rather than discarded as the old one-shot seek did.  The scan stops at the
    /// first base value / deletion (the context becomes terminal) or when the
    /// user key changes / the file ends.
    ///
    /// Returns `true` when `ctx` became terminal (a base or deletion was met) so
    /// the caller stops probing older files.  `seq_out` (when non-null) receives
    /// the sequence of the NEWEST entry seen for the key in this file (used by
    /// M7.5 range-tombstone shadowing / M7.6 conflict detection).  The iterator
    /// is always deinited.
    fn probeFile(
        gpa: std.mem.Allocator,
        tc: *TableCache,
        user_cmp: comparator.Comparator,
        f: FileMetaData,
        ctx: *GetContext,
        user_key: []const u8,
        lookup_ikey: []const u8,
        sequence: u64,
        seq_out: ?*u64,
    ) !bool {
        var it = try tc.newIterator(gpa, f.number, f.file_size);
        defer it.deinit();
        it.seek(lookup_ikey);

        while (true) {
            if (it.status()) |e| return e;
            if (!it.valid()) break;

            const stored_ikey = it.key();
            const stored_uk = internal_key.extractUserKey(stored_ikey);
            if (user_cmp.compare(stored_uk, user_key) != .eq) break;

            const parsed = internal_key.parseInternalKey(stored_ikey) catch return error.Corruption;
            // The seek trailer may land on an entry newer than `sequence`
            // (mergeGet seeks with a max-type trailer); skip not-yet-visible
            // versions instead of mistaking them for the answer.
            if (parsed.sequence > sequence) {
                it.next();
                continue;
            }
            // Record the sequence of the newest VISIBLE entry seen across the
            // whole probe (files are visited newest-first, so the FIRST one to
            // set it wins).  Sequence 0 is the "unset" sentinel (writes start at
            // 1), matching latestSequenceForKey.
            if (seq_out) |p| {
                if (p.* == 0) p.* = parsed.sequence;
            }
            if (try ctx.addEntry(parsed.type, it.value())) return true;
            it.next();
        }
        return false;
    }

    /// LSM point lookup across this Version for `user_key` visible at `sequence`.
    ///
    /// Builds an internal lookup key `user_key ++ trailer(sequence, seek)` and
    /// probes files newest-first:
    ///   * Level 0 files may overlap; every covering file is probed in
    ///     descending file-number order (newest first) and the FIRST hit wins.
    ///   * Levels >= 1 are sorted, non-overlapping; the single covering file (if
    ///     any) is located and probed.
    /// Returns `.found`/`.deleted` on the first match, or null if no file holds
    /// the user key (the caller then falls through to lower levels / null).
    pub fn get(
        self: *const Version,
        gpa: std.mem.Allocator,
        tc: *TableCache,
        user_cmp: comparator.Comparator,
        user_key: []const u8,
        sequence: u64,
    ) !?GetResult {
        return self.getWithSeq(gpa, tc, user_cmp, user_key, sequence, null);
    }

    /// Like `get`, but writes the sequence of the surfaced entry into `seq_out`
    /// (when non-null) on a `.found`/`.deleted` result — used by M7.5 to decide
    /// whether a covering range tombstone shadows the value.
    ///
    /// This is the NON-merge fast path: with no operator threaded in, the first
    /// entry the GetContext meets (a `.value` or a deletion) terminates the probe
    /// immediately, exactly like the old one-shot seek.  A bare run of `.merge`
    /// operands with no operator surfaces as null (a usage error elsewhere — an
    /// operand can never be interpreted without an operator).
    pub fn getWithSeq(
        self: *const Version,
        gpa: std.mem.Allocator,
        tc: *TableCache,
        user_cmp: comparator.Comparator,
        user_key: []const u8,
        sequence: u64,
        seq_out: ?*u64,
    ) !?GetResult {
        var ctx = GetContext.init(gpa, user_cmp, null);
        defer ctx.deinit();
        try self.probeInto(gpa, tc, user_cmp, user_key, sequence, &ctx, seq_out);
        return ctx.finish(user_key);
    }

    /// Merge-aware point lookup: accumulate `.merge` operands across this
    /// Version's files (newest-first) over an optional base, combining them via
    /// `merge_op`.  Returns `.found` (caller-owned bytes), `.deleted`, or null
    /// (absent).  The accumulation happens in a single forward pass per file
    /// (see `probeFile`), gathering operands across files rather than discarding
    /// them as the old one-shot probe did.
    pub fn getMerge(
        self: *const Version,
        gpa: std.mem.Allocator,
        tc: *TableCache,
        user_cmp: comparator.Comparator,
        user_key: []const u8,
        sequence: u64,
        merge_op: merge_operator.MergeOperator,
    ) !?GetResult {
        var ctx = GetContext.init(gpa, user_cmp, merge_op);
        defer ctx.deinit();
        try self.probeInto(gpa, tc, user_cmp, user_key, sequence, &ctx, null);
        return ctx.finish(user_key);
    }

    /// Probe every covering file newest-first, feeding `ctx` until it goes
    /// terminal (a base / deletion is reached).  Shared by `getWithSeq` and
    /// `getMerge`.
    fn probeInto(
        self: *const Version,
        gpa: std.mem.Allocator,
        tc: *TableCache,
        user_cmp: comparator.Comparator,
        user_key: []const u8,
        sequence: u64,
        ctx: *GetContext,
        seq_out: ?*u64,
    ) !void {
        // internal lookup key = user_key ++ fixed64(packSequenceAndType(seq, seek))
        var lookup: std.ArrayListUnmanaged(u8) = .empty;
        defer lookup.deinit(gpa);
        try lookup.appendSlice(gpa, user_key);
        const trailer = internal_key.packSequenceAndType(sequence, internal_key.kValueTypeForSeek);
        var tbuf: [8]u8 = undefined;
        coding.encodeFixed64(&tbuf, trailer);
        try lookup.appendSlice(gpa, &tbuf);
        const lookup_ikey = lookup.items;

        // -- Level 0: overlapping; probe covering files newest-first ---------
        // L0 is stored in insertion order (oldest first, newest last; see
        // applyEdit), and file numbers increase with write recency, so iterating
        // in reverse visits the newest files first.
        {
            const l0 = self.files[0].items;
            var i = l0.len;
            while (i > 0) {
                i -= 1;
                const f = l0[i];
                if (!fileCovers(user_cmp, f, user_key)) continue;
                if (try probeFile(gpa, tc, user_cmp, f, ctx, user_key, lookup_ikey, sequence, seq_out)) return;
            }
        }

        // -- Levels 1..N-1: sorted, non-overlapping; one covering file -------
        var level: usize = 1;
        while (level < kNumLevels) : (level += 1) {
            const files = self.files[level].items;
            for (files) |f| {
                if (!fileCovers(user_cmp, f, user_key)) continue;
                if (try probeFile(gpa, tc, user_cmp, f, ctx, user_key, lookup_ikey, sequence, seq_out)) return;
                // A non-overlapping level has at most one covering file; if it
                // did not terminate, no other file at this level can.
                break;
            }
        }
    }

    /// Total bytes of all files at `level`.
    pub fn totalFileSize(self: *const Version, level: usize) u64 {
        var sum: u64 = 0;
        for (self.files[level].items) |f| sum += f.file_size;
        return sum;
    }

    /// Number of files at `level`.
    pub fn numFiles(self: *const Version, level: usize) usize {
        return self.files[level].items.len;
    }

    /// Files at `level` whose USER-key range overlaps `[begin, end]` (both
    /// internal keys; null = unbounded on that side).  For level 0, where files
    /// may overlap arbitrarily, the result is expanded: after collecting the
    /// initial overlaps, the accumulated user-key range is widened by every
    /// newly-included file and the scan repeats until it stabilises — mirroring
    /// LevelDB's `Version::GetOverlappingInputs` loop for level 0.
    ///
    /// The returned list deep-owns its FileMetaData key bytes (caller frees each
    /// `.smallest`/`.largest` then `deinit`s the list — or hands it to a
    /// Compaction which does so).
    pub fn overlappingInputs(
        self: *const Version,
        gpa: std.mem.Allocator,
        level: usize,
        begin_ikey: ?[]const u8,
        end_ikey: ?[]const u8,
        user_cmp: comparator.Comparator,
    ) !std.ArrayListUnmanaged(FileMetaData) {
        var out: std.ArrayListUnmanaged(FileMetaData) = .empty;
        errdefer {
            for (out.items) |f| {
                gpa.free(f.smallest);
                gpa.free(f.largest);
            }
            out.deinit(gpa);
        }

        // Working user-key bounds (slices into the Version's owned bytes or the
        // caller-supplied keys; never duped because we only read them).
        var user_begin: ?[]const u8 = if (begin_ikey) |b| internal_key.extractUserKey(b) else null;
        var user_end: ?[]const u8 = if (end_ikey) |e| internal_key.extractUserKey(e) else null;

        const files = self.files[level].items;
        // Outer loop re-runs the scan whenever a level-0 file widens the range;
        // for levels >= 1 it runs exactly once.
        scan: while (true) {
            var i: usize = 0;
            while (i < files.len) : (i += 1) {
                const f = files[i];
                const f_start = internal_key.extractUserKey(f.smallest);
                const f_limit = internal_key.extractUserKey(f.largest);

                if (user_begin != null and user_cmp.compare(f_limit, user_begin.?) == .lt) {
                    continue; // file entirely before the range
                }
                if (user_end != null and user_cmp.compare(f_start, user_end.?) == .gt) {
                    continue; // file entirely after the range
                }

                // For level 0, a newly-overlapping file may widen the
                // accumulated range and so pull in files we already skipped.
                // When that happens, widen the bounds, drop everything collected
                // so far, and restart the scan from the beginning (LevelDB's
                // GetOverlappingInputs loop).
                if (level == 0) {
                    var restart = false;
                    if (user_begin == null or user_cmp.compare(f_start, user_begin.?) == .lt) {
                        user_begin = f_start;
                        restart = true;
                    }
                    if (user_end == null or user_cmp.compare(f_limit, user_end.?) == .gt) {
                        user_end = f_limit;
                        restart = true;
                    }
                    if (restart) {
                        for (out.items) |of| {
                            gpa.free(of.smallest);
                            gpa.free(of.largest);
                        }
                        out.clearRetainingCapacity();
                        continue :scan;
                    }
                }

                try out.append(gpa, .{
                    .number = f.number,
                    .file_size = f.file_size,
                    .smallest = try gpa.dupe(u8, f.smallest),
                    .largest = try gpa.dupe(u8, f.largest),
                    .smallest_seqno = f.smallest_seqno,
                    .largest_seqno = f.largest_seqno,
                });
                // The just-appended entry owns its bytes; on a later error the
                // errdefer above frees the whole list.
            }
            break;
        }
        return out;
    }

    /// Append one generic table iterator per file in this Version (all levels)
    /// to `list`.  Each appended iterator OWNS its backing adapter (freed by its
    /// `deinit`); the caller (DB.newIterator) merges them and frees them by
    /// deiniting the MergingIterator.
    pub fn addIterators(
        self: *const Version,
        gpa: std.mem.Allocator,
        tc: *TableCache,
        list: *std.ArrayListUnmanaged(iterator.Iterator),
    ) !void {
        var level: usize = 0;
        while (level < kNumLevels) : (level += 1) {
            for (self.files[level].items) |f| {
                const it = try tc.newIterator(gpa, f.number, f.file_size);
                errdefer it.deinit();
                try list.append(gpa, it);
            }
        }
    }
};

// ---------------------------------------------------------------------------
// VersionSet
// ---------------------------------------------------------------------------

pub const VersionSet = struct {
    gpa: std.mem.Allocator,
    env: env.Env,
    dbname: []u8, // owned
    options: options.Options,
    cmp: comparator.Comparator,

    current: Version,

    next_file_number: u64,
    manifest_file_number: u64,
    last_sequence: u64,
    log_number: u64,
    prev_log_number: u64,

    // Open MANIFEST descriptor log (lazily created on the first logAndApply).
    // In SHARED-MANIFEST mode (`shared != null`) these stay null: the single
    // SharedManifest owns the descriptor + CURRENT, the file-number space, and
    // the global last_sequence; this VersionSet only tracks its own CF's
    // Version, routing every logAndApply edit to the shared descriptor tagged
    // with `cf_id`.
    descriptor_file: ?env.WritableFile,
    descriptor_log: ?log_writer.Writer,

    /// Shared-MANIFEST coordinator (D1b-M4).  Null for a standalone single-CF
    /// VersionSet (the legacy per-DB path, unchanged).  Non-null when this
    /// VersionSet is one column family inside a multi-CF database sharing ONE
    /// MANIFEST: `logAndApply`/`newFileNumber`/`lastSequence` delegate to it.
    shared: ?*SharedManifest = null,
    /// Column-family id this VersionSet represents (0 = default).  Only
    /// meaningful in shared mode; every edit logged is tagged with it.
    cf_id: u32 = 0,

    pub fn init(
        gpa: std.mem.Allocator,
        e: env.Env,
        dbname: []const u8,
        opts: options.Options,
    ) !VersionSet {
        const owned = try gpa.dupe(u8, dbname);
        return .{
            .gpa = gpa,
            .env = e,
            .dbname = owned,
            .options = opts,
            .cmp = opts.comparator,
            .current = Version.initEmpty(),
            .next_file_number = 2, // 1 is reserved for the first MANIFEST.
            .manifest_file_number = 0,
            .last_sequence = 0,
            .log_number = 0,
            .prev_log_number = 0,
            .descriptor_file = null,
            .descriptor_log = null,
            .shared = null,
            .cf_id = 0,
        };
    }

    pub fn deinit(self: *VersionSet) void {
        if (self.descriptor_file) |f| f.close() catch {};
        self.descriptor_file = null;
        self.descriptor_log = null;
        self.current.deinit(self.gpa);
        self.gpa.free(self.dbname);
        self.* = undefined;
    }

    pub fn newFileNumber(self: *VersionSet) u64 {
        // Shared mode: one global file-number space across all CFs (RocksDB
        // semantics — file numbers are unique database-wide).
        if (self.shared) |sm| return sm.newFileNumber();
        const n = self.next_file_number;
        self.next_file_number += 1;
        return n;
    }

    /// Ensure `next_file_number` will not hand out `number` again.
    fn markFileNumberUsed(self: *VersionSet, number: u64) void {
        if (self.next_file_number <= number) self.next_file_number = number + 1;
    }

    // -- accessors -------------------------------------------------------

    pub fn currentVersion(self: *const VersionSet) *const Version {
        return &self.current;
    }

    pub fn lastSequence(self: *const VersionSet) u64 {
        if (self.shared) |sm| return sm.last_sequence;
        return self.last_sequence;
    }

    pub fn logNumber(self: *const VersionSet) u64 {
        return self.log_number;
    }

    pub fn prevLogNumber(self: *const VersionSet) u64 {
        return self.prev_log_number;
    }

    pub fn manifestFileNumber(self: *const VersionSet) u64 {
        return self.manifest_file_number;
    }

    pub fn nextFileNumber(self: *const VersionSet) u64 {
        if (self.shared) |sm| return sm.next_file_number;
        return self.next_file_number;
    }

    pub fn setLastSequence(self: *VersionSet, v: u64) void {
        if (self.shared) |sm| {
            sm.last_sequence = v;
            return;
        }
        self.last_sequence = v;
    }

    pub fn setLogNumber(self: *VersionSet, v: u64) void {
        self.log_number = v;
    }

    // -- compaction scoring ----------------------------------------------

    /// Byte budget for `level` (>= 1).  L1 = max_bytes_for_level_base; each
    /// deeper level multiplies by 10 (LevelDB's MaxBytesForLevel).
    pub fn maxBytesForLevel(self: *const VersionSet, level: usize) u64 {
        std.debug.assert(level >= 1);
        var result: u64 = self.options.max_bytes_for_level_base;
        var l: usize = 1;
        while (l < level) : (l += 1) {
            result *= 10;
        }
        return result;
    }

    /// Compaction score for `level`: L0 is scored by FILE COUNT relative to the
    /// trigger (L0 files overlap, so size is a poor proxy); deeper levels by
    /// TOTAL BYTES relative to their budget.  A score >= 1.0 means the level
    /// wants compaction.
    pub fn compactionScore(self: *const VersionSet, level: usize) f64 {
        const v = self.currentVersion();
        if (level == 0) {
            const trigger: f64 = @floatFromInt(self.options.level0_file_num_compaction_trigger);
            return @as(f64, @floatFromInt(v.numFiles(0))) / trigger;
        }
        const bytes: f64 = @floatFromInt(v.totalFileSize(level));
        return bytes / @as(f64, @floatFromInt(self.maxBytesForLevel(level)));
    }

    /// The level whose compaction score is highest, provided it is >= 1.0;
    /// otherwise null (nothing needs compacting).  Only levels 0..N-2 are
    /// candidates — the last level has nowhere to compact down to.
    pub fn pickCompactionLevel(self: *const VersionSet) ?usize {
        var best_level: ?usize = null;
        var best_score: f64 = 1.0;
        var level: usize = 0;
        while (level + 1 < kNumLevels) : (level += 1) {
            const score = self.compactionScore(level);
            if (score >= best_score) {
                best_score = score;
                best_level = level;
            }
        }
        return best_level;
    }

    // -- apply / log -----------------------------------------------------

    /// Build a new current Version = apply(edit, current), append the encoded
    /// edit to the MANIFEST, install the new Version, and free the old one.
    pub fn logAndApply(self: *VersionSet, edit: *VersionEdit) !void {
        // Shared-MANIFEST mode (D1b-M4): the single SharedManifest owns the
        // descriptor + CURRENT + global scalars.  Tag the edit with this CF's
        // id, route the write to the shared descriptor, then apply the Version
        // locally.
        if (self.shared) |sm| {
            var next = try applyEdit(self.gpa, &self.current, edit, self.cmp);
            errdefer next.deinit(self.gpa);

            edit.setColumnFamilyId(self.cf_id);
            try sm.writeEdit(edit);

            // Local CF scalars that the edit carries (log_number is per-CF for
            // a shared WAL design but recorded in the shared MANIFEST tagged by
            // cf).  Global scalars (file numbers, last_sequence) live in `sm`.
            if (edit.log_number) |v| self.log_number = v;
            if (edit.prev_log_number) |v| self.prev_log_number = v;

            var old = self.current;
            self.current = next;
            old.deinit(self.gpa);
            return;
        }

        // 1. Build the new Version from the current one + the edit.
        var next = try applyEdit(self.gpa, &self.current, edit, self.cmp);
        errdefer next.deinit(self.gpa);

        // 2. Ensure a MANIFEST exists; on the very first call create it and
        //    write CURRENT to point at it.
        if (self.descriptor_log == null) {
            try self.createDescriptor();
        }

        // 3. Encode and append the edit as one MANIFEST record.
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(self.gpa);
        try edit.encodeTo(&buf, self.gpa);
        try self.descriptor_log.?.addRecord(self.gpa, buf.items);
        try self.descriptor_file.?.flush();

        // 4. Update bookkeeping scalars from the edit.
        self.applyScalars(edit);

        // 5. Install the new Version and free the old one.
        var old = self.current;
        self.current = next;
        old.deinit(self.gpa);
    }

    /// Apply only the scalar fields of `edit` to the VersionSet bookkeeping.
    fn applyScalars(self: *VersionSet, edit: *const VersionEdit) void {
        if (edit.log_number) |v| self.log_number = v;
        if (edit.prev_log_number) |v| self.prev_log_number = v;
        if (edit.next_file_number) |v| self.markFileNumberUsed(v -| 1);
        if (edit.last_sequence) |v| self.last_sequence = v;
        // Files referenced by the edit must not be reallocated.
        for (edit.new_files.items) |nf| self.markFileNumberUsed(nf.meta.number);
    }

    /// Compute a fresh Version by applying `edit` to `base` (which is NOT
    /// modified or freed — the returned Version deep-owns its own key bytes):
    /// carry forward every surviving file (minus edit.deleted_files), add
    /// edit.new_files, then sort levels >= 1 by smallest internal key.  Level 0
    /// keeps insertion order (overlapping ranges, newest last).
    fn applyEdit(
        gpa: std.mem.Allocator,
        base: *const Version,
        edit: *const VersionEdit,
        cmp: comparator.Comparator,
    ) !Version {
        var next = Version.initEmpty();
        errdefer next.deinit(gpa);

        var level: usize = 0;
        while (level < kNumLevels) : (level += 1) {
            for (base.files[level].items) |f| {
                if (isDeleted(edit, level, f.number)) continue;
                try next.addFileOwned(gpa, level, f);
            }
            for (edit.new_files.items) |nf| {
                if (nf.level != level) continue;
                if (isDeleted(edit, level, nf.meta.number)) continue;
                try next.addFileOwned(gpa, level, nf.meta);
            }
            if (level >= 1) sortLevel(&next.files[level], cmp);
        }
        return next;
    }

    fn isDeleted(edit: *const VersionEdit, level: usize, number: u64) bool {
        for (edit.deleted_files.items) |df| {
            if (df.level == level and df.number == number) return true;
        }
        return false;
    }

    /// Sort a level by smallest internal key (ties broken by file number).
    fn sortLevel(level: *std.ArrayListUnmanaged(FileMetaData), cmp: comparator.Comparator) void {
        const Ctx = struct {
            c: comparator.Comparator,
            fn lessThan(ctx: @This(), a: FileMetaData, b: FileMetaData) bool {
                return switch (ctx.c.compare(a.smallest, b.smallest)) {
                    .lt => true,
                    .gt => false,
                    .eq => a.number < b.number,
                };
            }
        };
        std.mem.sort(FileMetaData, level.items, Ctx{ .c = cmp }, Ctx.lessThan);
    }

    // -- MANIFEST / CURRENT ----------------------------------------------

    /// Create a fresh MANIFEST descriptor log and point CURRENT at it.  The
    /// freshly created MANIFEST opens with a snapshot record describing the
    /// current Version so a recovery that starts here is self-contained.
    fn createDescriptor(self: *VersionSet) !void {
        const number = self.newFileNumber();
        self.manifest_file_number = number;

        const path = try filename.manifestFileName(self.gpa, self.dbname, number);
        defer self.gpa.free(path);

        var wf = try self.env.newWritableFile(self.gpa, path);
        errdefer wf.close() catch {};
        var writer = log_writer.Writer.init(wf);

        // Write a snapshot of the current state as the first record so the
        // MANIFEST is self-contained even though new edits will follow.
        try self.writeSnapshot(&writer, wf);

        self.descriptor_file = wf;
        self.descriptor_log = writer;

        // Atomically point CURRENT at the new MANIFEST.
        try self.setCurrentFile(number);
    }

    /// Emit a VersionEdit capturing the full current state into the descriptor
    /// log (comparator name, bookkeeping scalars, every live file, and a
    /// kColumnFamilyAdd record for the default CF so the snapshot is
    /// self-contained when read by a RocksDB-aware reader).
    fn writeSnapshot(self: *VersionSet, writer: *log_writer.Writer, wf: env.WritableFile) !void {
        // First record: kColumnFamilyAdd for the default CF (id=0, name="default").
        {
            var cf_edit = VersionEdit.init();
            defer cf_edit.deinit(self.gpa);
            cf_edit.setColumnFamilyId(0);
            try cf_edit.setColumnFamilyAdd(self.gpa, "default");

            var cf_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer cf_buf.deinit(self.gpa);
            try cf_edit.encodeTo(&cf_buf, self.gpa);
            try writer.addRecord(self.gpa, cf_buf.items);
        }

        // Second record: full state snapshot (comparator, scalars, files).
        var edit = VersionEdit.init();
        defer edit.deinit(self.gpa);

        try edit.setComparatorName(self.gpa, self.cmp.name());
        if (self.log_number != 0) edit.setLogNumber(self.log_number);
        if (self.prev_log_number != 0) edit.setPrevLogNumber(self.prev_log_number);
        edit.setNextFileNumber(self.next_file_number);
        edit.setLastSequence(self.last_sequence);

        var level: usize = 0;
        while (level < kNumLevels) : (level += 1) {
            for (self.current.files[level].items) |f| {
                try edit.addFile(self.gpa, @intCast(level), f.number, f.file_size, f.smallest, f.largest);
            }
        }

        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(self.gpa);
        try edit.encodeTo(&buf, self.gpa);
        try writer.addRecord(self.gpa, buf.items);
        try wf.flush();
    }

    /// Write `"MANIFEST-<n>\n"` to a temp file and rename it onto CURRENT.
    fn setCurrentFile(self: *VersionSet, manifest_number: u64) !void {
        // The CURRENT file holds the MANIFEST's basename (no directory part)
        // plus a trailing newline, matching LevelDB.
        const manifest_path = try filename.manifestFileName(self.gpa, self.dbname, manifest_number);
        defer self.gpa.free(manifest_path);
        const basename = std.fs.path.basename(manifest_path);

        const tmp = try filename.tempFileName(self.gpa, self.dbname, manifest_number);
        defer self.gpa.free(tmp);

        {
            var wf = try self.env.newWritableFile(self.gpa, tmp);
            errdefer wf.close() catch {};
            try wf.append(basename);
            try wf.append("\n");
            try wf.flush();
            try wf.close();
        }
        errdefer self.env.deleteFile(tmp) catch {};

        const current_path = try filename.currentFileName(self.gpa, self.dbname);
        defer self.gpa.free(current_path);
        try self.env.renameFile(tmp, current_path);
    }

    // -- recovery --------------------------------------------------------

    /// Reconstruct the VersionSet from CURRENT -> MANIFEST.  Reads every record
    /// (each an encoded VersionEdit), accumulates the layout into the current
    /// Version, and restores bookkeeping scalars.
    pub fn recover(self: *VersionSet) !void {
        // 1. Read CURRENT to learn the MANIFEST basename.
        const current_path = try filename.currentFileName(self.gpa, self.dbname);
        defer self.gpa.free(current_path);

        const current_contents = try readWholeFile(self.env, self.gpa, current_path);
        defer self.gpa.free(current_contents);

        // Strip the trailing newline.
        var basename: []const u8 = current_contents;
        if (basename.len > 0 and basename[basename.len - 1] == '\n') {
            basename = basename[0 .. basename.len - 1];
        }
        if (basename.len == 0) return error.Corruption;

        const manifest_path = try std.fmt.allocPrint(self.gpa, "{s}/{s}", .{ self.dbname, basename });
        defer self.gpa.free(manifest_path);

        // 2. Replay the MANIFEST records, accumulating layout + scalars.
        var sf = try self.env.newSequentialFile(self.gpa, manifest_path);
        defer sf.close() catch {};
        var reader = log_reader.Reader.init(sf);

        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(self.gpa);

        // Accumulate into a working Version, replacing self.current at the end.
        var built = Version.initEmpty();
        errdefer built.deinit(self.gpa);

        var have_next_file = false;
        var have_log = false;
        var have_prev_log = false;
        var have_last_seq = false;
        var next_file: u64 = 0;
        var last_seq: u64 = 0;
        var log_num: u64 = 0;
        var prev_log_num: u64 = 0;

        while (try reader.readRecord(self.gpa, &scratch)) |record| {
            var edit = try VersionEdit.decodeFrom(self.gpa, record);
            defer edit.deinit(self.gpa);

            const next = try applyEdit(self.gpa, &built, &edit, self.cmp);
            built.deinit(self.gpa);
            built = next;

            if (edit.log_number) |v| {
                log_num = v;
                have_log = true;
            }
            if (edit.prev_log_number) |v| {
                prev_log_num = v;
                have_prev_log = true;
            }
            if (edit.next_file_number) |v| {
                next_file = v;
                have_next_file = true;
            }
            if (edit.last_sequence) |v| {
                last_seq = v;
                have_last_seq = true;
            }
        }

        // 3. Install the recovered scalars and Version.
        if (have_log) self.log_number = log_num;
        if (have_prev_log) self.prev_log_number = prev_log_num;
        if (have_last_seq) self.last_sequence = last_seq;
        if (have_next_file) self.markFileNumberUsed(next_file -| 1);
        // Ensure every surviving file number stays reserved (defensive — a
        // well-formed MANIFEST's next_file_number already covers them).
        for (&built.files) |*level| {
            for (level.items) |f| self.markFileNumberUsed(f.number);
        }

        var old = self.current;
        self.current = built;
        old.deinit(self.gpa);
    }

    /// Apply a decoded `edit` to this VersionSet's current Version IN PLACE,
    /// without writing to any MANIFEST (used by SharedManifest.recover to
    /// replay CF-tagged records into the right CF's Version).  Also folds the
    /// edit's per-CF log_number into local bookkeeping and reserves any file
    /// numbers it references.
    pub fn applyEditInPlace(self: *VersionSet, edit: *const VersionEdit) !void {
        var next = try applyEdit(self.gpa, &self.current, edit, self.cmp);
        errdefer next.deinit(self.gpa);
        if (edit.log_number) |v| self.log_number = v;
        if (edit.prev_log_number) |v| self.prev_log_number = v;
        for (edit.new_files.items) |nf| self.markFileNumberUsed(nf.meta.number);
        var old = self.current;
        self.current = next;
        old.deinit(self.gpa);
    }
};

// ===========================================================================
// SharedManifest (D1b-M4) — ONE MANIFEST for a multi-CF database
// ===========================================================================
//
// In the multi-CF design every column family is its own sub-LSM (own subdir,
// memtable, table_cache, flush/compaction), but they SHARE one MANIFEST in the
// database root: each VersionEdit is tagged with `kColumnFamily` (its cf id)
// and appended to ONE descriptor log.  The SharedManifest owns:
//   * the descriptor (MANIFEST) writer + the CURRENT pointer (in `dbroot`);
//   * the GLOBAL file-number space (file numbers are unique database-wide);
//   * the GLOBAL last_sequence (the shared sequence space across all CFs).
// Each per-CF `VersionSet` keeps only its own `current` Version and points at
// this SharedManifest via `vs.shared`; `vs.logAndApply` tags + routes here.
//
// CF identity (name<->id) is tracked separately by CfDB's CF_LIST sidecar; the
// shared MANIFEST stores the per-CF FILE/Version state (tagged by cf id) plus a
// kColumnFamilyAdd record per CF in each snapshot so the descriptor is
// self-describing for a RocksDB-aware reader.

pub const SharedManifest = struct {
    gpa: std.mem.Allocator,
    env: env.Env,
    dbroot: []u8, // owned
    options: options.Options,
    cmp: comparator.Comparator,

    next_file_number: u64,
    manifest_file_number: u64,
    last_sequence: u64,

    descriptor_file: ?env.WritableFile,
    descriptor_log: ?log_writer.Writer,

    /// Registered per-CF VersionSets, keyed by cf id.  Borrowed pointers (owned
    /// by their DBs); the SharedManifest only routes edits/recovery to them.
    cfs: std.AutoHashMapUnmanaged(u32, *VersionSet),

    pub fn init(
        gpa: std.mem.Allocator,
        e: env.Env,
        dbroot: []const u8,
        opts: options.Options,
    ) !SharedManifest {
        const owned = try gpa.dupe(u8, dbroot);
        return .{
            .gpa = gpa,
            .env = e,
            .dbroot = owned,
            .options = opts,
            .cmp = opts.comparator,
            .next_file_number = 2, // 1 reserved for the first MANIFEST.
            .manifest_file_number = 0,
            .last_sequence = 0,
            .descriptor_file = null,
            .descriptor_log = null,
            .cfs = .empty,
        };
    }

    pub fn deinit(self: *SharedManifest) void {
        if (self.descriptor_file) |f| f.close() catch {};
        self.descriptor_file = null;
        self.descriptor_log = null;
        self.cfs.deinit(self.gpa);
        self.gpa.free(self.dbroot);
        self.* = undefined;
    }

    /// Register a per-CF VersionSet, wiring it into shared mode (sets
    /// `vs.shared = self`, `vs.cf_id = cf_id`).
    pub fn registerCf(self: *SharedManifest, cf_id: u32, vs: *VersionSet) !void {
        vs.shared = self;
        vs.cf_id = cf_id;
        try self.cfs.put(self.gpa, cf_id, vs);
    }

    /// Unregister a CF (on drop).  The VS is closed by its owner separately.
    pub fn unregisterCf(self: *SharedManifest, cf_id: u32) void {
        _ = self.cfs.remove(cf_id);
    }

    pub fn newFileNumber(self: *SharedManifest) u64 {
        const n = self.next_file_number;
        self.next_file_number += 1;
        return n;
    }

    fn markFileNumberUsed(self: *SharedManifest, number: u64) void {
        if (self.next_file_number <= number) self.next_file_number = number + 1;
    }

    /// Append one (already CF-tagged) VersionEdit to the shared descriptor,
    /// creating it (and writing CURRENT + a self-describing snapshot) on the
    /// first call.  Folds the edit's global scalars (next_file_number,
    /// last_sequence) into the shared bookkeeping.
    pub fn writeEdit(self: *SharedManifest, edit: *VersionEdit) !void {
        if (self.descriptor_log == null) try self.createDescriptor();

        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(self.gpa);
        try edit.encodeTo(&buf, self.gpa);
        try self.descriptor_log.?.addRecord(self.gpa, buf.items);
        try self.descriptor_file.?.flush();

        if (edit.next_file_number) |v| self.markFileNumberUsed(v -| 1);
        if (edit.last_sequence) |v| self.last_sequence = v;
        for (edit.new_files.items) |nf| self.markFileNumberUsed(nf.meta.number);
    }

    /// Create a fresh MANIFEST in `dbroot`, write a self-describing snapshot of
    /// every registered CF's current state, and point CURRENT at it.
    fn createDescriptor(self: *SharedManifest) !void {
        const number = self.newFileNumber();
        self.manifest_file_number = number;

        const path = try filename.manifestFileName(self.gpa, self.dbroot, number);
        defer self.gpa.free(path);

        var wf = try self.env.newWritableFile(self.gpa, path);
        errdefer wf.close() catch {};
        var writer = log_writer.Writer.init(wf);

        try self.writeSnapshot(&writer, wf);

        self.descriptor_file = wf;
        self.descriptor_log = writer;

        try self.setCurrentFile(number);
    }

    /// Snapshot every registered CF: for each, a kColumnFamilyAdd record
    /// (placeholder name = the CF id) followed by a full file/scalar snapshot
    /// tagged with that CF id.  CF NAMES live in CF_LIST; the snapshot's add
    /// record carries a synthetic name only so the descriptor parses as
    /// RocksDB-shaped — recovery routes by id, not name.
    fn writeSnapshot(self: *SharedManifest, writer: *log_writer.Writer, wf: env.WritableFile) !void {
        var it = self.cfs.iterator();
        while (it.next()) |entry| {
            const cf_id = entry.key_ptr.*;
            const vs = entry.value_ptr.*;

            var edit = VersionEdit.init();
            defer edit.deinit(self.gpa);
            edit.setColumnFamilyId(cf_id);
            try edit.setComparatorName(self.gpa, self.cmp.name());
            if (vs.log_number != 0) edit.setLogNumber(vs.log_number);
            edit.setNextFileNumber(self.next_file_number);
            edit.setLastSequence(self.last_sequence);

            var level: usize = 0;
            while (level < kNumLevels) : (level += 1) {
                for (vs.current.files[level].items) |f| {
                    try edit.addFile(self.gpa, @intCast(level), f.number, f.file_size, f.smallest, f.largest);
                    edit.setLastFileHasRangeTombstones(f.has_range_tombstones);
                }
            }

            var buf: std.ArrayListUnmanaged(u8) = .empty;
            defer buf.deinit(self.gpa);
            try edit.encodeTo(&buf, self.gpa);
            try writer.addRecord(self.gpa, buf.items);
        }
        try wf.flush();
    }

    fn setCurrentFile(self: *SharedManifest, manifest_number: u64) !void {
        const manifest_path = try filename.manifestFileName(self.gpa, self.dbroot, manifest_number);
        defer self.gpa.free(manifest_path);
        const basename = std.fs.path.basename(manifest_path);

        const tmp = try filename.tempFileName(self.gpa, self.dbroot, manifest_number);
        defer self.gpa.free(tmp);

        {
            var wf = try self.env.newWritableFile(self.gpa, tmp);
            errdefer wf.close() catch {};
            try wf.append(basename);
            try wf.append("\n");
            try wf.flush();
            try wf.close();
        }
        errdefer self.env.deleteFile(tmp) catch {};

        const current_path = try filename.currentFileName(self.gpa, self.dbroot);
        defer self.gpa.free(current_path);
        try self.env.renameFile(tmp, current_path);
    }

    /// Whether a shared MANIFEST already exists for this dbroot.
    pub fn exists(self: *SharedManifest) bool {
        const current_path = filename.currentFileName(self.gpa, self.dbroot) catch return false;
        defer self.gpa.free(current_path);
        return self.env.fileExists(current_path);
    }

    /// Recover all registered CFs from the shared MANIFEST: read CURRENT ->
    /// MANIFEST, decode each record (a CF-tagged VersionEdit) and apply it to
    /// the matching registered CF's Version, restoring the global
    /// next_file_number + last_sequence.  CFs must be registered BEFORE calling
    /// (CfDB registers every CF_LIST entry first).  Records for an unregistered
    /// cf id are skipped defensively.
    pub fn recover(self: *SharedManifest) !void {
        const current_path = try filename.currentFileName(self.gpa, self.dbroot);
        defer self.gpa.free(current_path);

        const current_contents = try readWholeFile(self.env, self.gpa, current_path);
        defer self.gpa.free(current_contents);

        var basename: []const u8 = current_contents;
        if (basename.len > 0 and basename[basename.len - 1] == '\n') {
            basename = basename[0 .. basename.len - 1];
        }
        if (basename.len == 0) return error.Corruption;

        const manifest_path = try std.fmt.allocPrint(self.gpa, "{s}/{s}", .{ self.dbroot, basename });
        defer self.gpa.free(manifest_path);

        var sf = try self.env.newSequentialFile(self.gpa, manifest_path);
        defer sf.close() catch {};
        var reader = log_reader.Reader.init(sf);

        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(self.gpa);

        var max_next_file: u64 = self.next_file_number;
        var max_last_seq: u64 = self.last_sequence;

        while (try reader.readRecord(self.gpa, &scratch)) |record| {
            var edit = try VersionEdit.decodeFrom(self.gpa, record);
            defer edit.deinit(self.gpa);

            if (edit.next_file_number) |v| max_next_file = @max(max_next_file, v);
            if (edit.last_sequence) |v| max_last_seq = @max(max_last_seq, v);
            for (edit.new_files.items) |nf| max_next_file = @max(max_next_file, nf.meta.number + 1);

            // Route the file/version mutations to the matching CF.  A record
            // with no cf id targets the default CF (0).  Add/drop records carry
            // no file mutations here (CF identity is tracked by CF_LIST), so
            // they are harmless to apply.
            const cf_id = edit.column_family_id orelse 0;
            if (self.cfs.get(cf_id)) |vs| {
                try vs.applyEditInPlace(&edit);
            }
        }

        self.next_file_number = @max(self.next_file_number, max_next_file);
        self.last_sequence = max_last_seq;

        // Reserve every surviving file number across all CFs.
        var it = self.cfs.valueIterator();
        while (it.next()) |vs_ptr| {
            const vs = vs_ptr.*;
            for (&vs.current.files) |*level| {
                for (level.items) |f| self.markFileNumberUsed(f.number);
            }
        }
    }
};

// ---------------------------------------------------------------------------
// Small file helper
// ---------------------------------------------------------------------------

/// Read an entire (small) file into a freshly-allocated buffer (caller frees).
fn readWholeFile(e: env.Env, gpa: std.mem.Allocator, path: []const u8) ![]u8 {
    var sf = try e.newSequentialFile(gpa, path);
    defer sf.close() catch {};
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(gpa);
    var chunk: [256]u8 = undefined;
    while (true) {
        const n = try sf.read(&chunk);
        if (n == 0) break;
        try out.appendSlice(gpa, chunk[0..n]);
    }
    return out.toOwnedSlice(gpa);
}

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;

/// Build an internal key from a user key + a zero trailer (8 bytes), matching
/// the convention used in version_edit.zig tests.
fn ikey(comptime user: []const u8) []const u8 {
    return user ++ [_]u8{0} ** 8;
}

/// Count files in a level of the current version.
fn levelFileCount(vs: *const VersionSet, level: usize) usize {
    return vs.currentVersion().files[level].items.len;
}

test "init: empty version, next_file_number starts at 2" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    var lvl: usize = 0;
    while (lvl < kNumLevels) : (lvl += 1) {
        try testing.expectEqual(@as(usize, 0), levelFileCount(&vs, lvl));
    }
    try testing.expectEqual(@as(u64, 2), vs.nextFileNumber());
}

test "newFileNumber is monotonic starting at 2" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    try testing.expectEqual(@as(u64, 2), vs.newFileNumber());
    try testing.expectEqual(@as(u64, 3), vs.newFileNumber());
    try testing.expectEqual(@as(u64, 4), vs.newFileNumber());
}

test "logAndApply: files land in their levels" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    try edit.addFile(gpa, 0, 10, 100, ikey("a"), ikey("b"));
    try edit.addFile(gpa, 0, 11, 100, ikey("c"), ikey("d"));
    try edit.addFile(gpa, 1, 20, 200, ikey("m"), ikey("n"));
    try vs.logAndApply(&edit);

    try testing.expectEqual(@as(usize, 2), levelFileCount(&vs, 0));
    try testing.expectEqual(@as(usize, 1), levelFileCount(&vs, 1));
}

test "logAndApply: delete then add updates layout" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    {
        var edit = VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.addFile(gpa, 1, 20, 200, ikey("m"), ikey("n"));
        try edit.addFile(gpa, 1, 21, 200, ikey("p"), ikey("q"));
        try vs.logAndApply(&edit);
    }
    try testing.expectEqual(@as(usize, 2), levelFileCount(&vs, 1));

    {
        var edit = VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.removeFile(gpa, 1, 20);
        try edit.addFile(gpa, 1, 22, 200, ikey("a"), ikey("c"));
        try vs.logAndApply(&edit);
    }

    // 22 added, 20 removed -> still 2 files but {21, 22}.
    try testing.expectEqual(@as(usize, 2), levelFileCount(&vs, 1));
    const files = vs.currentVersion().files[1].items;
    var saw_21 = false;
    var saw_22 = false;
    var saw_20 = false;
    for (files) |f| {
        if (f.number == 20) saw_20 = true;
        if (f.number == 21) saw_21 = true;
        if (f.number == 22) saw_22 = true;
    }
    try testing.expect(!saw_20);
    try testing.expect(saw_21);
    try testing.expect(saw_22);
}

test "logAndApply: levels >= 1 stay sorted by smallest key" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    // Add out of order; expect sorted by smallest internal key on level 1.
    try edit.addFile(gpa, 1, 30, 100, ikey("m"), ikey("n"));
    try edit.addFile(gpa, 1, 31, 100, ikey("a"), ikey("c"));
    try edit.addFile(gpa, 1, 32, 100, ikey("e"), ikey("g"));
    try vs.logAndApply(&edit);

    const files = vs.currentVersion().files[1].items;
    try testing.expectEqual(@as(usize, 3), files.len);
    try testing.expectEqual(@as(u64, 31), files[0].number); // a..c
    try testing.expectEqual(@as(u64, 32), files[1].number); // e..g
    try testing.expectEqual(@as(u64, 30), files[2].number); // m..n
}

test "MANIFEST round-trip: recover reproduces identical state" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    // --- Build & persist with the first VersionSet ----------------------
    {
        var vs = try VersionSet.init(gpa, me.env(), "db", .{});
        defer vs.deinit();

        var edit = VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.setComparatorName(gpa, "leveldb.BytewiseComparator");
        edit.setLogNumber(5);
        edit.setNextFileNumber(50);
        edit.setLastSequence(999);
        try edit.addFile(gpa, 0, 10, 100, ikey("a"), ikey("b"));
        try edit.addFile(gpa, 0, 11, 150, ikey("c"), ikey("d"));
        try edit.addFile(gpa, 1, 20, 200, ikey("e"), ikey("g"));
        try edit.addFile(gpa, 2, 30, 300, ikey("x"), ikey("z"));
        try vs.logAndApply(&edit);
    }

    // CURRENT must exist and point at a MANIFEST.
    {
        const cur = try filename.currentFileName(gpa, "db");
        defer gpa.free(cur);
        try testing.expect(me.env().fileExists(cur));
    }

    // --- Recover into a fresh VersionSet --------------------------------
    var vs2 = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs2.deinit();
    try vs2.recover();

    try testing.expectEqual(@as(u64, 5), vs2.logNumber());
    try testing.expectEqual(@as(u64, 999), vs2.lastSequence());
    // next_file_number was set to 50 in the edit; recover must honour it.
    try testing.expect(vs2.newFileNumber() >= 50);

    const v = vs2.currentVersion();
    try testing.expectEqual(@as(usize, 2), v.files[0].items.len);
    try testing.expectEqual(@as(usize, 1), v.files[1].items.len);
    try testing.expectEqual(@as(usize, 1), v.files[2].items.len);

    // Level 0 keeps insertion order (10, 11).
    try testing.expectEqual(@as(u64, 10), v.files[0].items[0].number);
    try testing.expectEqual(@as(u64, 11), v.files[0].items[1].number);
    try testing.expectEqual(@as(u64, 100), v.files[0].items[0].file_size);
    try testing.expectEqualSlices(u8, ikey("a"), v.files[0].items[0].smallest);
    try testing.expectEqualSlices(u8, ikey("b"), v.files[0].items[0].largest);

    try testing.expectEqual(@as(u64, 20), v.files[1].items[0].number);
    try testing.expectEqual(@as(u64, 200), v.files[1].items[0].file_size);
    try testing.expectEqual(@as(u64, 30), v.files[2].items[0].number);
}

test "writeSnapshot emits kColumnFamilyAdd for default CF" {
    // After logAndApply, the MANIFEST is created via createDescriptor which calls
    // writeSnapshot.  Recovery must succeed even with the CF records prepended.
    // This test verifies that:
    //   (a) The MANIFEST is self-contained after writeSnapshot (recovery works).
    //   (b) The recovered VersionSet has the correct scalars (not confused by
    //       the CF tag records).
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    {
        var vs = try VersionSet.init(gpa, me.env(), "cfdb", .{});
        defer vs.deinit();

        var edit = VersionEdit.init();
        defer edit.deinit(gpa);
        edit.setLogNumber(3);
        edit.setLastSequence(50);
        edit.setNextFileNumber(10);
        try vs.logAndApply(&edit);
    }

    // Recover must succeed: the MANIFEST now starts with a kColumnFamilyAdd record.
    var vs2 = try VersionSet.init(gpa, me.env(), "cfdb", .{});
    defer vs2.deinit();
    try vs2.recover();

    try testing.expectEqual(@as(u64, 3), vs2.logNumber());
    try testing.expectEqual(@as(u64, 50), vs2.lastSequence());
    try testing.expect(vs2.newFileNumber() >= 10);
}

test "recover across multiple edits accumulates layout" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    {
        var vs = try VersionSet.init(gpa, me.env(), "db", .{});
        defer vs.deinit();

        {
            var edit = VersionEdit.init();
            defer edit.deinit(gpa);
            edit.setLastSequence(10);
            try edit.addFile(gpa, 1, 20, 200, ikey("e"), ikey("g"));
            try edit.addFile(gpa, 1, 21, 200, ikey("m"), ikey("p"));
            try vs.logAndApply(&edit);
        }
        {
            var edit = VersionEdit.init();
            defer edit.deinit(gpa);
            edit.setLastSequence(20);
            try edit.removeFile(gpa, 1, 20);
            try edit.addFile(gpa, 1, 22, 200, ikey("a"), ikey("c"));
            try vs.logAndApply(&edit);
        }
    }

    var vs2 = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs2.deinit();
    try vs2.recover();

    try testing.expectEqual(@as(u64, 20), vs2.lastSequence());
    const v = vs2.currentVersion();
    try testing.expectEqual(@as(usize, 2), v.files[1].items.len);
    // Sorted by smallest: 22 (a..c) then 21 (m..p); 20 was removed.
    try testing.expectEqual(@as(u64, 22), v.files[1].items[0].number);
    try testing.expectEqual(@as(u64, 21), v.files[1].items[1].number);
}

// ===========================================================================
// M6.2 — Version compaction helpers (size/count + overlapping inputs + score).
// ===========================================================================

/// Free a list returned by overlappingInputs (deep-owned key bytes + list).
fn freeFileList(gpa: std.mem.Allocator, list: *std.ArrayListUnmanaged(FileMetaData)) void {
    for (list.items) |f| {
        gpa.free(f.smallest);
        gpa.free(f.largest);
    }
    list.deinit(gpa);
}

test "M6.2: totalFileSize / numFiles per level" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    try edit.addFile(gpa, 0, 10, 100, ikey("a"), ikey("b"));
    try edit.addFile(gpa, 0, 11, 250, ikey("c"), ikey("d"));
    try edit.addFile(gpa, 1, 20, 1000, ikey("m"), ikey("n"));
    try vs.logAndApply(&edit);

    const v = vs.currentVersion();
    try testing.expectEqual(@as(usize, 2), v.numFiles(0));
    try testing.expectEqual(@as(usize, 1), v.numFiles(1));
    try testing.expectEqual(@as(usize, 0), v.numFiles(2));
    try testing.expectEqual(@as(u64, 350), v.totalFileSize(0));
    try testing.expectEqual(@as(u64, 1000), v.totalFileSize(1));
}

test "M6.2: overlappingInputs on a sorted level picks only the overlapping files" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    // Level 1 (sorted, non-overlapping): [a,c] [e,g] [m,p].
    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    try edit.addFile(gpa, 1, 31, 100, ikey("a"), ikey("c"));
    try edit.addFile(gpa, 1, 32, 100, ikey("e"), ikey("g"));
    try edit.addFile(gpa, 1, 33, 100, ikey("m"), ikey("p"));
    try vs.logAndApply(&edit);

    const v = vs.currentVersion();

    // Query [b, f] overlaps [a,c] and [e,g] but not [m,p].
    {
        var list = try v.overlappingInputs(gpa, 1, ikey("b"), ikey("f"), comparator.bytewise);
        defer freeFileList(gpa, &list);
        try testing.expectEqual(@as(usize, 2), list.items.len);
        try testing.expectEqual(@as(u64, 31), list.items[0].number);
        try testing.expectEqual(@as(u64, 32), list.items[1].number);
    }

    // Unbounded (null,null) → every file.
    {
        var list = try v.overlappingInputs(gpa, 1, null, null, comparator.bytewise);
        defer freeFileList(gpa, &list);
        try testing.expectEqual(@as(usize, 3), list.items.len);
    }

    // Query [x, z] overlaps nothing.
    {
        var list = try v.overlappingInputs(gpa, 1, ikey("x"), ikey("z"), comparator.bytewise);
        defer freeFileList(gpa, &list);
        try testing.expectEqual(@as(usize, 0), list.items.len);
    }
}

test "M6.2: overlappingInputs expands the range for level 0 (overlapping files)" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    // L0 files overlap arbitrarily:
    //   f10 = [a, c], f11 = [b, e], f12 = [d, f], f13 = [x, z]
    // A query that touches f11 ([b,e]) must expand to also include f10 (touches
    // b) and f12 (touches d/e), but NOT f13 ([x,z]).
    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    try edit.addFile(gpa, 0, 10, 100, ikey("a"), ikey("c"));
    try edit.addFile(gpa, 0, 11, 100, ikey("b"), ikey("e"));
    try edit.addFile(gpa, 0, 12, 100, ikey("d"), ikey("f"));
    try edit.addFile(gpa, 0, 13, 100, ikey("x"), ikey("z"));
    try vs.logAndApply(&edit);

    const v = vs.currentVersion();

    // Query exactly file 11's range [b, e]; expansion pulls in 10 and 12.
    var list = try v.overlappingInputs(gpa, 0, ikey("b"), ikey("e"), comparator.bytewise);
    defer freeFileList(gpa, &list);

    var saw = [_]bool{false} ** 4; // index 0..3 -> numbers 10..13
    for (list.items) |f| {
        try testing.expect(f.number >= 10 and f.number <= 13);
        saw[f.number - 10] = true;
    }
    try testing.expect(saw[0]); // 10 [a,c]
    try testing.expect(saw[1]); // 11 [b,e]
    try testing.expect(saw[2]); // 12 [d,f]
    try testing.expect(!saw[3]); // 13 [x,z] — disjoint
    try testing.expectEqual(@as(usize, 3), list.items.len);
}

test "M6.2: pickCompactionLevel — L0 by file count, Ln by size" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    // Small triggers so the test can force a pick deterministically.
    const opts = options.Options{
        .level0_file_num_compaction_trigger = 4,
        .max_bytes_for_level_base = 1000,
    };

    // No files: nothing to compact.
    {
        var vs = try VersionSet.init(gpa, me.env(), "noc", opts);
        defer vs.deinit();
        try testing.expect(vs.pickCompactionLevel() == null);
    }

    // 4 L0 files reach the trigger (score 1.0) → pick L0.
    {
        var vs = try VersionSet.init(gpa, me.env(), "l0", opts);
        defer vs.deinit();
        var edit = VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.addFile(gpa, 0, 10, 10, ikey("a"), ikey("b"));
        try edit.addFile(gpa, 0, 11, 10, ikey("c"), ikey("d"));
        try edit.addFile(gpa, 0, 12, 10, ikey("e"), ikey("f"));
        try edit.addFile(gpa, 0, 13, 10, ikey("g"), ikey("h"));
        try vs.logAndApply(&edit);
        try testing.expectEqual(@as(usize, 0), vs.pickCompactionLevel().?);
    }

    // L1 over its byte budget (max_bytes_for_level_base=1000) → pick L1.
    {
        var vs = try VersionSet.init(gpa, me.env(), "l1", opts);
        defer vs.deinit();
        var edit = VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.addFile(gpa, 1, 20, 1500, ikey("a"), ikey("z"));
        try vs.logAndApply(&edit);
        try testing.expectEqual(@as(usize, 1), vs.pickCompactionLevel().?);
    }
}

// ===========================================================================
// Version.get / addIterators tests (M6.0 — SST read path).
// ===========================================================================

const table_builder = @import("../format/table_builder.zig");
const bloom = @import("../format/bloom.zig");

/// An internal-key entry to write into an SST, plus its decoded user value.
const SSTEntry = struct { user: []const u8, seq: u64, t: internal_key.ValueType, value: []const u8 };

/// Encode `user ++ fixed64(packSequenceAndType(seq, t))` into `buf` (caller owns).
fn encodeIkey(gpa: std.mem.Allocator, user: []const u8, seq: u64, t: internal_key.ValueType) ![]u8 {
    const out = try gpa.alloc(u8, user.len + 8);
    @memcpy(out[0..user.len], user);
    coding.encodeFixed64(out[user.len..][0..8], internal_key.packSequenceAndType(seq, t));
    return out;
}

/// Build an SST at `db/<number>.sst` from `entries`, which MUST already be in
/// internal-key order (user ascending, then sequence descending).  Returns the
/// file size; the smallest/largest internal keys are written into `smallest`/
/// `largest` (caller-owned).
fn buildInternalSST(
    gpa: std.mem.Allocator,
    e: env.Env,
    dbname: []const u8,
    number: u64,
    policy: bloom.BloomFilterPolicy,
    entries: []const SSTEntry,
    smallest: *[]u8,
    largest: *[]u8,
) !u64 {
    const path = try filename.tableFileName(gpa, dbname, number);
    defer gpa.free(path);

    var first: ?[]u8 = null;
    var last: ?[]u8 = null;
    errdefer {
        if (first) |s| gpa.free(s);
        if (last) |l| gpa.free(l);
    }

    // SSTs store internal keys, so build/sort them with the IKC (user asc, then
    // trailer DESC) — the same comparator the TableCache opens tables with.
    var ikc = internal_key.InternalKeyComparator{ .user = comparator.bytewise };
    const build_opts = options.Options{ .comparator = ikc.comparatorInterface() };

    var wf = try e.newWritableFile(gpa, path);
    errdefer wf.close() catch {};
    var tb = try table_builder.TableBuilder.init(gpa, build_opts, wf, policy);
    defer tb.deinit();
    for (entries) |en| {
        const ik = try encodeIkey(gpa, en.user, en.seq, en.t);
        defer gpa.free(ik);
        try tb.add(ik, en.value);
        if (first == null) first = try gpa.dupe(u8, ik);
        if (last) |l| gpa.free(l);
        last = try gpa.dupe(u8, ik);
    }
    try tb.finish();
    try wf.close();

    smallest.* = first.?;
    largest.* = last.?;
    return e.getFileSize(path);
}

// A VersionSet uses the InternalKeyComparator over internal keys, so the SST is
// built/sorted with that comparator.  Construct a Version holding one L0 file.
test "Version.get: found / deleted / absent / snapshot semantics" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);
    const user_cmp = comparator.bytewise;

    // Entries in internal-key order: user asc (each user once — the M6.0 block
    // builder sorts data blocks bytewise, so distinct user keys are required for
    // bytewise == InternalKeyComparator order within a file; multi-version per
    // file is M6.1+ once the block builder learns the IKC).
    //   a: value@5 = "a5"
    //   b: value@10 = "b10"
    //   c: deletion@7
    const entries = [_]SSTEntry{
        .{ .user = "a", .seq = 5, .t = .value, .value = "a5" },
        .{ .user = "b", .seq = 10, .t = .value, .value = "b10" },
        .{ .user = "c", .seq = 7, .t = .deletion, .value = "" },
    };

    var smallest: []u8 = undefined;
    var largest: []u8 = undefined;
    const size = try buildInternalSST(gpa, e, "db", 7, policy, &entries, &smallest, &largest);
    defer gpa.free(smallest);
    defer gpa.free(largest);

    // Build a Version with that file at L0.
    var version = Version.initEmpty();
    defer version.deinit(gpa);
    try version.addFileOwned(gpa, 0, .{ .number = 7, .file_size = size, .smallest = smallest, .largest = largest });

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    // b at a high snapshot → value "b10".
    {
        const r = (try version.get(gpa, &tc, user_cmp, "b", 1000)) orelse return error.TestExpectedFound;
        switch (r) {
            .found => |val| {
                defer gpa.free(val);
                try testing.expectEqualStrings("b10", val);
            },
            .deleted => return error.TestUnexpectedDeleted,
        }
    }

    // a → "a5".
    {
        const r = (try version.get(gpa, &tc, user_cmp, "a", 1000)) orelse return error.TestExpectedFound;
        switch (r) {
            .found => |val| {
                defer gpa.free(val);
                try testing.expectEqualStrings("a5", val);
            },
            .deleted => return error.TestUnexpectedDeleted,
        }
    }

    // c → tombstone → .deleted.
    {
        const r = (try version.get(gpa, &tc, user_cmp, "c", 1000)) orelse return error.TestExpectedDeleted;
        try testing.expect(r == .deleted);
    }

    // Absent user "z" → not covered (> largest) → null.
    try testing.expect((try version.get(gpa, &tc, user_cmp, "z", 1000)) == null);

    // Absent user that sorts before the file's range → not covered → null.
    try testing.expect((try version.get(gpa, &tc, user_cmp, "0", 1000)) == null);

    // Snapshot hides newer entries: b@10 is invisible at snapshot 4 → null.
    try testing.expect((try version.get(gpa, &tc, user_cmp, "b", 4)) == null);

    // a@5 IS visible at snapshot 5 (seq <= snapshot).
    {
        const r = (try version.get(gpa, &tc, user_cmp, "a", 5)) orelse return error.TestExpectedFound;
        switch (r) {
            .found => |val| {
                defer gpa.free(val);
                try testing.expectEqualStrings("a5", val);
            },
            .deleted => return error.TestUnexpectedDeleted,
        }
    }

    // a@5 invisible at snapshot 4 → null.
    try testing.expect((try version.get(gpa, &tc, user_cmp, "a", 4)) == null);

    // Snapshot below c's tombstone (seq 7) → not visible → null.
    try testing.expect((try version.get(gpa, &tc, user_cmp, "c", 6)) == null);
}

test "Version.get: L0 newest-file shadows older overlapping file" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);
    const user_cmp = comparator.bytewise;

    // Older file (number 7): k = value@1 "old".
    const old_entries = [_]SSTEntry{.{ .user = "k", .seq = 1, .t = .value, .value = "old" }};
    var s_old: []u8 = undefined;
    var l_old: []u8 = undefined;
    const size_old = try buildInternalSST(gpa, e, "db", 7, policy, &old_entries, &s_old, &l_old);
    defer gpa.free(s_old);
    defer gpa.free(l_old);

    // Newer file (number 9): k = value@5 "new".
    const new_entries = [_]SSTEntry{.{ .user = "k", .seq = 5, .t = .value, .value = "new" }};
    var s_new: []u8 = undefined;
    var l_new: []u8 = undefined;
    const size_new = try buildInternalSST(gpa, e, "db", 9, policy, &new_entries, &s_new, &l_new);
    defer gpa.free(s_new);
    defer gpa.free(l_new);

    var version = Version.initEmpty();
    defer version.deinit(gpa);
    // Insertion order = oldest first (7), then newest (9): newest is probed first.
    try version.addFileOwned(gpa, 0, .{ .number = 7, .file_size = size_old, .smallest = s_old, .largest = l_old });
    try version.addFileOwned(gpa, 0, .{ .number = 9, .file_size = size_new, .smallest = s_new, .largest = l_new });

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    // High snapshot → newest file (9) wins → "new".
    {
        const r = (try version.get(gpa, &tc, user_cmp, "k", 1000)) orelse return error.TestExpectedFound;
        switch (r) {
            .found => |val| {
                defer gpa.free(val);
                try testing.expectEqualStrings("new", val);
            },
            .deleted => return error.TestUnexpectedDeleted,
        }
    }

    // Snapshot 1 hides the @5 entry; the older file (@1) supplies "old".
    {
        const r = (try version.get(gpa, &tc, user_cmp, "k", 1)) orelse return error.TestExpectedFound;
        switch (r) {
            .found => |val| {
                defer gpa.free(val);
                try testing.expectEqualStrings("old", val);
            },
            .deleted => return error.TestUnexpectedDeleted,
        }
    }
}

test "Version.addIterators: merged scan yields every file's entries" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const entries = [_]SSTEntry{
        .{ .user = "a", .seq = 1, .t = .value, .value = "av" },
        .{ .user = "b", .seq = 1, .t = .value, .value = "bv" },
    };
    var smallest: []u8 = undefined;
    var largest: []u8 = undefined;
    const size = try buildInternalSST(gpa, e, "db", 7, policy, &entries, &smallest, &largest);
    defer gpa.free(smallest);
    defer gpa.free(largest);

    var version = Version.initEmpty();
    defer version.deinit(gpa);
    try version.addFileOwned(gpa, 0, .{ .number = 7, .file_size = size, .smallest = smallest, .largest = largest });

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    var list: std.ArrayListUnmanaged(iterator.Iterator) = .empty;
    defer {
        for (list.items) |it| it.deinit();
        list.deinit(gpa);
    }
    try version.addIterators(gpa, &tc, &list);
    try testing.expectEqual(@as(usize, 1), list.items.len);

    // Scan the single table iterator: 2 internal-key entries in order.
    const it = list.items[0];
    it.seekToFirst();
    var count: usize = 0;
    while (it.valid()) : (it.next()) {
        const uk = internal_key.extractUserKey(it.key());
        if (count == 0) try testing.expectEqualStrings("a", uk);
        if (count == 1) try testing.expectEqualStrings("b", uk);
        count += 1;
    }
    try testing.expectEqual(@as(usize, 2), count);
}

// ===========================================================================
// getcontext — merge-operand accumulation across files (probeFile forward scan).
// ===========================================================================

fn u64le(buf: *[8]u8, v: u64) []const u8 {
    std.mem.writeInt(u64, buf, v, .little);
    return buf[0..];
}

fn decU64(bytes: []const u8) u64 {
    return std.mem.readInt(u64, bytes[0..8], .little);
}

test "getcontext: Version.getMerge accumulates operands across multiple L0 files" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);
    const user_cmp = comparator.bytewise;

    var add = merge_operator.Uint64AddOperator{};
    const merge_op = add.operator();

    var b: [8]u8 = undefined;

    // Three separate SST files, each holding ONE entry for user key "c", written
    // oldest -> newest (file numbers increasing).  The oldest is the Put base,
    // then two merge operands in newer files:
    //   file 7  (oldest):  c = value@1  (base 100)
    //   file 8:            c = merge@3  (+5)
    //   file 9  (newest):  c = merge@5  (+7)
    // A newest-first probe must gather operands [+7, +5] then meet the base 100
    // and sum to 112.
    const f7 = [_]SSTEntry{.{ .user = "c", .seq = 1, .t = .value, .value = u64le(&b, 100) }};
    var s7: []u8 = undefined;
    var l7: []u8 = undefined;
    const sz7 = try buildInternalSST(gpa, e, "db", 7, policy, &f7, &s7, &l7);
    defer gpa.free(s7);
    defer gpa.free(l7);

    var b8: [8]u8 = undefined;
    const f8 = [_]SSTEntry{.{ .user = "c", .seq = 3, .t = .merge, .value = u64le(&b8, 5) }};
    var s8: []u8 = undefined;
    var l8: []u8 = undefined;
    const sz8 = try buildInternalSST(gpa, e, "db", 8, policy, &f8, &s8, &l8);
    defer gpa.free(s8);
    defer gpa.free(l8);

    var b9: [8]u8 = undefined;
    const f9 = [_]SSTEntry{.{ .user = "c", .seq = 5, .t = .merge, .value = u64le(&b9, 7) }};
    var s9: []u8 = undefined;
    var l9: []u8 = undefined;
    const sz9 = try buildInternalSST(gpa, e, "db", 9, policy, &f9, &s9, &l9);
    defer gpa.free(s9);
    defer gpa.free(l9);

    var version = Version.initEmpty();
    defer version.deinit(gpa);
    // L0 insertion order = oldest first; reverse iteration probes newest first.
    try version.addFileOwned(gpa, 0, .{ .number = 7, .file_size = sz7, .smallest = s7, .largest = l7 });
    try version.addFileOwned(gpa, 0, .{ .number = 8, .file_size = sz8, .smallest = s8, .largest = l8 });
    try version.addFileOwned(gpa, 0, .{ .number = 9, .file_size = sz9, .smallest = s9, .largest = l9 });

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    // High snapshot: base 100 + 5 + 7 = 112.
    {
        const r = (try version.getMerge(gpa, &tc, user_cmp, "c", 1000, merge_op)) orelse
            return error.TestExpectedFound;
        switch (r) {
            .found => |val| {
                defer gpa.free(val);
                try testing.expectEqual(@as(u64, 112), decU64(val));
            },
            .deleted => return error.TestUnexpectedDeleted,
        }
    }

    // Snapshot 4 hides file 9's @5 operand: base 100 + 5 = 105.
    {
        const r = (try version.getMerge(gpa, &tc, user_cmp, "c", 4, merge_op)) orelse
            return error.TestExpectedFound;
        switch (r) {
            .found => |val| {
                defer gpa.free(val);
                try testing.expectEqual(@as(u64, 105), decU64(val));
            },
            .deleted => return error.TestUnexpectedDeleted,
        }
    }

    // Snapshot 2 hides both operands; only the base 100 is visible.
    {
        const r = (try version.getMerge(gpa, &tc, user_cmp, "c", 2, merge_op)) orelse
            return error.TestExpectedFound;
        switch (r) {
            .found => |val| {
                defer gpa.free(val);
                try testing.expectEqual(@as(u64, 100), decU64(val));
            },
            .deleted => return error.TestUnexpectedDeleted,
        }
    }
}

test "getcontext: operands with no base sum from 0; a deletion stops accumulation" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);
    const user_cmp = comparator.bytewise;

    var add = merge_operator.Uint64AddOperator{};
    const merge_op = add.operator();

    var b: [8]u8 = undefined;

    // No base — pure operands across two files: +4 (older) and +9 (newer) = 13.
    const fa = [_]SSTEntry{.{ .user = "k", .seq = 2, .t = .merge, .value = u64le(&b, 4) }};
    var sa: []u8 = undefined;
    var la: []u8 = undefined;
    const sza = try buildInternalSST(gpa, e, "db", 20, policy, &fa, &sa, &la);
    defer gpa.free(sa);
    defer gpa.free(la);

    var bb: [8]u8 = undefined;
    const fb = [_]SSTEntry{.{ .user = "k", .seq = 6, .t = .merge, .value = u64le(&bb, 9) }};
    var sb: []u8 = undefined;
    var lb: []u8 = undefined;
    const szb = try buildInternalSST(gpa, e, "db", 21, policy, &fb, &sb, &lb);
    defer gpa.free(sb);
    defer gpa.free(lb);

    var version = Version.initEmpty();
    defer version.deinit(gpa);
    try version.addFileOwned(gpa, 0, .{ .number = 20, .file_size = sza, .smallest = sa, .largest = la });
    try version.addFileOwned(gpa, 0, .{ .number = 21, .file_size = szb, .smallest = sb, .largest = lb });

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    // Pure operands sum from 0: 4 + 9 = 13.
    {
        const r = (try version.getMerge(gpa, &tc, user_cmp, "k", 1000, merge_op)) orelse
            return error.TestExpectedFound;
        switch (r) {
            .found => |val| {
                defer gpa.free(val);
                try testing.expectEqual(@as(u64, 13), decU64(val));
            },
            .deleted => return error.TestUnexpectedDeleted,
        }
    }
}
