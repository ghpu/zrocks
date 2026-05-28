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
    /// `user_key ++ trailer(sequence, seek)`).  Opens a table iterator via the
    /// cache, seeks, and on an exact user-key hit returns the parsed result
    /// (value duped with `gpa`, or `.deleted`).  Returns null when the file does
    /// not contain `user_key`.  The iterator is always deinited.
    fn probeFile(
        gpa: std.mem.Allocator,
        tc: *TableCache,
        user_cmp: comparator.Comparator,
        f: FileMetaData,
        user_key: []const u8,
        lookup_ikey: []const u8,
    ) !?GetResult {
        var it = try tc.newIterator(gpa, f.number, f.file_size);
        defer it.deinit();
        it.seek(lookup_ikey);
        if (it.status()) |e| return e;
        if (!it.valid()) return null;

        const stored_ikey = it.key();
        const stored_uk = internal_key.extractUserKey(stored_ikey);
        if (user_cmp.compare(stored_uk, user_key) != .eq) return null;

        const parsed = internal_key.parseInternalKey(stored_ikey) catch return error.Corruption;
        switch (parsed.type) {
            .value => return .{ .found = try gpa.dupe(u8, it.value()) },
            .deletion, .single_deletion, .range_deletion => return .deleted,
            .merge => return null, // merge operands not handled at this layer
        }
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
        // RED: LSM point lookup across levels not implemented yet.
        _ = self;
        _ = gpa;
        _ = tc;
        _ = user_cmp;
        _ = user_key;
        _ = sequence;
        return error.NotImplemented;
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
        // RED: per-file table iterators not implemented yet.
        _ = self;
        _ = gpa;
        _ = tc;
        _ = list;
        return error.NotImplemented;
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
    descriptor_file: ?env.WritableFile,
    descriptor_log: ?log_writer.Writer,

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
        return self.next_file_number;
    }

    pub fn setLastSequence(self: *VersionSet, v: u64) void {
        self.last_sequence = v;
    }

    pub fn setLogNumber(self: *VersionSet, v: u64) void {
        self.log_number = v;
    }

    // -- apply / log -----------------------------------------------------

    /// Build a new current Version = apply(edit, current), append the encoded
    /// edit to the MANIFEST, install the new Version, and free the old one.
    pub fn logAndApply(self: *VersionSet, edit: *VersionEdit) !void {
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
    /// log (comparator name, bookkeeping scalars, and every live file).
    fn writeSnapshot(self: *VersionSet, writer: *log_writer.Writer, wf: env.WritableFile) !void {
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
