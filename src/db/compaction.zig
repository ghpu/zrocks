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
//! What is implemented vs left as TODO:
//!   * Size-based output split — implemented (correctness-sufficient).
//!   * Tombstone drop at the base level — implemented (isBaseLevelForKey).
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
        // accumulated range (L0 files overlap arbitrarily).
        const first = v.files[0].items[0];
        var expanded = try v.overlappingInputs(gpa, 0, first.smallest, first.largest, user_cmp);
        // `expanded` already deep-owns its bytes; move it into inputs[0].
        c.inputs[0] = expanded;
        expanded = .empty;
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
    var lvl1 = try v.overlappingInputs(gpa, level + 1, range.smallest, range.largest, user_cmp);
    c.inputs[1] = lvl1;
    lvl1 = .empty;

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

    // last_user_key holds the user key of the most recent OUTPUT/seen entry, in
    // a stable buffer (iterator slices are transient).
    var last_user_key: std.ArrayListUnmanaged(u8) = .empty;
    defer last_user_key.deinit(gpa);
    var has_last_user_key = false;

    mit.seekToFirst();
    while (mit.valid()) : (mit.next()) {
        if (mit.status()) |err| return err;

        const ikey = mit.key();
        const value = mit.value();

        // On a parse failure, keep the entry verbatim (defensive — should not
        // happen for well-formed SSTs).
        var drop = false;
        var parsed_ok = true;
        const parsed = internal_key.parseInternalKey(ikey) catch blk: {
            parsed_ok = false;
            break :blk internal_key.ParsedInternalKey{
                .user_key = ikey,
                .sequence = 0,
                .type = .value,
            };
        };

        if (parsed_ok) {
            const user_key = parsed.user_key;
            const first_for_key = !has_last_user_key or
                user_cmp.compare(user_key, last_user_key.items) != .eq;

            if (first_for_key) {
                // Remember this user key in a stable buffer.
                last_user_key.clearRetainingCapacity();
                try last_user_key.appendSlice(gpa, user_key);
                has_last_user_key = true;
            }

            if (!first_for_key and parsed.sequence <= smallest_snapshot) {
                // An older version of a user key already emitted, hidden by the
                // newer one and below the snapshot → drop.
                drop = true;
            } else if (parsed.type == .deletion and
                parsed.sequence <= smallest_snapshot and
                isBaseLevelForKey(versions, compaction.level, user_key, user_cmp))
            {
                // A tombstone no longer needed (nothing deeper would resurface) →
                // drop.
                drop = true;
            }
        }

        if (drop) continue;

        // --- emit the surviving entry into the current output builder ------
        if (builder == null) {
            cur_number = versions.newFileNumber();
            const path = try filename.tableFileName(gpa, dbname, cur_number);
            defer gpa.free(path);
            cur_file = try e.newWritableFile(gpa, path);
            builder = try table_builder_mod.TableBuilder.init(gpa, build_opts, cur_file.?, policy);
            cur_smallest = null;
            cur_largest = null;
        }

        try builder.?.add(ikey, value);
        if (cur_smallest == null) cur_smallest = try gpa.dupe(u8, ikey);
        if (cur_largest) |l| gpa.free(l);
        cur_largest = try gpa.dupe(u8, ikey);

        // Roll over to a fresh output once the current one reaches target size.
        if (builder.?.fileSize() >= options.target_file_size_base) {
            try finishOutput(gpa, &builder, &cur_file, cur_number, &cur_smallest, &cur_largest, &outputs);
        }
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
