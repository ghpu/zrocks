//! compaction.zig — leveled (LevelDB-style) compaction (M6.2).
//!
//! RED phase: declarations with stubs + the full test suite (targeted +
//! randomized gate).  GREEN phase fills in the bodies.

const std = @import("std");

const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const comparator = @import("../util/comparator.zig");
const internal_key = @import("../format/internal_key.zig");
const version_set = @import("../version/version_set.zig");
const version_edit = @import("../version/version_edit.zig");

const FileMetaData = version_edit.FileMetaData;
const VersionSet = version_set.VersionSet;

pub const Compaction = struct {
    /// The level being compacted; output files land at `level + 1`.
    level: usize,
    /// inputs[0] = files chosen from `level`; inputs[1] = the overlapping files
    /// at `level + 1`.  Each list deep-owns its FileMetaData key bytes.
    inputs: [2]std.ArrayListUnmanaged(FileMetaData),

    pub fn deinit(self: *Compaction, gpa: std.mem.Allocator) void {
        _ = self;
        _ = gpa;
        @panic("compaction.Compaction.deinit not implemented");
    }
};

pub fn pickCompaction(
    gpa: std.mem.Allocator,
    versions: *VersionSet,
    user_cmp: comparator.Comparator,
) !?Compaction {
    _ = gpa;
    _ = versions;
    _ = user_cmp;
    @panic("compaction.pickCompaction not implemented");
}

pub fn isBaseLevelForKey(
    versions: *VersionSet,
    level: usize,
    user_key: []const u8,
    user_cmp: comparator.Comparator,
) bool {
    _ = versions;
    _ = level;
    _ = user_key;
    _ = user_cmp;
    @panic("compaction.isBaseLevelForKey not implemented");
}

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
    _ = gpa;
    _ = e;
    _ = dbname;
    _ = options;
    _ = ikc;
    _ = user_cmp;
    _ = versions;
    _ = compaction;
    _ = smallest_snapshot;
    @panic("compaction.doCompaction not implemented");
}

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const db_mod = @import("db.zig");
const DB = db_mod.DB;
const filename = @import("../version/filename.zig");

/// Count files across all levels of the DB's current version.
fn totalSSTFiles(db: *DB) usize {
    var n: usize = 0;
    const v = db.versions.currentVersion();
    for (&v.files) |level| n += level.items.len;
    return n;
}

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
