//! db.zig — the in-memory embedded key/value store (M4.1).
//!
//! Ties the building blocks into a usable DB: a single MemTable behind a
//! write-ahead log, with snapshot-aware point lookups and a tombstone-hiding
//! forward iterator.  No persistence/recovery (Phase 5) and no flush to SST /
//! compaction (Phase 6) yet.
//!
//! Single source for reads (the live MemTable).  `newIterator` wraps the
//! memtable's internal iterator behind the generic `iterator.Iterator` and then
//! a `DBIterator` for user-facing snapshot/tombstone semantics; later phases add
//! SST sources by composing a MergingIterator in that same slot.
//!
//! Standalone test note (Zig 0.16): this file uses `../...` imports that only
//! resolve when compiled as part of the `src`-rooted module.  To run the suite:
//!   printf 'test { _ = @import("db/db.zig"); }' > src/_verify.zig \
//!     && zig test src/_verify.zig && rm src/_verify.zig

// RED phase: declarations with @panic stubs + full tests.

const std = @import("std");

const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const comparator = @import("../util/comparator.zig");
const coding = @import("../util/coding.zig");
const internal_key = @import("../format/internal_key.zig");
const memtable_mod = @import("../memtable/memtable.zig");
const write_batch = @import("../format/write_batch.zig");
const log_writer = @import("../format/log_writer.zig");
const iterator = @import("../iterator/iterator.zig");

const write_path = @import("write_path.zig");
const db_iter = @import("db_iter.zig");
const snapshot_mod = @import("snapshot.zig");

const Options = options_mod.Options;
const ReadOptions = options_mod.ReadOptions;
const WriteOptions = options_mod.WriteOptions;
const MemTable = memtable_mod.MemTable;
const WriteBatch = write_batch.WriteBatch;

pub const Snapshot = snapshot_mod.Snapshot;
pub const DBIterator = db_iter.DBIterator;

pub const DB = struct {
    gpa: std.mem.Allocator,
    env: env.Env,
    options: Options,
    name: []u8,
    ikcmp: internal_key.InternalKeyComparator,
    mem: *MemTable,
    wal_file: env.WritableFile,
    wal: log_writer.Writer,
    last_sequence: u64,
    // TODO(concurrency): DB write mutex (single-threaded for M4.1).

    pub fn open(gpa: std.mem.Allocator, e: env.Env, name: []const u8, options: Options) !*DB {
        _ = gpa;
        _ = e;
        _ = name;
        _ = options;
        @panic("RED: DB.open unimplemented");
    }

    pub fn close(self: *DB) void {
        _ = self;
        @panic("RED: DB.close unimplemented");
    }

    pub fn put(self: *DB, wopts: WriteOptions, key: []const u8, value: []const u8) !void {
        _ = self;
        _ = wopts;
        _ = key;
        _ = value;
        @panic("RED: DB.put unimplemented");
    }

    pub fn delete(self: *DB, wopts: WriteOptions, key: []const u8) !void {
        _ = self;
        _ = wopts;
        _ = key;
        @panic("RED: DB.delete unimplemented");
    }

    pub fn write(self: *DB, wopts: WriteOptions, batch: *WriteBatch) !void {
        _ = self;
        _ = wopts;
        _ = batch;
        @panic("RED: DB.write unimplemented");
    }

    pub fn get(self: *DB, ropts: ReadOptions, key: []const u8) !?[]u8 {
        _ = self;
        _ = ropts;
        _ = key;
        @panic("RED: DB.get unimplemented");
    }

    pub fn newIterator(self: *DB, gpa: std.mem.Allocator, ropts: ReadOptions) !DBIterator {
        _ = self;
        _ = gpa;
        _ = ropts;
        @panic("RED: DB.newIterator unimplemented");
    }

    pub fn getSnapshot(self: *DB) Snapshot {
        _ = self;
        @panic("RED: DB.getSnapshot unimplemented");
    }

    pub fn releaseSnapshot(self: *DB, snap: Snapshot) void {
        _ = self;
        _ = snap;
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const MemEnv = env.MemEnv;

fn openTestDB(gpa: std.mem.Allocator, me: *MemEnv) !*DB {
    return DB.open(gpa, me.env(), "testdb", .{});
}

test "put/get basic" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v1");
    const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("v1", got);
}

test "put overwrite returns newest" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v1");
    try db.put(.{}, "k", "v2");
    const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("v2", got);
}

test "delete removes key" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v1");
    try db.delete(.{}, "k");
    try testing.expect((try db.get(.{}, "k")) == null);
}

test "get absent key is null" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try testing.expect((try db.get(.{}, "missing")) == null);
}

test "write applies multi-op batch atomically" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    // Seed a so the delete has something to remove.
    try db.put(.{}, "a", "a-old");

    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);
    try wb.put(gpa, "a", "a-new");
    try wb.put(gpa, "b", "b-val");
    try wb.delete(gpa, "a");
    try db.write(.{}, &wb);

    // a: put then delete in the same batch → gone.
    try testing.expect((try db.get(.{}, "a")) == null);
    // b present.
    const b = try db.get(.{}, "b") orelse return error.TestExpectedFound;
    defer gpa.free(b);
    try testing.expectEqualStrings("b-val", b);
}

test "snapshot isolation" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v1");
    try db.put(.{}, "k", "v2");
    const snap = db.getSnapshot();
    defer db.releaseSnapshot(snap);

    try db.put(.{}, "k", "v3");

    // Read at the snapshot → v2 (the value as of the snapshot).
    const at_snap = try db.get(.{ .snapshot = snap.sequence }, "k") orelse return error.TestExpectedFound;
    defer gpa.free(at_snap);
    try testing.expectEqualStrings("v2", at_snap);

    // Read without snapshot → v3.
    const latest = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(latest);
    try testing.expectEqualStrings("v3", latest);
}

test "newIterator: latest-only sorted scan with tombstone hidden, seek works" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    // Insert a..e with an overwrite of "b" and a delete of "d".
    try db.put(.{}, "a", "a1");
    try db.put(.{}, "b", "b1");
    try db.put(.{}, "c", "c1");
    try db.put(.{}, "d", "d1");
    try db.put(.{}, "e", "e1");
    try db.put(.{}, "b", "b2"); // overwrite
    try db.delete(.{}, "d"); // tombstone

    var it = try db.newIterator(gpa, .{});
    defer it.deinit();

    const exp_k = [_][]const u8{ "a", "b", "c", "e" };
    const exp_v = [_][]const u8{ "a1", "b2", "c1", "e1" };
    var i: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        try testing.expect(i < exp_k.len);
        try testing.expectEqualStrings(exp_k[i], it.key());
        try testing.expectEqualStrings(exp_v[i], it.value());
        i += 1;
    }
    try testing.expectEqual(exp_k.len, i);

    // seek("c") lands on c.
    it.seek("c");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("c", it.key());
    try testing.expectEqualStrings("c1", it.value());
}

test "sequence numbers count single puts" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    const n: u64 = 5;
    var i: u64 = 0;
    while (i < n) : (i += 1) {
        try db.put(.{}, "k", "v");
    }
    try testing.expectEqual(n, db.last_sequence);
}

test "WAL file is non-empty after writes" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v");

    const size = try me.env().getFileSize("testdb/000001.log");
    try testing.expect(size > 0);
}
