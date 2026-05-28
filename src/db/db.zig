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

    /// Open (create) a DB rooted at directory `name` on `e`.
    ///
    /// Creates the directory (ok if it already exists), opens a fresh WAL at
    /// `<name>/000001.log`, and starts with an empty MemTable.  No recovery /
    /// WAL replay yet (Phase 5).  Returns a heap-allocated *DB; the caller must
    /// eventually call `close`.
    pub fn open(gpa: std.mem.Allocator, e: env.Env, name: []const u8, options: Options) !*DB {
        const self = try gpa.create(DB);
        errdefer gpa.destroy(self);

        self.gpa = gpa;
        self.env = e;
        self.options = options;
        self.last_sequence = 0;
        // ikcmp must live at a stable address (the memtable's entry comparator
        // points at it); `self` is heap-allocated so &self.ikcmp is stable.
        self.ikcmp = .{ .user = options.comparator };

        self.name = try gpa.dupe(u8, name);
        errdefer gpa.free(self.name);

        // Directory (no-op success if it already exists on MemEnv).
        try e.makeDir(name);

        const wal_path = try std.fmt.allocPrint(gpa, "{s}/000001.log", .{name});
        defer gpa.free(wal_path);

        self.wal_file = try e.newWritableFile(gpa, wal_path);
        errdefer self.wal_file.close() catch {};

        self.mem = try MemTable.init(gpa, options.comparator);
        errdefer self.mem.deinit();

        self.wal = log_writer.Writer.init(self.wal_file);
        return self;
    }

    /// Flush+close the WAL, deinit the MemTable, free the DB.
    pub fn close(self: *DB) void {
        const gpa = self.gpa;
        self.wal_file.close() catch {};
        self.mem.deinit();
        gpa.free(self.name);
        gpa.destroy(self);
    }

    /// Put a single key/value (a one-op WriteBatch under the hood).
    pub fn put(self: *DB, wopts: WriteOptions, key: []const u8, value: []const u8) !void {
        var batch = try WriteBatch.init(self.gpa);
        defer batch.deinit(self.gpa);
        try batch.put(self.gpa, key, value);
        try self.write(wopts, &batch);
    }

    /// Delete a single key (a one-op WriteBatch under the hood).
    pub fn delete(self: *DB, wopts: WriteOptions, key: []const u8) !void {
        var batch = try WriteBatch.init(self.gpa);
        defer batch.deinit(self.gpa);
        try batch.delete(self.gpa, key);
        try self.write(wopts, &batch);
    }

    /// Atomically apply `batch`: stamp its sequence, append it to the WAL
    /// (unless disabled), insert its records into the MemTable, and advance the
    /// last sequence by the batch's record count.
    pub fn write(self: *DB, wopts: WriteOptions, batch: *WriteBatch) !void {
        // TODO(concurrency): acquire DB write mutex here (single-threaded now).
        const first_sequence = self.last_sequence + 1;
        batch.setSequence(first_sequence);

        if (!wopts.disable_wal) {
            try self.wal.addRecord(self.gpa, batch.contents());
            // Make the record visible/durable.  MemEnv only commits buffered
            // bytes on flush/sync, so flush after every record so the on-"disk"
            // WAL reflects the write (and getFileSize sees it).
            if (wopts.sync) {
                try self.wal_file.sync();
            } else {
                try self.wal_file.flush();
            }
        }

        try write_path.insertBatch(self.mem, batch, first_sequence);
        self.last_sequence += batch.count();
    }

    /// Point lookup visible at the snapshot (`ropts.snapshot` or the latest
    /// sequence).  Returns a freshly duped value the CALLER OWNS and must free,
    /// or null if the key is absent or deleted at that snapshot.
    pub fn get(self: *DB, ropts: ReadOptions, key: []const u8) !?[]u8 {
        const seq = ropts.snapshot orelse self.last_sequence;
        var lookup = try memtable_mod.LookupKey.init(self.gpa, key, seq);
        defer lookup.deinit(self.gpa);

        switch (self.mem.get(lookup) orelse return null) {
            .found => |v| return try self.gpa.dupe(u8, v),
            .deleted => return null,
        }
    }

    /// Forward, snapshot-aware, tombstone-hiding iterator over the live
    /// MemTable.  Caller must call `.deinit()` on the returned iterator.
    ///
    /// The single MemTable source is wrapped behind the generic
    /// `iterator.Iterator` by a small heap-allocated adapter (so its address is
    /// stable behind the returned-by-value DBIterator); the DBIterator owns and
    /// frees that adapter on deinit.  Later phases slot a MergingIterator over
    /// MemTable + SSTs into the same `inner` position.
    pub fn newIterator(self: *DB, gpa: std.mem.Allocator, ropts: ReadOptions) !DBIterator {
        const seq = ropts.snapshot orelse self.last_sequence;

        const adapter = try gpa.create(MemIterAdapter);
        errdefer gpa.destroy(adapter);
        adapter.* = .{ .gpa = gpa, .it = MemTable.Iterator.init(self.mem) };

        var dbit = DBIterator.init(gpa, adapter.genericIterator(), self.options.comparator, seq);
        dbit.owned_inner = adapter;
        dbit.owned_inner_destroy = MemIterAdapter.destroy;
        return dbit;
    }

    /// A snapshot pinned at the current latest sequence.
    pub fn getSnapshot(self: *DB) Snapshot {
        return .{ .sequence = self.last_sequence };
    }

    /// No-op for M4.1 (no SnapshotList / compaction pinning yet).
    pub fn releaseSnapshot(self: *DB, snap: Snapshot) void {
        _ = self;
        _ = snap;
    }
};

// ---------------------------------------------------------------------------
// MemTable -> generic Iterator adapter.
// ---------------------------------------------------------------------------
//
// The MemTable.Iterator yields `internalKey()` / `value()` but its `seek` takes
// a length-prefixed *memtable key* (varint32(ikey_len) ++ internal_key).  The
// generic Iterator contract — and DBIterator — pass raw internal keys, so this
// adapter re-prefixes on seek.  An owned scratch buffer holds the wrapped key.
const MemIterAdapter = struct {
    gpa: std.mem.Allocator,
    it: MemTable.Iterator,
    seek_buf: std.ArrayListUnmanaged(u8) = .empty,

    fn genericIterator(self: *MemIterAdapter) iterator.Iterator {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn destroy(gpa: std.mem.Allocator, ctx: *anyopaque) void {
        const self: *MemIterAdapter = @ptrCast(@alignCast(ctx));
        self.seek_buf.deinit(gpa);
        gpa.destroy(self);
    }

    const vtable = iterator.Iterator.VTable{
        .seekToFirst = vSeekToFirst,
        .seekToLast = vSeekToLast,
        .seek = vSeek,
        .next = vNext,
        .prev = vPrev,
        .valid = vValid,
        .key = vKey,
        .value = vValue,
        .status = vStatus,
    };

    fn cast(ctx: *anyopaque) *MemIterAdapter {
        return @ptrCast(@alignCast(ctx));
    }

    fn vSeekToFirst(ctx: *anyopaque) void {
        cast(ctx).it.seekToFirst();
    }

    fn vSeekToLast(ctx: *anyopaque) void {
        _ = ctx; // unused: DBIterator never seeks the inner source backward.
    }

    fn vSeek(ctx: *anyopaque, target: []const u8) void {
        // `target` is a raw internal key; wrap it as a memtable key
        // (varint32(len) ++ internal_key) into the owned scratch buffer.
        const self = cast(ctx);
        self.seek_buf.clearRetainingCapacity();
        coding.putVarint32(&self.seek_buf, self.gpa, @intCast(target.len)) catch return;
        self.seek_buf.appendSlice(self.gpa, target) catch return;
        self.it.seek(self.seek_buf.items);
    }

    fn vNext(ctx: *anyopaque) void {
        cast(ctx).it.next();
    }

    fn vPrev(ctx: *anyopaque) void {
        _ = ctx; // forward-only inner cursor for M4.1.
    }

    fn vValid(ctx: *anyopaque) bool {
        return cast(ctx).it.valid();
    }

    fn vKey(ctx: *anyopaque) []const u8 {
        return cast(ctx).it.internalKey();
    }

    fn vValue(ctx: *anyopaque) []const u8 {
        return cast(ctx).it.value();
    }

    fn vStatus(ctx: *anyopaque) ?anyerror {
        _ = ctx;
        return null;
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
