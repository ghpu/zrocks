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

const version_set = @import("../version/version_set.zig");
const version_edit = @import("../version/version_edit.zig");
const filename = @import("../version/filename.zig");
const log_format = @import("../format/log_format.zig");

const write_path = @import("write_path.zig");
const db_iter = @import("db_iter.zig");
const snapshot_mod = @import("snapshot.zig");
const recovery = @import("recovery.zig");

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
    versions: *version_set.VersionSet,
    wal_file: env.WritableFile,
    wal: log_writer.Writer,
    last_sequence: u64,
    // TODO(concurrency): DB write mutex (single-threaded for M4.1/M5.2).

    /// Open a DB rooted at directory `name` on `e`, recovering durable state.
    ///
    /// If a `CURRENT` file exists, the VersionSet is recovered from the
    /// MANIFEST and the active WAL is replayed into the MemTable; the SAME log
    /// is reopened for appending so subsequent writes continue in it (the
    /// "reuse-logs" design — recovered data lives in the single MemTable kept
    /// durable by the reused log, since there is no flush layer yet, Phase 6).
    ///
    /// Otherwise a fresh DB is created: the directory is made, a MANIFEST +
    /// CURRENT are written via the VersionSet, and a new empty WAL is opened.
    ///
    /// Returns a heap-allocated *DB; the caller must eventually call `close`.
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

        self.mem = try MemTable.init(gpa, options.comparator);
        errdefer self.mem.deinit();

        const vs = try gpa.create(version_set.VersionSet);
        errdefer gpa.destroy(vs);
        vs.* = try version_set.VersionSet.init(gpa, e, name, options);
        errdefer vs.deinit();
        self.versions = vs;

        const current_path = try filename.currentFileName(gpa, name);
        defer gpa.free(current_path);

        if (e.fileExists(current_path)) {
            // ----- recover an existing DB -----------------------------------
            try vs.recover();

            const log_number = vs.logNumber();
            const log_path = try filename.logFileName(gpa, name, log_number);
            defer gpa.free(log_path);

            // Replay the active WAL into the memtable.  Sequences are assigned
            // from each batch's own header; max_seq is the highest seen.
            const max_seq = try recovery.replayLog(
                gpa,
                e,
                log_path,
                self.mem,
                vs.lastSequence() + 1,
            );
            self.last_sequence = @max(vs.lastSequence(), max_seq);

            // Reuse the SAME log: reopen it for appending and resume the writer
            // mid-block so the next reopen replays everything (reuse-logs).
            const file_size = e.getFileSize(log_path) catch 0;
            self.wal_file = try e.newAppendableFile(gpa, log_path);
            errdefer self.wal_file.close() catch {};
            self.wal = log_writer.Writer.initWithOffset(
                self.wal_file,
                @intCast(file_size % log_format.kBlockSize),
            );
        } else {
            // ----- fresh DB --------------------------------------------------
            const log_number = vs.newFileNumber();

            var edit = version_edit.VersionEdit.init();
            defer edit.deinit(gpa);
            try edit.setComparatorName(gpa, options.comparator.name());
            edit.setLogNumber(log_number);
            edit.setNextFileNumber(vs.nextFileNumber());
            edit.setLastSequence(0);
            try vs.logAndApply(&edit);

            const log_path = try filename.logFileName(gpa, name, log_number);
            defer gpa.free(log_path);

            self.wal_file = try e.newWritableFile(gpa, log_path);
            errdefer self.wal_file.close() catch {};
            self.wal = log_writer.Writer.init(self.wal_file);
            self.last_sequence = 0;
        }

        return self;
    }

    /// Flush+close the WAL, deinit the VersionSet + MemTable, free the DB.
    pub fn close(self: *DB) void {
        const gpa = self.gpa;
        self.wal_file.close() catch {};
        self.versions.deinit();
        gpa.destroy(self.versions);
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

    // The active WAL is named by the VersionSet's log number; on a fresh DB
    // that is the first file number handed out.
    const log_path = try filename.logFileName(gpa, "testdb", db.versions.logNumber());
    defer gpa.free(log_path);
    const size = try me.env().getFileSize(log_path);
    try testing.expect(size > 0);
}

// ---------------------------------------------------------------------------
// M5.2 — durability / recovery
// ---------------------------------------------------------------------------

test "fresh DB creates CURRENT, MANIFEST, and a log file" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const db = try DB.open(gpa, e, "freshdb", .{});
    defer db.close();

    const cur = try filename.currentFileName(gpa, "freshdb");
    defer gpa.free(cur);
    try testing.expect(e.fileExists(cur));

    // CURRENT names a MANIFEST that exists.
    const manifest = try filename.manifestFileName(gpa, "freshdb", db.versions.manifestFileNumber());
    defer gpa.free(manifest);
    try testing.expect(e.fileExists(manifest));

    // The active log file exists.
    const log_path = try filename.logFileName(gpa, "freshdb", db.versions.logNumber());
    defer gpa.free(log_path);
    try testing.expect(e.fileExists(log_path));
}

test "reopen recovers data from the WAL" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        const db = try DB.open(gpa, e, "recdb", .{});
        defer db.close();
        try db.put(.{}, "a", "1");
        try db.put(.{}, "b", "2");
        try db.delete(.{}, "a");
    }

    // Reopen the SAME MemEnv + name.
    {
        const db = try DB.open(gpa, e, "recdb", .{});
        defer db.close();

        try testing.expect((try db.get(.{}, "a")) == null);

        const b = try db.get(.{}, "b") orelse return error.TestExpectedFound;
        defer gpa.free(b);
        try testing.expectEqualStrings("2", b);
    }
}

test "multi-session append: writes accumulate across reopens" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        const db = try DB.open(gpa, e, "msdb", .{});
        defer db.close();
        try db.put(.{}, "a", "1");
        try db.put(.{}, "b", "2");
        try db.delete(.{}, "a");
    }
    {
        const db = try DB.open(gpa, e, "msdb", .{});
        defer db.close();
        try db.put(.{}, "c", "3");
    }
    {
        const db = try DB.open(gpa, e, "msdb", .{});
        defer db.close();

        try testing.expect((try db.get(.{}, "a")) == null);

        const b = try db.get(.{}, "b") orelse return error.TestExpectedFound;
        defer gpa.free(b);
        try testing.expectEqualStrings("2", b);

        const c = try db.get(.{}, "c") orelse return error.TestExpectedFound;
        defer gpa.free(c);
        try testing.expectEqualStrings("3", c);
    }
}

test "sequence continuity: a new put outranks all recovered writes" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        const db = try DB.open(gpa, e, "seqdb", .{});
        defer db.close();
        try db.put(.{}, "x", "old1");
        try db.put(.{}, "x", "old2");
        try db.put(.{}, "y", "yv");
    }

    {
        const db = try DB.open(gpa, e, "seqdb", .{});
        defer db.close();

        // Recovered last_sequence covers all 3 prior writes.
        try testing.expect(db.last_sequence >= 3);
        const recovered_seq = db.last_sequence;

        // A snapshot taken right after reopen sees the recovered values.
        const snap = db.getSnapshot();
        defer db.releaseSnapshot(snap);

        // A new write must get a strictly higher sequence (no regression).
        try db.put(.{}, "x", "new");
        try testing.expect(db.last_sequence > recovered_seq);

        // The pre-reopen value of x is still readable at the post-reopen snapshot.
        const at_snap = try db.get(.{ .snapshot = snap.sequence }, "x") orelse return error.TestExpectedFound;
        defer gpa.free(at_snap);
        try testing.expectEqualStrings("old2", at_snap);

        // The latest read sees the new write.
        const latest = try db.get(.{}, "x") orelse return error.TestExpectedFound;
        defer gpa.free(latest);
        try testing.expectEqualStrings("new", latest);
    }
}

test "crash recovery: data is present without a clean close" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Open, write, but deliberately do NOT close this handle in the usual way.
    // The WAL is flushed per write, so the bytes are committed to the MemEnv.
    const db1 = try DB.open(gpa, e, "crashdb", .{});
    try db1.put(.{}, "k1", "v1");
    try db1.put(.{}, "k2", "v2");

    // Simulate a crash + restart: open a SECOND handle on the same MemEnv.
    {
        const db2 = try DB.open(gpa, e, "crashdb", .{});
        defer db2.close();

        const v1 = try db2.get(.{}, "k1") orelse return error.TestExpectedFound;
        defer gpa.free(v1);
        try testing.expectEqualStrings("v1", v1);

        const v2 = try db2.get(.{}, "k2") orelse return error.TestExpectedFound;
        defer gpa.free(v2);
        try testing.expectEqualStrings("v2", v2);
    }

    // Tidy up the leaked-on-purpose first handle so the test is leak-free.
    db1.close();
}

test "corrupt WAL tail: committed prefix is recovered, no error" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    var log_path: []u8 = undefined;
    {
        const db = try DB.open(gpa, e, "corruptdb", .{});
        defer db.close();
        try db.put(.{}, "a", "1");
        try db.put(.{}, "b", "2");
        log_path = try filename.logFileName(gpa, "corruptdb", db.versions.logNumber());
    }
    defer gpa.free(log_path);

    // Corrupt the LAST few bytes of the committed WAL in the MemEnv.
    {
        const bytes = try readAllBytes(e, gpa, log_path);
        defer gpa.free(bytes);
        try testing.expect(bytes.len > 4);
        // Flip the trailing bytes (the last record's tail).
        var i: usize = bytes.len - 4;
        while (i < bytes.len) : (i += 1) bytes[i] ^= 0xff;

        var wf = try e.newWritableFile(gpa, log_path); // truncates + rewrites
        errdefer wf.close() catch {};
        try wf.append(bytes);
        try wf.flush();
        try wf.close();
    }

    // Reopen: the committed prefix ("a") must be recovered; no error thrown.
    {
        const db = try DB.open(gpa, e, "corruptdb", .{});
        defer db.close();

        const a = try db.get(.{}, "a") orelse return error.TestExpectedFound;
        defer gpa.free(a);
        try testing.expectEqualStrings("1", a);
    }
}

/// Read the entire committed contents of `path` (caller frees).
fn readAllBytes(e: env.Env, gpa: std.mem.Allocator, path: []const u8) ![]u8 {
    var sf = try e.newSequentialFile(gpa, path);
    defer sf.close() catch {};
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(gpa);
    var chunk: [4096]u8 = undefined;
    while (true) {
        const n = try sf.read(&chunk);
        if (n == 0) break;
        try out.appendSlice(gpa, chunk[0..n]);
    }
    return out.toOwnedSlice(gpa);
}
