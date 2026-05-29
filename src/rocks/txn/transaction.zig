//! transaction.zig — shared transaction base for M7.6 (optimistic + pessimistic).
//!
//! A `Transaction` buffers writes in a `WriteBatch` (applied atomically by
//! `db.write` at commit) plus a *read-your-own-writes* (RYOW) index — a
//! WriteBatchWithIndex-lite — mapping each written user key to its LATEST op so
//! `get` inside the txn sees uncommitted writes.  Reads that miss the index fall
//! through to `db.get` at the txn's BEGIN snapshot (`snapshot_seq`), giving
//! snapshot isolation: a txn never observes external commits made after it began.
//!
//! This base owns no concurrency control of its own; the optimistic / pessimistic
//! flavours layer conflict detection (commit-time validation) / locking on top.
//! It DOES own all duped key/value bytes in the RYOW index and frees them in
//! `deinit`/`rollback`.
//!
//! Standalone test note (Zig 0.16): `../../...` imports resolve only inside the
//! `src`-rooted module — compile the suite via the _verify.zig shim, not this
//! file directly.

const std = @import("std");

const db_mod = @import("../../db/db.zig");
const write_batch = @import("../../format/write_batch.zig");
const options_mod = @import("../../options.zig");

const DB = db_mod.DB;
const WriteBatch = write_batch.WriteBatch;
const ReadOptions = options_mod.ReadOptions;
const WriteOptions = options_mod.WriteOptions;

/// A buffered write operation recorded in the RYOW index (the latest op for a
/// key).  `put`/`merge` own a duped value slice; `delete` carries none.
pub const WriteOp = union(enum) {
    put: []const u8,
    delete,
    merge: []const u8,
};

/// The shared transaction state.  Embedded (`base`) by the optimistic /
/// pessimistic transactions, which add their own conflict-control fields.
pub const Transaction = struct {
    gpa: std.mem.Allocator,
    db: *DB,
    /// Buffered writes, applied atomically by `db.write` at commit time.
    batch: WriteBatch,
    /// Read-your-own-writes index: user key -> latest buffered op.  Owns its
    /// duped key bytes (the map keys) AND the duped value bytes in put/merge ops.
    ryow: std.StringHashMapUnmanaged(WriteOp) = .empty,
    /// Snapshot sequence captured at begin (reads see DB state at-or-below this).
    snapshot_seq: u64,
    /// Set once `commit` succeeds (guards double-commit / post-commit writes).
    committed: bool = false,

    /// Initialise a base transaction over `db`, capturing the current sequence
    /// as the read snapshot.  Caller must `deinit` (or `rollback`, then `deinit`).
    ///
    /// The BEGIN snapshot is read via `db.lastSequence()`, which takes the DB
    /// write mutex (D2b1).  A bare `db.last_sequence` read raced the write path's
    /// `last_sequence += batch.count()` — a torn/stale snapshot would have broken
    /// snapshot isolation (the txn could miss or partially see a concurrent
    /// commit).  Allocate the batch BEFORE taking the snapshot so the (fallible)
    /// allocation never holds the DB lock.
    pub fn init(gpa: std.mem.Allocator, db: *DB) !Transaction {
        const batch = try WriteBatch.init(gpa);
        return .{
            .gpa = gpa,
            .db = db,
            .batch = batch,
            .snapshot_seq = db.lastSequence(),
        };
    }

    /// Free the buffered batch and every duped key/value in the RYOW index.
    pub fn deinit(self: *Transaction) void {
        self.clearRyow();
        self.ryow.deinit(self.gpa);
        self.batch.deinit(self.gpa);
        self.* = undefined;
    }

    /// Free all RYOW entries (keys + put/merge values) and empty the map.
    fn clearRyow(self: *Transaction) void {
        var it = self.ryow.iterator();
        while (it.next()) |e| {
            self.gpa.free(e.key_ptr.*);
            switch (e.value_ptr.*) {
                .put, .merge => |v| self.gpa.free(v),
                .delete => {},
            }
        }
        self.ryow.clearRetainingCapacity();
    }

    /// Record `op` for `key` in the RYOW index, replacing (and freeing) any prior
    /// op/value for the same key.  Takes ownership of the duped `op` value bytes;
    /// dupes the key only when inserting a new entry.
    fn recordRyow(self: *Transaction, key: []const u8, op: WriteOp) !void {
        const gop = try self.ryow.getOrPut(self.gpa, key);
        if (gop.found_existing) {
            // Replace the previous op for this key, freeing its old value.
            switch (gop.value_ptr.*) {
                .put, .merge => |v| self.gpa.free(v),
                .delete => {},
            }
        } else {
            // New entry: own a duped copy of the key as the map key.
            gop.key_ptr.* = self.gpa.dupe(u8, key) catch |err| {
                // Roll back the slot we just reserved to avoid a dangling key.
                _ = self.ryow.remove(key);
                return err;
            };
        }
        gop.value_ptr.* = op;
    }

    /// Buffer a put of `value` under `key`: append to the commit batch AND record
    /// it in the RYOW index (latest op wins).
    pub fn put(self: *Transaction, key: []const u8, value: []const u8) !void {
        std.debug.assert(!self.committed);
        // Append to the batch first (the fallible step that does NOT transfer
        // ownership), then dupe + record in the RYOW index — `recordRyow` takes
        // ownership of `owned` only on success, so the errdefer covers just the
        // window before that and never double-frees.
        try self.batch.put(self.gpa, key, value);
        const owned = try self.gpa.dupe(u8, value);
        errdefer self.gpa.free(owned);
        try self.recordRyow(key, .{ .put = owned });
    }

    /// Buffer a delete of `key`: append to the commit batch AND record a delete in
    /// the RYOW index (so `get` returns null for it within the txn).
    pub fn delete(self: *Transaction, key: []const u8) !void {
        std.debug.assert(!self.committed);
        try self.batch.delete(self.gpa, key);
        try self.recordRyow(key, .delete);
    }

    /// Buffer a merge operand for `key`.  The RYOW index records the operand as a
    /// `.merge` op; an in-txn `get` returns the raw buffered operand bytes (full
    /// operand resolution against the base value happens at commit/read time in
    /// the DB — the txn does not run the merge operator itself).
    pub fn merge(self: *Transaction, key: []const u8, value: []const u8) !void {
        std.debug.assert(!self.committed);
        try self.batch.merge(self.gpa, key, value);
        const owned = try self.gpa.dupe(u8, value);
        errdefer self.gpa.free(owned);
        try self.recordRyow(key, .{ .merge = owned });
    }

    /// READ-YOUR-OWN-WRITES point lookup.  If `key` was written in this txn, the
    /// buffered op decides the result (put/merge -> its value; delete -> null).
    /// Otherwise the DB is read at the txn's BEGIN snapshot, giving snapshot
    /// isolation.  Returns a freshly gpa-allocated value the CALLER OWNS, or null.
    pub fn get(self: *Transaction, gpa: std.mem.Allocator, key: []const u8) !?[]u8 {
        if (self.ryow.get(key)) |op| {
            return switch (op) {
                .put, .merge => |v| try gpa.dupe(u8, v),
                .delete => null,
            };
        }
        return self.db.get(.{ .snapshot = self.snapshot_seq }, key);
    }

    /// Discard all buffered writes (resets the commit batch + clears the RYOW
    /// index).  The base form releases no locks; the pessimistic flavour overrides
    /// `rollback` to also release its held locks.  Safe to call once.
    pub fn rollbackBase(self: *Transaction) !void {
        self.clearRyow();
        // Reset the batch to an empty header.
        self.batch.deinit(self.gpa);
        self.batch = try WriteBatch.init(self.gpa);
    }

    /// True when the txn has buffered no writes (used to skip empty commits).
    pub fn isEmpty(self: *const Transaction) bool {
        return self.batch.count() == 0;
    }
};

// ---------------------------------------------------------------------------
// D2b1 — the BEGIN snapshot is captured under the DB write mutex (no race).
// ---------------------------------------------------------------------------

const testing = std.testing;
const MemEnv = @import("../../env/env.zig").MemEnv;

test "D2b1: Transaction.init captures last_sequence via the locked accessor" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try DB.open(gpa, me.env(), "txnseq", .{});
    defer db.close();

    // Commit some writes through the DB so last_sequence advances.
    try db.put(.{}, "a", "1");
    try db.put(.{}, "b", "2");

    // A txn begun now must capture EXACTLY the current locked last sequence as
    // its BEGIN snapshot — read via db.lastSequence() (the mutex-guarded accessor
    // the init now uses), not a bare field read that raced the write path.
    var txn = try Transaction.init(gpa, db);
    defer txn.deinit();
    try testing.expectEqual(db.lastSequence(), txn.snapshot_seq);

    // Init left the DB write mutex free (it locked, read, then unlocked): a
    // subsequent write does not deadlock against the just-finished init.
    try testing.expect(db.write_mutex.tryLock());
    db.write_mutex.unlock(db.io);
    try db.put(.{}, "c", "3");

    // Writes after BEGIN do not move the txn's captured snapshot.
    try testing.expectEqual(@as(u64, 2), txn.snapshot_seq);
}

test "D2b1: txn read-your-own-writes + snapshot isolation still hold under the lock" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try DB.open(gpa, me.env(), "txniso", .{});
    defer db.close();

    try db.put(.{}, "k", "committed");

    var txn = try Transaction.init(gpa, db);
    defer txn.deinit();

    // Read-your-own-writes: a buffered put is visible inside the txn.
    try txn.put("k", "buffered");
    const own = try txn.get(gpa, "k") orelse return error.TestExpectedFound;
    defer gpa.free(own);
    try testing.expectEqualStrings("buffered", own);

    // Snapshot isolation: a key only the DB has (at-or-below the BEGIN snapshot)
    // is read through the captured snapshot_seq.
    const base = try txn.get(gpa, "missing");
    try testing.expectEqual(@as(?[]u8, null), base);
}
