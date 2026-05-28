//! pessimistic.zig — pessimistic (locking) transactions (M7.6).
//!
//! A pessimistic transaction acquires an exclusive lock on each key the FIRST
//! time it writes that key (`put`/`delete`/`merge`), via a shared `LockManager`.
//! If another open transaction already holds the key the write fails immediately
//! with `error.Busy` (the DB is single-threaded, so locks detect conflicts
//! between INTERLEAVED open txns rather than blocking — see lock_manager.zig).
//! Holding the lock for the txn's lifetime means commit never needs the
//! optimistic sequence-based validation: by construction no other txn could have
//! written a locked key.  `commit` applies the buffered batch atomically then
//! releases all locks; `rollback` discards the batch and releases all locks.
//!
//! TODO(concurrency): real blocking acquisition (wait instead of Busy) once the
//! DB has a write mutex / background threads.

const std = @import("std");

const db_mod = @import("../../db/db.zig");
const txn_mod = @import("transaction.zig");
const lock_mod = @import("lock_manager.zig");
const options_mod = @import("../../options.zig");

const DB = db_mod.DB;
const Transaction = txn_mod.Transaction;
const LockManager = lock_mod.LockManager;
const WriteOptions = options_mod.WriteOptions;

/// A pessimistic transaction: the shared base plus a unique id and a borrowed
/// `*LockManager` (owned by the `PessimisticTransactionDB`).
pub const PessimisticTransaction = struct {
    base: Transaction,
    id: u64,
    locks: *LockManager,

    /// Begin a transaction on `db` with the given unique `id`, locking through
    /// the shared `lock_manager`.  Caller must `commit`/`rollback`, then `deinit`.
    pub fn begin(gpa: std.mem.Allocator, db: *DB, id: u64, lock_manager: *LockManager) !PessimisticTransaction {
        return .{
            .base = try Transaction.init(gpa, db),
            .id = id,
            .locks = lock_manager,
        };
    }

    /// Release any still-held locks defensively, then free the base.  (A committed
    /// or rolled-back txn already released its locks; `unlockAll` is idempotent.)
    pub fn deinit(self: *PessimisticTransaction) void {
        self.locks.unlockAll(self.id);
        self.base.deinit();
    }

    /// Acquire the exclusive lock on `key` for this txn; `error.Busy` if another
    /// open txn holds it.  Re-locking a key this txn already owns is a no-op.
    fn lockKey(self: *PessimisticTransaction, key: []const u8) !void {
        if (!(try self.locks.tryLock(key, self.id))) return error.Busy;
    }

    pub fn put(self: *PessimisticTransaction, key: []const u8, value: []const u8) !void {
        try self.lockKey(key);
        return self.base.put(key, value);
    }

    pub fn delete(self: *PessimisticTransaction, key: []const u8) !void {
        try self.lockKey(key);
        return self.base.delete(key);
    }

    pub fn merge(self: *PessimisticTransaction, key: []const u8, value: []const u8) !void {
        try self.lockKey(key);
        return self.base.merge(key, value);
    }

    pub fn get(self: *PessimisticTransaction, gpa: std.mem.Allocator, key: []const u8) !?[]u8 {
        return self.base.get(gpa, key);
    }

    /// Apply the buffered batch atomically, then release all held locks.  No
    /// sequence validation is needed: the locks guaranteed exclusivity.
    pub fn commit(self: *PessimisticTransaction, wopts: WriteOptions) !void {
        std.debug.assert(!self.base.committed);
        if (!self.base.isEmpty()) {
            try self.base.db.write(wopts, &self.base.batch);
        }
        self.base.committed = true;
        self.locks.unlockAll(self.id);
    }

    /// Discard buffered writes and release all held locks.
    pub fn rollback(self: *PessimisticTransaction) !void {
        try self.base.rollbackBase();
        self.locks.unlockAll(self.id);
    }
};

/// Holder mirroring RocksDB's `(Pessimistic)TransactionDB`: owns the shared
/// `LockManager` and hands out transactions with monotonically increasing ids.
pub const PessimisticTransactionDB = struct {
    db: *DB,
    locks: LockManager,
    next_id: u64 = 1,

    pub fn init(gpa: std.mem.Allocator, db: *DB) PessimisticTransactionDB {
        return .{ .db = db, .locks = LockManager.init(gpa) };
    }

    pub fn deinit(self: *PessimisticTransactionDB) void {
        self.locks.deinit();
        self.* = undefined;
    }

    pub fn beginTransaction(self: *PessimisticTransactionDB, gpa: std.mem.Allocator) !PessimisticTransaction {
        const id = self.next_id;
        self.next_id += 1;
        return PessimisticTransaction.begin(gpa, self.db, id, &self.locks);
    }
};

// ---------------------------------------------------------------------------
// Tests (via MemEnv)
// ---------------------------------------------------------------------------

const testing = std.testing;
const env = @import("../../env/env.zig");
const MemEnv = env.MemEnv;

fn openTestDB(gpa: std.mem.Allocator, me: *MemEnv) !*DB {
    return DB.open(gpa, me.env(), "ptxndb", .{});
}

test "pessimistic: lock blocks a second txn, then succeeds after the first commits" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    var tdb = PessimisticTransactionDB.init(gpa, db);
    defer tdb.deinit();

    var a = try tdb.beginTransaction(gpa);
    defer a.deinit();
    var b = try tdb.beginTransaction(gpa);
    defer b.deinit();

    try a.put("k", "a"); // a locks k
    try testing.expectError(error.Busy, b.put("k", "b")); // lock held by a

    try a.commit(.{}); // releases the lock

    try b.put("k", "b"); // now b can lock + write
    try b.commit(.{});

    const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("b", got);
}

test "pessimistic: rollback releases locks so another txn can proceed" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "orig");

    var tdb = PessimisticTransactionDB.init(gpa, db);
    defer tdb.deinit();

    var a = try tdb.beginTransaction(gpa);
    defer a.deinit();
    try a.put("k", "a-buffered");
    try a.rollback(); // discard + release lock

    var b = try tdb.beginTransaction(gpa);
    defer b.deinit();
    try b.put("k", "b"); // lock free now
    try b.commit(.{});

    const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("b", got); // a's buffered write never applied
}

test "pessimistic: commit applies a multi-op batch atomically" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "x", "x-old");

    var tdb = PessimisticTransactionDB.init(gpa, db);
    defer tdb.deinit();

    var t = try tdb.beginTransaction(gpa);
    defer t.deinit();
    try t.put("x", "x-new");
    try t.put("y", "y-val");
    try t.delete("x"); // re-lock own key (no-op), buffered delete after put
    try t.commit(.{});

    // put then delete of x in the same txn -> gone.
    try testing.expect((try db.get(.{}, "x")) == null);
    const y = try db.get(.{}, "y") orelse return error.TestExpectedFound;
    defer gpa.free(y);
    try testing.expectEqualStrings("y-val", y);
}

test "pessimistic: read-your-own-writes + snapshot isolation" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v0");

    var tdb = PessimisticTransactionDB.init(gpa, db);
    defer tdb.deinit();

    var t = try tdb.beginTransaction(gpa);
    defer t.deinit();

    try t.put("k", "v-txn");
    {
        const inside = try t.get(gpa, "k") orelse return error.TestExpectedFound;
        defer gpa.free(inside);
        try testing.expectEqualStrings("v-txn", inside);
    }
    {
        const outside = try db.get(.{}, "k") orelse return error.TestExpectedFound;
        defer gpa.free(outside);
        try testing.expectEqualStrings("v0", outside);
    }

    try t.delete("k");
    try testing.expect((try t.get(gpa, "k")) == null);

    // External commit after begin is invisible to the txn's snapshot read.
    try db.put(.{}, "ext", "external");
    try testing.expect((try t.get(gpa, "ext")) == null);

    try t.rollback();
}
