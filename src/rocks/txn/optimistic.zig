//! optimistic.zig — optimistic transactions (M7.6).
//!
//! An optimistic transaction holds NO locks while it runs: writes are merely
//! buffered (in the shared `Transaction` base) and conflicts are detected only at
//! `commit`.  Validation compares, for every key the txn wrote, the DB's CURRENT
//! newest sequence for that key against the txn's BEGIN snapshot: if some commit
//! advanced the key past the snapshot, a write-write conflict exists and commit
//! fails with `error.Busy` (the caller may retry).  When every written key is
//! clean the buffered batch is applied atomically via `db.write`.
//!
//! This suits low-contention workloads (no lock bookkeeping); the pessimistic
//! flavour (pessimistic.zig) instead locks keys eagerly on first write.
//!
//! TODO(2pc): no WritePrepared/WriteUnprepared two-phase commit — single-phase
//! atomic apply only.

const std = @import("std");

const db_mod = @import("../../db/db.zig");
const txn_mod = @import("transaction.zig");
const options_mod = @import("../../options.zig");

const DB = db_mod.DB;
const Transaction = txn_mod.Transaction;
const WriteOptions = options_mod.WriteOptions;

/// An optimistic transaction: the shared base plus commit-time validation.
pub const OptimisticTransaction = struct {
    base: Transaction,

    /// Begin a transaction on `db`, capturing `db.last_sequence` as the read
    /// snapshot.  Caller must `commit` or `rollback`, then `deinit`.
    pub fn begin(gpa: std.mem.Allocator, db: *DB) !OptimisticTransaction {
        return .{ .base = try Transaction.init(gpa, db) };
    }

    pub fn deinit(self: *OptimisticTransaction) void {
        self.base.deinit();
    }

    pub fn put(self: *OptimisticTransaction, key: []const u8, value: []const u8) !void {
        return self.base.put(key, value);
    }

    pub fn delete(self: *OptimisticTransaction, key: []const u8) !void {
        return self.base.delete(key);
    }

    pub fn merge(self: *OptimisticTransaction, key: []const u8, value: []const u8) !void {
        return self.base.merge(key, value);
    }

    pub fn get(self: *OptimisticTransaction, gpa: std.mem.Allocator, key: []const u8) !?[]u8 {
        return self.base.get(gpa, key);
    }

    /// Validate then apply.  For each key the txn wrote, if the DB's newest
    /// sequence for that key exceeds the txn's begin snapshot, another commit
    /// touched it since begin -> `error.Busy` (nothing is applied).  Otherwise the
    /// buffered batch is written atomically and the txn is marked committed.
    pub fn commit(self: *OptimisticTransaction, wopts: WriteOptions) !void {
        std.debug.assert(!self.base.committed);

        // Conflict validation: walk the written-key set (the RYOW index keys).
        var it = self.base.ryow.iterator();
        while (it.next()) |e| {
            const key = e.key_ptr.*;
            if (self.base.db.latestSequenceForKey(key) > self.base.snapshot_seq) {
                return error.Busy; // write-write conflict; abort (caller may retry)
            }
        }

        if (!self.base.isEmpty()) {
            try self.base.db.write(wopts, &self.base.batch);
        }
        self.base.committed = true;
    }

    /// Discard buffered writes (optimistic txns hold no locks to release).
    pub fn rollback(self: *OptimisticTransaction) !void {
        try self.base.rollbackBase();
    }
};

/// Thin holder mirroring RocksDB's `OptimisticTransactionDB`: just opens
/// transactions over an existing `*DB` (no extra shared state needed since
/// optimistic txns coordinate purely through the DB's sequence numbers).
pub const OptimisticTransactionDB = struct {
    db: *DB,

    pub fn init(db: *DB) OptimisticTransactionDB {
        return .{ .db = db };
    }

    pub fn beginTransaction(self: *OptimisticTransactionDB, gpa: std.mem.Allocator) !OptimisticTransaction {
        return OptimisticTransaction.begin(gpa, self.db);
    }
};

// ---------------------------------------------------------------------------
// Tests (via MemEnv)
// ---------------------------------------------------------------------------

const testing = std.testing;
const env = @import("../../env/env.zig");
const MemEnv = env.MemEnv;

fn openTestDB(gpa: std.mem.Allocator, me: *MemEnv) !*DB {
    return DB.open(gpa, me.env(), "txndb", .{});
}

test "optimistic: write-write conflict aborts the second commit with error.Busy" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    var a = try OptimisticTransaction.begin(gpa, db);
    defer a.deinit();
    var b = try OptimisticTransaction.begin(gpa, db); // SAME begin snapshot
    defer b.deinit();

    try a.put("k", "a");
    try a.commit(.{}); // ok: nothing changed k since a's snapshot

    try b.put("k", "b");
    try testing.expectError(error.Busy, b.commit(.{})); // k advanced past b's snapshot

    const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("a", got); // a's value survives, b aborted
}

test "optimistic: disjoint-key txns both commit" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    var a = try OptimisticTransaction.begin(gpa, db);
    defer a.deinit();
    var b = try OptimisticTransaction.begin(gpa, db);
    defer b.deinit();

    try a.put("ka", "va");
    try b.put("kb", "vb");
    try a.commit(.{});
    try b.commit(.{}); // different key -> no conflict

    const va = try db.get(.{}, "ka") orelse return error.TestExpectedFound;
    defer gpa.free(va);
    const vb = try db.get(.{}, "kb") orelse return error.TestExpectedFound;
    defer gpa.free(vb);
    try testing.expectEqualStrings("va", va);
    try testing.expectEqualStrings("vb", vb);
}

test "optimistic: commit applies a multi-op batch atomically" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "x", "x-old");

    var t = try OptimisticTransaction.begin(gpa, db);
    defer t.deinit();
    try t.put("x", "x-new");
    try t.put("y", "y-val");
    try t.delete("z-absent");
    try t.commit(.{});

    const x = try db.get(.{}, "x") orelse return error.TestExpectedFound;
    defer gpa.free(x);
    const y = try db.get(.{}, "y") orelse return error.TestExpectedFound;
    defer gpa.free(y);
    try testing.expectEqualStrings("x-new", x);
    try testing.expectEqualStrings("y-val", y);
}

test "optimistic: rollback discards buffered writes, DB unchanged" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "orig");

    var t = try OptimisticTransaction.begin(gpa, db);
    defer t.deinit();
    try t.put("k", "buffered");
    try t.put("new", "buffered");
    try t.rollback();

    // After rollback the txn is empty; committing applies nothing.
    try t.commit(.{});

    const k = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(k);
    try testing.expectEqualStrings("orig", k);
    try testing.expect((try db.get(.{}, "new")) == null);
}

test "optimistic: read-your-own-writes + snapshot isolation" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v0");

    var t = try OptimisticTransaction.begin(gpa, db); // snapshot sees k=v0

    // RYOW: an uncommitted put is visible inside the txn...
    try t.put("k", "v-txn");
    {
        const inside = try t.get(gpa, "k") orelse return error.TestExpectedFound;
        defer gpa.free(inside);
        try testing.expectEqualStrings("v-txn", inside);
    }
    // ...but NOT to a fresh DB read outside the txn.
    {
        const outside = try db.get(.{}, "k") orelse return error.TestExpectedFound;
        defer gpa.free(outside);
        try testing.expectEqualStrings("v0", outside);
    }

    // A buffered delete makes the in-txn read null.
    try t.delete("k");
    try testing.expect((try t.get(gpa, "k")) == null);

    // Snapshot isolation: an external commit AFTER begin is invisible to the txn.
    try db.put(.{}, "ext", "external");
    try testing.expect((try t.get(gpa, "ext")) == null);

    try t.rollback(); // discard so commit doesn't conflict; just cleaning up
    t.deinit();
}

test "optimistic: OptimisticTransactionDB.beginTransaction works" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    var tdb = OptimisticTransactionDB.init(db);
    var t = try tdb.beginTransaction(gpa);
    defer t.deinit();
    try t.put("k", "v");
    try t.commit(.{});

    const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("v", got);
}
