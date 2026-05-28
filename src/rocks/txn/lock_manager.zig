//! lock_manager.zig — per-key exclusive locks for pessimistic transactions (M7.6).
//!
//! Maps each locked user key to the id of the transaction that owns it.  The DB
//! is single-threaded, so these locks do NOT block on an OS mutex; instead they
//! detect conflicts between INTERLEAVED open transactions in the same thread —
//! `tryLock` returns false when another live txn already holds the key, and the
//! caller surfaces `error.Busy`.  A txn re-locking its OWN key is a no-op success
//! (re-entrant).
//!
//! TODO(concurrency): real blocking locks via std.Io.Mutex once the DB grows a
//! write mutex / background threads; for now contention is reported, not awaited.

const std = @import("std");

/// A registry of held per-key locks.  Owns the duped key bytes used as map keys
/// and frees them on `unlock`/`deinit`.
pub const LockManager = struct {
    gpa: std.mem.Allocator,
    /// user key -> owning transaction id.
    locks: std.StringHashMapUnmanaged(u64) = .empty,

    pub fn init(gpa: std.mem.Allocator) LockManager {
        return .{ .gpa = gpa };
    }

    /// Free every remaining lock entry (defensive — txns should `unlockAll`).
    pub fn deinit(self: *LockManager) void {
        var it = self.locks.iterator();
        while (it.next()) |e| self.gpa.free(e.key_ptr.*);
        self.locks.deinit(self.gpa);
        self.* = undefined;
    }

    /// Try to acquire the exclusive lock on `key` for `txn_id`.
    ///   * returns true  if the key was free (now owned by `txn_id`) or already
    ///     owned by `txn_id` (re-entrant);
    ///   * returns false if another txn holds it (the caller raises error.Busy).
    pub fn tryLock(self: *LockManager, key: []const u8, txn_id: u64) !bool {
        if (self.locks.get(key)) |owner| {
            return owner == txn_id; // re-entrant ok; held by another -> false.
        }
        const owned = try self.gpa.dupe(u8, key);
        errdefer self.gpa.free(owned);
        try self.locks.put(self.gpa, owned, txn_id);
        return true;
    }

    /// Release `key` if (and only if) it is held by `txn_id`.  No-op otherwise.
    pub fn unlock(self: *LockManager, key: []const u8, txn_id: u64) void {
        if (self.locks.getEntry(key)) |e| {
            if (e.value_ptr.* != txn_id) return;
            const owned_key = e.key_ptr.*;
            _ = self.locks.remove(key);
            self.gpa.free(owned_key);
        }
    }

    /// Release every lock held by `txn_id` (called at commit/rollback).
    pub fn unlockAll(self: *LockManager, txn_id: u64) void {
        // Collect the keys to free first; mutating the map mid-iteration is unsafe.
        var to_free: std.ArrayListUnmanaged([]const u8) = .empty;
        defer to_free.deinit(self.gpa);

        var it = self.locks.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.* == txn_id) to_free.append(self.gpa, e.key_ptr.*) catch {
                // On OOM while collecting, fall back to a best-effort sweep below.
                break;
            };
        }
        for (to_free.items) |k| {
            _ = self.locks.remove(k);
            self.gpa.free(k);
        }
    }

    /// True if `key` is currently locked by ANY transaction (test helper).
    pub fn isLocked(self: *const LockManager, key: []const u8) bool {
        return self.locks.contains(key);
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "LockManager: tryLock grants a free key, blocks another txn, is re-entrant" {
    var lm = LockManager.init(testing.allocator);
    defer lm.deinit();

    try testing.expect(try lm.tryLock("k", 1)); // free -> granted to txn 1
    try testing.expect(try lm.tryLock("k", 1)); // re-entrant for txn 1
    try testing.expect(!(try lm.tryLock("k", 2))); // held by txn 1 -> blocked

    lm.unlock("k", 1);
    try testing.expect(try lm.tryLock("k", 2)); // now free for txn 2
    lm.unlockAll(2);
    try testing.expect(!lm.isLocked("k"));
}

test "LockManager: unlockAll releases all of a txn's keys, leaves others" {
    var lm = LockManager.init(testing.allocator);
    defer lm.deinit();

    try testing.expect(try lm.tryLock("a", 1));
    try testing.expect(try lm.tryLock("b", 1));
    try testing.expect(try lm.tryLock("c", 2));

    lm.unlockAll(1);
    try testing.expect(!lm.isLocked("a"));
    try testing.expect(!lm.isLocked("b"));
    try testing.expect(lm.isLocked("c")); // txn 2's lock untouched
    lm.unlockAll(2);
}

test "LockManager: unlock by a non-owner is a no-op" {
    var lm = LockManager.init(testing.allocator);
    defer lm.deinit();

    try testing.expect(try lm.tryLock("k", 1));
    lm.unlock("k", 2); // txn 2 doesn't own it
    try testing.expect(lm.isLocked("k"));
    lm.unlock("k", 1);
    try testing.expect(!lm.isLocked("k"));
}
