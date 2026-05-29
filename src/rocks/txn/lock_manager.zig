//! lock_manager.zig — per-key exclusive locks for pessimistic transactions (M7.6,
//! blocking acquisition added in D2b2).
//!
//! Maps each locked user key to a heap-allocated `LockEntry` that records the id
//! of the transaction currently owning the key plus a `std.Io.Condition` other
//! transactions wait on.  Two acquisition modes are offered:
//!
//!   * `tryLock` — non-blocking.  Returns true if the key was free (now owned by
//!     `txn_id`) or already owned by `txn_id` (re-entrant); false if another txn
//!     holds it (the caller raises `error.Busy`).
//!   * `lock`    — BLOCKING (D2b2).  If another txn holds the key, the caller's
//!     fiber sleeps on the entry's condition (releasing the manager mutex) until
//!     the holder releases the key, then takes ownership and returns.
//!
//! HEAP-ALLOCATE the entry — DO NOT store a `std.Io.Condition` by value inside a
//! hashmap.  The condition's `epoch`/`state` words are the futex addresses a
//! waiting fiber sleeps on; a hashmap rehash MOVES its values, so a by-value
//! condition would have its futex address change out from under a sleeping
//! waiter (it would wake on a stale address or miss the wake entirely — unsound).
//! By boxing each entry behind a `*LockEntry`, the condition lives at a stable
//! address for the entry's whole lifetime regardless of map growth.
//!
//! Concurrency contract: a single `std.Io.Mutex` guards BOTH the `locks` map and
//! every `LockEntry`'s mutable fields (`owner`, `waiters`).  `Condition.wait`
//! atomically drops that mutex while sleeping and re-acquires it on wake, so the
//! owner/waiters bookkeeping is always observed under the lock.  The `io` is the
//! SAME capability the owning DB/Env uses — never an ambient/global io.

const std = @import("std");

/// One per-key lock: the owning txn id, a waiter count, and the condition that
/// blocked txns sleep on.  HEAP-ALLOCATED so its `cond` (a futex address) never
/// moves under a sleeping waiter when the map rehashes.
pub const LockEntry = struct {
    /// id of the transaction currently holding the key, or 0 if momentarily
    /// unheld but kept alive because `waiters > 0` (a waiter will claim it).
    owner: u64,
    /// number of transactions currently blocked in `lock` on this key.  The
    /// entry is freed only when it becomes unowned AND no one is waiting.
    waiters: u32 = 0,
    /// blocked txns sleep here; the releaser broadcasts on unlock.
    cond: std.Io.Condition = .init,
};

/// A registry of held per-key locks.  Owns the duped key bytes used as map keys
/// and the heap-allocated `LockEntry` values; frees both on `unlock`/`deinit`.
pub const LockManager = struct {
    gpa: std.mem.Allocator,
    /// Concurrency capability backing `mutex`/`cond` — the SAME `std.Io` the
    /// owning DB/Env uses (no ambient io).
    io: std.Io,
    /// Guards `locks` AND the fields of every `*LockEntry`.  `Condition.wait`
    /// drops/reacquires it around the sleep, so all bookkeeping is under it.
    mutex: std.Io.Mutex = .init,
    /// user key -> heap-allocated owning `LockEntry`.
    locks: std.StringHashMapUnmanaged(*LockEntry) = .empty,

    pub fn init(gpa: std.mem.Allocator, io: std.Io) LockManager {
        return .{ .gpa = gpa, .io = io };
    }

    /// Free every remaining lock entry (defensive — txns should `unlockAll`).
    pub fn deinit(self: *LockManager) void {
        var it = self.locks.iterator();
        while (it.next()) |e| {
            self.gpa.free(e.key_ptr.*);
            self.gpa.destroy(e.value_ptr.*);
        }
        self.locks.deinit(self.gpa);
        self.* = undefined;
    }

    /// Try to acquire the exclusive lock on `key` for `txn_id` WITHOUT blocking.
    ///   * returns true  if the key was free (now owned by `txn_id`) or already
    ///     owned by `txn_id` (re-entrant);
    ///   * returns false if another txn holds it (the caller raises error.Busy).
    pub fn tryLock(self: *LockManager, key: []const u8, txn_id: u64) !bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.locks.get(key)) |entry| {
            if (entry.owner == txn_id) return true; // re-entrant ok
            if (entry.owner == 0) {
                // Unheld but kept alive by waiters; claim it.
                entry.owner = txn_id;
                return true;
            }
            return false; // held by another -> blocked
        }
        try self.installFreshEntry(key, txn_id);
        return true;
    }

    /// BLOCKING acquire of the exclusive lock on `key` for `txn_id`.  If another
    /// txn holds the key, this fiber sleeps on the entry's condition (releasing
    /// the manager mutex) until the holder releases it, then takes ownership.
    /// Re-locking a key this txn already owns returns immediately (re-entrant).
    pub fn lock(self: *LockManager, key: []const u8, txn_id: u64) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.locks.get(key)) |entry| {
            if (entry.owner == txn_id) return; // re-entrant ok
            // Wait until the key is unowned, then claim it.  Count ourselves as a
            // waiter so the releaser keeps the entry alive across the sleep.
            entry.waiters += 1;
            defer entry.waiters -= 1;
            while (entry.owner != 0) {
                try entry.cond.wait(self.io, &self.mutex);
            }
            entry.owner = txn_id;
            return;
        }
        try self.installFreshEntry(key, txn_id);
    }

    /// Create a brand-new heap entry owned by `txn_id` and insert it.  Caller
    /// must hold `mutex`.
    fn installFreshEntry(self: *LockManager, key: []const u8, txn_id: u64) !void {
        const owned = try self.gpa.dupe(u8, key);
        errdefer self.gpa.free(owned);
        const entry = try self.gpa.create(LockEntry);
        errdefer self.gpa.destroy(entry);
        entry.* = .{ .owner = txn_id };
        try self.locks.put(self.gpa, owned, entry);
    }

    /// Release `key` if (and only if) it is held by `txn_id`.  No-op otherwise.
    /// Wakes any blocked waiters; frees the entry only when no waiter remains.
    pub fn unlock(self: *LockManager, key: []const u8, txn_id: u64) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.releaseLocked(key, txn_id);
    }

    /// Release the lock on `key` held by `txn_id`.  Caller must hold `mutex`.
    fn releaseLocked(self: *LockManager, key: []const u8, txn_id: u64) void {
        const e = self.locks.getEntry(key) orelse return;
        const entry = e.value_ptr.*;
        if (entry.owner != txn_id) return;

        if (entry.waiters > 0) {
            // Hand the key off: mark unheld and wake the waiters; the entry stays
            // resident (a waiter will claim it).  The futex address is stable
            // because the entry is heap-boxed.
            entry.owner = 0;
            entry.cond.broadcast(self.io);
            return;
        }
        // No waiters — remove and free the key + entry.
        const owned_key = e.key_ptr.*;
        _ = self.locks.remove(key);
        self.gpa.free(owned_key);
        self.gpa.destroy(entry);
    }

    /// Release every lock held by `txn_id` (called at commit/rollback).
    pub fn unlockAll(self: *LockManager, txn_id: u64) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        // Collect the keys to release first; mutating the map mid-iteration is
        // unsafe.  (We re-look-up each key in releaseLocked.)
        var to_release: std.ArrayListUnmanaged([]const u8) = .empty;
        defer to_release.deinit(self.gpa);

        var it = self.locks.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.*.owner == txn_id) to_release.append(self.gpa, e.key_ptr.*) catch {
                // On OOM while collecting, release what we gathered and bail; the
                // remaining locks are swept defensively at deinit.
                break;
            };
        }
        for (to_release.items) |k| self.releaseLocked(k, txn_id);
    }

    /// True if `key` is currently OWNED by some transaction (test helper).  An
    /// entry kept alive only by waiters (owner == 0) counts as unlocked.
    pub fn isLocked(self: *LockManager, key: []const u8) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const entry = self.locks.get(key) orelse return false;
        return entry.owner != 0;
    }

    /// Number of transactions currently blocked in `lock` on `key` (test helper:
    /// lets a test deterministically wait until a contending txn has actually
    /// parked on the condition before the holder releases).
    pub fn waiterCount(self: *LockManager, key: []const u8) u32 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const entry = self.locks.get(key) orelse return 0;
        return entry.waiters;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const builtin = @import("builtin");
const testing = std.testing;

fn testIo() std.Io {
    return std.testing.io;
}

test "LockManager: tryLock grants a free key, blocks another txn, is re-entrant" {
    var lm = LockManager.init(testing.allocator, testIo());
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
    var lm = LockManager.init(testing.allocator, testIo());
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
    var lm = LockManager.init(testing.allocator, testIo());
    defer lm.deinit();

    try testing.expect(try lm.tryLock("k", 1));
    lm.unlock("k", 2); // txn 2 doesn't own it
    try testing.expect(lm.isLocked("k"));
    lm.unlock("k", 1);
    try testing.expect(!lm.isLocked("k"));
}

test "LockManager: lock (non-contended) grants and is re-entrant" {
    var lm = LockManager.init(testing.allocator, testIo());
    defer lm.deinit();

    try lm.lock("k", 1); // free -> granted
    try lm.lock("k", 1); // re-entrant, returns immediately
    try testing.expect(lm.isLocked("k"));
    lm.unlock("k", 1);
    try testing.expect(!lm.isLocked("k"));
}

// A blocked `lock` proceeds only after the holder releases.  Two fibers (real
// threads under the Threaded io): the holder takes the key, the waiter blocks
// in `lock`, then the holder — once it observes the waiter parked on the
// condition — releases, and the waiter wakes and acquires.  The handoff is
// deterministic: the holder spins on `waiterCount("k") == 1` (no sleeps), and
// `released` is set true ONLY after the waiter is provably blocked, so the
// waiter's assertion `released == true` proves it really waited.
const BlockingScenario = struct {
    lm: *LockManager,
    io: std.Io,
    /// flipped true by the holder immediately before it unlocks, AFTER the
    /// waiter is confirmed parked.  The waiter reads it post-acquire.
    released: std.atomic.Value(bool) = .init(false),
    /// set by the holder once it has acquired the key (the waiter spins on this
    /// so it only attempts to lock while there is genuine contention).
    holder_has_key: std.atomic.Value(bool) = .init(false),
    waiter_acquired_after_release: bool = false,

    fn holder(self: *BlockingScenario) void {
        self.lm.lock("k", 1) catch unreachable;
        self.holder_has_key.store(true, .release);
        // Wait until the waiter has actually parked on the condition.
        while (self.lm.waiterCount("k") != 1) {}
        self.released.store(true, .release); // mark: we are about to release
        self.lm.unlock("k", 1);
    }

    fn waiter(self: *BlockingScenario) void {
        // Spin until the holder has taken the key, so we really contend.
        while (!self.holder_has_key.load(.acquire)) {}
        self.lm.lock("k", 2) catch unreachable; // blocks until holder releases
        // We only reach here after the holder set released=true.
        self.waiter_acquired_after_release = self.released.load(.acquire);
        self.lm.unlock("k", 2);
    }
};

test "LockManager: a blocked lock proceeds after the holder releases" {
    if (!builtin.is_test) return;
    const gpa = testing.allocator;
    const io = testIo();
    var lm = LockManager.init(gpa, io);
    defer lm.deinit();

    var sc = BlockingScenario{ .lm = &lm, .io = io };

    var hf = try std.Io.concurrent(io, BlockingScenario.holder, .{&sc});
    var wf = try std.Io.concurrent(io, BlockingScenario.waiter, .{&sc});
    hf.await(io);
    wf.await(io);

    try testing.expect(sc.waiter_acquired_after_release);
    try testing.expect(!lm.isLocked("k")); // both released
}
