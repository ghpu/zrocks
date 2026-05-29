/// cache.zig — sharded LRU block cache.
///
/// Modelled on LevelDB's `util/cache.cc` `ShardedLRUCache`, rewritten in
/// idiomatic Zig 0.16. Keys are arbitrary `[]const u8` (duped internally);
/// values are owned `[]u8` byte blobs (e.g. decoded SST block contents) that
/// the cache frees on eviction/erase/deinit. Ref-counted `Handle`s pin entries
/// against eviction; the caller must `release` every handle returned by
/// `insert`/`lookup`.
///
/// The cache is split into N shards keyed by a hash of the key, to spread
/// future lock contention and reduce per-structure size. Each shard owns a
/// hash table (key -> *Entry) plus an intrusive LRU doubly-linked list of the
/// entries that are *cached but unreferenced* (i.e. evictable). Entries that
/// are pinned by an outstanding handle are removed from the LRU list, so the
/// eviction sweep can simply walk the LRU tail and free the oldest unpinned
/// entries until `usage <= capacity`.
///
/// Ref-counting (LevelDB semantics): an entry resident in the hash table holds
/// one "in-cache" reference; each outstanding handle adds one more. When the
/// count reaches zero the entry's storage (key dup + value blob + node) is
/// freed. A freshly inserted entry therefore starts at refs == 2 (one for the
/// table, one for the returned handle).
///
/// CONCURRENCY (D2b-3): each shard carries its own `std.Io.Mutex`, taken around
/// every operation that touches that shard's hash table / LRU list / usage
/// tally. Because the cache shards by key hash, two operations on keys in
/// different shards proceed in parallel — the lock is fine-grained, not global.
///
/// io-vs-VTable decision (resolved here): the cache stores a `std.Io` capability
/// at CONSTRUCTION (`Cache.init(gpa, io, capacity)`), mirroring `TableCache`.
/// The lock/unlock are pure in-memory critical-section guards; no file I/O ever
/// happens under the lock. The real call sites — `table_reader.readBlockCached`
/// and the SST table-iterator VTable — do NOT need an `io` of their own: the
/// `Cache` they consult already carries one. Threading `io` through the generic
/// `Iterator` VTable was rejected: it would force an `io` slot onto every
/// iterator implementation (Vector/Merging/TwoLevel/memtable) that holds no
/// cache at all, a far more invasive, capability-polluting change. The cache's
/// infallible API (`lookup`/`release`/`value`/`erase`) uses `lockUncancelable`
/// so those signatures stay error-free; the critical sections are tiny.
const std = @import("std");

/// Number of shards (power of two so the shard index is a cheap mask).
const kNumShards: usize = 16;
const kShardMask: u64 = kNumShards - 1;

/// One cache entry. Lives on the heap; owns its key dup and value blob.
const Entry = struct {
    /// Owned key copy (duped on insert, freed when the entry is freed).
    key: []u8,
    /// Owned value blob handed to the cache on insert.
    val: []u8,
    /// Caller-supplied charge counted against the shard/cache capacity.
    charge: usize,
    /// Total references = 1 (in-cache) + number of outstanding handles.
    refs: u32,
    /// True while the entry is still present in the shard's hash table.
    in_cache: bool,
    /// Owning shard (used by `release`, which only has the handle).
    shard: *Shard,
    /// Intrusive LRU links. Only meaningful while the entry is on the LRU
    /// list (in_cache and refs == 1); null otherwise.
    prev: ?*Entry,
    next: ?*Entry,
};

/// A single shard: a hash table plus an intrusive LRU list and a usage tally.
const Shard = struct {
    gpa: std.mem.Allocator,
    /// The `std.Io` capability used to lock/unlock this shard's mutex (the SAME
    /// one handed to `Cache.init`; never an ambient/global io).
    io: std.Io,
    /// Guards EVERY field below (table / lru_head / lru_tail / usage) and the
    /// refcount mutations on this shard's entries. Held only for the duration
    /// of a single cache op — no file I/O is ever performed under it.
    mutex: std.Io.Mutex,
    capacity: usize,
    usage: usize,
    /// key -> entry. The map's own key memory is the Entry's owned key dup,
    /// so we store keys as `[]const u8` slices aliasing `Entry.key`.
    table: std.StringHashMapUnmanaged(*Entry),
    /// LRU list of evictable (cached, unreferenced) entries.
    /// `lru_head` is the MRU end, `lru_tail` is the LRU end.
    lru_head: ?*Entry,
    lru_tail: ?*Entry,

    fn init(gpa: std.mem.Allocator, io: std.Io, capacity: usize) Shard {
        return .{
            .gpa = gpa,
            .io = io,
            .mutex = .init,
            .capacity = capacity,
            .usage = 0,
            .table = .empty,
            .lru_head = null,
            .lru_tail = null,
        };
    }

    /// Free every entry the shard still owns. Outstanding handles are tolerated:
    /// we drop the in-cache ref on all entries and free their storage anyway
    /// (deinit is the cache's terminal operation).
    fn deinit(self: *Shard) void {
        var it = self.table.iterator();
        while (it.next()) |kv| {
            const e = kv.value_ptr.*;
            self.freeEntry(e);
        }
        self.table.deinit(self.gpa);
        self.* = undefined;
    }

    /// Free an entry's storage (key dup, value blob, node). Does not touch the
    /// hash table or LRU list.
    fn freeEntry(self: *Shard, e: *Entry) void {
        self.gpa.free(e.key);
        self.gpa.free(e.val);
        self.gpa.destroy(e);
    }

    // -- LRU list helpers (head = MRU, tail = LRU) -----------------------

    fn lruRemove(self: *Shard, e: *Entry) void {
        if (e.prev) |p| p.next = e.next else self.lru_head = e.next;
        if (e.next) |n| n.prev = e.prev else self.lru_tail = e.prev;
        e.prev = null;
        e.next = null;
    }

    /// Push `e` to the MRU (head) end of the LRU list.
    fn lruPushFront(self: *Shard, e: *Entry) void {
        e.prev = null;
        e.next = self.lru_head;
        if (self.lru_head) |h| h.prev = e else self.lru_tail = e;
        self.lru_head = e;
    }

    /// Drop one reference. When it reaches zero the entry is freed. When it
    /// drops to exactly the in-cache ref (1) while still cached, the entry
    /// becomes evictable and is (re)inserted at the MRU end of the LRU list.
    fn unref(self: *Shard, e: *Entry) void {
        std.debug.assert(e.refs > 0);
        e.refs -= 1;
        if (e.refs == 0) {
            // Last reference gone: the entry must already be out of the table.
            std.debug.assert(!e.in_cache);
            self.freeEntry(e);
        } else if (e.in_cache and e.refs == 1) {
            // No external handles left: becomes evictable -> MRU of LRU list.
            self.lruPushFront(e);
        }
    }

    /// Evict unpinned entries from the LRU tail until usage <= capacity.
    fn evictToCapacity(self: *Shard) void {
        while (self.usage > self.capacity) {
            const victim = self.lru_tail orelse break; // all remaining pinned
            self.removeFromCache(victim);
        }
    }

    /// Finish detaching an entry that is ALREADY removed from the hash table:
    /// unlink it from the LRU list (if present), drop the shard usage, clear
    /// `in_cache`, and drop the in-cache reference. The storage is freed once
    /// the last outstanding handle (if any) is released.
    fn detach(self: *Shard, e: *Entry) void {
        std.debug.assert(e.in_cache);
        // On the LRU list only when evictable (refs == 1).
        if (e.refs == 1) self.lruRemove(e);
        self.usage -= e.charge;
        e.in_cache = false;
        self.unref(e); // drop the in-cache reference
    }

    /// Remove `e` from the hash table and detach it.
    fn removeFromCache(self: *Shard, e: *Entry) void {
        _ = self.table.remove(e.key);
        self.detach(e);
    }

    fn insert(self: *Shard, key: []const u8, val: []u8, charge: usize) !*Entry {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        const e = try self.gpa.create(Entry);
        errdefer self.gpa.destroy(e);
        const key_dup = try self.gpa.dupe(u8, key);
        errdefer self.gpa.free(key_dup);

        e.* = .{
            .key = key_dup,
            .val = val, // cache takes ownership
            .charge = charge,
            .refs = 2, // one for the cache, one for the returned handle
            .in_cache = true,
            .shard = self,
            .prev = null,
            .next = null,
        };

        // If an entry already exists under this key, evict it first so the new
        // value wins (LevelDB does the same).
        if (self.table.fetchRemove(key)) |old| self.detach(old.value);

        try self.table.put(self.gpa, key_dup, e);
        self.usage += charge;

        self.evictToCapacity();
        return e;
    }

    fn lookup(self: *Shard, key: []const u8) ?*Entry {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        const e = self.table.get(key) orelse return null;
        // Pin: if it was evictable (refs == 1), take it off the LRU list.
        if (e.refs == 1) self.lruRemove(e);
        e.refs += 1;
        return e;
    }

    fn release(self: *Shard, e: *Entry) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        self.unref(e);
    }

    fn erase(self: *Shard, key: []const u8) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.table.get(key)) |e| {
            self.removeFromCache(e);
        }
    }
};

pub const Cache = struct {
    /// Opaque to callers; internally an `*Entry`.
    pub const Handle = opaque {};

    gpa: std.mem.Allocator,
    /// The `std.Io` capability shared by every shard's mutex (see the module
    /// doc comment's io-vs-VTable decision). Caller-supplied at construction.
    io: std.Io,
    shards: [kNumShards]Shard,
    /// Observability counters (cumulative across shards). Atomic so concurrent
    /// `lookup`s on different shards bump them race-free; read with `hitCount`
    /// / `missCount`.
    hits: std.atomic.Value(usize) = .init(0),
    misses: std.atomic.Value(usize) = .init(0),

    pub fn init(gpa: std.mem.Allocator, io: std.Io, capacity: usize) Cache {
        var self = Cache{
            .gpa = gpa,
            .io = io,
            .shards = undefined,
        };
        // Distribute capacity across shards, rounding up so the sum is >=
        // the requested capacity (matches LevelDB's per-shard split).
        const per_shard = (capacity + kNumShards - 1) / kNumShards;
        for (&self.shards) |*s| s.* = Shard.init(gpa, io, per_shard);
        return self;
    }

    pub fn deinit(self: *Cache) void {
        for (&self.shards) |*s| s.deinit();
        self.* = undefined;
    }

    /// Map a key to its shard via a stable, internal-only hash.
    fn shardFor(self: *Cache, key: []const u8) *Shard {
        const h = std.hash.Wyhash.hash(0, key);
        return &self.shards[@intCast(h & kShardMask)];
    }

    /// Insert `key` -> `val` (charge `charge`). The cache duplicates the key and
    /// TAKES OWNERSHIP of `val` (freed on eviction/erase/deinit). Returns a
    /// pinned handle (ref held); the caller must `release` it.
    pub fn insert(self: *Cache, key: []const u8, val: []u8, charge: usize) !*Handle {
        const e = try self.shardFor(key).insert(key, val, charge);
        return @ptrCast(e);
    }

    /// Look up `key`. On hit returns a pinned handle (ref held; the entry is
    /// promoted toward MRU) and bumps `hits`; on miss returns null and bumps
    /// `misses`. The caller must `release` any returned handle.
    pub fn lookup(self: *Cache, key: []const u8) ?*Handle {
        if (self.shardFor(key).lookup(key)) |e| {
            _ = self.hits.fetchAdd(1, .monotonic);
            return @ptrCast(e);
        }
        _ = self.misses.fetchAdd(1, .monotonic);
        return null;
    }

    /// Cumulative cache hits observed by `lookup`.
    pub fn hitCount(self: *const Cache) usize {
        return self.hits.load(.monotonic);
    }

    /// Cumulative cache misses observed by `lookup`.
    pub fn missCount(self: *const Cache) usize {
        return self.misses.load(.monotonic);
    }

    /// The stored bytes for `handle` (valid until the handle is released).
    pub fn value(self: *Cache, handle: *Handle) []u8 {
        _ = self;
        const e: *Entry = @ptrCast(@alignCast(handle));
        return e.val;
    }

    /// Drop the caller's reference to `handle`. If this was the last reference
    /// to an already-evicted entry, its storage is freed here.
    pub fn release(self: *Cache, handle: *Handle) void {
        _ = self;
        const e: *Entry = @ptrCast(@alignCast(handle));
        e.shard.release(e);
    }

    /// Remove `key` from the cache (frees it once no handles remain).
    pub fn erase(self: *Cache, key: []const u8) void {
        self.shardFor(key).erase(key);
    }

    /// Total live charge across all shards. Best-effort snapshot: it reads each
    /// shard's `usage` WITHOUT taking the shard lock (an observability helper),
    /// so under concurrent mutation the sum is approximate. Callers wanting an
    /// exact figure must quiesce the cache first (e.g. join all workers).
    pub fn totalCharge(self: *const Cache) usize {
        var total: usize = 0;
        for (&self.shards) |*s| total += s.usage;
        return total;
    }
};

// ===========================================================================
// Tests — Part A
// ===========================================================================

const testing = std.testing;

/// Heap-dup a string literal into a mutable owned `[]u8` for cache insertion
/// (the cache takes ownership of the value blob).
fn ownVal(gpa: std.mem.Allocator, s: []const u8) ![]u8 {
    return gpa.dupe(u8, s);
}

/// The LRU-ordering / eviction tests need a single global LRU order, but the
/// cache shards by key hash. These helpers confine such tests to ONE shard so
/// the ordering is deterministic while still exercising real sharded code.
/// Which shard a key maps to (mirrors `Cache.shardFor`).
fn shardOf(key: []const u8) usize {
    return @intCast(std.hash.Wyhash.hash(0, key) & kShardMask);
}

/// A global capacity that gives the target shard a per-shard budget of
/// `per_shard` (the cache splits capacity evenly across `kNumShards`).
fn capacityForPerShard(per_shard: usize) usize {
    return per_shard * kNumShards;
}

/// Fill `out` with `out.len` distinct keys (written into the per-key buffers in
/// `bufs`) that all map to shard 0, in generation order. Each key is "s<id>".
fn sameShardKeys(bufs: [][16]u8, out: [][]const u8) void {
    std.debug.assert(bufs.len == out.len);
    var found: usize = 0;
    var id: usize = 0;
    while (found < out.len) : (id += 1) {
        const k = std.fmt.bufPrint(&bufs[found], "s{d}", .{id}) catch unreachable;
        if (shardOf(k) == 0) {
            out[found] = k;
            found += 1;
        }
    }
}

test "cache: insert + lookup hit returns the value; absent -> null; counters" {
    const gpa = testing.allocator;
    var c = Cache.init(gpa, testIo(), 1024);
    defer c.deinit();

    const h = try c.insert("k1", try ownVal(gpa, "v1"), 2);
    c.release(h);

    const got = c.lookup("k1");
    try testing.expect(got != null);
    try testing.expectEqualStrings("v1", c.value(got.?));
    c.release(got.?);

    try testing.expect(c.lookup("absent") == null);

    try testing.expectEqual(@as(usize, 1), c.hitCount());
    try testing.expectEqual(@as(usize, 1), c.missCount());
}

test "cache: eviction of LRU unpinned entries keeps usage <= capacity" {
    const gpa = testing.allocator;
    // Target shard budget 30; each entry charge 10 => at most 3 live in it.
    var c = Cache.init(gpa, testIo(), capacityForPerShard(30));
    defer c.deinit();

    var bufs: [6][16]u8 = undefined;
    var keys: [6][]const u8 = undefined;
    sameShardKeys(&bufs, &keys);

    for (keys) |k| {
        const h = try c.insert(k, try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }

    // Oldest three must be gone; newest three remain.
    inline for (.{ 0, 1, 2 }) |idx| {
        try testing.expect(c.lookup(keys[idx]) == null);
    }
    inline for (.{ 3, 4, 5 }) |idx| {
        const h = c.lookup(keys[idx]);
        try testing.expect(h != null);
        c.release(h.?);
    }
    try testing.expect(c.totalCharge() <= capacityForPerShard(30));
    try testing.expectEqual(@as(usize, 30), c.totalCharge());
}

test "cache: pinned entry is not evicted until released" {
    const gpa = testing.allocator;
    var c = Cache.init(gpa, testIo(), capacityForPerShard(30));
    defer c.deinit();

    var bufs: [14][16]u8 = undefined;
    var keys: [14][]const u8 = undefined;
    sameShardKeys(&bufs, &keys);

    // Pin keys[0] by NOT releasing its handle.
    const pinned = try c.insert(keys[0], try ownVal(gpa, "PINNEDPINN"), 10);

    var i: usize = 1;
    while (i < 8) : (i += 1) {
        const h = try c.insert(keys[i], try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }

    // The pinned handle's value is still valid even though the shard overflowed
    // (a pinned entry is never evicted), and it is still looked-up-able.
    try testing.expectEqualStrings("PINNEDPINN", c.value(pinned));
    if (c.lookup(keys[0])) |h| c.release(h) else try testing.expect(false);

    // Now release it; it becomes evictable. Insert more to force eviction.
    c.release(pinned);
    i = 8;
    while (i < 14) : (i += 1) {
        const h = try c.insert(keys[i], try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }
    try testing.expect(c.lookup(keys[0]) == null);
    try testing.expect(c.totalCharge() <= capacityForPerShard(30));
}

test "cache: erase removes and frees an entry" {
    const gpa = testing.allocator;
    var c = Cache.init(gpa, testIo(), 1024);
    defer c.deinit();

    const h = try c.insert("gone", try ownVal(gpa, "data"), 4);
    c.release(h);
    // Confirm present, releasing the lookup handle.
    if (c.lookup("gone")) |hh| c.release(hh) else try testing.expect(false);

    c.erase("gone");
    try testing.expect(c.lookup("gone") == null);
    try testing.expectEqual(@as(usize, 0), c.totalCharge());
}

test "cache: lookup promotes to MRU and survives eviction" {
    const gpa = testing.allocator;
    var c = Cache.init(gpa, testIo(), capacityForPerShard(30)); // 3 entries of charge 10
    defer c.deinit();

    var bufs: [4][16]u8 = undefined;
    var keys: [4][]const u8 = undefined;
    sameShardKeys(&bufs, &keys);
    const a = keys[0];
    const b = keys[1];
    const cc = keys[2];
    const d = keys[3];

    inline for (.{ 0, 1, 2 }) |idx| {
        const h = try c.insert(keys[idx], try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }
    // Touch `a` so it becomes MRU; `b` is now the LRU.
    if (c.lookup(a)) |h| c.release(h) else try testing.expect(false);

    // Insert `d`: should evict `b` (LRU), not `a`.
    {
        const h = try c.insert(d, try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }
    try testing.expect(c.lookup(b) == null);
    if (c.lookup(a)) |h| c.release(h) else try testing.expect(false);
    if (c.lookup(cc)) |h| c.release(h) else try testing.expect(false);
    if (c.lookup(d)) |h| c.release(h) else try testing.expect(false);
}

test "cache: totalCharge equals sum of live entry charges" {
    const gpa = testing.allocator;
    var c = Cache.init(gpa, testIo(), 10_000);
    defer c.deinit();

    var expected: usize = 0;
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        var kb: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kb, "key{d}", .{i});
        const charge = 3 + i;
        const h = try c.insert(k, try ownVal(gpa, "x"), charge);
        c.release(h);
        expected += charge;
    }
    try testing.expectEqual(expected, c.totalCharge());
}

test "cache: zero leaks with outstanding handle at deinit" {
    const gpa = testing.allocator;
    var c = Cache.init(gpa, testIo(), 1024);

    // Insert and keep a handle outstanding; deinit must free gracefully.
    const h = try c.insert("held", try ownVal(gpa, "value"), 5);
    _ = h; // intentionally not released before deinit
    c.deinit();
}

// ===========================================================================
// Tests — Part B: per-shard mutex (D2b-3)
//
// The cache now carries a `std.Io` capability and a per-shard `std.Io.Mutex`,
// so concurrent readers/writers spread across the threaded io are race-free.
// These tests hammer the cache from several fibers (real threads under the
// Threaded io) and assert the structure stays consistent.
// ===========================================================================

const builtin = @import("builtin");

fn testIo() std.Io {
    return std.testing.io;
}

/// A worker that repeatedly inserts/looks-up/releases against a SHARED cache,
/// exercising the per-shard locks under genuine concurrency.
const HammerWorker = struct {
    cache: *Cache,
    gpa: std.mem.Allocator,
    base: usize,
    iters: usize,
    ok: bool = false,

    fn run(self: *HammerWorker) void {
        var buf: [32]u8 = undefined;
        var i: usize = 0;
        while (i < self.iters) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{d}-{d}", .{ self.base, i }) catch unreachable;
            const v = self.gpa.dupe(u8, k) catch unreachable;
            const h = self.cache.insert(k, v, k.len) catch unreachable;
            self.cache.release(h);

            if (self.cache.lookup(k)) |lh| {
                self.cache.release(lh);
            }
            self.cache.erase(k);
        }
        self.ok = true;
    }
};

test "cache: concurrent insert/lookup/erase across fibers is race-free" {
    if (!builtin.is_test) return;
    const gpa = testing.allocator;
    const io = testIo();

    // Modest capacity so eviction also runs under contention.
    var c = Cache.init(gpa, io, 4096);
    defer c.deinit();

    var w0 = HammerWorker{ .cache = &c, .gpa = gpa, .base = 0, .iters = 200 };
    var w1 = HammerWorker{ .cache = &c, .gpa = gpa, .base = 1, .iters = 200 };
    var w2 = HammerWorker{ .cache = &c, .gpa = gpa, .base = 2, .iters = 200 };

    var f0 = try std.Io.concurrent(io, HammerWorker.run, .{&w0});
    var f1 = try std.Io.concurrent(io, HammerWorker.run, .{&w1});
    var f2 = try std.Io.concurrent(io, HammerWorker.run, .{&w2});
    f0.await(io);
    f1.await(io);
    f2.await(io);

    try testing.expect(w0.ok and w1.ok and w2.ok);
    // Every key was erased by its worker, so the cache is empty again.
    try testing.expectEqual(@as(usize, 0), c.totalCharge());
}
