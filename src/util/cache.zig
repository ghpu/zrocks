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
// TODO(concurrency): per-shard std.Io.Mutex — single-threaded for now, so no
// locking is taken. The sharding structure exists to make adding per-shard
// locks (and reducing contention) a localised change later.
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
    capacity: usize,
    usage: usize,
    /// key -> entry. The map's own key memory is the Entry's owned key dup,
    /// so we store keys as `[]const u8` slices aliasing `Entry.key`.
    table: std.StringHashMapUnmanaged(*Entry),
    /// LRU list of evictable (cached, unreferenced) entries.
    /// `lru_head` is the MRU end, `lru_tail` is the LRU end.
    lru_head: ?*Entry,
    lru_tail: ?*Entry,

    fn init(gpa: std.mem.Allocator, capacity: usize) Shard {
        return .{
            .gpa = gpa,
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

    /// Detach `e` from the hash table and LRU list, drop usage and the in-cache
    /// reference. The storage is freed once the last handle (if any) is gone.
    fn removeFromCache(self: *Shard, e: *Entry) void {
        std.debug.assert(e.in_cache);
        // Remove from LRU list only if it is currently on it (refs == 1).
        if (e.refs == 1) self.lruRemove(e);
        _ = self.table.remove(e.key);
        self.usage -= e.charge;
        e.in_cache = false;
        self.unref(e); // drop the in-cache reference
    }

    fn insert(self: *Shard, key: []const u8, val: []u8, charge: usize) !*Entry {
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
        if (self.table.fetchRemove(key)) |old| {
            const old_e = old.value;
            if (old_e.refs == 1) self.lruRemove(old_e);
            self.usage -= old_e.charge;
            old_e.in_cache = false;
            self.unref(old_e);
        }

        try self.table.put(self.gpa, key_dup, e);
        self.usage += charge;

        self.evictToCapacity();
        return e;
    }

    fn lookup(self: *Shard, key: []const u8) ?*Entry {
        const e = self.table.get(key) orelse return null;
        // Pin: if it was evictable (refs == 1), take it off the LRU list.
        if (e.refs == 1) self.lruRemove(e);
        e.refs += 1;
        return e;
    }

    fn release(self: *Shard, e: *Entry) void {
        self.unref(e);
    }

    fn erase(self: *Shard, key: []const u8) void {
        if (self.table.get(key)) |e| {
            self.removeFromCache(e);
        }
    }
};

pub const Cache = struct {
    /// Opaque to callers; internally an `*Entry`.
    pub const Handle = opaque {};

    gpa: std.mem.Allocator,
    shards: [kNumShards]Shard,
    /// Observability counters (cumulative across shards).
    hits: usize = 0,
    misses: usize = 0,

    pub fn init(gpa: std.mem.Allocator, capacity: usize) Cache {
        var self = Cache{
            .gpa = gpa,
            .shards = undefined,
        };
        // Distribute capacity across shards, rounding up so the sum is >=
        // the requested capacity (matches LevelDB's per-shard split).
        const per_shard = (capacity + kNumShards - 1) / kNumShards;
        for (&self.shards) |*s| s.* = Shard.init(gpa, per_shard);
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
            self.hits += 1;
            return @ptrCast(e);
        }
        self.misses += 1;
        return null;
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

    /// Total live charge across all shards.
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
    var c = Cache.init(gpa, 1024);
    defer c.deinit();

    const h = try c.insert("k1", try ownVal(gpa, "v1"), 2);
    c.release(h);

    const got = c.lookup("k1");
    try testing.expect(got != null);
    try testing.expectEqualStrings("v1", c.value(got.?));
    c.release(got.?);

    try testing.expect(c.lookup("absent") == null);

    try testing.expectEqual(@as(usize, 1), c.hits);
    try testing.expectEqual(@as(usize, 1), c.misses);
}

test "cache: eviction of LRU unpinned entries keeps usage <= capacity" {
    const gpa = testing.allocator;
    // Target shard budget 30; each entry charge 10 => at most 3 live in it.
    var c = Cache.init(gpa, capacityForPerShard(30));
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
    var c = Cache.init(gpa, capacityForPerShard(30));
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
    var c = Cache.init(gpa, 1024);
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
    var c = Cache.init(gpa, capacityForPerShard(30)); // 3 entries of charge 10
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
    var c = Cache.init(gpa, 10_000);
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
    var c = Cache.init(gpa, 1024);

    // Insert and keep a handle outstanding; deinit must free gracefully.
    const h = try c.insert("held", try ownVal(gpa, "value"), 5);
    _ = h; // intentionally not released before deinit
    c.deinit();
}
