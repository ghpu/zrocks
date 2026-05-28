/// cache.zig — sharded LRU block cache (RED stub).
///
/// TDD RED phase: tests reference the intended public API; the stub does not
/// implement the behaviour, so the assertions fail for the right reason.
const std = @import("std");

pub const Cache = struct {
    pub const Handle = opaque {};

    hits: usize = 0,
    misses: usize = 0,

    pub fn init(gpa: std.mem.Allocator, capacity: usize) Cache {
        _ = gpa;
        _ = capacity;
        return .{};
    }

    pub fn deinit(self: *Cache) void {
        _ = self;
    }

    pub fn insert(self: *Cache, key: []const u8, val: []u8, charge: usize) !*Handle {
        _ = self;
        _ = key;
        _ = val;
        _ = charge;
        return error.NotImplemented;
    }

    pub fn lookup(self: *Cache, key: []const u8) ?*Handle {
        _ = self;
        _ = key;
        return null;
    }

    pub fn value(self: *Cache, handle: *Handle) []u8 {
        _ = self;
        _ = handle;
        return &.{};
    }

    pub fn release(self: *Cache, handle: *Handle) void {
        _ = self;
        _ = handle;
    }

    pub fn erase(self: *Cache, key: []const u8) void {
        _ = self;
        _ = key;
    }

    pub fn totalCharge(self: *const Cache) usize {
        _ = self;
        return 0;
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
    // capacity 30; each entry charge 10 => at most 3 live.
    var c = Cache.init(gpa, 30);
    defer c.deinit();

    var i: usize = 0;
    while (i < 6) : (i += 1) {
        var kb: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kb, "k{d}", .{i});
        const h = try c.insert(k, try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }

    // Oldest (k0,k1,k2) must be gone; newest (k3,k4,k5) remain.
    try testing.expect(c.lookup("k0") == null);
    try testing.expect(c.lookup("k1") == null);
    try testing.expect(c.lookup("k2") == null);
    inline for (.{ "k3", "k4", "k5" }) |k| {
        const h = c.lookup(k);
        try testing.expect(h != null);
        c.release(h.?);
    }
    try testing.expect(c.totalCharge() <= 30);
}

test "cache: pinned entry is not evicted until released" {
    const gpa = testing.allocator;
    var c = Cache.init(gpa, 30);
    defer c.deinit();

    // Pin k0 by NOT releasing its handle.
    const pinned = try c.insert("k0", try ownVal(gpa, "PINNEDPINN"), 10);

    var i: usize = 1;
    while (i < 8) : (i += 1) {
        var kb: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kb, "k{d}", .{i});
        const h = try c.insert(k, try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }

    // The pinned handle's value is still valid even though capacity overflowed.
    try testing.expectEqualStrings("PINNEDPINN", c.value(pinned));

    // Now release it; it becomes evictable. Insert more to force eviction.
    c.release(pinned);
    i = 8;
    while (i < 14) : (i += 1) {
        var kb: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kb, "k{d}", .{i});
        const h = try c.insert(k, try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }
    try testing.expect(c.lookup("k0") == null);
    try testing.expect(c.totalCharge() <= 30);
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
    var c = Cache.init(gpa, 30); // 3 entries of charge 10
    defer c.deinit();

    inline for (.{ "a", "b", "c" }) |k| {
        const h = try c.insert(k, try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }
    // Touch "a" so it becomes MRU; "b" is now the LRU.
    if (c.lookup("a")) |h| c.release(h);

    // Insert "d": should evict "b" (LRU), not "a".
    {
        const h = try c.insert("d", try ownVal(gpa, "0123456789"), 10);
        c.release(h);
    }
    try testing.expect(c.lookup("b") == null);
    if (c.lookup("a")) |h| c.release(h) else try testing.expect(false);
    if (c.lookup("d")) |h| c.release(h) else try testing.expect(false);
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
