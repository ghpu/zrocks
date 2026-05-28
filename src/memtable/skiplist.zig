//! Concurrent ordered map backing the MemTable — a LevelDB-style skiplist.
//!
//! Keys are arbitrary `[]const u8`, ordered by an injected `Comparator`.  On
//! insert the key bytes are COPIED into an `Arena`; the skiplist owns them and
//! frees nothing per-node (the whole arena is freed at once).
//!
//! Concurrency model (single writer / many concurrent readers, LevelDB-style):
//! `next` pointers are `std.atomic.Value(?*Node)`.  Readers load with `.acquire`
//! and the lone writer publishes with `.release`, so a reader that observes a
//! spliced-in node is guaranteed to also observe that node's fully-written key
//! and `next` array.  (Tests here are single-threaded; the atomics encode the
//! design invariant.)

// STUB for the RED phase — intentionally unimplemented so the spec tests fail
// for the right reason.

const std = @import("std");
const arena = @import("../util/arena.zig");
const comparator = @import("../util/comparator.zig");

pub const SkipList = struct {
    pub const kMaxHeight = 12;

    pub fn init(a: *arena.Arena, cmp: comparator.Comparator, seed: u64) SkipList {
        _ = a;
        _ = cmp;
        _ = seed;
        @panic("unimplemented");
    }

    pub fn insert(self: *SkipList, key: []const u8) !void {
        _ = self;
        _ = key;
        @panic("unimplemented");
    }

    pub fn contains(self: *const SkipList, key: []const u8) bool {
        _ = self;
        _ = key;
        @panic("unimplemented");
    }

    pub const Iterator = struct {
        pub fn init(list: *const SkipList) Iterator {
            _ = list;
            @panic("unimplemented");
        }
        pub fn valid(self: *const Iterator) bool {
            _ = self;
            @panic("unimplemented");
        }
        pub fn key(self: *const Iterator) []const u8 {
            _ = self;
            @panic("unimplemented");
        }
        pub fn next(self: *Iterator) void {
            _ = self;
            @panic("unimplemented");
        }
        pub fn prev(self: *Iterator) void {
            _ = self;
            @panic("unimplemented");
        }
        pub fn seekToFirst(self: *Iterator) void {
            _ = self;
            @panic("unimplemented");
        }
        pub fn seekToLast(self: *Iterator) void {
            _ = self;
            @panic("unimplemented");
        }
        pub fn seek(self: *Iterator, target: []const u8) void {
            _ = self;
            _ = target;
            @panic("unimplemented");
        }
    };
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

/// Collect the full iteration order (seekToFirst → next…) into an owned list.
fn collect(gpa: std.mem.Allocator, list: *const SkipList) !std.ArrayListUnmanaged([]const u8) {
    var out: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer out.deinit(gpa);
    var it = SkipList.Iterator.init(list);
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        try out.append(gpa, it.key());
    }
    return out;
}

test "insert out of order, iterate in sorted order" {
    var a = arena.Arena.init(testing.allocator);
    defer a.deinit();
    var list = SkipList.init(&a, comparator.bytewise, 0xC0FFEE);

    const keys = [_][]const u8{ "delta", "alpha", "charlie", "bravo", "echo" };
    for (keys) |k| try list.insert(k);

    var got = try collect(testing.allocator, &list);
    defer got.deinit(testing.allocator);

    const expected = [_][]const u8{ "alpha", "bravo", "charlie", "delta", "echo" };
    try testing.expectEqual(expected.len, got.items.len);
    for (expected, got.items) |e, g| try testing.expectEqualStrings(e, g);
}

test "contains: present and absent" {
    var a = arena.Arena.init(testing.allocator);
    defer a.deinit();
    var list = SkipList.init(&a, comparator.bytewise, 1);

    const keys = [_][]const u8{ "cat", "dog", "fish", "bird" };
    for (keys) |k| try list.insert(k);

    for (keys) |k| try testing.expect(list.contains(k));
    try testing.expect(!list.contains("aardvark"));
    try testing.expect(!list.contains("zebra"));
    try testing.expect(!list.contains("ca")); // prefix of "cat"
    try testing.expect(!list.contains("cats")); // "cat" is a prefix
    try testing.expect(!list.contains("")); // empty absent
}

test "seek: exact present, between keys, and past end" {
    var a = arena.Arena.init(testing.allocator);
    defer a.deinit();
    var list = SkipList.init(&a, comparator.bytewise, 7);

    // Use spaced keys so "between" cases are unambiguous.
    const keys = [_][]const u8{ "b", "d", "f", "h" };
    for (keys) |k| try list.insert(k);

    var it = SkipList.Iterator.init(&list);

    // Exact present.
    it.seek("d");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("d", it.key());

    // Between keys → first >= target.
    it.seek("c");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("d", it.key());

    it.seek("e");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("f", it.key());

    // Before first → first node.
    it.seek("a");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("b", it.key());

    // Exactly first.
    it.seek("b");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("b", it.key());

    // Past end → invalid.
    it.seek("z");
    try testing.expect(!it.valid());

    // Exactly last.
    it.seek("h");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("h", it.key());
}

test "seekToLast and reverse iteration via prev" {
    var a = arena.Arena.init(testing.allocator);
    defer a.deinit();
    var list = SkipList.init(&a, comparator.bytewise, 99);

    const keys = [_][]const u8{ "one", "two", "three", "four" };
    for (keys) |k| try list.insert(k);

    const sorted = [_][]const u8{ "four", "one", "three", "two" };

    var it = SkipList.Iterator.init(&list);
    it.seekToLast();
    try testing.expect(it.valid());
    try testing.expectEqualStrings("two", it.key());

    // Walk backwards through the whole list.
    var idx: usize = sorted.len;
    while (it.valid()) {
        idx -= 1;
        try testing.expectEqualStrings(sorted[idx], it.key());
        it.prev();
    }
    try testing.expectEqual(@as(usize, 0), idx);
    try testing.expect(!it.valid());
}

test "empty skiplist iteration is immediately invalid" {
    var a = arena.Arena.init(testing.allocator);
    defer a.deinit();
    var list = SkipList.init(&a, comparator.bytewise, 3);

    var it = SkipList.Iterator.init(&list);
    it.seekToFirst();
    try testing.expect(!it.valid());
    it.seekToLast();
    try testing.expect(!it.valid());
    it.seek("anything");
    try testing.expect(!it.valid());

    try testing.expect(!list.contains("x"));
}

test "randomized stress: 1000 distinct keys cross-checked against sorted reference" {
    const gpa = testing.allocator;

    var a = arena.Arena.init(gpa);
    defer a.deinit();
    var list = SkipList.init(&a, comparator.bytewise, 0xDEADBEEF);

    // Reference structures.
    var ref_keys: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (ref_keys.items) |k| gpa.free(k);
        ref_keys.deinit(gpa);
    }
    var present = std.StringHashMap(void).init(gpa);
    defer present.deinit();

    var prng = std.Random.DefaultPrng.init(0x1234_5678);
    const rnd = prng.random();

    const N = 1000;
    var inserted: usize = 0;
    while (inserted < N) {
        // Random length 1..16 byte key.
        const len = rnd.intRangeLessThan(usize, 1, 17);
        var buf: [16]u8 = undefined;
        for (buf[0..len]) |*b| b.* = rnd.int(u8);
        const slice = buf[0..len];

        // Skip duplicates — skiplist assumes distinct keys.
        if (present.contains(slice)) continue;

        const owned = try gpa.dupe(u8, slice);
        errdefer gpa.free(owned);
        try ref_keys.append(gpa, owned);
        try present.put(owned, {});
        try list.insert(slice);
        inserted += 1;
    }

    // 1) Iteration order matches sorted reference exactly.
    std.mem.sort([]u8, ref_keys.items, {}, struct {
        fn lt(_: void, x: []u8, y: []u8) bool {
            return std.mem.order(u8, x, y) == .lt;
        }
    }.lt);

    var got = try collect(gpa, &list);
    defer got.deinit(gpa);

    try testing.expectEqual(ref_keys.items.len, got.items.len);
    for (ref_keys.items, got.items) |e, g| try testing.expectEqualSlices(u8, e, g);

    // 2) contains() matches the reference for all present keys.
    for (ref_keys.items) |k| try testing.expect(list.contains(k));

    // 3) contains() matches for random probe keys (present-or-absent).
    var probe: usize = 0;
    while (probe < 2000) : (probe += 1) {
        const len = rnd.intRangeLessThan(usize, 1, 17);
        var buf: [16]u8 = undefined;
        for (buf[0..len]) |*b| b.* = rnd.int(u8);
        const slice = buf[0..len];
        try testing.expectEqual(present.contains(slice), list.contains(slice));
    }

    // 4) seek() lands on the first key >= target for random targets.
    probe = 0;
    while (probe < 500) : (probe += 1) {
        const len = rnd.intRangeLessThan(usize, 1, 17);
        var buf: [16]u8 = undefined;
        for (buf[0..len]) |*b| b.* = rnd.int(u8);
        const target = buf[0..len];

        // Reference: first sorted key >= target (lower_bound).
        var ref_idx: usize = ref_keys.items.len;
        for (ref_keys.items, 0..) |k, i| {
            if (std.mem.order(u8, k, target) != .lt) {
                ref_idx = i;
                break;
            }
        }

        var it = SkipList.Iterator.init(&list);
        it.seek(target);
        if (ref_idx == ref_keys.items.len) {
            try testing.expect(!it.valid());
        } else {
            try testing.expect(it.valid());
            try testing.expectEqualSlices(u8, ref_keys.items[ref_idx], it.key());
        }
    }
}
