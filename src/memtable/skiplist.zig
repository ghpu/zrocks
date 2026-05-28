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
//!
//! Standalone test note (Zig 0.16): this file uses `../util/...` imports, which
//! resolve when it is compiled as part of the `src`-rooted `zrocks` module
//! (`zig build test`).  A bare `zig test src/memtable/skiplist.zig` roots the
//! module at `src/memtable/`, so `../util/` falls "outside module path" and is
//! rejected.  To run the suite standalone, root the module at `src/`, e.g.:
//!   printf 'test { _ = @import("memtable/skiplist.zig"); }' > src/_t.zig \
//!     && zig test src/_t.zig && rm src/_t.zig

const std = @import("std");
const arena = @import("../util/arena.zig");
const comparator = @import("../util/comparator.zig");

/// One next-pointer slot.  `?*Node` (a single pointer / usize) is naturally
/// lock-free on every target we care about.
const AtomicLink = std.atomic.Value(?*Node);

/// A skiplist node.
///
/// # Arena memory layout (single allocation per node)
/// Each node is one arena allocation laid out as:
///
///     [ Node header ][ next[0] ][ next[1] ] … [ next[height-1] ]
///
/// i.e. a `Node` struct immediately followed, in the same arena block, by a
/// `height`-element flexible array of `AtomicLink`.  We size the allocation as
///   @sizeOf(Node) + height * @sizeOf(AtomicLink)
/// aligned to `@max(@alignOf(Node), @alignOf(AtomicLink))`, then reach the
/// inline array via `next_base()` (pointer arithmetic past the header).  The
/// key bytes are a SEPARATE arena allocation (copied in on insert); the node
/// stores that owned slice.  Nothing is ever individually freed — the whole
/// arena is released at once.
const Node = struct {
    /// Owned (arena-copied) key bytes.
    key: []const u8,
    /// Node height; the inline `next[]` array has exactly this many slots.
    height: usize,

    /// Pointer to the inline `next[]` array that follows this header.
    fn nextBase(self: *Node) [*]AtomicLink {
        const raw: [*]u8 = @ptrCast(self);
        const off = std.mem.alignForward(usize, @sizeOf(Node), @alignOf(AtomicLink));
        return @alignCast(@ptrCast(raw + off));
    }

    /// Read `next[level]` with acquire ordering (reader side).
    fn next(self: *Node, level: usize) ?*Node {
        std.debug.assert(level < self.height);
        return self.nextBase()[level].load(.acquire);
    }

    /// Publish `next[level]` with release ordering (writer side).
    fn setNext(self: *Node, level: usize, x: ?*Node) void {
        std.debug.assert(level < self.height);
        self.nextBase()[level].store(x, .release);
    }

    /// Non-atomic store used only during node construction, before the node is
    /// linked into the list and thus before any reader can observe it.
    fn setNextRaw(self: *Node, level: usize, x: ?*Node) void {
        std.debug.assert(level < self.height);
        self.nextBase()[level] = AtomicLink.init(x);
    }
};

pub const SkipList = struct {
    pub const kMaxHeight = 12;
    /// Branching factor: ~1/kBranching chance to grow one more level.
    const kBranching = 4;

    arena: *arena.Arena,
    cmp: comparator.Comparator,
    head: *Node,
    /// Current height of the entire list (1..=kMaxHeight); only ever grows.
    /// Atomic because a reader may read it while the writer raises it.
    max_height: std.atomic.Value(usize),
    rnd: std.Random.DefaultPrng,

    pub fn init(a: *arena.Arena, cmp: comparator.Comparator, seed: u64) SkipList {
        // The head sentinel is a full-height node with an empty key; its key is
        // never compared (we never call cmp on the head).
        const head = newNode(a, &.{}, kMaxHeight) catch
            @panic("skiplist: arena OOM constructing head");
        return .{
            .arena = a,
            .cmp = cmp,
            .head = head,
            .max_height = std.atomic.Value(usize).init(1),
            .rnd = std.Random.DefaultPrng.init(seed),
        };
    }

    /// Allocate a node of the given height from the arena, copying nothing for
    /// the key (caller passes an already-owned slice — empty for the head).
    /// `next[]` slots are initialised to null (raw, pre-publication).
    fn newNode(a: *arena.Arena, owned_key: []const u8, height: usize) !*Node {
        std.debug.assert(height >= 1 and height <= kMaxHeight);
        const align_ = @max(@alignOf(Node), @alignOf(AtomicLink));
        const header = std.mem.alignForward(usize, @sizeOf(Node), @alignOf(AtomicLink));
        const total = header + height * @sizeOf(AtomicLink);
        const raw = try a.allocAligned(total, align_);
        const node: *Node = @alignCast(@ptrCast(raw.ptr));
        node.* = .{ .key = owned_key, .height = height };
        var l: usize = 0;
        while (l < height) : (l += 1) node.setNextRaw(l, null);
        return node;
    }

    fn currentHeight(self: *const SkipList) usize {
        return self.max_height.load(.acquire);
    }

    /// Roll a random height in 1..=kMaxHeight (1/kBranching per extra level).
    fn randomHeight(self: *SkipList) usize {
        var height: usize = 1;
        while (height < kMaxHeight and
            self.rnd.random().intRangeLessThan(u32, 0, kBranching) == 0) : (height += 1)
        {}
        return height;
    }

    /// key < node.key ?  (node must not be the head sentinel.)
    fn keyIsAfterNode(self: *const SkipList, key: []const u8, n: ?*Node) bool {
        // True iff n is non-null and n.key < key.
        return (n != null) and (self.cmp.compare(n.?.key, key) == .lt);
    }

    /// Find the first node whose key is >= `key`.  If `prev` is non-null it is
    /// filled (length kMaxHeight) with, for each level, the last node that
    /// precedes the returned position — exactly what `insert` needs to splice.
    fn findGreaterOrEqual(self: *const SkipList, key: []const u8, prev: ?[]*Node) ?*Node {
        var x: *Node = self.head;
        var level: usize = self.currentHeight() - 1;
        while (true) {
            const nxt = x.next(level);
            if (self.keyIsAfterNode(key, nxt)) {
                // key > nxt.key — keep moving right on this level.
                x = nxt.?;
            } else {
                // nxt is null or nxt.key >= key — drop down (or stop).
                if (prev) |p| p[level] = x;
                if (level == 0) return nxt;
                level -= 1;
            }
        }
    }

    /// Find the last node whose key is strictly < `key` (the head if none).
    fn findLessThan(self: *const SkipList, key: []const u8) *Node {
        var x: *Node = self.head;
        var level: usize = self.currentHeight() - 1;
        while (true) {
            const nxt = x.next(level);
            // Advance while nxt exists and nxt.key < key.
            if (nxt != null and self.cmp.compare(nxt.?.key, key) == .lt) {
                x = nxt.?;
            } else {
                if (level == 0) return x;
                level -= 1;
            }
        }
    }

    /// Find the last node in the list (the head if the list is empty).
    fn findLast(self: *const SkipList) *Node {
        var x: *Node = self.head;
        var level: usize = self.currentHeight() - 1;
        while (true) {
            const nxt = x.next(level);
            if (nxt) |n| {
                x = n;
            } else {
                if (level == 0) return x;
                level -= 1;
            }
        }
    }

    /// Insert `key`, copying its bytes into the arena.  Assumes no equal key is
    /// already present (the MemTable guarantees uniqueness via sequence nums).
    pub fn insert(self: *SkipList, key: []const u8) !void {
        var prev: [kMaxHeight]*Node = undefined;
        const x = self.findGreaterOrEqual(key, prev[0..]);

        // Debug-only uniqueness check.
        std.debug.assert(x == null or self.cmp.compare(x.?.key, key) != .eq);

        const height = self.randomHeight();
        const cur = self.currentHeight();
        if (height > cur) {
            // New levels point down from the head.  Safe to publish: a
            // concurrent reader that observes the raised height will also
            // observe head.next[level] == this new node (set below); one that
            // reads the old height simply never visits the new levels.
            var l = cur;
            while (l < height) : (l += 1) prev[l] = self.head;
            self.max_height.store(height, .release);
        }

        const owned_key = try self.arena.alloc(key.len);
        @memcpy(owned_key, key);
        const node = try newNode(self.arena, owned_key, height);

        // Splice in bottom-up.  We set this node's next pointers first (no
        // reader can see `node` yet), then publish each predecessor's link with
        // release ordering.
        var l: usize = 0;
        while (l < height) : (l += 1) {
            node.setNextRaw(l, prev[l].next(l));
            prev[l].setNext(l, node);
        }
    }

    pub fn contains(self: *const SkipList, key: []const u8) bool {
        const x = self.findGreaterOrEqual(key, null);
        return x != null and self.cmp.compare(x.?.key, key) == .eq;
    }

    /// Forward+backward cursor over the skiplist.  `prev()` is implemented by
    /// re-search (`findLessThan`), matching LevelDB.
    pub const Iterator = struct {
        list: *const SkipList,
        node: ?*Node,

        pub fn init(list: *const SkipList) Iterator {
            return .{ .list = list, .node = null };
        }

        pub fn valid(self: *const Iterator) bool {
            return self.node != null;
        }

        pub fn key(self: *const Iterator) []const u8 {
            std.debug.assert(self.valid());
            return self.node.?.key;
        }

        pub fn next(self: *Iterator) void {
            std.debug.assert(self.valid());
            self.node = self.node.?.next(0);
        }

        /// Move to the last node with key < current key (re-search). Becomes
        /// invalid when stepping before the first node.
        pub fn prev(self: *Iterator) void {
            std.debug.assert(self.valid());
            const p = self.list.findLessThan(self.node.?.key);
            self.node = if (p == self.list.head) null else p;
        }

        pub fn seekToFirst(self: *Iterator) void {
            self.node = self.list.head.next(0);
        }

        pub fn seekToLast(self: *Iterator) void {
            const p = self.list.findLast();
            self.node = if (p == self.list.head) null else p;
        }

        /// Position at the first node with key >= `target`.
        pub fn seek(self: *Iterator, target: []const u8) void {
            self.node = self.list.findGreaterOrEqual(target, null);
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
