//! MergingIterator — merges N child `Iterator`s into one ordered stream.
//!
//! Generalizes LevelDB's `MergingIterator`: given several already-sorted child
//! iterators and a `Comparator`, it yields entries in fully merged sorted order
//! (forward) or reverse-sorted order (backward).  The DB read path layers this
//! over the memtable iterator and the per-level table iterators.
//!
//! Equal keys across children are yielded in CHILD ORDER (stable): the child
//! with the smaller index wins ties.  Deduplication by sequence number is the
//! responsibility of the DB layer above; this generic layer preserves every
//! entry.
//!
//! KEY/VALUE LIFETIME: `key()` / `value()` delegate to the current child and so
//! inherit that child's lifetime contract (valid until the next mutating call).

const std = @import("std");
const comparator = @import("../util/comparator.zig");
const iterator = @import("iterator.zig");

const Iterator = iterator.Iterator;

pub const MergingIterator = struct {
    gpa: std.mem.Allocator,
    cmp: comparator.Comparator,
    children: []Iterator,
    /// Index of the child currently providing key/value, or null when invalid.
    current: ?usize,
    /// Tracks whether we last moved forward or backward, so a direction switch
    /// can re-position the non-current children (LevelDB's approach).
    direction: Direction,

    const Direction = enum { forward, reverse };

    /// Build a merging iterator over `children`.  The children slice is copied
    /// into an owned buffer; the child `Iterator` values themselves (and their
    /// backing sources) remain owned by the caller.
    pub fn init(
        gpa: std.mem.Allocator,
        cmp: comparator.Comparator,
        children: []const Iterator,
    ) !MergingIterator {
        const owned = try gpa.alloc(Iterator, children.len);
        @memcpy(owned, children);
        return .{
            .gpa = gpa,
            .cmp = cmp,
            .children = owned,
            .current = null,
            .direction = .forward,
        };
    }

    pub fn deinit(self: *MergingIterator) void {
        self.gpa.free(self.children);
        self.* = undefined;
    }

    pub fn iterator(self: *MergingIterator) Iterator {
        return .{ .ctx = self, .vtable = &vtable };
    }

    // --- internal positioning helpers --------------------------------------

    fn cast(ctx: *anyopaque) *MergingIterator {
        return @ptrCast(@alignCast(ctx));
    }

    /// Pick the child with the smallest key as `current` (forward order).
    fn findSmallest(self: *MergingIterator) void {
        var smallest: ?usize = null;
        for (self.children, 0..) |child, i| {
            if (!child.valid()) continue;
            if (smallest) |s| {
                if (self.cmp.compare(child.key(), self.children[s].key()) == .lt) {
                    smallest = i;
                }
            } else {
                smallest = i;
            }
        }
        self.current = smallest;
    }

    /// Pick the child with the largest key as `current` (reverse order).
    /// On ties the LATER child wins, mirroring the forward stable order so a
    /// reverse scan is the exact inverse of a forward scan.
    fn findLargest(self: *MergingIterator) void {
        if (self.children.len == 0) {
            self.current = null;
            return;
        }
        var largest: ?usize = null;
        var i: usize = self.children.len;
        while (i > 0) {
            i -= 1;
            const child = self.children[i];
            if (!child.valid()) continue;
            if (largest) |l| {
                if (self.cmp.compare(child.key(), self.children[l].key()) == .gt) {
                    largest = i;
                }
            } else {
                largest = i;
            }
        }
        self.current = largest;
    }

    const vtable = Iterator.VTable{
        .seekToFirst = seekToFirstImpl,
        .seekToLast = seekToLastImpl,
        .seek = seekImpl,
        .next = nextImpl,
        .prev = prevImpl,
        .valid = validImpl,
        .key = keyImpl,
        .value = valueImpl,
        .status = statusImpl,
    };

    fn seekToFirstImpl(ctx: *anyopaque) void {
        const self = cast(ctx);
        for (self.children) |child| child.seekToFirst();
        self.direction = .forward;
        self.findSmallest();
    }

    fn seekToLastImpl(ctx: *anyopaque) void {
        const self = cast(ctx);
        for (self.children) |child| child.seekToLast();
        self.direction = .reverse;
        self.findLargest();
    }

    fn seekImpl(ctx: *anyopaque, target: []const u8) void {
        const self = cast(ctx);
        for (self.children) |child| child.seek(target);
        self.direction = .forward;
        self.findSmallest();
    }

    fn nextImpl(ctx: *anyopaque) void {
        const self = cast(ctx);
        std.debug.assert(self.current != null);
        const cur = self.current.?;

        // If we were going in reverse, the non-current children are positioned
        // at-or-before the current key.  To go forward we must move each of
        // them just past the current key (LevelDB's direction-switch fixup).
        if (self.direction != .forward) {
            const cur_key = self.children[cur].key();
            for (self.children, 0..) |child, i| {
                if (i == cur) continue;
                child.seek(cur_key);
                // `seek` lands on the first entry >= cur_key; if that equals
                // cur_key, step past it so the current child's entry is the
                // unique smallest.
                if (child.valid() and self.cmp.compare(child.key(), cur_key) == .eq) {
                    child.next();
                }
            }
            self.direction = .forward;
        }

        self.children[cur].next();
        self.findSmallest();
    }

    fn prevImpl(ctx: *anyopaque) void {
        const self = cast(ctx);
        std.debug.assert(self.current != null);
        const cur = self.current.?;

        // If we were going forward, the non-current children are positioned
        // at-or-after the current key.  To go reverse we must move each of
        // them just before the current key.
        if (self.direction != .reverse) {
            const cur_key = self.children[cur].key();
            for (self.children, 0..) |child, i| {
                if (i == cur) continue;
                child.seek(cur_key);
                if (child.valid()) {
                    // child is at first entry >= cur_key; step back to land at
                    // the last entry < cur_key.
                    child.prev();
                } else {
                    // No entry >= cur_key; the largest entry is the last one.
                    child.seekToLast();
                }
            }
            self.direction = .reverse;
        }

        self.children[cur].prev();
        self.findLargest();
    }

    fn validImpl(ctx: *anyopaque) bool {
        const self = cast(ctx);
        return self.current != null;
    }

    fn keyImpl(ctx: *anyopaque) []const u8 {
        const self = cast(ctx);
        return self.children[self.current.?].key();
    }

    fn valueImpl(ctx: *anyopaque) []const u8 {
        const self = cast(ctx);
        return self.children[self.current.?].value();
    }

    fn statusImpl(ctx: *anyopaque) ?anyerror {
        const self = cast(ctx);
        for (self.children) |child| {
            if (child.status()) |s| return s;
        }
        return null;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;
const VectorIterator = iterator.VectorIterator;

fn e(k: []const u8, v: []const u8) VectorIterator.Entry {
    return .{ .key = k, .value = v };
}

fn collectForward(gpa: std.mem.Allocator, it: Iterator) !std.ArrayList([]u8) {
    var out: std.ArrayList([]u8) = .empty;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        const s = try std.fmt.allocPrint(gpa, "{s}={s}", .{ it.key(), it.value() });
        try out.append(gpa, s);
    }
    return out;
}

fn collectReverse(gpa: std.mem.Allocator, it: Iterator) !std.ArrayList([]u8) {
    var out: std.ArrayList([]u8) = .empty;
    it.seekToLast();
    while (it.valid()) : (it.prev()) {
        const s = try std.fmt.allocPrint(gpa, "{s}={s}", .{ it.key(), it.value() });
        try out.append(gpa, s);
    }
    return out;
}

fn freeList(gpa: std.mem.Allocator, list: *std.ArrayList([]u8)) void {
    for (list.items) |s| gpa.free(s);
    list.deinit(gpa);
}

test "MergingIterator: merge 3 sorted vectors -> fully sorted forward" {
    const gpa = testing.allocator;
    const a = [_]VectorIterator.Entry{ e("a", "1"), e("d", "4"), e("g", "7") };
    const b = [_]VectorIterator.Entry{ e("b", "2"), e("e", "5"), e("h", "8") };
    const c = [_]VectorIterator.Entry{ e("c", "3"), e("f", "6"), e("i", "9") };
    var va = VectorIterator.init(&a);
    var vb = VectorIterator.init(&b);
    var vc = VectorIterator.init(&c);
    const children = [_]Iterator{
        va.iterator(comparator.bytewise),
        vb.iterator(comparator.bytewise),
        vc.iterator(comparator.bytewise),
    };
    var mi = try MergingIterator.init(gpa, comparator.bytewise, &children);
    defer mi.deinit();
    const it = mi.iterator();

    var got = try collectForward(gpa, it);
    defer freeList(gpa, &got);

    const want = [_][]const u8{ "a=1", "b=2", "c=3", "d=4", "e=5", "f=6", "g=7", "h=8", "i=9" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| try testing.expectEqualStrings(w, g);
}

test "MergingIterator: seek lands on first >= target" {
    const gpa = testing.allocator;
    const a = [_]VectorIterator.Entry{ e("a", "1"), e("d", "4") };
    const b = [_]VectorIterator.Entry{ e("b", "2"), e("e", "5") };
    var va = VectorIterator.init(&a);
    var vb = VectorIterator.init(&b);
    const children = [_]Iterator{
        va.iterator(comparator.bytewise),
        vb.iterator(comparator.bytewise),
    };
    var mi = try MergingIterator.init(gpa, comparator.bytewise, &children);
    defer mi.deinit();
    const it = mi.iterator();

    it.seek("c");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("d", it.key());
    it.next();
    try testing.expectEqualStrings("e", it.key());

    it.seek("z");
    try testing.expect(!it.valid());

    it.seek("");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("a", it.key());
}

test "MergingIterator: single child" {
    const gpa = testing.allocator;
    const a = [_]VectorIterator.Entry{ e("x", "1"), e("y", "2") };
    var va = VectorIterator.init(&a);
    const children = [_]Iterator{va.iterator(comparator.bytewise)};
    var mi = try MergingIterator.init(gpa, comparator.bytewise, &children);
    defer mi.deinit();
    const it = mi.iterator();

    var got = try collectForward(gpa, it);
    defer freeList(gpa, &got);
    try testing.expectEqual(@as(usize, 2), got.items.len);
    try testing.expectEqualStrings("x=1", got.items[0]);
    try testing.expectEqualStrings("y=2", got.items[1]);
}

test "MergingIterator: empty children list" {
    const gpa = testing.allocator;
    const children = [_]Iterator{};
    var mi = try MergingIterator.init(gpa, comparator.bytewise, &children);
    defer mi.deinit();
    const it = mi.iterator();

    it.seekToFirst();
    try testing.expect(!it.valid());
    it.seekToLast();
    try testing.expect(!it.valid());
    it.seek("k");
    try testing.expect(!it.valid());
}

test "MergingIterator: one empty + others non-empty" {
    const gpa = testing.allocator;
    const a = [_]VectorIterator.Entry{ e("a", "1"), e("c", "3") };
    const empty = [_]VectorIterator.Entry{};
    const c = [_]VectorIterator.Entry{ e("b", "2"), e("d", "4") };
    var va = VectorIterator.init(&a);
    var ve = VectorIterator.init(&empty);
    var vc = VectorIterator.init(&c);
    const children = [_]Iterator{
        va.iterator(comparator.bytewise),
        ve.iterator(comparator.bytewise),
        vc.iterator(comparator.bytewise),
    };
    var mi = try MergingIterator.init(gpa, comparator.bytewise, &children);
    defer mi.deinit();
    const it = mi.iterator();

    var got = try collectForward(gpa, it);
    defer freeList(gpa, &got);
    const want = [_][]const u8{ "a=1", "b=2", "c=3", "d=4" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| try testing.expectEqualStrings(w, g);
}

test "MergingIterator: duplicate keys across children appear (count preserved, child order)" {
    const gpa = testing.allocator;
    const a = [_]VectorIterator.Entry{ e("k", "A"), e("m", "A") };
    const b = [_]VectorIterator.Entry{ e("k", "B"), e("m", "B") };
    var va = VectorIterator.init(&a);
    var vb = VectorIterator.init(&b);
    const children = [_]Iterator{
        va.iterator(comparator.bytewise),
        vb.iterator(comparator.bytewise),
    };
    var mi = try MergingIterator.init(gpa, comparator.bytewise, &children);
    defer mi.deinit();
    const it = mi.iterator();

    var got = try collectForward(gpa, it);
    defer freeList(gpa, &got);
    // Both "k" entries then both "m" entries; on ties, child 0 precedes child 1.
    const want = [_][]const u8{ "k=A", "k=B", "m=A", "m=B" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| try testing.expectEqualStrings(w, g);
}

test "MergingIterator: reverse scan yields reverse-sorted order" {
    const gpa = testing.allocator;
    const a = [_]VectorIterator.Entry{ e("a", "1"), e("d", "4"), e("g", "7") };
    const b = [_]VectorIterator.Entry{ e("b", "2"), e("e", "5"), e("h", "8") };
    const c = [_]VectorIterator.Entry{ e("c", "3"), e("f", "6"), e("i", "9") };
    var va = VectorIterator.init(&a);
    var vb = VectorIterator.init(&b);
    var vc = VectorIterator.init(&c);
    const children = [_]Iterator{
        va.iterator(comparator.bytewise),
        vb.iterator(comparator.bytewise),
        vc.iterator(comparator.bytewise),
    };
    var mi = try MergingIterator.init(gpa, comparator.bytewise, &children);
    defer mi.deinit();
    const it = mi.iterator();

    var got = try collectReverse(gpa, it);
    defer freeList(gpa, &got);
    const want = [_][]const u8{ "i=9", "h=8", "g=7", "f=6", "e=5", "d=4", "c=3", "b=2", "a=1" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| try testing.expectEqualStrings(w, g);
}

test "MergingIterator: reverse then forward direction switch" {
    const gpa = testing.allocator;
    const a = [_]VectorIterator.Entry{ e("a", "1"), e("d", "4") };
    const b = [_]VectorIterator.Entry{ e("b", "2"), e("c", "3") };
    var va = VectorIterator.init(&a);
    var vb = VectorIterator.init(&b);
    const children = [_]Iterator{
        va.iterator(comparator.bytewise),
        vb.iterator(comparator.bytewise),
    };
    var mi = try MergingIterator.init(gpa, comparator.bytewise, &children);
    defer mi.deinit();
    const it = mi.iterator();

    it.seekToLast();
    try testing.expectEqualStrings("d", it.key()); // d
    it.prev();
    try testing.expectEqualStrings("c", it.key()); // c
    it.prev();
    try testing.expectEqualStrings("b", it.key()); // b
    // Switch direction forward: next should yield c, then d.
    it.next();
    try testing.expectEqualStrings("c", it.key());
    it.next();
    try testing.expectEqualStrings("d", it.key());
    it.next();
    try testing.expect(!it.valid());
}

test "MergingIterator: forward then reverse direction switch" {
    const gpa = testing.allocator;
    const a = [_]VectorIterator.Entry{ e("a", "1"), e("d", "4") };
    const b = [_]VectorIterator.Entry{ e("b", "2"), e("c", "3") };
    var va = VectorIterator.init(&a);
    var vb = VectorIterator.init(&b);
    const children = [_]Iterator{
        va.iterator(comparator.bytewise),
        vb.iterator(comparator.bytewise),
    };
    var mi = try MergingIterator.init(gpa, comparator.bytewise, &children);
    defer mi.deinit();
    const it = mi.iterator();

    it.seekToFirst();
    try testing.expectEqualStrings("a", it.key()); // a
    it.next();
    try testing.expectEqualStrings("b", it.key()); // b
    it.next();
    try testing.expectEqualStrings("c", it.key()); // c
    // Now switch direction: prev should yield b, then a.
    it.prev();
    try testing.expectEqualStrings("b", it.key());
    it.prev();
    try testing.expectEqualStrings("a", it.key());
    it.prev();
    try testing.expect(!it.valid());
}
