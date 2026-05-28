//! TwoLevelIterator — flattens an index→data two-level iteration.
//!
//! Generalizes the SST table iterator (index block -> data blocks): an outer
//! "index" iterator yields entries whose VALUE encodes how to build the
//! corresponding inner "second-level" iterator (e.g. a BlockHandle pointing at
//! a data block).  This combinator drives the pair so callers see a single flat
//! ordered stream of the second-level entries.
//!
//! The `makeSecondLevel` callback receives the current index value and returns
//! a freshly-constructed `Iterator` (unpositioned).  TwoLevelIterator owns that
//! returned iterator's *positioning* (it seeks it) but NOT its lifetime beyond
//! replacing it: when it crosses a boundary it simply drops its reference and
//! asks for the next one.  Because the generic `Iterator` interface has no
//! `deinit`, second-level sources whose construction allocates must arrange
//! their own cleanup (e.g. an arena owned by `ctx`); the SST adapter in M4.1
//! does exactly this.  The combinators here (VectorIterator) allocate nothing.
//!
//! KEY/VALUE LIFETIME: delegates to the current second-level iterator.

const std = @import("std");
const iterator = @import("iterator.zig");

const Iterator = iterator.Iterator;

/// Signature of the user callback that turns an index VALUE into a second-level
/// iterator.  Errors are surfaced through `TwoLevelIterator.status`.
pub const MakeSecondLevelFn = *const fn (ctx: *anyopaque, index_value: []const u8) anyerror!Iterator;

pub const TwoLevelIterator = struct {
    index_iter: Iterator,
    ctx: *anyopaque,
    make: MakeSecondLevelFn,
    /// The currently-open second-level iterator, or null when none is open.
    data_iter: ?Iterator,
    err: ?anyerror,

    pub fn init(
        index_iter: Iterator,
        ctx: *anyopaque,
        makeSecondLevel: MakeSecondLevelFn,
    ) TwoLevelIterator {
        return .{
            .index_iter = index_iter,
            .ctx = ctx,
            .make = makeSecondLevel,
            .data_iter = null,
            .err = null,
        };
    }

    pub fn iterator(self: *TwoLevelIterator) Iterator {
        return .{ .ctx = self, .vtable = &vtable };
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
        _ = ctx;
        @panic("RED: not implemented");
    }
    fn seekToLastImpl(ctx: *anyopaque) void {
        _ = ctx;
        @panic("RED: not implemented");
    }
    fn seekImpl(ctx: *anyopaque, target: []const u8) void {
        _ = ctx;
        _ = target;
        @panic("RED: not implemented");
    }
    fn nextImpl(ctx: *anyopaque) void {
        _ = ctx;
        @panic("RED: not implemented");
    }
    fn prevImpl(ctx: *anyopaque) void {
        _ = ctx;
        @panic("RED: not implemented");
    }
    fn validImpl(ctx: *anyopaque) bool {
        _ = ctx;
        return false;
    }
    fn keyImpl(ctx: *anyopaque) []const u8 {
        _ = ctx;
        @panic("RED: not implemented");
    }
    fn valueImpl(ctx: *anyopaque) []const u8 {
        _ = ctx;
        @panic("RED: not implemented");
    }
    fn statusImpl(ctx: *anyopaque) ?anyerror {
        _ = ctx;
        return null;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;
const comparator = @import("../util/comparator.zig");
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

fn freeList(gpa: std.mem.Allocator, list: *std.ArrayList([]u8)) void {
    for (list.items) |s| gpa.free(s);
    list.deinit(gpa);
}

// Test harness: three "blocks" of entries selected by a one-byte index value.
const Blocks = struct {
    block0: []const VectorIterator.Entry,
    block1: []const VectorIterator.Entry,
    block2: []const VectorIterator.Entry,
    // Reusable backing VectorIterators (one open at a time during a scan).
    vi: VectorIterator = undefined,

    fn pick(self: *Blocks, index_value: []const u8) []const VectorIterator.Entry {
        return switch (index_value[0]) {
            '0' => self.block0,
            '1' => self.block1,
            '2' => self.block2,
            else => unreachable,
        };
    }

    fn make(ctx: *anyopaque, index_value: []const u8) anyerror!Iterator {
        const self: *Blocks = @ptrCast(@alignCast(ctx));
        self.vi = VectorIterator.init(self.pick(index_value));
        return self.vi.iterator(comparator.bytewise);
    }
};

test "TwoLevelIterator: flatten forward scan yields all entries in order" {
    const gpa = testing.allocator;
    const b0 = [_]VectorIterator.Entry{ e("a", "1"), e("b", "2") };
    const b1 = [_]VectorIterator.Entry{ e("c", "3"), e("d", "4") };
    const b2 = [_]VectorIterator.Entry{ e("e", "5"), e("f", "6") };
    var blocks = Blocks{ .block0 = &b0, .block1 = &b1, .block2 = &b2 };
    // Index: value selects the block; key is the block's first key (>= search).
    const index = [_]VectorIterator.Entry{ e("a", "0"), e("c", "1"), e("e", "2") };
    var index_vi = VectorIterator.init(&index);

    var tli = TwoLevelIterator.init(
        index_vi.iterator(comparator.bytewise),
        &blocks,
        Blocks.make,
    );
    const it = tli.iterator();

    var got = try collectForward(gpa, it);
    defer freeList(gpa, &got);
    const want = [_][]const u8{ "a=1", "b=2", "c=3", "d=4", "e=5", "f=6" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| try testing.expectEqualStrings(w, g);
}

test "TwoLevelIterator: seek across block boundaries" {
    const b0 = [_]VectorIterator.Entry{ e("a", "1"), e("b", "2") };
    const b1 = [_]VectorIterator.Entry{ e("c", "3"), e("d", "4") };
    const b2 = [_]VectorIterator.Entry{ e("e", "5"), e("f", "6") };
    var blocks = Blocks{ .block0 = &b0, .block1 = &b1, .block2 = &b2 };
    const index = [_]VectorIterator.Entry{ e("b", "0"), e("d", "1"), e("f", "2") };
    var index_vi = VectorIterator.init(&index);

    var tli = TwoLevelIterator.init(
        index_vi.iterator(comparator.bytewise),
        &blocks,
        Blocks.make,
    );
    const it = tli.iterator();

    // Seek into the second block.
    it.seek("c");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("c", it.key());
    it.next();
    try testing.expectEqualStrings("d", it.key());
    it.next();
    try testing.expectEqualStrings("e", it.key());

    // Seek that lands at the very last entry.
    it.seek("f");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("f", it.key());
    it.next();
    try testing.expect(!it.valid());

    // Seek past the end.
    it.seek("z");
    try testing.expect(!it.valid());
}

test "TwoLevelIterator: empty middle block skipped" {
    const gpa = testing.allocator;
    const b0 = [_]VectorIterator.Entry{ e("a", "1"), e("b", "2") };
    const b1 = [_]VectorIterator.Entry{}; // empty middle block
    const b2 = [_]VectorIterator.Entry{ e("e", "5"), e("f", "6") };
    var blocks = Blocks{ .block0 = &b0, .block1 = &b1, .block2 = &b2 };
    const index = [_]VectorIterator.Entry{ e("a", "0"), e("c", "1"), e("e", "2") };
    var index_vi = VectorIterator.init(&index);

    var tli = TwoLevelIterator.init(
        index_vi.iterator(comparator.bytewise),
        &blocks,
        Blocks.make,
    );
    const it = tli.iterator();

    var got = try collectForward(gpa, it);
    defer freeList(gpa, &got);
    const want = [_][]const u8{ "a=1", "b=2", "e=5", "f=6" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| try testing.expectEqualStrings(w, g);
}
