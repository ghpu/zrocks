//! Iterator — generic ordered key/value cursor (runtime-vtable interface).
//!
//! This is the common abstraction that the DB read path and (later) compaction
//! build on.  An `Iterator` is a small fat pointer `(ctx, vtable)`, following
//! the same capability pattern used by `util/comparator.zig` and `env/env.zig`.
//! Concrete sources (memtable, SST table, the merging/two-level combinators in
//! the sibling files) all expose themselves through this single interface so
//! that higher layers can compose them uniformly.
//!
//! POSITIONING MODEL (mirrors LevelDB):
//!   * An iterator is either *valid* (positioned at a live entry) or *invalid*
//!     (before-first, past-last, empty source, or an error occurred).
//!   * `seekToFirst` / `seekToLast` / `seek` establish an initial position.
//!   * `next` / `prev` move forward / backward; calling them is only legal
//!     while `valid()`.
//!   * `key` / `value` may only be read while `valid()`.
//!
//! KEY/VALUE LIFETIME CONTRACT:
//!   The slices returned by `key()` and `value()` are owned by the iterator and
//!   are valid ONLY until the next mutating call on that same iterator
//!   (`seekToFirst`, `seekToLast`, `seek`, `next`, `prev`) or until the backing
//!   source is destroyed.  Callers that need to retain a key or value MUST copy
//!   the bytes.

const std = @import("std");
const comparator = @import("../util/comparator.zig");

// ---------------------------------------------------------------------------
// Iterator — runtime-vtable fat pointer
// ---------------------------------------------------------------------------

pub const Iterator = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        seekToFirst: *const fn (ctx: *anyopaque) void,
        seekToLast: *const fn (ctx: *anyopaque) void,
        seek: *const fn (ctx: *anyopaque, target: []const u8) void,
        next: *const fn (ctx: *anyopaque) void,
        prev: *const fn (ctx: *anyopaque) void,
        valid: *const fn (ctx: *anyopaque) bool,
        key: *const fn (ctx: *anyopaque) []const u8,
        value: *const fn (ctx: *anyopaque) []const u8,
        status: *const fn (ctx: *anyopaque) ?anyerror,
    };

    // Thin method wrappers --------------------------------------------------

    /// Position at the first entry (smallest key under the source's order).
    pub fn seekToFirst(self: Iterator) void {
        self.vtable.seekToFirst(self.ctx);
    }

    /// Position at the last entry (largest key under the source's order).
    pub fn seekToLast(self: Iterator) void {
        self.vtable.seekToLast(self.ctx);
    }

    /// Position at the first entry whose key is `>= target`.
    pub fn seek(self: Iterator, target: []const u8) void {
        self.vtable.seek(self.ctx, target);
    }

    /// Advance to the next entry.  Requires `valid()`.
    pub fn next(self: Iterator) void {
        self.vtable.next(self.ctx);
    }

    /// Step back to the previous entry.  Requires `valid()`.
    pub fn prev(self: Iterator) void {
        self.vtable.prev(self.ctx);
    }

    /// Whether the iterator is positioned at a live entry.
    pub fn valid(self: Iterator) bool {
        return self.vtable.valid(self.ctx);
    }

    /// Current key.  Requires `valid()`.  See lifetime contract above.
    pub fn key(self: Iterator) []const u8 {
        return self.vtable.key(self.ctx);
    }

    /// Current value.  Requires `valid()`.  See lifetime contract above.
    pub fn value(self: Iterator) []const u8 {
        return self.vtable.value(self.ctx);
    }

    /// First error encountered while positioning, or null if healthy.
    pub fn status(self: Iterator) ?anyerror {
        return self.vtable.status(self.ctx);
    }
};

// ---------------------------------------------------------------------------
// VectorIterator — concrete Iterator over an already-sorted entry slice
// ---------------------------------------------------------------------------
//
// The caller owns the backing bytes; VectorIterator borrows the slice and the
// key/value bytes within it.  It is the test workhorse for the combinators and
// is reused by later milestones, so it is kept `pub` and self-contained.
//
// `entries` MUST already be sorted ascending under `cmp`.  Forward, reverse and
// `seek` are all supported (seek uses binary search).

pub const VectorIterator = struct {
    entries: []const Entry,
    cmp: comparator.Comparator,
    /// Index into `entries`; `entries.len` means "invalid / past-the-end".
    pos: usize,

    pub const Entry = struct {
        key: []const u8,
        value: []const u8,
    };

    /// Create a VectorIterator over `entries` (must be sorted ascending by the
    /// comparator later supplied to `iterator`).  Starts invalid.
    pub fn init(entries: []const Entry) VectorIterator {
        return .{
            .entries = entries,
            .cmp = comparator.bytewise,
            .pos = 0,
        };
    }

    /// Obtain the generic `Iterator` view, binding the comparator used for
    /// `seek`'s binary search.
    pub fn iterator(self: *VectorIterator, cmp: comparator.Comparator) Iterator {
        self.cmp = cmp;
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

    fn cast(ctx: *anyopaque) *VectorIterator {
        return @ptrCast(@alignCast(ctx));
    }

    fn seekToFirstImpl(ctx: *anyopaque) void {
        const self = cast(ctx);
        self.pos = 0;
    }

    fn seekToLastImpl(ctx: *anyopaque) void {
        const self = cast(ctx);
        // entries.len == 0 -> pos = (0 -% 1) which is huge => invalid; guard it.
        self.pos = if (self.entries.len == 0) 0 else self.entries.len - 1;
    }

    fn seekImpl(ctx: *anyopaque, target: []const u8) void {
        const self = cast(ctx);
        // Binary search for the first entry whose key is >= target.
        var lo: usize = 0;
        var hi: usize = self.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (self.cmp.compare(self.entries[mid].key, target)) {
                .lt => lo = mid + 1,
                .eq, .gt => hi = mid,
            }
        }
        self.pos = lo;
    }

    fn nextImpl(ctx: *anyopaque) void {
        const self = cast(ctx);
        std.debug.assert(self.pos < self.entries.len);
        self.pos += 1;
    }

    fn prevImpl(ctx: *anyopaque) void {
        const self = cast(ctx);
        std.debug.assert(self.pos < self.entries.len);
        if (self.pos == 0) {
            // Step before the first entry -> invalid sentinel.
            self.pos = self.entries.len;
        } else {
            self.pos -= 1;
        }
    }

    fn validImpl(ctx: *anyopaque) bool {
        const self = cast(ctx);
        return self.pos < self.entries.len;
    }

    fn keyImpl(ctx: *anyopaque) []const u8 {
        const self = cast(ctx);
        std.debug.assert(self.pos < self.entries.len);
        return self.entries[self.pos].key;
    }

    fn valueImpl(ctx: *anyopaque) []const u8 {
        const self = cast(ctx);
        std.debug.assert(self.pos < self.entries.len);
        return self.entries[self.pos].value;
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

fn e(k: []const u8, v: []const u8) VectorIterator.Entry {
    return .{ .key = k, .value = v };
}

/// Drains an iterator forward into a list of "k=v" strings (caller frees).
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

test "VectorIterator: forward scan" {
    const gpa = testing.allocator;
    const entries = [_]VectorIterator.Entry{ e("a", "1"), e("c", "3"), e("e", "5") };
    var vi = VectorIterator.init(&entries);
    const it = vi.iterator(comparator.bytewise);

    var got = try collectForward(gpa, it);
    defer freeList(gpa, &got);

    try testing.expectEqual(@as(usize, 3), got.items.len);
    try testing.expectEqualStrings("a=1", got.items[0]);
    try testing.expectEqualStrings("c=3", got.items[1]);
    try testing.expectEqualStrings("e=5", got.items[2]);
}

test "VectorIterator: reverse scan" {
    const entries = [_]VectorIterator.Entry{ e("a", "1"), e("c", "3"), e("e", "5") };
    var vi = VectorIterator.init(&entries);
    const it = vi.iterator(comparator.bytewise);

    it.seekToLast();
    try testing.expect(it.valid());
    try testing.expectEqualStrings("e", it.key());
    it.prev();
    try testing.expectEqualStrings("c", it.key());
    it.prev();
    try testing.expectEqualStrings("a", it.key());
    it.prev();
    try testing.expect(!it.valid());
}

test "VectorIterator: seek present / between / before / past-end" {
    const entries = [_]VectorIterator.Entry{ e("b", "2"), e("d", "4"), e("f", "6") };
    var vi = VectorIterator.init(&entries);
    const it = vi.iterator(comparator.bytewise);

    // Present.
    it.seek("d");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("d", it.key());

    // Between b and d -> lands on d.
    it.seek("c");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("d", it.key());

    // Before first -> lands on b.
    it.seek("a");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("b", it.key());

    // Past end -> invalid.
    it.seek("z");
    try testing.expect(!it.valid());
}

test "VectorIterator: empty" {
    const entries = [_]VectorIterator.Entry{};
    var vi = VectorIterator.init(&entries);
    const it = vi.iterator(comparator.bytewise);

    it.seekToFirst();
    try testing.expect(!it.valid());
    it.seekToLast();
    try testing.expect(!it.valid());
    it.seek("anything");
    try testing.expect(!it.valid());
    try testing.expectEqual(@as(?anyerror, null), it.status());
}
