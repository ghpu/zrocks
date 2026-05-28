const std = @import("std");

// ---------------------------------------------------------------------------
// Comparator — runtime vtable interface (capability pattern)
// ---------------------------------------------------------------------------
// A `Comparator` is a small fat pointer: (ctx, vtable).  The DB, memtable,
// and table layers will hold one and call through it at runtime, so they can
// accept either the built-in Bytewise comparator or any user-supplied one.
// ---------------------------------------------------------------------------

pub const Comparator = struct {
    ctx: *const anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        compare: *const fn (ctx: *const anyopaque, a: []const u8, b: []const u8) std.math.Order,
        name: *const fn (ctx: *const anyopaque) []const u8,
        findShortestSeparator: *const fn (ctx: *const anyopaque, start: *std.ArrayList(u8), limit: []const u8) void,
        findShortSuccessor: *const fn (ctx: *const anyopaque, key: *std.ArrayList(u8)) void,
    };

    // Thin method wrappers -----------------------------------------------

    pub fn compare(self: Comparator, a: []const u8, b: []const u8) std.math.Order {
        return self.vtable.compare(self.ctx, a, b);
    }

    pub fn name(self: Comparator) []const u8 {
        return self.vtable.name(self.ctx);
    }

    pub fn findShortestSeparator(self: Comparator, start: *std.ArrayList(u8), limit: []const u8) void {
        self.vtable.findShortestSeparator(self.ctx, start, limit);
    }

    pub fn findShortSuccessor(self: Comparator, key: *std.ArrayList(u8)) void {
        self.vtable.findShortSuccessor(self.ctx, key);
    }
};

// ---------------------------------------------------------------------------
// Bytewise comparator — singleton
// ---------------------------------------------------------------------------

const bytewise_vtable = Comparator.VTable{
    .compare = bytewiseCompare,
    .name = bytewiseName,
    .findShortestSeparator = bytewiseFindShortestSeparator,
    .findShortSuccessor = bytewiseFindShortSuccessor,
};

const bytewise_ctx: u8 = 0;

pub const bytewise: Comparator = .{
    .ctx = &bytewise_ctx,
    .vtable = &bytewise_vtable,
};

fn bytewiseCompare(_: *const anyopaque, a: []const u8, b: []const u8) std.math.Order {
    return std.mem.order(u8, a, b);
}

fn bytewiseName(_: *const anyopaque) []const u8 {
    return "leveldb.BytewiseComparator";
}

/// LevelDB findShortestSeparator algorithm (byte-exact).
/// Finds the common prefix length; if one string is a prefix of the other,
/// leaves `start` unchanged.  Otherwise, at the first differing byte i:
/// if start[i] != 0xff and start[i]+1 < limit[i], increment and truncate.
fn bytewiseFindShortestSeparator(_: *const anyopaque, start: *std.ArrayList(u8), limit: []const u8) void {
    const min_len = @min(start.items.len, limit.len);
    var diff_index: usize = 0;
    while (diff_index < min_len and start.items[diff_index] == limit[diff_index]) {
        diff_index += 1;
    }
    // One is a prefix of the other — do nothing.
    if (diff_index >= min_len) return;

    const b = start.items[diff_index];
    if (b != 0xff and b + 1 < limit[diff_index]) {
        start.items[diff_index] = b + 1;
        start.shrinkRetainingCapacity(diff_index + 1);
    }
}

/// LevelDB findShortSuccessor algorithm.
/// Scans for the first non-0xff byte, increments it, and truncates there.
/// If all bytes are 0xff (or empty), leaves the key unchanged.
fn bytewiseFindShortSuccessor(_: *const anyopaque, key: *std.ArrayList(u8)) void {
    for (key.items, 0..) |b, i| {
        if (b != 0xff) {
            key.items[i] = b + 1;
            key.shrinkRetainingCapacity(i + 1);
            return;
        }
    }
    // All 0xff or empty — leave unchanged.
}

// ---------------------------------------------------------------------------
// Reverse bytewise comparator — singleton
// ---------------------------------------------------------------------------

const reverse_bytewise_vtable = Comparator.VTable{
    .compare = reverseBytewiseCompare,
    .name = reverseBytewiseName,
    .findShortestSeparator = reverseBytewiseFindShortestSeparator,
    .findShortSuccessor = reverseBytewiseFindShortSuccessor,
};

const reverse_bytewise_ctx: u8 = 0;

pub const reverse_bytewise: Comparator = .{
    .ctx = &reverse_bytewise_ctx,
    .vtable = &reverse_bytewise_vtable,
};

fn reverseBytewiseCompare(_: *const anyopaque, a: []const u8, b: []const u8) std.math.Order {
    return std.mem.order(u8, b, a); // swap a and b to reverse the order
}

fn reverseBytewiseName(_: *const anyopaque) []const u8 {
    return "rocksdb.ReverseBytewiseComparator";
}

fn reverseBytewiseFindShortestSeparator(_: *const anyopaque, _: *std.ArrayList(u8), _: []const u8) void {}

fn reverseBytewiseFindShortSuccessor(_: *const anyopaque, _: *std.ArrayList(u8)) void {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "bytewise compare" {
    try std.testing.expectEqual(std.math.Order.lt, bytewise.compare("a", "b"));
    try std.testing.expectEqual(std.math.Order.gt, bytewise.compare("b", "a"));
    try std.testing.expectEqual(std.math.Order.eq, bytewise.compare("ab", "ab"));
    try std.testing.expectEqual(std.math.Order.lt, bytewise.compare("ab", "abc"));
}

test "reverse_bytewise compare" {
    try std.testing.expectEqual(std.math.Order.gt, reverse_bytewise.compare("a", "b"));
    try std.testing.expectEqual(std.math.Order.lt, reverse_bytewise.compare("b", "a"));
    try std.testing.expectEqual(std.math.Order.eq, reverse_bytewise.compare("ab", "ab"));
}

test "comparator names" {
    try std.testing.expectEqualStrings("leveldb.BytewiseComparator", bytewise.name());
    try std.testing.expectEqualStrings("rocksdb.ReverseBytewiseComparator", reverse_bytewise.name());
}

test "findShortestSeparator: abc -> abz shortens to abd" {
    const gpa = std.testing.allocator;
    var start: std.ArrayList(u8) = .empty;
    defer start.deinit(gpa);
    try start.appendSlice(gpa, "abc");
    bytewise.findShortestSeparator(&start, "abz");
    try std.testing.expectEqualStrings("abd", start.items);
}

test "findShortestSeparator: foo vs foo (identical) — unchanged" {
    const gpa = std.testing.allocator;
    var start: std.ArrayList(u8) = .empty;
    defer start.deinit(gpa);
    try start.appendSlice(gpa, "foo");
    bytewise.findShortestSeparator(&start, "foo");
    try std.testing.expectEqualStrings("foo", start.items);
}

test "findShortestSeparator: start is prefix of limit — unchanged" {
    const gpa = std.testing.allocator;
    var start: std.ArrayList(u8) = .empty;
    defer start.deinit(gpa);
    try start.appendSlice(gpa, "abc");
    bytewise.findShortestSeparator(&start, "abcdefghi");
    try std.testing.expectEqualStrings("abc", start.items);
}

test "findShortestSeparator: no shortening when start[i]+1 == limit[i]" {
    const gpa = std.testing.allocator;
    var start: std.ArrayList(u8) = .empty;
    defer start.deinit(gpa);
    try start.appendSlice(gpa, "abd");
    bytewise.findShortestSeparator(&start, "abe");
    try std.testing.expectEqualStrings("abd", start.items);
}

test "findShortSuccessor: abc -> b" {
    const gpa = std.testing.allocator;
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(gpa);
    try key.appendSlice(gpa, "abc");
    bytewise.findShortSuccessor(&key);
    try std.testing.expectEqualStrings("b", key.items);
}

test "findShortSuccessor: all 0xff unchanged" {
    const gpa = std.testing.allocator;
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(gpa);
    try key.append(gpa, 0xff);
    try key.append(gpa, 0xff);
    bytewise.findShortSuccessor(&key);
    try std.testing.expectEqual(@as(usize, 2), key.items.len);
    try std.testing.expectEqual(@as(u8, 0xff), key.items[0]);
    try std.testing.expectEqual(@as(u8, 0xff), key.items[1]);
}

test "findShortSuccessor: empty key unchanged" {
    const gpa = std.testing.allocator;
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(gpa);
    bytewise.findShortSuccessor(&key);
    try std.testing.expectEqual(@as(usize, 0), key.items.len);
}

test "reverse comparator separator is a no-op (safe invariant)" {
    const gpa = std.testing.allocator;
    var start: std.ArrayList(u8) = .empty;
    defer start.deinit(gpa);
    try start.appendSlice(gpa, "abc");
    reverse_bytewise.findShortestSeparator(&start, "aaa");
    try std.testing.expectEqualStrings("abc", start.items);
}

test "reverse comparator successor is a no-op (safe invariant)" {
    const gpa = std.testing.allocator;
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(gpa);
    try key.appendSlice(gpa, "abc");
    reverse_bytewise.findShortSuccessor(&key);
    try std.testing.expectEqualStrings("abc", key.items);
}
