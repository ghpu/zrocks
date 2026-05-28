const std = @import("std");

// ---------------------------------------------------------------------------
// Byte-slice vocabulary used throughout the zrocks engine.
// Stubs — implementations will follow in the green commit.
// ---------------------------------------------------------------------------

pub fn compare(a: []const u8, b: []const u8) std.math.Order {
    _ = a;
    _ = b;
    @panic("TODO");
}

pub fn equal(a: []const u8, b: []const u8) bool {
    _ = a;
    _ = b;
    @panic("TODO");
}

pub fn startsWith(slice: []const u8, prefix: []const u8) bool {
    _ = slice;
    _ = prefix;
    @panic("TODO");
}

pub fn isEmpty(s: []const u8) bool {
    _ = s;
    @panic("TODO");
}

pub fn removePrefix(s: []const u8, n: usize) []const u8 {
    _ = s;
    _ = n;
    @panic("TODO");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "compare: equal slices" {
    try std.testing.expectEqual(std.math.Order.eq, compare("abc", "abc"));
}

test "compare: less than" {
    try std.testing.expectEqual(std.math.Order.lt, compare("abc", "abd"));
}

test "compare: greater than" {
    try std.testing.expectEqual(std.math.Order.gt, compare("abd", "abc"));
}

test "compare: empty slices are equal" {
    try std.testing.expectEqual(std.math.Order.eq, compare("", ""));
}

test "compare: empty is less than non-empty" {
    try std.testing.expectEqual(std.math.Order.lt, compare("", "a"));
}

test "compare: shorter prefix is less" {
    try std.testing.expectEqual(std.math.Order.lt, compare("ab", "abc"));
}

test "equal: same content" {
    try std.testing.expect(equal("hello", "hello"));
}

test "equal: different content" {
    try std.testing.expect(!equal("hello", "world"));
}

test "equal: empty slices" {
    try std.testing.expect(equal("", ""));
}

test "startsWith: matching prefix" {
    try std.testing.expect(startsWith("foobar", "foo"));
}

test "startsWith: no match" {
    try std.testing.expect(!startsWith("foobar", "bar"));
}

test "startsWith: empty prefix always matches" {
    try std.testing.expect(startsWith("anything", ""));
}

test "startsWith: prefix longer than slice" {
    try std.testing.expect(!startsWith("ab", "abc"));
}

test "isEmpty: empty slice" {
    try std.testing.expect(isEmpty(""));
}

test "isEmpty: non-empty slice" {
    try std.testing.expect(!isEmpty("x"));
}

test "removePrefix: strip n bytes" {
    try std.testing.expectEqualStrings("bar", removePrefix("foobar", 3));
}

test "removePrefix: strip zero bytes" {
    try std.testing.expectEqualStrings("hello", removePrefix("hello", 0));
}

test "removePrefix: strip all bytes" {
    try std.testing.expectEqualStrings("", removePrefix("hi", 2));
}
