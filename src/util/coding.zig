/// coding.zig — LevelDB/RocksDB-compatible varint and fixed-width encoding.
/// Byte-compatible with RocksDB's util/coding.h / util/coding.cc.
const std = @import("std");

pub const Error = error{Corruption};

// ---------------------------------------------------------------------------
// Fixed-width little-endian helpers (to be implemented)
// ---------------------------------------------------------------------------

pub fn encodeFixed32(dst: *[4]u8, v: u32) void {
    _ = dst;
    _ = v;
    @panic("not implemented");
}

pub fn decodeFixed32(src: *const [4]u8) u32 {
    _ = src;
    @panic("not implemented");
}

pub fn encodeFixed64(dst: *[8]u8, v: u64) void {
    _ = dst;
    _ = v;
    @panic("not implemented");
}

pub fn decodeFixed64(src: *const [8]u8) u64 {
    _ = src;
    @panic("not implemented");
}

// ---------------------------------------------------------------------------
// ArrayList append helpers (to be implemented)
// ---------------------------------------------------------------------------

pub fn putFixed32(list: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator, v: u32) !void {
    _ = list;
    _ = gpa;
    _ = v;
    @panic("not implemented");
}

pub fn putFixed64(list: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator, v: u64) !void {
    _ = list;
    _ = gpa;
    _ = v;
    @panic("not implemented");
}

pub fn putVarint32(list: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator, v: u32) !void {
    _ = list;
    _ = gpa;
    _ = v;
    @panic("not implemented");
}

pub fn putVarint64(list: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator, v: u64) !void {
    _ = list;
    _ = gpa;
    _ = v;
    @panic("not implemented");
}

pub fn putLengthPrefixedSlice(
    list: *std.ArrayListUnmanaged(u8),
    gpa: std.mem.Allocator,
    data: []const u8,
) !void {
    _ = list;
    _ = gpa;
    _ = data;
    @panic("not implemented");
}

// ---------------------------------------------------------------------------
// Slice-consuming decoders (to be implemented)
// ---------------------------------------------------------------------------

pub fn getVarint32(input: *[]const u8) Error!u32 {
    _ = input;
    @panic("not implemented");
}

pub fn getVarint64(input: *[]const u8) Error!u64 {
    _ = input;
    @panic("not implemented");
}

pub fn getLengthPrefixedSlice(input: *[]const u8) Error![]const u8 {
    _ = input;
    @panic("not implemented");
}

// ---------------------------------------------------------------------------
// Utility (to be implemented)
// ---------------------------------------------------------------------------

pub fn varintLength(v: u64) usize {
    _ = v;
    @panic("not implemented");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "fixed32 golden vectors" {
    var buf: [4]u8 = undefined;

    encodeFixed32(&buf, 1);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x01, 0x00, 0x00, 0x00 }, &buf);
    try std.testing.expectEqual(@as(u32, 1), decodeFixed32(&buf));

    encodeFixed32(&buf, 0x04030201);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x01, 0x02, 0x03, 0x04 }, &buf);
    try std.testing.expectEqual(@as(u32, 0x04030201), decodeFixed32(&buf));
}

test "fixed64 golden vectors" {
    var buf: [8]u8 = undefined;

    encodeFixed64(&buf, 1);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x01, 0, 0, 0, 0, 0, 0, 0 }, &buf);
    try std.testing.expectEqual(@as(u64, 1), decodeFixed64(&buf));
}

test "varint32 golden vectors" {
    const gpa = std.testing.allocator;
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(gpa);

    // 0 -> {0x00}
    list.clearRetainingCapacity();
    try putVarint32(&list, gpa, 0);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x00}, list.items);

    // 127 -> {0x7f}
    list.clearRetainingCapacity();
    try putVarint32(&list, gpa, 127);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x7f}, list.items);

    // 128 -> {0x80, 0x01}
    list.clearRetainingCapacity();
    try putVarint32(&list, gpa, 128);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x80, 0x01 }, list.items);

    // 300 -> {0xac, 0x02}
    list.clearRetainingCapacity();
    try putVarint32(&list, gpa, 300);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xac, 0x02 }, list.items);

    // 16384 -> {0x80, 0x80, 0x01}
    list.clearRetainingCapacity();
    try putVarint32(&list, gpa, 16384);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x80, 0x80, 0x01 }, list.items);

    // 0xffffffff -> {0xff, 0xff, 0xff, 0xff, 0x0f}
    list.clearRetainingCapacity();
    try putVarint32(&list, gpa, 0xffffffff);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xff, 0xff, 0xff, 0xff, 0x0f }, list.items);
}

test "varint64 golden vector max u64" {
    const gpa = std.testing.allocator;
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(gpa);

    try putVarint64(&list, gpa, 0xffffffffffffffff);
    try std.testing.expectEqualSlices(
        u8,
        &[_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 },
        list.items,
    );
}

test "length-prefixed slice golden vector" {
    const gpa = std.testing.allocator;
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(gpa);

    try putLengthPrefixedSlice(&list, gpa, "hello");
    try std.testing.expectEqualSlices(
        u8,
        &[_]u8{ 0x05, 'h', 'e', 'l', 'l', 'o' },
        list.items,
    );
}

test "varint32 round-trip sweep" {
    const gpa = std.testing.allocator;
    const values = [_]u32{ 0, 1, 127, 128, 16383, 16384, std.math.maxInt(u32) / 2, std.math.maxInt(u32) };

    for (values) |v| {
        var list: std.ArrayListUnmanaged(u8) = .empty;
        defer list.deinit(gpa);
        try putVarint32(&list, gpa, v);

        var slice: []const u8 = list.items;
        const decoded = try getVarint32(&slice);
        try std.testing.expectEqual(v, decoded);
        try std.testing.expectEqual(@as(usize, 0), slice.len);
    }
}

test "varint64 round-trip sweep" {
    const gpa = std.testing.allocator;
    const values = [_]u64{
        0,
        1,
        127,
        128,
        16383,
        16384,
        @as(u64, 1) << 31,
        std.math.maxInt(u64),
    };

    for (values) |v| {
        var list: std.ArrayListUnmanaged(u8) = .empty;
        defer list.deinit(gpa);
        try putVarint64(&list, gpa, v);

        var slice: []const u8 = list.items;
        const decoded = try getVarint64(&slice);
        try std.testing.expectEqual(v, decoded);
        try std.testing.expectEqual(@as(usize, 0), slice.len);
    }
}

test "varint length" {
    try std.testing.expectEqual(@as(usize, 1), varintLength(0));
    try std.testing.expectEqual(@as(usize, 1), varintLength(127));
    try std.testing.expectEqual(@as(usize, 2), varintLength(128));
    try std.testing.expectEqual(@as(usize, 2), varintLength(300));
    try std.testing.expectEqual(@as(usize, 3), varintLength(16384));
    try std.testing.expectEqual(@as(usize, 5), varintLength(0xffffffff));
    try std.testing.expectEqual(@as(usize, 10), varintLength(std.math.maxInt(u64)));
}

test "truncated varint returns Corruption" {
    // {0x80} alone — continuation bit set but no next byte
    var slice: []const u8 = &[_]u8{0x80};
    try std.testing.expectError(error.Corruption, getVarint32(&slice));

    var slice64: []const u8 = &[_]u8{0x80};
    try std.testing.expectError(error.Corruption, getVarint64(&slice64));
}

test "truncated length-prefixed slice returns Corruption" {
    // claims length 10 but only 3 bytes of payload follow
    const gpa = std.testing.allocator;
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(gpa);
    try putVarint32(&list, gpa, 10); // length prefix = 10
    try list.appendSlice(gpa, "abc"); // only 3 bytes

    var slice: []const u8 = list.items;
    try std.testing.expectError(error.Corruption, getLengthPrefixedSlice(&slice));
}

test "getLengthPrefixedSlice round-trip" {
    const gpa = std.testing.allocator;
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(gpa);

    try putLengthPrefixedSlice(&list, gpa, "world");
    var slice: []const u8 = list.items;
    const decoded = try getLengthPrefixedSlice(&slice);
    try std.testing.expectEqualSlices(u8, "world", decoded);
    try std.testing.expectEqual(@as(usize, 0), slice.len);
}
