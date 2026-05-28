/// coding.zig — LevelDB/RocksDB-compatible varint and fixed-width encoding.
/// Byte-compatible with RocksDB's util/coding.h / util/coding.cc.
const std = @import("std");

pub const Error = error{Corruption};

// ---------------------------------------------------------------------------
// Fixed-width little-endian helpers
// ---------------------------------------------------------------------------

pub fn encodeFixed32(dst: *[4]u8, v: u32) void {
    std.mem.writeInt(u32, dst, v, .little);
}

pub fn decodeFixed32(src: *const [4]u8) u32 {
    return std.mem.readInt(u32, src, .little);
}

pub fn encodeFixed64(dst: *[8]u8, v: u64) void {
    std.mem.writeInt(u64, dst, v, .little);
}

pub fn decodeFixed64(src: *const [8]u8) u64 {
    return std.mem.readInt(u64, src, .little);
}

// ---------------------------------------------------------------------------
// ArrayList append helpers
// ---------------------------------------------------------------------------

pub fn putFixed32(list: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator, v: u32) !void {
    var buf: [4]u8 = undefined;
    encodeFixed32(&buf, v);
    try list.appendSlice(gpa, &buf);
}

pub fn putFixed64(list: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator, v: u64) !void {
    var buf: [8]u8 = undefined;
    encodeFixed64(&buf, v);
    try list.appendSlice(gpa, &buf);
}

pub fn putVarint32(list: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator, v: u32) !void {
    // Promote to u64 to share the varint64 encoder.
    try putVarint64(list, gpa, @as(u64, v));
}

pub fn putVarint64(list: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator, v: u64) !void {
    var val = v;
    while (true) {
        const byte: u8 = @intCast(val & 0x7f);
        val >>= 7;
        if (val == 0) {
            try list.append(gpa, byte);
            break;
        } else {
            try list.append(gpa, byte | 0x80);
        }
    }
}

pub fn putLengthPrefixedSlice(
    list: *std.ArrayListUnmanaged(u8),
    gpa: std.mem.Allocator,
    data: []const u8,
) !void {
    try putVarint32(list, gpa, @intCast(data.len));
    try list.appendSlice(gpa, data);
}

// ---------------------------------------------------------------------------
// Slice-consuming decoders (advance input past consumed bytes)
// ---------------------------------------------------------------------------

pub fn getVarint32(input: *[]const u8) Error!u32 {
    const result = try getVarint64(input);
    // Values that overflow u32 are corrupt for a varint32 field.
    if (result > 0xffffffff) return error.Corruption;
    return @intCast(result);
}

pub fn getVarint64(input: *[]const u8) Error!u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    const src = input.*;
    while (i < src.len) {
        const byte = src[i];
        i += 1;
        if (shift == 63 and byte > 1) {
            // Would overflow 64 bits.
            return error.Corruption;
        }
        result |= @as(u64, byte & 0x7f) << shift;
        if (byte & 0x80 == 0) {
            input.* = src[i..];
            return result;
        }
        // Varint64 is at most 10 bytes (ceil(64/7)=10).
        if (i >= 10) return error.Corruption;
        shift += 7;
    }
    // Ran out of bytes without a terminating byte.
    return error.Corruption;
}

pub fn getLengthPrefixedSlice(input: *[]const u8) Error![]const u8 {
    const len = try getVarint32(input);
    if (len > input.len) return error.Corruption;
    const data = input.*[0..len];
    input.* = input.*[len..];
    return data;
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

pub fn varintLength(v: u64) usize {
    var val = v;
    var len: usize = 1;
    while (val >= 0x80) {
        val >>= 7;
        len += 1;
    }
    return len;
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
        try std.testing.expectEqual(@as(usize, 0), slice.len); // fully consumed
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
