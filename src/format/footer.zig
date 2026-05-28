/// footer.zig — RocksDB block-based table footer (format_version 5).
/// Byte-compatible with RocksDB's table/format.h / table/format.cc.
/// 53-byte non-legacy footer layout:
///   byte[0]        : checksum_type (u8)
///   bytes[1..41]   : metaindex_handle ++ index_handle (varint-encoded), zero-padded to 40 bytes
///   bytes[41..45]  : format_version (fixed32 LE)
///   bytes[45..53]  : table magic number (fixed64 LE)
const std = @import("std");
const coding = @import("../util/coding.zig");

// ---------------------------------------------------------------------------
// BlockHandle
// ---------------------------------------------------------------------------

pub const BlockHandle = struct {
    offset: u64,
    size: u64,

    /// Maximum encoded length: 2 × varint64 max (10 bytes each).
    pub const kMaxEncodedLength: usize = 20;

    /// Append varint encoding of offset then size to buf.
    pub fn encodeTo(self: BlockHandle, buf: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator) !void {
        _ = self;
        _ = buf;
        _ = gpa;
        @panic("TODO: implement BlockHandle.encodeTo");
    }

    /// Decode a BlockHandle from the front of input, advancing input past the consumed bytes.
    /// Returns error.Corruption on truncation or malformed varint.
    pub fn decodeFrom(input: *[]const u8) !BlockHandle {
        _ = input;
        @panic("TODO: implement BlockHandle.decodeFrom");
    }
};

// ---------------------------------------------------------------------------
// Footer constants
// ---------------------------------------------------------------------------

pub const kEncodedLength: usize = 53;
pub const kBlockBasedTableMagicNumber: u64 = 0x88e241b785f4cff7;

pub const ChecksumType = enum(u8) {
    none = 0,
    crc32c = 1,
    xxhash = 2,
    xxhash64 = 3,
    xxh3 = 4,
};

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------

pub const Footer = struct {
    metaindex_handle: BlockHandle,
    index_handle: BlockHandle,
    format_version: u32 = 5,
    checksum_type: ChecksumType = .crc32c,

    /// Encode the footer to exactly 53 bytes, appended to buf.
    pub fn encodeTo(self: Footer, buf: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator) !void {
        _ = self;
        _ = buf;
        _ = gpa;
        @panic("TODO: implement Footer.encodeTo");
    }

    /// Decode a Footer from data (must be >= kEncodedLength bytes).
    /// Reads the magic from the LAST 8 bytes; returns error.BadMagic if wrong.
    pub fn decodeFrom(data: []const u8) !Footer {
        _ = data;
        @panic("TODO: implement Footer.decodeFrom");
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "BlockHandle zero encodes to two zero bytes" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);

    const h = BlockHandle{ .offset = 0, .size = 0 };
    try h.encodeTo(&buf, gpa);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x00 }, buf.items);
}

test "BlockHandle round-trip offset=255 size=128" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);

    const h = BlockHandle{ .offset = 255, .size = 128 };
    try h.encodeTo(&buf, gpa);

    var slice: []const u8 = buf.items;
    const decoded = try BlockHandle.decodeFrom(&slice);
    try std.testing.expectEqual(h.offset, decoded.offset);
    try std.testing.expectEqual(h.size, decoded.size);
    try std.testing.expectEqual(@as(usize, 0), slice.len);
}

test "BlockHandle decodeFrom truncated returns error" {
    var slice: []const u8 = &[_]u8{0x80}; // continuation bit set, no terminator
    try std.testing.expectError(error.Corruption, BlockHandle.decodeFrom(&slice));
}

test "Footer encodeTo produces exactly 53 bytes with golden magic" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);

    const footer = Footer{
        .metaindex_handle = .{ .offset = 10, .size = 5 },
        .index_handle = .{ .offset = 20, .size = 7 },
        .format_version = 5,
        .checksum_type = .crc32c,
    };

    try footer.encodeTo(&buf, gpa);
    try std.testing.expectEqual(@as(usize, 53), buf.items.len);

    // byte[0] == 1 (crc32c)
    try std.testing.expectEqual(@as(u8, 1), buf.items[0]);

    // bytes[41..45] == fixed32 LE of 5 == {0x05, 0x00, 0x00, 0x00}
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x05, 0x00, 0x00, 0x00 }, buf.items[41..45]);

    // last 8 bytes == fixed64 LE of 0x88e241b785f4cff7
    const golden_magic = [_]u8{ 0xf7, 0xcf, 0xf4, 0x85, 0xb7, 0x41, 0xe2, 0x88 };
    try std.testing.expectEqualSlices(u8, &golden_magic, buf.items[45..53]);
}

test "Footer round-trip" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);

    const footer = Footer{
        .metaindex_handle = .{ .offset = 10, .size = 5 },
        .index_handle = .{ .offset = 20, .size = 7 },
        .format_version = 5,
        .checksum_type = .crc32c,
    };

    try footer.encodeTo(&buf, gpa);

    const decoded = try Footer.decodeFrom(buf.items);
    try std.testing.expectEqual(footer.metaindex_handle.offset, decoded.metaindex_handle.offset);
    try std.testing.expectEqual(footer.metaindex_handle.size, decoded.metaindex_handle.size);
    try std.testing.expectEqual(footer.index_handle.offset, decoded.index_handle.offset);
    try std.testing.expectEqual(footer.index_handle.size, decoded.index_handle.size);
    try std.testing.expectEqual(footer.format_version, decoded.format_version);
    try std.testing.expectEqual(footer.checksum_type, decoded.checksum_type);
}

test "Footer decodeFrom bad magic returns error" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);

    const footer = Footer{
        .metaindex_handle = .{ .offset = 10, .size = 5 },
        .index_handle = .{ .offset = 20, .size = 7 },
        .format_version = 5,
        .checksum_type = .crc32c,
    };

    try footer.encodeTo(&buf, gpa);

    // Corrupt the last 8 bytes (magic)
    buf.items[45] = 0xDE;
    buf.items[46] = 0xAD;

    try std.testing.expectError(error.BadMagic, Footer.decodeFrom(buf.items));
}

test "Footer decodeFrom too short returns error" {
    const data = [_]u8{0} ** 10;
    try std.testing.expectError(error.Corruption, Footer.decodeFrom(&data));
}
