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
        try coding.putVarint64(buf, gpa, self.offset);
        try coding.putVarint64(buf, gpa, self.size);
    }

    /// Decode a BlockHandle from the front of input, advancing input past the consumed bytes.
    /// Returns error.Corruption on truncation or malformed varint.
    pub fn decodeFrom(input: *[]const u8) !BlockHandle {
        const offset = try coding.getVarint64(input);
        const size = try coding.getVarint64(input);
        return BlockHandle{ .offset = offset, .size = size };
    }
};

// ---------------------------------------------------------------------------
// Footer constants
// ---------------------------------------------------------------------------

/// Total size of an encoded footer in bytes.
pub const kEncodedLength: usize = 53;
/// RocksDB block-based table magic number (identifies file type).
pub const kBlockBasedTableMagicNumber: u64 = 0x88e241b785f4cff7;
/// Width of the handles region within the footer (bytes[1..41]).
const kHandlesRegionSize: usize = 40;

pub const ChecksumType = enum(u8) {
    none = 0,
    crc32c = 1,
    xxhash = 2,
    xxhash64 = 3,
    xxh3 = 4,

    /// Parse from a raw byte; returns error.Corruption for unknown values.
    /// Uses inline-for because std.meta.intToEnum was removed in Zig 0.16.
    pub fn fromInt(v: u8) error{Corruption}!ChecksumType {
        inline for (std.meta.fields(ChecksumType)) |f| {
            if (f.value == v) return @field(ChecksumType, f.name);
        }
        return error.Corruption;
    }
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
    /// Layout:
    ///   [0]      checksum_type (u8)
    ///   [1..41]  metaindex_handle ++ index_handle (varint), zero-padded to 40 bytes
    ///   [41..45] format_version (fixed32 LE)
    ///   [45..53] magic number (fixed64 LE)
    pub fn encodeTo(self: Footer, buf: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator) !void {
        const start = buf.items.len;

        // byte[0]: checksum type
        try buf.append(gpa, @intFromEnum(self.checksum_type));

        // bytes[1..41]: handles region (40 bytes, zero-padded)
        const handles_start = buf.items.len;
        try self.metaindex_handle.encodeTo(buf, gpa);
        try self.index_handle.encodeTo(buf, gpa);
        const handles_written = buf.items.len - handles_start;
        // zero-pad to kHandlesRegionSize bytes
        const pad = kHandlesRegionSize - handles_written;
        try buf.appendNTimes(gpa, 0, pad);

        // bytes[41..45]: format_version (fixed32 LE)
        var fv_buf: [4]u8 = undefined;
        coding.encodeFixed32(&fv_buf, self.format_version);
        try buf.appendSlice(gpa, &fv_buf);

        // bytes[45..53]: magic number (fixed64 LE)
        var magic_buf: [8]u8 = undefined;
        coding.encodeFixed64(&magic_buf, kBlockBasedTableMagicNumber);
        try buf.appendSlice(gpa, &magic_buf);

        std.debug.assert(buf.items.len - start == kEncodedLength);
    }

    /// Decode a Footer from data (must be >= kEncodedLength bytes).
    /// Reads the magic from the LAST 8 bytes; returns error.BadMagic if wrong.
    /// Reads format_version from bytes[len-12..len-8].
    /// Reads checksum_type from byte[0].
    /// Parses the two handles from bytes[1..41].
    pub fn decodeFrom(data: []const u8) !Footer {
        if (data.len < kEncodedLength) return error.Corruption;

        // Verify magic from last 8 bytes
        const magic_bytes: *const [8]u8 = data[data.len - 8 ..][0..8];
        const magic = coding.decodeFixed64(magic_bytes);
        if (magic != kBlockBasedTableMagicNumber) return error.BadMagic;

        // Read format_version from bytes[len-12..len-8]
        const fv_bytes: *const [4]u8 = data[data.len - 12 ..][0..4];
        const format_version = coding.decodeFixed32(fv_bytes);

        // Read checksum_type from byte[0]
        const checksum_type = try ChecksumType.fromInt(data[0]);

        // Parse the two handles from bytes[1..1+kHandlesRegionSize]
        var handles_slice: []const u8 = data[1 .. 1 + kHandlesRegionSize];
        const metaindex_handle = try BlockHandle.decodeFrom(&handles_slice);
        const index_handle = try BlockHandle.decodeFrom(&handles_slice);

        return Footer{
            .metaindex_handle = metaindex_handle,
            .index_handle = index_handle,
            .format_version = format_version,
            .checksum_type = checksum_type,
        };
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
