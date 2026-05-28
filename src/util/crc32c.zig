// CRC32C (Castagnoli) utilities — byte-compatible with RocksDB/LevelDB.
//
// RocksDB stores a *masked* CRC in WAL record headers and SST block trailers
// so that a checksum embedded in data doesn't happen to equal the checksum of
// that data itself.

const std = @import("std");

// --------------------------------------------------------------------------
// Public API — stubs (RED state: tests will fail)
// --------------------------------------------------------------------------

pub fn value(data: []const u8) u32 {
    _ = data;
    return 0xdeadbeef; // stub
}

pub fn extend(crc: u32, data: []const u8) u32 {
    _ = crc;
    _ = data;
    return 0xdeadbeef; // stub
}

pub const kMaskDelta: u32 = 0xa282ead8;

pub fn mask(crc: u32) u32 {
    _ = crc;
    return 0xdeadbeef; // stub
}

pub fn unmask(masked_crc: u32) u32 {
    _ = masked_crc;
    return 0xdeadbeef; // stub
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

test "value(\"\") == 0" {
    try std.testing.expectEqual(@as(u32, 0x00000000), value(""));
}

test "value(\"123456789\") == CRC-32/ISCSI check value" {
    try std.testing.expectEqual(@as(u32, 0xe3069283), value("123456789"));
}

test "extend splits correctly across \"12345\" / \"6789\"" {
    const full = value("123456789");
    const split = extend(value("12345"), "6789");
    try std.testing.expectEqual(full, split);
    try std.testing.expectEqual(@as(u32, 0xe3069283), split);
}

test "mask/unmask round-trip on empty CRC" {
    const crc = value("");
    try std.testing.expectEqual(crc, unmask(mask(crc)));
}

test "mask/unmask round-trip on \"123456789\" CRC" {
    const crc = value("123456789");
    try std.testing.expectEqual(crc, unmask(mask(crc)));
    // mask must actually change a non-zero CRC
    try std.testing.expect(mask(crc) != crc);
}
