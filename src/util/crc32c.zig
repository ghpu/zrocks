// CRC32C (Castagnoli) utilities — byte-compatible with RocksDB/LevelDB.
//
// RocksDB stores a *masked* CRC in WAL record headers and SST block trailers
// so that a checksum embedded in data doesn't happen to equal the checksum of
// that data itself.

const std = @import("std");
const Crc32Iscsi = std.hash.crc.Crc32Iscsi;

// --------------------------------------------------------------------------
// Public API
// --------------------------------------------------------------------------

/// Compute CRC32C of data.
pub fn value(data: []const u8) u32 {
    return Crc32Iscsi.hash(data);
}

/// Continue a CRC32C computation over additional data.
///
/// Satisfies: extend(value(a), b) == value(a ++ b)
///
/// Implementation note: Crc32Iscsi uses initial=0xffffffff and xor_output=0xffffffff
/// (reflected). The internal register after producing `crc` is `crc ^ 0xffffffff`,
/// so we reconstruct it, feed the new bytes, and call final().
pub fn extend(crc: u32, data: []const u8) u32 {
    var state = Crc32Iscsi{ .crc = crc ^ 0xffffffff };
    state.update(data);
    return state.final();
}

/// Delta used in the mask/unmask rotation (same constant as RocksDB).
pub const kMaskDelta: u32 = 0xa282ead8;

/// Return a masked CRC suitable for embedding in on-disk structures.
/// mask(crc) = rotate_right(crc, 15) + kMaskDelta  (wrapping)
pub fn mask(crc: u32) u32 {
    return ((crc >> 15) | (crc << 17)) +% kMaskDelta;
}

/// Recover the original CRC from a masked value.
pub fn unmask(masked_crc: u32) u32 {
    const rot = masked_crc -% kMaskDelta;
    return (rot >> 17) | (rot << 15);
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
