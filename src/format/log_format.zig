//! log_format.zig — shared constants and types for the write-ahead log (WAL)
//! and MANIFEST record format.
//!
//! Byte-compatible with the LevelDB/RocksDB *legacy* log record format
//! (db/log_format.h). The recyclable-log format (kRecyclable* types) is a
//! deliberate non-goal here.
//!
//! Physical layout:
//!
//!     block   := record* trailer?
//!     record  := [checksum: u32 LE][length: u16 LE][type: u8][payload: length bytes]
//!
//! A block is exactly `kBlockSize` bytes. Records never span a block boundary
//! without being fragmented: the writer emits one `full` fragment when a whole
//! logical record fits in the remaining block space, otherwise a `first`
//! fragment, zero or more `middle` fragments, and a `last` fragment.
//!
//! If fewer than `kHeaderSize` (7) bytes remain in a block, the writer
//! zero-fills the remainder (the trailer) and starts the next block.

const std = @import("std");
const crc32c = @import("../util/crc32c.zig");

/// Physical block size. Every block is exactly this many bytes (the final
/// block may be short on disk, but logically blocks are this size).
pub const kBlockSize: usize = 32768;

/// Record header size in bytes: u32 checksum + u16 length + u8 type.
pub const kHeaderSize: usize = 4 + 2 + 1;

/// Fragment types. `zero` is reserved (preallocated-but-unwritten file regions
/// in some implementations); the writer never emits it.
pub const RecordType = enum(u8) {
    zero = 0,
    full = 1,
    first = 2,
    middle = 3,
    last = 4,
};

/// Highest valid record type value (LevelDB's kMaxRecordType).
pub const kMaxRecordType: u8 = @intFromEnum(RecordType.last);

/// Compute the masked CRC32C stored in a record header.
///
/// The checksum covers the single type byte concatenated with the fragment
/// payload bytes: `mask(crc32c([type] ++ payload))`. This matches LevelDB's
/// `log::Writer::EmitPhysicalRecord`, which seeds the CRC with the type byte
/// and extends it over the payload.
pub fn checksum(record_type: RecordType, payload: []const u8) u32 {
    const type_byte = [_]u8{@intFromEnum(record_type)};
    const crc = crc32c.extend(crc32c.value(&type_byte), payload);
    return crc32c.mask(crc);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "constants match the legacy format" {
    try std.testing.expectEqual(@as(usize, 32768), kBlockSize);
    try std.testing.expectEqual(@as(usize, 7), kHeaderSize);
    try std.testing.expectEqual(@as(u8, 4), kMaxRecordType);
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(RecordType.zero));
    try std.testing.expectEqual(@as(u8, 1), @intFromEnum(RecordType.full));
    try std.testing.expectEqual(@as(u8, 2), @intFromEnum(RecordType.first));
    try std.testing.expectEqual(@as(u8, 3), @intFromEnum(RecordType.middle));
    try std.testing.expectEqual(@as(u8, 4), @intFromEnum(RecordType.last));
}

test "checksum equals mask(crc over [type] ++ payload)" {
    // full record, payload "hello": crc over {1} ++ "hello".
    const expected = crc32c.mask(crc32c.value(&[_]u8{1} ++ "hello".*));
    try std.testing.expectEqual(expected, checksum(.full, "hello"));
}

test "checksum of empty payload covers only the type byte" {
    const expected = crc32c.mask(crc32c.value(&[_]u8{@intFromEnum(RecordType.full)}));
    try std.testing.expectEqual(expected, checksum(.full, ""));
}
