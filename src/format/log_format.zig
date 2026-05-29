//! log_format.zig — shared constants and types for the write-ahead log (WAL)
//! and MANIFEST record format.
//!
//! Byte-compatible with the LevelDB/RocksDB *legacy* log record format
//! (db/log_format.h) AND with the RocksDB *recyclable* log record format
//! (kRecyclable* types 5-8). The legacy format remains the default and is
//! always readable; the recyclable format is supported additively for
//! interop and for writers that recycle log files.
//!
//! Physical layout (legacy, types 1-4):
//!
//!     block   := record* trailer?
//!     record  := [checksum: u32 LE][length: u16 LE][type: u8][payload: length bytes]
//!
//! Physical layout (recyclable, types 5-8):
//!
//!     record  := [checksum: u32 LE][length: u16 LE][type: u8]
//!                [log_number: u32 LE][payload: length bytes]
//!
//! The recyclable header is `kRecyclableHeaderSize` (11) bytes: the legacy
//! 7-byte header plus a 4-byte little-endian log number. The log number lets a
//! reader reject stale records left over in a recycled (reused) log file: a
//! record whose log_number does not match the expected one is treated as the
//! end of the current log's data.
//!
//! A block is exactly `kBlockSize` bytes. Records never span a block boundary
//! without being fragmented: the writer emits one `full` fragment when a whole
//! logical record fits in the remaining block space, otherwise a `first`
//! fragment, zero or more `middle` fragments, and a `last` fragment.
//!
//! If fewer than the active header size remains in a block, the writer
//! zero-fills the remainder (the trailer) and starts the next block.

const std = @import("std");
const crc32c = @import("../util/crc32c.zig");

/// Physical block size. Every block is exactly this many bytes (the final
/// block may be short on disk, but logically blocks are this size).
pub const kBlockSize: usize = 32768;

/// Legacy record header size in bytes: u32 checksum + u16 length + u8 type.
pub const kHeaderSize: usize = 4 + 2 + 1;

/// Recyclable record header size in bytes: legacy header + u32 log_number.
pub const kRecyclableHeaderSize: usize = kHeaderSize + 4;

/// Fragment types. `zero` is reserved (preallocated-but-unwritten file regions
/// in some implementations); the writer never emits it.
///
/// Types 1-4 are the legacy LevelDB fragment kinds. Types 5-8 are the RocksDB
/// recyclable-log fragment kinds, which carry an extra log_number in their
/// 11-byte header. They mirror the legacy kinds (full/first/middle/last) but
/// are distinguished so the reader knows to parse the longer header.
pub const RecordType = enum(u8) {
    zero = 0,
    full = 1,
    first = 2,
    middle = 3,
    last = 4,
    recyclable_full = 5,
    recyclable_first = 6,
    recyclable_middle = 7,
    recyclable_last = 8,
};

/// Highest valid legacy record type value (LevelDB's kMaxRecordType).
pub const kMaxRecordType: u8 = @intFromEnum(RecordType.last);

/// Highest valid recyclable record type value (RocksDB's kMaxRecordType when
/// recycling is enabled).
pub const kMaxRecyclableRecordType: u8 = @intFromEnum(RecordType.recyclable_last);

/// True for the recyclable fragment kinds (types 5-8) that carry a log_number.
pub fn isRecyclable(record_type: RecordType) bool {
    return switch (record_type) {
        .recyclable_full, .recyclable_first, .recyclable_middle, .recyclable_last => true,
        else => false,
    };
}

/// Header size for a given record type: 11 bytes for recyclable kinds, 7 for
/// the legacy kinds (and `zero`).
pub fn headerSizeFor(record_type: RecordType) usize {
    return if (isRecyclable(record_type)) kRecyclableHeaderSize else kHeaderSize;
}

/// Compute the masked CRC32C stored in a legacy record header.
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

/// Compute the masked CRC32C stored in a recyclable record header.
///
/// The checksum covers the type byte, the 4-byte little-endian log number, and
/// the fragment payload: `mask(crc32c([type] ++ log_number_LE ++ payload))`.
/// This matches RocksDB's `log::Writer::EmitPhysicalRecord` for recyclable
/// records, which extends the type-seeded CRC over the log_number field of the
/// header and then over the payload.
pub fn recyclableChecksum(record_type: RecordType, log_number: u32, payload: []const u8) u32 {
    var log_number_le: [4]u8 = undefined;
    std.mem.writeInt(u32, &log_number_le, log_number, .little);
    const type_byte = [_]u8{@intFromEnum(record_type)};
    var crc = crc32c.value(&type_byte);
    crc = crc32c.extend(crc, &log_number_le);
    crc = crc32c.extend(crc, payload);
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

test "recyclable constants and type classification" {
    try std.testing.expectEqual(@as(usize, 11), kRecyclableHeaderSize);
    try std.testing.expectEqual(@as(u8, 5), @intFromEnum(RecordType.recyclable_full));
    try std.testing.expectEqual(@as(u8, 6), @intFromEnum(RecordType.recyclable_first));
    try std.testing.expectEqual(@as(u8, 7), @intFromEnum(RecordType.recyclable_middle));
    try std.testing.expectEqual(@as(u8, 8), @intFromEnum(RecordType.recyclable_last));
    try std.testing.expectEqual(@as(u8, 8), kMaxRecyclableRecordType);

    // Legacy kinds are not recyclable; recyclable kinds are.
    try std.testing.expect(!isRecyclable(.full));
    try std.testing.expect(!isRecyclable(.first));
    try std.testing.expect(!isRecyclable(.last));
    try std.testing.expect(!isRecyclable(.zero));
    try std.testing.expect(isRecyclable(.recyclable_full));
    try std.testing.expect(isRecyclable(.recyclable_first));
    try std.testing.expect(isRecyclable(.recyclable_middle));
    try std.testing.expect(isRecyclable(.recyclable_last));

    // Header size depends on the kind.
    try std.testing.expectEqual(kHeaderSize, headerSizeFor(.full));
    try std.testing.expectEqual(kRecyclableHeaderSize, headerSizeFor(.recyclable_full));
}

test "recyclable checksum equals mask(crc over [type] ++ log_number_LE ++ payload)" {
    const log_number: u32 = 0x04030201;
    var log_number_le: [4]u8 = undefined;
    std.mem.writeInt(u32, &log_number_le, log_number, .little);

    // recyclable_full (5), log_number, payload "hello".
    var crc = crc32c.value(&[_]u8{5});
    crc = crc32c.extend(crc, &log_number_le);
    crc = crc32c.extend(crc, "hello");
    const expected = crc32c.mask(crc);

    try std.testing.expectEqual(
        expected,
        recyclableChecksum(.recyclable_full, log_number, "hello"),
    );
}

test "recyclable checksum of empty payload covers type byte and log_number" {
    const log_number: u32 = 42;
    var log_number_le: [4]u8 = undefined;
    std.mem.writeInt(u32, &log_number_le, log_number, .little);
    var crc = crc32c.value(&[_]u8{@intFromEnum(RecordType.recyclable_full)});
    crc = crc32c.extend(crc, &log_number_le);
    const expected = crc32c.mask(crc);
    try std.testing.expectEqual(expected, recyclableChecksum(.recyclable_full, log_number, ""));
}
