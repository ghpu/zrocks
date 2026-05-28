/// internal_key.zig — RocksDB-compatible InternalKey format and comparator.
/// Byte-compatible with RocksDB's db/dbformat.h / db/dbformat.cc.
///
/// RED phase: type declarations + full tests; all logic functions are stubs.
/// Import as named modules for standalone test:
///   zig test --dep coding --dep comparator \
///       -Mtest=src/format/internal_key.zig \
///       -Mcoding=src/util/coding.zig \
///       -Mcomparator=src/util/comparator.zig
const std = @import("std");
const coding = @import("coding");
const comparator = @import("comparator");

// ---------------------------------------------------------------------------
// ValueType
// ---------------------------------------------------------------------------

pub const ValueType = enum(u8) {
    deletion = 0x0,
    value = 0x1,
    merge = 0x2,
    single_deletion = 0x7,
    range_deletion = 0xF,
};

pub const kMaxSequenceNumber: u64 = (1 << 56) - 1;

/// The value type used when constructing a lookup key for seeks.
pub const kValueTypeForSeek: ValueType = .value;

// ---------------------------------------------------------------------------
// Trailer packing / unpacking — STUBS
// ---------------------------------------------------------------------------

pub fn packSequenceAndType(seq: u64, t: ValueType) u64 {
    _ = seq;
    _ = t;
    @panic("TODO: implement packSequenceAndType");
}

pub const UnpackedTrailer = struct { sequence: u64, value_type: ValueType };

pub fn unpackSequenceAndType(trailer: u64) UnpackedTrailer {
    _ = trailer;
    @panic("TODO: implement unpackSequenceAndType");
}

// ---------------------------------------------------------------------------
// ParsedInternalKey
// ---------------------------------------------------------------------------

pub const ParsedInternalKey = struct {
    user_key: []const u8,
    sequence: u64,
    type: ValueType,
};

// ---------------------------------------------------------------------------
// appendInternalKey — STUB
// ---------------------------------------------------------------------------

pub fn appendInternalKey(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, pik: ParsedInternalKey) !void {
    _ = buf;
    _ = gpa;
    _ = pik;
    @panic("TODO: implement appendInternalKey");
}

// ---------------------------------------------------------------------------
// parseInternalKey — STUB
// ---------------------------------------------------------------------------

pub fn parseInternalKey(internal_key: []const u8) error{Corruption}!ParsedInternalKey {
    _ = internal_key;
    @panic("TODO: implement parseInternalKey");
}

// ---------------------------------------------------------------------------
// extractUserKey — STUB
// ---------------------------------------------------------------------------

pub fn extractUserKey(internal_key: []const u8) []const u8 {
    _ = internal_key;
    @panic("TODO: implement extractUserKey");
}

// ---------------------------------------------------------------------------
// InternalKeyComparator — STUB
// ---------------------------------------------------------------------------

pub const InternalKeyComparator = struct {
    user: comparator.Comparator,

    pub fn comparatorInterface(self: *const InternalKeyComparator) comparator.Comparator {
        _ = self;
        @panic("TODO: implement comparatorInterface");
    }
};

// ---------------------------------------------------------------------------
// Tests (RED — these will panic at runtime on stub functions)
// ---------------------------------------------------------------------------

test "ValueType enum values" {
    try std.testing.expectEqual(@as(u8, 0x0), @intFromEnum(ValueType.deletion));
    try std.testing.expectEqual(@as(u8, 0x1), @intFromEnum(ValueType.value));
    try std.testing.expectEqual(@as(u8, 0x2), @intFromEnum(ValueType.merge));
    try std.testing.expectEqual(@as(u8, 0x7), @intFromEnum(ValueType.single_deletion));
    try std.testing.expectEqual(@as(u8, 0xF), @intFromEnum(ValueType.range_deletion));
}

test "kMaxSequenceNumber" {
    try std.testing.expectEqual(@as(u64, (1 << 56) - 1), kMaxSequenceNumber);
}

test "packSequenceAndType golden: seq=1, type=value => 0x101" {
    try std.testing.expectEqual(@as(u64, 0x101), packSequenceAndType(1, .value));
}

test "packSequenceAndType round-trip via unpack" {
    const trailer = packSequenceAndType(0xABCDEF, .merge);
    const result = unpackSequenceAndType(trailer);
    try std.testing.expectEqual(@as(u64, 0xABCDEF), result.sequence);
    try std.testing.expectEqual(ValueType.merge, result.value_type);
}

test "packSequenceAndType: kMaxSequenceNumber round-trip" {
    const trailer = packSequenceAndType(kMaxSequenceNumber, .deletion);
    const result = unpackSequenceAndType(trailer);
    try std.testing.expectEqual(kMaxSequenceNumber, result.sequence);
    try std.testing.expectEqual(ValueType.deletion, result.value_type);
}

test "appendInternalKey golden: foo seq=1 value" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);

    const pik = ParsedInternalKey{ .user_key = "foo", .sequence = 1, .type = .value };
    try appendInternalKey(&buf, gpa, pik);

    // packed = (1 << 8) | 1 = 0x0000000000000101
    // LE bytes: 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    const expected = "foo" ++ [_]u8{ 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    try std.testing.expectEqualSlices(u8, expected, buf.items);
}

test "parseInternalKey round-trips appendInternalKey" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);

    const pik = ParsedInternalKey{ .user_key = "hello", .sequence = 42, .type = .merge };
    try appendInternalKey(&buf, gpa, pik);

    const parsed = try parseInternalKey(buf.items);
    try std.testing.expectEqualSlices(u8, "hello", parsed.user_key);
    try std.testing.expectEqual(@as(u64, 42), parsed.sequence);
    try std.testing.expectEqual(ValueType.merge, parsed.type);
}

test "parseInternalKey: too-short returns Corruption" {
    try std.testing.expectError(error.Corruption, parseInternalKey(""));
    try std.testing.expectError(error.Corruption, parseInternalKey("short")); // 5 bytes
    try std.testing.expectError(error.Corruption, parseInternalKey("1234567")); // 7 bytes
}

test "parseInternalKey: unknown type byte returns Corruption" {
    // Craft an 8-byte internal key whose low trailer byte = 0x03 (not a valid ValueType).
    var buf: [8]u8 = [_]u8{0} ** 8;
    coding.encodeFixed64(&buf, 0x03);
    try std.testing.expectError(error.Corruption, parseInternalKey(&buf));
}

test "extractUserKey: returns user key portion" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);

    const pik = ParsedInternalKey{ .user_key = "foo", .sequence = 7, .type = .value };
    try appendInternalKey(&buf, gpa, pik);

    const uk = extractUserKey(buf.items);
    try std.testing.expectEqualSlices(u8, "foo", uk);
}

test "InternalKeyComparator: different user keys order by user key" {
    const gpa = std.testing.allocator;
    var a_buf: std.ArrayList(u8) = .empty;
    defer a_buf.deinit(gpa);
    var b_buf: std.ArrayList(u8) = .empty;
    defer b_buf.deinit(gpa);

    try appendInternalKey(&a_buf, gpa, .{ .user_key = "a", .sequence = 5, .type = .value });
    try appendInternalKey(&b_buf, gpa, .{ .user_key = "b", .sequence = 1, .type = .value });

    const ikc = InternalKeyComparator{ .user = comparator.bytewise };
    const cmp = ikc.comparatorInterface();
    try std.testing.expectEqual(std.math.Order.lt, cmp.compare(a_buf.items, b_buf.items));
    try std.testing.expectEqual(std.math.Order.gt, cmp.compare(b_buf.items, a_buf.items));
}

test "InternalKeyComparator: same user key, higher seq sorts first" {
    const gpa = std.testing.allocator;
    var hi_buf: std.ArrayList(u8) = .empty;
    defer hi_buf.deinit(gpa);
    var lo_buf: std.ArrayList(u8) = .empty;
    defer lo_buf.deinit(gpa);

    try appendInternalKey(&hi_buf, gpa, .{ .user_key = "foo", .sequence = 5, .type = .value });
    try appendInternalKey(&lo_buf, gpa, .{ .user_key = "foo", .sequence = 2, .type = .value });

    const ikc = InternalKeyComparator{ .user = comparator.bytewise };
    const cmp = ikc.comparatorInterface();
    try std.testing.expectEqual(std.math.Order.lt, cmp.compare(hi_buf.items, lo_buf.items));
    try std.testing.expectEqual(std.math.Order.gt, cmp.compare(lo_buf.items, hi_buf.items));
}

test "InternalKeyComparator: same user key & seq, value sorts before deletion" {
    const gpa = std.testing.allocator;
    var val_buf: std.ArrayList(u8) = .empty;
    defer val_buf.deinit(gpa);
    var del_buf: std.ArrayList(u8) = .empty;
    defer del_buf.deinit(gpa);

    const seq: u64 = 10;
    try appendInternalKey(&val_buf, gpa, .{ .user_key = "foo", .sequence = seq, .type = .value });
    try appendInternalKey(&del_buf, gpa, .{ .user_key = "foo", .sequence = seq, .type = .deletion });

    const ikc = InternalKeyComparator{ .user = comparator.bytewise };
    const cmp = ikc.comparatorInterface();
    try std.testing.expectEqual(std.math.Order.lt, cmp.compare(val_buf.items, del_buf.items));
    try std.testing.expectEqual(std.math.Order.gt, cmp.compare(del_buf.items, val_buf.items));
}

test "InternalKeyComparator: identical internal keys => .eq" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);

    try appendInternalKey(&buf, gpa, .{ .user_key = "foo", .sequence = 3, .type = .value });

    const ikc = InternalKeyComparator{ .user = comparator.bytewise };
    const cmp = ikc.comparatorInterface();
    try std.testing.expectEqual(std.math.Order.eq, cmp.compare(buf.items, buf.items));
}

test "InternalKeyComparator name" {
    const ikc = InternalKeyComparator{ .user = comparator.bytewise };
    const cmp = ikc.comparatorInterface();
    try std.testing.expectEqualStrings("leveldb.InternalKeyComparator", cmp.name());
}

test "InternalKeyComparator findShortestSeparator re-appends seek trailer" {
    const gpa = std.testing.allocator;

    var start: std.ArrayList(u8) = .empty;
    defer start.deinit(gpa);
    try appendInternalKey(&start, gpa, .{ .user_key = "abc", .sequence = 5, .type = .value });

    var limit_buf: std.ArrayList(u8) = .empty;
    defer limit_buf.deinit(gpa);
    try appendInternalKey(&limit_buf, gpa, .{ .user_key = "abz", .sequence = 3, .type = .value });

    const ikc = InternalKeyComparator{ .user = comparator.bytewise };
    const cmp = ikc.comparatorInterface();
    cmp.findShortestSeparator(&start, limit_buf.items);

    try std.testing.expect(start.items.len >= 8);

    const seek_trailer = packSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek);
    var expected_trailer: [8]u8 = undefined;
    coding.encodeFixed64(&expected_trailer, seek_trailer);
    const actual_trailer = start.items[start.items.len - 8 ..];
    try std.testing.expectEqualSlices(u8, &expected_trailer, actual_trailer);
}

test "InternalKeyComparator findShortSuccessor re-appends seek trailer" {
    const gpa = std.testing.allocator;

    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(gpa);
    try appendInternalKey(&key, gpa, .{ .user_key = "abc", .sequence = 5, .type = .value });

    const ikc = InternalKeyComparator{ .user = comparator.bytewise };
    const cmp = ikc.comparatorInterface();
    cmp.findShortSuccessor(&key);

    try std.testing.expect(key.items.len >= 8);

    const seek_trailer = packSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek);
    var expected_trailer: [8]u8 = undefined;
    coding.encodeFixed64(&expected_trailer, seek_trailer);
    const actual_trailer = key.items[key.items.len - 8 ..];
    try std.testing.expectEqualSlices(u8, &expected_trailer, actual_trailer);
}
