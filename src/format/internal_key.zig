/// internal_key.zig — RocksDB-compatible InternalKey format and comparator.
/// Byte-compatible with RocksDB's db/dbformat.h / db/dbformat.cc.
///
/// RED phase: declarations with @panic stubs + full tests.
/// Import as named modules when testing standalone:
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

/// Convert a raw u8 to a ValueType, returning an error if the value is not
/// one of the known tag values. Used instead of std.meta.intToEnum (removed
/// in Zig 0.16).
fn valueTypeFromInt(v: u8) error{Corruption}!ValueType {
    inline for (std.meta.fields(ValueType)) |f| {
        if (f.value == v) return @field(ValueType, f.name);
    }
    return error.Corruption;
}

pub const kMaxSequenceNumber: u64 = (1 << 56) - 1;

/// The value type used when constructing a lookup key for seeks.
/// RocksDB uses kTypeValue here — it has the highest numeric value among the
/// non-special types at any given sequence, so seeks with this type will be
/// ordered before any deletion at the same seq.
pub const kValueTypeForSeek: ValueType = .value;

// ---------------------------------------------------------------------------
// Trailer packing / unpacking
// ---------------------------------------------------------------------------

/// packed trailer = (sequence << 8) | value_type_byte
pub fn packSequenceAndType(seq: u64, t: ValueType) u64 {
    return (seq << 8) | @as(u64, @intFromEnum(t));
}

pub const UnpackedTrailer = struct { sequence: u64, value_type: ValueType };

/// Decode a packed trailer back into sequence and value type.
/// Panics if the type byte is not a known ValueType — callers should use
/// parseInternalKey which returns error.Corruption instead.
pub fn unpackSequenceAndType(trailer: u64) UnpackedTrailer {
    const type_byte: u8 = @intCast(trailer & 0xff);
    const seq: u64 = trailer >> 8;
    const vt = valueTypeFromInt(type_byte) catch unreachable;
    return .{ .sequence = seq, .value_type = vt };
}

// ---------------------------------------------------------------------------
// ParsedInternalKey
// ---------------------------------------------------------------------------

pub const ParsedInternalKey = struct {
    user_key: []const u8,
    sequence: u64,
    /// Field name uses @"type" to sidestep the keyword; callers can also
    /// write `.value_type` via the dedicated alias below if preferred.
    /// We match the RocksDB struct field name exactly ("type").
    type: ValueType,
};

// ---------------------------------------------------------------------------
// appendInternalKey
// ---------------------------------------------------------------------------

/// Appends the RocksDB internal-key encoding of `pik` to `buf`.
/// Format: user_key_bytes ++ fixed64-LE(packed_trailer)
/// The `gpa` allocator is forwarded to the ArrayList grow operations.
pub fn appendInternalKey(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, pik: ParsedInternalKey) !void {
    try buf.appendSlice(gpa, pik.user_key);
    var trailer_buf: [8]u8 = undefined;
    coding.encodeFixed64(&trailer_buf, packSequenceAndType(pik.sequence, pik.type));
    try buf.appendSlice(gpa, &trailer_buf);
}

// ---------------------------------------------------------------------------
// parseInternalKey
// ---------------------------------------------------------------------------

/// Parse a raw internal-key slice.
/// Returns error.Corruption if:
///   - the slice is shorter than 8 bytes, or
///   - the type byte in the trailer is not a recognised ValueType.
pub fn parseInternalKey(internal_key: []const u8) error{Corruption}!ParsedInternalKey {
    if (internal_key.len < 8) return error.Corruption;
    const user_key = internal_key[0 .. internal_key.len - 8];
    const trailer_bytes: *const [8]u8 = internal_key[internal_key.len - 8 ..][0..8];
    const trailer = coding.decodeFixed64(trailer_bytes);
    const type_byte: u8 = @intCast(trailer & 0xff);
    const vt = try valueTypeFromInt(type_byte);
    const seq: u64 = trailer >> 8;
    return ParsedInternalKey{ .user_key = user_key, .sequence = seq, .type = vt };
}

// ---------------------------------------------------------------------------
// extractUserKey
// ---------------------------------------------------------------------------

/// Return the user-key portion of a raw internal-key slice.
/// Asserts `internal_key.len >= 8`.
pub fn extractUserKey(internal_key: []const u8) []const u8 {
    std.debug.assert(internal_key.len >= 8);
    return internal_key[0 .. internal_key.len - 8];
}

// ---------------------------------------------------------------------------
// InternalKeyComparator
// ---------------------------------------------------------------------------

/// Wraps a user Comparator and produces a Comparator over raw internal keys.
///
/// Ordering:
///   1. Compare the user-key portions using the wrapped user comparator.
///   2. On tie, compare packed trailers DESCENDING — a higher trailer value
///      (= higher sequence number, or same seq with higher type byte) sorts
///      FIRST (i.e. is "less than" in the ordered sense).
///
/// This matches RocksDB's InternalKeyComparator exactly.
pub const InternalKeyComparator = struct {
    user: comparator.Comparator,

    /// Return a Comparator fat-pointer backed by this InternalKeyComparator.
    /// The caller MUST keep this InternalKeyComparator alive for the lifetime
    /// of the returned Comparator (the ctx pointer will point into it).
    pub fn comparatorInterface(self: *const InternalKeyComparator) comparator.Comparator {
        return comparator.Comparator{
            .ctx = self,
            .vtable = &ikc_vtable,
        };
    }
};

// ---------------------------------------------------------------------------
// InternalKeyComparator vtable implementations
// ---------------------------------------------------------------------------

fn ikcCompare(ctx: *const anyopaque, a: []const u8, b: []const u8) std.math.Order {
    const self: *const InternalKeyComparator = @ptrCast(@alignCast(ctx));
    // Both slices must be valid internal keys (len >= 8); callers guarantee this.
    const ua = extractUserKey(a);
    const ub = extractUserKey(b);
    const user_ord = self.user.compare(ua, ub);
    if (user_ord != .eq) return user_ord;
    // Same user key: compare trailers descending (higher trailer = smaller order).
    const ta_bytes: *const [8]u8 = a[a.len - 8 ..][0..8];
    const tb_bytes: *const [8]u8 = b[b.len - 8 ..][0..8];
    const ta = coding.decodeFixed64(ta_bytes);
    const tb = coding.decodeFixed64(tb_bytes);
    if (ta > tb) return .lt; // a's trailer is bigger → a sorts first
    if (ta < tb) return .gt;
    return .eq;
}

fn ikcName(_: *const anyopaque) []const u8 {
    return "leveldb.InternalKeyComparator";
}

/// RocksDB-compatible findShortestSeparator for internal keys.
///
/// Algorithm (mirrors InternalKeyComparator::FindShortestSeparator in
/// RocksDB's db/dbformat.cc):
///   1. Extract user-key portions from start and limit.
///   2. Truncate start to just its user-key bytes.
///   3. Delegate to the user comparator's findShortestSeparator.
///   4. Re-append a seek trailer: (kMaxSequenceNumber << 8 | kValueTypeForSeek).
///
/// If either key is malformed (< 8 bytes) we leave start unchanged to be safe.
fn ikcFindShortestSeparator(ctx: *const anyopaque, start: *std.ArrayList(u8), limit: []const u8) void {
    const self: *const InternalKeyComparator = @ptrCast(@alignCast(ctx));
    if (start.items.len < 8 or limit.len < 8) return;

    const user_start_len = start.items.len - 8;
    const user_limit = limit[0 .. limit.len - 8];

    // Reduce start to its user-key portion.
    start.shrinkRetainingCapacity(user_start_len);

    // Let the user comparator shorten the user-key.
    self.user.findShortestSeparator(start, user_limit);

    // Re-append the seek trailer so the result is a valid internal key.
    const seek_trailer = packSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek);
    var tbuf: [8]u8 = undefined;
    coding.encodeFixed64(&tbuf, seek_trailer);
    // appendSlice needs a gpa; ArrayList in Zig 0.16 is unmanaged — use the
    // testing allocator can't be passed here.  We borrow the list's existing
    // capacity first; if that's not enough we accept a potential OOM (the
    // comparator contract allows leaving the key unchanged on failure).
    // In practice the key only shrank, so capacity is always sufficient.
    start.appendSliceAssumeCapacity(&tbuf);
}

/// RocksDB-compatible findShortSuccessor for internal keys.
///
/// Algorithm (mirrors InternalKeyComparator::FindShortSuccessor):
///   1. Truncate key to its user-key bytes.
///   2. Delegate to the user comparator's findShortSuccessor.
///   3. Re-append the seek trailer.
fn ikcFindShortSuccessor(ctx: *const anyopaque, key: *std.ArrayList(u8)) void {
    const self: *const InternalKeyComparator = @ptrCast(@alignCast(ctx));
    if (key.items.len < 8) return;

    const user_key_len = key.items.len - 8;

    // Reduce to user-key portion.
    key.shrinkRetainingCapacity(user_key_len);

    // Let user comparator find a short successor.
    self.user.findShortSuccessor(key);

    // Re-append the seek trailer.
    const seek_trailer = packSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek);
    var tbuf: [8]u8 = undefined;
    coding.encodeFixed64(&tbuf, seek_trailer);
    key.appendSliceAssumeCapacity(&tbuf);
}

const ikc_vtable = comparator.Comparator.VTable{
    .compare = ikcCompare,
    .name = ikcName,
    .findShortestSeparator = ikcFindShortestSeparator,
    .findShortSuccessor = ikcFindShortSuccessor,
};

// ---------------------------------------------------------------------------
// Tests
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
    // "a"/seq=5 vs "b"/seq=1 → bytewise "a"<"b" → .lt regardless of seq
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
    // internal("foo", seq=5, value) < internal("foo", seq=2, value)
    // because 5 > 2 and higher seq → lower order
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
    // value type = 1, deletion type = 0.
    // Same seq → packed_value = (seq<<8)|1 > packed_deletion = (seq<<8)|0
    // → value has bigger trailer → value sorts first → compare(value_ik, del_ik) == .lt
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

    // Must be a valid internal key (>= 8 bytes).
    try std.testing.expect(start.items.len >= 8);

    // Last 8 bytes must be the seek trailer.
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
