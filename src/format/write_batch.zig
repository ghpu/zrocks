/// write_batch.zig — LevelDB/RocksDB-compatible WriteBatch wire format.
///
/// Wire format (byte-exact):
///   rep := header records
///   header := sequence (fixed64 LE, 8 bytes) ++ count (fixed32 LE, 4 bytes)
///   record (Put)    := 0x01  varint32(len(key)) key  varint32(len(value)) value
///   record (Delete) := 0x00  varint32(len(key)) key
///   record (Merge)  := 0x02  varint32(len(key)) key  varint32(len(value)) value
///
/// The type bytes 0x01 / 0x00 / 0x02 match ValueType.value / .deletion / .merge.
///
/// M7.0 Column-family tagging: a record targeting a NON-default column family
/// (cf_id != 0) is prefixed with `kColumnFamilyTag` (0x10) ++ varint32(cf_id),
/// then the ordinary record bytes follow.  Default-CF records (cf_id 0) are left
/// UNTAGGED, so existing single-CF batches are byte-for-byte unchanged and remain
/// back-compatible (the WAL replay + iterate paths treat an untagged record as
/// cf 0).  `iterate` yields `(cf_id, op)` by calling `handler.putCF(cf_id, ...)`
/// etc. when those methods exist, falling back to the cf-0 `put`/`delete`/...
/// methods otherwise.
const std = @import("std");
const coding = @import("../util/coding.zig");
const internal_key = @import("internal_key.zig");

const ValueType = internal_key.ValueType;

pub const Error = error{Corruption} || std.mem.Allocator.Error;

/// Record-prefix tag introducing a non-default column-family id (M7.0).  The
/// next bytes are varint32(cf_id) followed by the ordinary record (type byte +
/// fields).  Chosen distinct from every ValueType tag used in a batch record
/// (0x00/0x01/0x02/0x0F), so the first byte of a record unambiguously says
/// whether a CF id precedes it.
pub const kColumnFamilyTag: u8 = 0x10;

/// Byte offset of the sequence number in the header.
const kSeqOffset: usize = 0;
/// Byte offset of the record count in the header.
const kCountOffset: usize = 8;
/// Total header size in bytes.
const kHeaderSize: usize = 12;

pub const WriteBatch = struct {
    rep: std.ArrayList(u8),

    /// Initialise a new WriteBatch with a zeroed 12-byte header.
    pub fn init(gpa: std.mem.Allocator) !WriteBatch {
        var rep: std.ArrayList(u8) = .empty;
        const zero_header = [_]u8{0} ** kHeaderSize;
        try rep.appendSlice(gpa, &zero_header);
        return WriteBatch{ .rep = rep };
    }

    pub fn deinit(self: *WriteBatch, gpa: std.mem.Allocator) void {
        self.rep.deinit(gpa);
    }

    /// Append a Put record and increment the count.
    pub fn put(self: *WriteBatch, gpa: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        try self.rep.append(gpa, @intFromEnum(ValueType.value));
        try coding.putLengthPrefixedSlice(&self.rep, gpa, key);
        try coding.putLengthPrefixedSlice(&self.rep, gpa, value);
        self.setCount(self.count() + 1);
    }

    /// Append a Delete record and increment the count.
    pub fn delete(self: *WriteBatch, gpa: std.mem.Allocator, key: []const u8) !void {
        try self.rep.append(gpa, @intFromEnum(ValueType.deletion));
        try coding.putLengthPrefixedSlice(&self.rep, gpa, key);
        self.setCount(self.count() + 1);
    }

    /// Append a Merge record (a read-modify-write operand, M7.1) and increment
    /// the count.  Wire format mirrors Put but with the merge type byte 0x02.
    pub fn merge(self: *WriteBatch, gpa: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        try self.rep.append(gpa, @intFromEnum(ValueType.merge));
        try coding.putLengthPrefixedSlice(&self.rep, gpa, key);
        try coding.putLengthPrefixedSlice(&self.rep, gpa, value);
        self.setCount(self.count() + 1);
    }

    /// Append a DeleteRange record (M7.5) and increment the count.  It deletes
    /// every key in `[begin, end)` (half-open, `end` exclusive) as of this
    /// record's sequence.  Wire format: type byte 0x0F (range_deletion) followed
    /// by length-prefixed `begin` then length-prefixed `end`.
    pub fn deleteRange(self: *WriteBatch, gpa: std.mem.Allocator, begin: []const u8, end: []const u8) !void {
        try self.rep.append(gpa, @intFromEnum(ValueType.range_deletion));
        try coding.putLengthPrefixedSlice(&self.rep, gpa, begin);
        try coding.putLengthPrefixedSlice(&self.rep, gpa, end);
        self.setCount(self.count() + 1);
    }

    /// Emit the CF prefix `kColumnFamilyTag ++ varint32(cf_id)` when `cf_id` is
    /// non-default; default (0) emits nothing so the record stays untagged.
    fn putCfPrefix(self: *WriteBatch, gpa: std.mem.Allocator, cf_id: u32) !void {
        if (cf_id == 0) return;
        try self.rep.append(gpa, kColumnFamilyTag);
        try coding.putVarint32(&self.rep, gpa, cf_id);
    }

    /// Append a Put record targeting column family `cf_id` and increment the
    /// count.  `cf_id == 0` is identical to `put` (untagged, back-compatible).
    pub fn putCF(self: *WriteBatch, gpa: std.mem.Allocator, cf_id: u32, key: []const u8, value: []const u8) !void {
        try self.putCfPrefix(gpa, cf_id);
        try self.rep.append(gpa, @intFromEnum(ValueType.value));
        try coding.putLengthPrefixedSlice(&self.rep, gpa, key);
        try coding.putLengthPrefixedSlice(&self.rep, gpa, value);
        self.setCount(self.count() + 1);
    }

    /// Append a Delete record targeting column family `cf_id`.
    pub fn deleteCF(self: *WriteBatch, gpa: std.mem.Allocator, cf_id: u32, key: []const u8) !void {
        try self.putCfPrefix(gpa, cf_id);
        try self.rep.append(gpa, @intFromEnum(ValueType.deletion));
        try coding.putLengthPrefixedSlice(&self.rep, gpa, key);
        self.setCount(self.count() + 1);
    }

    /// Append a Merge operand targeting column family `cf_id`.
    pub fn mergeCF(self: *WriteBatch, gpa: std.mem.Allocator, cf_id: u32, key: []const u8, value: []const u8) !void {
        try self.putCfPrefix(gpa, cf_id);
        try self.rep.append(gpa, @intFromEnum(ValueType.merge));
        try coding.putLengthPrefixedSlice(&self.rep, gpa, key);
        try coding.putLengthPrefixedSlice(&self.rep, gpa, value);
        self.setCount(self.count() + 1);
    }

    /// Append a DeleteRange record targeting column family `cf_id`.
    pub fn deleteRangeCF(self: *WriteBatch, gpa: std.mem.Allocator, cf_id: u32, begin: []const u8, end: []const u8) !void {
        try self.putCfPrefix(gpa, cf_id);
        try self.rep.append(gpa, @intFromEnum(ValueType.range_deletion));
        try coding.putLengthPrefixedSlice(&self.rep, gpa, begin);
        try coding.putLengthPrefixedSlice(&self.rep, gpa, end);
        self.setCount(self.count() + 1);
    }

    /// Read the record count from the header (fixed32 LE at offset 8).
    pub fn count(self: *const WriteBatch) u32 {
        const bytes: *const [4]u8 = self.rep.items[kCountOffset..][0..4];
        return coding.decodeFixed32(bytes);
    }

    /// Write the record count into the header.
    pub fn setCount(self: *WriteBatch, n: u32) void {
        const bytes: *[4]u8 = self.rep.items[kCountOffset..][0..4];
        coding.encodeFixed32(bytes, n);
    }

    /// Read the sequence number from the header (fixed64 LE at offset 0).
    pub fn sequence(self: *const WriteBatch) u64 {
        const bytes: *const [8]u8 = self.rep.items[kSeqOffset..][0..8];
        return coding.decodeFixed64(bytes);
    }

    /// Write the sequence number into the header.
    pub fn setSequence(self: *WriteBatch, seq: u64) void {
        const bytes: *[8]u8 = self.rep.items[kSeqOffset..][0..8];
        coding.encodeFixed64(bytes, seq);
    }

    /// Reset to an empty 12-byte zeroed header, discarding all records.
    pub fn clear(self: *WriteBatch, gpa: std.mem.Allocator) !void {
        self.rep.clearRetainingCapacity();
        const zero_header = [_]u8{0} ** kHeaderSize;
        try self.rep.appendSlice(gpa, &zero_header);
    }

    /// Return the raw byte representation.
    pub fn contents(self: *const WriteBatch) []const u8 {
        return self.rep.items;
    }

    /// Replace the rep with the provided bytes (e.g. from WAL replay).
    pub fn setContents(self: *WriteBatch, gpa: std.mem.Allocator, bytes: []const u8) !void {
        self.rep.clearRetainingCapacity();
        try self.rep.appendSlice(gpa, bytes);
    }

    /// Total byte size of the batch.
    pub fn byteSize(self: *const WriteBatch) usize {
        return self.rep.items.len;
    }

    /// Iterate over all records in the batch, calling handler methods for each.
    ///
    /// The handler must implement the cf-0 methods:
    ///   fn put(self, key, value) !void
    ///   fn delete(self, key) !void
    ///   fn merge(self, key, value) !void
    ///   (optionally) fn deleteRange(self, begin, end) !void
    ///
    /// A CF-AWARE handler additionally declares any of:
    ///   fn putCF(self, cf_id: u32, key, value) !void
    ///   fn deleteCF(self, cf_id: u32, key) !void
    ///   fn mergeCF(self, cf_id: u32, key, value) !void
    ///   fn deleteRangeCF(self, cf_id: u32, begin, end) !void
    /// When present, records are dispatched to the `*CF` method with the parsed
    /// column-family id (0 for untagged records); otherwise records are routed to
    /// the cf-0 method and a NON-default cf id is an error (a usage error: a
    /// CF-tagged batch reached a CF-unaware handler).
    ///
    /// Returns error.Corruption if:
    ///   - the rep is shorter than the header,
    ///   - a record type byte is unknown,
    ///   - a length-prefixed field is truncated, or
    ///   - the number of parsed records does not match the header count.
    pub fn iterate(self: *const WriteBatch, handler: anytype) !void {
        if (self.rep.items.len < kHeaderSize) return error.Corruption;

        const Handler = @typeInfo(@TypeOf(handler)).pointer.child;
        const expected_count = self.count();
        var input: []const u8 = self.rep.items[kHeaderSize..];
        var parsed_count: u32 = 0;

        while (input.len > 0) {
            // Optional CF prefix: kColumnFamilyTag ++ varint32(cf_id).
            var cf_id: u32 = 0;
            if (input[0] == kColumnFamilyTag) {
                input = input[1..];
                cf_id = coding.getVarint32(&input) catch return error.Corruption;
            }

            // Read the type byte.
            if (input.len == 0) return error.Corruption;
            const type_byte = input[0];
            input = input[1..];

            if (type_byte == @intFromEnum(ValueType.value)) {
                const key = coding.getLengthPrefixedSlice(&input) catch return error.Corruption;
                const value = coding.getLengthPrefixedSlice(&input) catch return error.Corruption;
                if (@hasDecl(Handler, "putCF")) {
                    try handler.putCF(cf_id, key, value);
                } else {
                    if (cf_id != 0) return error.Corruption;
                    try handler.put(key, value);
                }
                parsed_count += 1;
            } else if (type_byte == @intFromEnum(ValueType.deletion)) {
                const key = coding.getLengthPrefixedSlice(&input) catch return error.Corruption;
                if (@hasDecl(Handler, "deleteCF")) {
                    try handler.deleteCF(cf_id, key);
                } else {
                    if (cf_id != 0) return error.Corruption;
                    try handler.delete(key);
                }
                parsed_count += 1;
            } else if (type_byte == @intFromEnum(ValueType.merge)) {
                const key = coding.getLengthPrefixedSlice(&input) catch return error.Corruption;
                const value = coding.getLengthPrefixedSlice(&input) catch return error.Corruption;
                if (@hasDecl(Handler, "mergeCF")) {
                    try handler.mergeCF(cf_id, key, value);
                } else {
                    if (cf_id != 0) return error.Corruption;
                    try handler.merge(key, value);
                }
                parsed_count += 1;
            } else if (type_byte == @intFromEnum(ValueType.range_deletion)) {
                const begin = coding.getLengthPrefixedSlice(&input) catch return error.Corruption;
                const end = coding.getLengthPrefixedSlice(&input) catch return error.Corruption;
                if (@hasDecl(Handler, "deleteRangeCF")) {
                    try handler.deleteRangeCF(cf_id, begin, end);
                } else if (@hasDecl(Handler, "deleteRange")) {
                    // A range record reaching a CF-unaware handler with only the
                    // cf-0 method must target the default CF.
                    if (cf_id != 0) return error.Corruption;
                    try handler.deleteRange(begin, end);
                } else {
                    // A range record reaching a put/delete-only handler is a usage
                    // error (e.g. an old handler without range support).
                    return error.Corruption;
                }
                parsed_count += 1;
            } else {
                return error.Corruption;
            }
        }

        if (parsed_count != expected_count) return error.Corruption;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "golden bytes: put(foo, bar) produces exact wire encoding" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "foo", "bar");

    // Expected: seq=0 (8 bytes LE), count=1 (4 bytes LE), 0x01, 0x03,'f','o','o', 0x03,'b','a','r'
    const expected = [_]u8{
        0, 0, 0, 0, 0, 0, 0, 0, // seq=0 fixed64 LE
        1, 0, 0, 0, // count=1 fixed32 LE
        0x01, // ValueType.value
        0x03, 'f', 'o', 'o', // varint32(3) + "foo"
        0x03, 'b', 'a', 'r', // varint32(3) + "bar"
    };
    try std.testing.expectEqualSlices(u8, &expected, wb.contents());
}

test "golden bytes: merge(c, op) produces exact wire encoding (type 0x02)" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.merge(gpa, "c", "op");

    // seq=0 (8B), count=1 (4B), 0x02, varint32(1)+"c", varint32(2)+"op".
    const expected = [_]u8{
        0, 0, 0, 0, 0, 0, 0, 0, // seq=0 fixed64 LE
        1, 0, 0, 0, // count=1 fixed32 LE
        0x02, // ValueType.merge
        0x01, 'c',
        0x02, 'o', 'p',
    };
    try std.testing.expectEqualSlices(u8, &expected, wb.contents());
}

test "iterate: merge handler called with key + operand" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "p", "pv");
    try wb.merge(gpa, "c", "op1");
    try wb.merge(gpa, "c", "op2");

    const Handler = struct {
        n_put: usize = 0,
        n_merge: usize = 0,
        last_merge_key: []const u8 = "",
        last_merge_val: []const u8 = "",

        pub fn put(self: *@This(), _: []const u8, _: []const u8) !void {
            self.n_put += 1;
        }
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn merge(self: *@This(), key: []const u8, value: []const u8) !void {
            self.n_merge += 1;
            self.last_merge_key = key;
            self.last_merge_val = value;
        }
    };

    var h = Handler{};
    try wb.iterate(&h);
    try std.testing.expectEqual(@as(usize, 1), h.n_put);
    try std.testing.expectEqual(@as(usize, 2), h.n_merge);
    try std.testing.expectEqualStrings("c", h.last_merge_key);
    try std.testing.expectEqualStrings("op2", h.last_merge_val);
}

test "count increments: put then delete" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "foo", "bar");
    try std.testing.expectEqual(@as(u32, 1), wb.count());

    try wb.delete(gpa, "baz");
    try std.testing.expectEqual(@as(u32, 2), wb.count());

    // Verify delete record bytes are appended correctly.
    const bytes = wb.contents();
    // Delete record starts at offset 12 + 1 + 1+3 + 1+3 = 12 + 9 = 21
    const delete_start: usize = kHeaderSize + 1 + 1 + 3 + 1 + 3;
    const delete_record = bytes[delete_start..];
    const expected_delete = [_]u8{ 0x00, 0x03, 'b', 'a', 'z' };
    try std.testing.expectEqualSlices(u8, &expected_delete, delete_record);
}

test "setSequence/sequence round-trip" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    wb.setSequence(42);
    try std.testing.expectEqual(@as(u64, 42), wb.sequence());

    // Verify first 8 bytes are fixed64 LE of 42.
    const bytes = wb.contents();
    const expected_seq = [_]u8{ 42, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectEqualSlices(u8, &expected_seq, bytes[0..8]);
}

test "iterate: put + delete handler called in order" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "foo", "bar");
    try wb.delete(gpa, "baz");

    const Record = union(enum) {
        put: struct { key: []const u8, value: []const u8 },
        del: struct { key: []const u8 },
        mrg: struct { key: []const u8, value: []const u8 },
    };

    const Handler = struct {
        records: [4]Record = undefined,
        n: usize = 0,

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            self.records[self.n] = .{ .put = .{ .key = key, .value = value } };
            self.n += 1;
        }

        pub fn delete(self: *@This(), key: []const u8) !void {
            self.records[self.n] = .{ .del = .{ .key = key } };
            self.n += 1;
        }

        pub fn merge(self: *@This(), key: []const u8, value: []const u8) !void {
            self.records[self.n] = .{ .mrg = .{ .key = key, .value = value } };
            self.n += 1;
        }
    };

    var handler = Handler{};
    try wb.iterate(&handler);

    try std.testing.expectEqual(@as(usize, 2), handler.n);

    switch (handler.records[0]) {
        .put => |p| {
            try std.testing.expectEqualSlices(u8, "foo", p.key);
            try std.testing.expectEqualSlices(u8, "bar", p.value);
        },
        else => return error.TestUnexpectedResult,
    }

    switch (handler.records[1]) {
        .del => |d| {
            try std.testing.expectEqualSlices(u8, "baz", d.key);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "round-trip via setContents" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "hello", "world");
    wb.setSequence(7);

    var wb2 = try WriteBatch.init(gpa);
    defer wb2.deinit(gpa);
    try wb2.setContents(gpa, wb.contents());

    try std.testing.expectEqual(wb.count(), wb2.count());
    try std.testing.expectEqual(wb.sequence(), wb2.sequence());
    try std.testing.expectEqualSlices(u8, wb.contents(), wb2.contents());

    const Handler = struct {
        keys: [4][]const u8 = undefined,
        values: [4][]const u8 = undefined,
        n: usize = 0,

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            self.keys[self.n] = key;
            self.values[self.n] = value;
            self.n += 1;
        }

        pub fn delete(_: *@This(), _: []const u8) !void {}

        pub fn merge(self: *@This(), key: []const u8, value: []const u8) !void {
            self.keys[self.n] = key;
            self.values[self.n] = value;
            self.n += 1;
        }
    };

    var h1 = Handler{};
    var h2 = Handler{};
    try wb.iterate(&h1);
    try wb2.iterate(&h2);

    try std.testing.expectEqual(h1.n, h2.n);
    for (0..h1.n) |i| {
        try std.testing.expectEqualSlices(u8, h1.keys[i], h2.keys[i]);
        try std.testing.expectEqualSlices(u8, h1.values[i], h2.values[i]);
    }
}

test "corruption: truncated value in put record" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "foo", "bar");

    // Drop the last 2 bytes (truncate the value) by passing a sub-slice.
    const full = wb.contents();
    const truncated = full[0 .. full.len - 2];

    var wb2 = try WriteBatch.init(gpa);
    defer wb2.deinit(gpa);
    try wb2.setContents(gpa, truncated);

    const NoopHandler = struct {
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn merge(_: *@This(), _: []const u8, _: []const u8) !void {}
    };
    var h = NoopHandler{};
    try std.testing.expectError(error.Corruption, wb2.iterate(&h));
}

test "corruption: header count exceeds actual records" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "foo", "bar");

    // Lie about the count — set it to 5 when only 1 record exists.
    wb.setCount(5);

    const NoopHandler = struct {
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn merge(_: *@This(), _: []const u8, _: []const u8) !void {}
    };
    var h = NoopHandler{};
    try std.testing.expectError(error.Corruption, wb.iterate(&h));
}

test "clear resets to empty 12-byte header" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "foo", "bar");
    wb.setSequence(99);
    try std.testing.expect(wb.byteSize() > kHeaderSize);

    try wb.clear(gpa);
    try std.testing.expectEqual(@as(usize, kHeaderSize), wb.byteSize());
    try std.testing.expectEqual(@as(u32, 0), wb.count());
    try std.testing.expectEqual(@as(u64, 0), wb.sequence());
}

test "byteSize reflects rep length" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try std.testing.expectEqual(@as(usize, kHeaderSize), wb.byteSize());
    try wb.put(gpa, "k", "v");
    // header(12) + type(1) + varint(1)+'k'(1) + varint(1)+'v'(1) = 17
    try std.testing.expectEqual(@as(usize, 17), wb.byteSize());
}

// ---------------------------------------------------------------------------
// M7.5 — DeleteRange (range tombstones)
// ---------------------------------------------------------------------------

test "M7.5 golden bytes: deleteRange(b, d) produces type 0x0F wire encoding" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.deleteRange(gpa, "b", "d");

    // seq=0 (8B), count=1 (4B), 0x0F, varint32(1)+"b", varint32(1)+"d".
    const expected = [_]u8{
        0, 0, 0, 0, 0, 0, 0, 0, // seq=0 fixed64 LE
        1, 0, 0, 0, // count=1 fixed32 LE
        0x0F, // ValueType.range_deletion
        0x01, 'b',
        0x01, 'd',
    };
    try std.testing.expectEqualSlices(u8, &expected, wb.contents());
}

// ---------------------------------------------------------------------------
// M7.0 — Column-family tagging
// ---------------------------------------------------------------------------

test "M7.0 golden bytes: putCF(cf=0) is untagged (back-compatible with put)" {
    const gpa = std.testing.allocator;
    var a = try WriteBatch.init(gpa);
    defer a.deinit(gpa);
    var b = try WriteBatch.init(gpa);
    defer b.deinit(gpa);

    try a.put(gpa, "foo", "bar");
    try b.putCF(gpa, 0, "foo", "bar");
    try std.testing.expectEqualSlices(u8, a.contents(), b.contents());
}

test "M7.0 golden bytes: putCF(cf=5) prefixes kColumnFamilyTag + varint(5)" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.putCF(gpa, 5, "k", "v");
    const expected = [_]u8{
        0, 0, 0, 0, 0, 0, 0, 0, // seq=0
        1, 0, 0, 0, // count=1
        kColumnFamilyTag, 0x05, // CF prefix: tag + varint(5)
        0x01, // ValueType.value
        0x01, 'k',
        0x01, 'v',
    };
    try std.testing.expectEqualSlices(u8, &expected, wb.contents());
}

test "M7.0 iterate: CF-aware handler receives per-record cf ids" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "d", "dv"); // cf 0 (untagged)
    try wb.putCF(gpa, 1, "u", "uv"); // cf 1
    try wb.deleteCF(gpa, 2, "o"); // cf 2

    const Handler = struct {
        ids: [8]u32 = undefined,
        n: usize = 0,
        pub fn putCF(self: *@This(), cf_id: u32, _: []const u8, _: []const u8) !void {
            self.ids[self.n] = cf_id;
            self.n += 1;
        }
        pub fn deleteCF(self: *@This(), cf_id: u32, _: []const u8) !void {
            self.ids[self.n] = cf_id;
            self.n += 1;
        }
        pub fn mergeCF(self: *@This(), cf_id: u32, _: []const u8, _: []const u8) !void {
            self.ids[self.n] = cf_id;
            self.n += 1;
        }
    };
    var h = Handler{};
    try wb.iterate(&h);
    try std.testing.expectEqual(@as(usize, 3), h.n);
    try std.testing.expectEqual(@as(u32, 0), h.ids[0]);
    try std.testing.expectEqual(@as(u32, 1), h.ids[1]);
    try std.testing.expectEqual(@as(u32, 2), h.ids[2]);
}

test "M7.0 iterate: CF-tagged record rejected by a CF-unaware handler" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);
    try wb.putCF(gpa, 3, "k", "v");

    const Handler = struct {
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn merge(_: *@This(), _: []const u8, _: []const u8) !void {}
    };
    var h = Handler{};
    try std.testing.expectError(error.Corruption, wb.iterate(&h));
}

test "M7.5 iterate: deleteRange handler called with begin + end" {
    const gpa = std.testing.allocator;
    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);

    try wb.put(gpa, "p", "pv");
    try wb.deleteRange(gpa, "b", "d");

    const Handler = struct {
        n_put: usize = 0,
        n_range: usize = 0,
        last_begin: []const u8 = "",
        last_end: []const u8 = "",

        pub fn put(self: *@This(), _: []const u8, _: []const u8) !void {
            self.n_put += 1;
        }
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn merge(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn deleteRange(self: *@This(), begin: []const u8, end: []const u8) !void {
            self.n_range += 1;
            self.last_begin = begin;
            self.last_end = end;
        }
    };

    var h = Handler{};
    try wb.iterate(&h);
    try std.testing.expectEqual(@as(usize, 1), h.n_put);
    try std.testing.expectEqual(@as(usize, 1), h.n_range);
    try std.testing.expectEqualStrings("b", h.last_begin);
    try std.testing.expectEqualStrings("d", h.last_end);
}
