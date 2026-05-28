/// write_batch.zig — LevelDB/RocksDB-compatible WriteBatch wire format.
///
/// Wire format (byte-exact):
///   rep := header records
///   header := sequence (fixed64 LE, 8 bytes) ++ count (fixed32 LE, 4 bytes)
///   record (Put)    := 0x01  varint32(len(key)) key  varint32(len(value)) value
///   record (Delete) := 0x00  varint32(len(key)) key
///
/// The type bytes 0x01 / 0x00 match ValueType.value / ValueType.deletion.
const std = @import("std");
const coding = @import("../util/coding.zig");
const internal_key = @import("internal_key.zig");

const ValueType = internal_key.ValueType;

pub const Error = error{Corruption} || std.mem.Allocator.Error;

/// Byte offset of the sequence number in the header.
const kSeqOffset: usize = 0;
/// Byte offset of the record count in the header.
const kCountOffset: usize = 8;
/// Total header size in bytes.
const kHeaderSize: usize = 12;

pub const WriteBatch = struct {
    rep: std.ArrayList(u8),

    pub fn init(gpa: std.mem.Allocator) !WriteBatch {
        _ = gpa;
        @panic("TODO");
    }

    pub fn deinit(self: *WriteBatch, gpa: std.mem.Allocator) void {
        _ = self;
        _ = gpa;
        @panic("TODO");
    }

    pub fn put(self: *WriteBatch, gpa: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        _ = self;
        _ = gpa;
        _ = key;
        _ = value;
        @panic("TODO");
    }

    pub fn delete(self: *WriteBatch, gpa: std.mem.Allocator, key: []const u8) !void {
        _ = self;
        _ = gpa;
        _ = key;
        @panic("TODO");
    }

    pub fn count(self: *const WriteBatch) u32 {
        _ = self;
        @panic("TODO");
    }

    pub fn setCount(self: *WriteBatch, n: u32) void {
        _ = self;
        _ = n;
        @panic("TODO");
    }

    pub fn sequence(self: *const WriteBatch) u64 {
        _ = self;
        @panic("TODO");
    }

    pub fn setSequence(self: *WriteBatch, seq: u64) void {
        _ = self;
        _ = seq;
        @panic("TODO");
    }

    pub fn clear(self: *WriteBatch, gpa: std.mem.Allocator) !void {
        _ = self;
        _ = gpa;
        @panic("TODO");
    }

    pub fn contents(self: *const WriteBatch) []const u8 {
        _ = self;
        @panic("TODO");
    }

    pub fn setContents(self: *WriteBatch, gpa: std.mem.Allocator, bytes: []const u8) !void {
        _ = self;
        _ = gpa;
        _ = bytes;
        @panic("TODO");
    }

    pub fn byteSize(self: *const WriteBatch) usize {
        _ = self;
        @panic("TODO");
    }

    pub fn iterate(self: *const WriteBatch, handler: anytype) !void {
        _ = self;
        _ = handler;
        @panic("TODO");
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
    };

    var handler = Handler{};
    try wb.iterate(&handler);

    try std.testing.expectEqual(@as(usize, 2), handler.n);

    switch (handler.records[0]) {
        .put => |p| {
            try std.testing.expectEqualSlices(u8, "foo", p.key);
            try std.testing.expectEqualSlices(u8, "bar", p.value);
        },
        .del => return error.TestUnexpectedResult,
    }

    switch (handler.records[1]) {
        .del => |d| {
            try std.testing.expectEqualSlices(u8, "baz", d.key);
        },
        .put => return error.TestUnexpectedResult,
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
