/// version_edit.zig — LevelDB-compatible VersionEdit encoding/decoding.
///
/// Ownership model:
///   VersionEdit OWNS all variable-length byte slices it holds:
///   - comparator_name: duped on setComparatorName / decodeFrom; freed in deinit.
///   - FileMetaData.smallest / .largest: duped on addFile / decodeFrom; freed in deinit.
///   Callers may discard their original buffers after calling these methods.
///
/// Tag table (LevelDB-compatible subset):
///   1  kComparator       length-prefixed comparator name
///   2  kLogNumber        varint64
///   3  kNextFileNumber   varint64
///   4  kLastSequence     varint64
///   5  kCompactPointer   varint32 level + length-prefixed internal key
///   6  kDeletedFile      varint32 level + varint64 file number
///   7  kNewFile          varint32 level + varint64 file# + varint64 file size
///                         + length-prefixed smallest key + length-prefixed largest key
///   9  kPrevLogNumber    varint64
///
/// TODO(interop): RocksDB kNewFile4=100 for real-RocksDB-manifest read (M5.x).
const std = @import("std");
const coding = @import("../util/coding.zig");

// ---------------------------------------------------------------------------
// Error set
// ---------------------------------------------------------------------------

pub const Error = error{Corruption} || std.mem.Allocator.Error;

// ---------------------------------------------------------------------------
// Tag constants
// ---------------------------------------------------------------------------

const Tag = struct {
    const kComparator: u32 = 1;
    const kLogNumber: u32 = 2;
    const kNextFileNumber: u32 = 3;
    const kLastSequence: u32 = 4;
    const kCompactPointer: u32 = 5;
    const kDeletedFile: u32 = 6;
    const kNewFile: u32 = 7;
    const kPrevLogNumber: u32 = 9;
};

// ---------------------------------------------------------------------------
// FileMetaData
// ---------------------------------------------------------------------------

/// Metadata for a single SSTable file.
/// The byte slices `smallest` and `largest` are internal-key bytes.
/// When held inside a VersionEdit, the VersionEdit owns these bytes (duped).
/// When returned standalone (e.g. from a VersionSet), the caller is responsible
/// for documenting ownership at that level.
pub const FileMetaData = struct {
    number: u64,
    file_size: u64,
    /// Internal-key bytes for the smallest key in the file.
    smallest: []const u8,
    /// Internal-key bytes for the largest key in the file.
    largest: []const u8,
    // allowed_seeks, refs etc. can be added later (M5.1+).
};

// ---------------------------------------------------------------------------
// VersionEdit — STUB (RED phase)
// ---------------------------------------------------------------------------

pub const VersionEdit = struct {
    comparator_name: ?[]const u8 = null,
    log_number: ?u64 = null,
    prev_log_number: ?u64 = null,
    next_file_number: ?u64 = null,
    last_sequence: ?u64 = null,
    deleted_files: std.ArrayListUnmanaged(DeletedFile) = .empty,
    new_files: std.ArrayListUnmanaged(NewFileEntry) = .empty,

    pub const DeletedFile = struct { level: u32, number: u64 };
    pub const NewFileEntry = struct { level: u32, meta: FileMetaData };

    pub fn init() VersionEdit {
        return .{};
    }

    pub fn deinit(self: *VersionEdit, gpa: std.mem.Allocator) void {
        _ = self;
        _ = gpa;
        // TODO: implement
    }

    pub fn setComparatorName(self: *VersionEdit, gpa: std.mem.Allocator, name: []const u8) !void {
        _ = self;
        _ = gpa;
        _ = name;
        // TODO: implement
    }

    pub fn setLogNumber(self: *VersionEdit, v: u64) void {
        _ = self;
        _ = v;
        // TODO: implement
    }

    pub fn setPrevLogNumber(self: *VersionEdit, v: u64) void {
        _ = self;
        _ = v;
    }

    pub fn setNextFileNumber(self: *VersionEdit, v: u64) void {
        _ = self;
        _ = v;
    }

    pub fn setLastSequence(self: *VersionEdit, v: u64) void {
        _ = self;
        _ = v;
    }

    pub fn addFile(
        self: *VersionEdit,
        gpa: std.mem.Allocator,
        level: u32,
        number: u64,
        file_size: u64,
        smallest: []const u8,
        largest: []const u8,
    ) !void {
        _ = self;
        _ = gpa;
        _ = level;
        _ = number;
        _ = file_size;
        _ = smallest;
        _ = largest;
        // TODO: implement
    }

    pub fn removeFile(
        self: *VersionEdit,
        gpa: std.mem.Allocator,
        level: u32,
        number: u64,
    ) !void {
        _ = self;
        _ = gpa;
        _ = level;
        _ = number;
        // TODO: implement
    }

    pub fn encodeTo(
        self: *const VersionEdit,
        buf: *std.ArrayListUnmanaged(u8),
        gpa: std.mem.Allocator,
    ) !void {
        _ = self;
        _ = buf;
        _ = gpa;
        // TODO: implement
    }

    pub fn decodeFrom(gpa: std.mem.Allocator, data: []const u8) Error!VersionEdit {
        _ = gpa;
        _ = data;
        // TODO: implement
        return error.Corruption;
    }
};

// ===========================================================================
// Tests
// ===========================================================================

test "golden: log_number=5 encodes to {0x02, 0x05}" {
    const gpa = std.testing.allocator;
    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    edit.setLogNumber(5);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try edit.encodeTo(&buf, gpa);

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x02, 0x05 }, buf.items);
}

test "golden: last_sequence=100 encodes to {0x04, 0x64}" {
    const gpa = std.testing.allocator;
    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    edit.setLastSequence(100);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try edit.encodeTo(&buf, gpa);

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x04, 0x64 }, buf.items);
}

test "empty edit encodes to empty and decodes to empty" {
    const gpa = std.testing.allocator;
    var edit = VersionEdit.init();
    defer edit.deinit(gpa);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try edit.encodeTo(&buf, gpa);
    try std.testing.expectEqual(@as(usize, 0), buf.items.len);

    var edit2 = try VersionEdit.decodeFrom(gpa, buf.items);
    defer edit2.deinit(gpa);
    try std.testing.expect(edit2.comparator_name == null);
    try std.testing.expect(edit2.log_number == null);
    try std.testing.expect(edit2.prev_log_number == null);
    try std.testing.expect(edit2.next_file_number == null);
    try std.testing.expect(edit2.last_sequence == null);
    try std.testing.expectEqual(@as(usize, 0), edit2.deleted_files.items.len);
    try std.testing.expectEqual(@as(usize, 0), edit2.new_files.items.len);
}

test "full round-trip" {
    const gpa = std.testing.allocator;

    const smallest = "a" ++ [_]u8{0} ** 8;
    const largest = "z" ++ [_]u8{0} ** 8;

    var edit = VersionEdit.init();
    defer edit.deinit(gpa);

    try edit.setComparatorName(gpa, "leveldb.BytewiseComparator");
    edit.setLogNumber(5);
    edit.setPrevLogNumber(4);
    edit.setNextFileNumber(10);
    edit.setLastSequence(100);
    try edit.addFile(gpa, 1, 7, 1000, smallest, largest);
    try edit.removeFile(gpa, 0, 3);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try edit.encodeTo(&buf, gpa);

    var edit2 = try VersionEdit.decodeFrom(gpa, buf.items);
    defer edit2.deinit(gpa);

    try std.testing.expectEqualStrings("leveldb.BytewiseComparator", edit2.comparator_name.?);
    try std.testing.expectEqual(@as(u64, 5), edit2.log_number.?);
    try std.testing.expectEqual(@as(u64, 4), edit2.prev_log_number.?);
    try std.testing.expectEqual(@as(u64, 10), edit2.next_file_number.?);
    try std.testing.expectEqual(@as(u64, 100), edit2.last_sequence.?);

    try std.testing.expectEqual(@as(usize, 1), edit2.new_files.items.len);
    const nf = edit2.new_files.items[0];
    try std.testing.expectEqual(@as(u32, 1), nf.level);
    try std.testing.expectEqual(@as(u64, 7), nf.meta.number);
    try std.testing.expectEqual(@as(u64, 1000), nf.meta.file_size);
    try std.testing.expectEqualSlices(u8, smallest, nf.meta.smallest);
    try std.testing.expectEqualSlices(u8, largest, nf.meta.largest);

    try std.testing.expectEqual(@as(usize, 1), edit2.deleted_files.items.len);
    const df = edit2.deleted_files.items[0];
    try std.testing.expectEqual(@as(u32, 0), df.level);
    try std.testing.expectEqual(@as(u64, 3), df.number);
}

test "multiple new and deleted files preserved in order" {
    const gpa = std.testing.allocator;
    const key_a = "key_a" ++ [_]u8{0} ** 8;
    const key_b = "key_b" ++ [_]u8{0} ** 8;
    const key_c = "key_c" ++ [_]u8{0} ** 8;
    const key_d = "key_d" ++ [_]u8{0} ** 8;

    var edit = VersionEdit.init();
    defer edit.deinit(gpa);

    try edit.addFile(gpa, 0, 10, 512, key_a, key_b);
    try edit.addFile(gpa, 1, 20, 1024, key_c, key_d);
    try edit.removeFile(gpa, 0, 1);
    try edit.removeFile(gpa, 0, 2);
    try edit.removeFile(gpa, 1, 5);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try edit.encodeTo(&buf, gpa);

    var edit2 = try VersionEdit.decodeFrom(gpa, buf.items);
    defer edit2.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 2), edit2.new_files.items.len);
    try std.testing.expectEqual(@as(u64, 10), edit2.new_files.items[0].meta.number);
    try std.testing.expectEqual(@as(u64, 20), edit2.new_files.items[1].meta.number);

    try std.testing.expectEqual(@as(usize, 3), edit2.deleted_files.items.len);
    try std.testing.expectEqual(@as(u64, 1), edit2.deleted_files.items[0].number);
    try std.testing.expectEqual(@as(u64, 2), edit2.deleted_files.items[1].number);
    try std.testing.expectEqual(@as(u64, 5), edit2.deleted_files.items[2].number);
}

test "corruption: truncated varint payload" {
    const gpa = std.testing.allocator;
    const data = [_]u8{0x02};
    try std.testing.expectError(error.Corruption, VersionEdit.decodeFrom(gpa, &data));
}

test "corruption: truncated length-prefixed string" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try coding.putVarint32(&buf, gpa, Tag.kComparator);
    try coding.putVarint32(&buf, gpa, 10);
    try buf.appendSlice(gpa, "abc");
    try std.testing.expectError(error.Corruption, VersionEdit.decodeFrom(gpa, buf.items));
}

test "corruption: unknown tag" {
    const gpa = std.testing.allocator;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try coding.putVarint32(&buf, gpa, 42);
    try std.testing.expectError(error.Corruption, VersionEdit.decodeFrom(gpa, buf.items));
}
