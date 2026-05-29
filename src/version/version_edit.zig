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
///   100 kNewFile4        RocksDB extended new-file record:
///                         varint32 level + varint64 file# + varint64 file size
///                         + length-prefixed smallest + length-prefixed largest
///                         + varint64 smallest_seqno + varint64 largest_seqno
///                         + zero or more custom fields, each
///                           (varint32 custom_tag + length-prefixed value),
///                         terminated by custom_tag == kTerminate(1).
///                         Custom tags with the 0x40 non-safe-to-ignore bit set
///                         that we do not understand are a Corruption; other
///                         unknown custom tags are skipped.
const std = @import("std");
const coding = @import("../util/coding.zig");

// ---------------------------------------------------------------------------
// Error set
// ---------------------------------------------------------------------------

pub const Error = error{Corruption} || std.mem.Allocator.Error;

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Dupe `src` into `gpa`-owned memory.  Caller must free on error or when done.
fn dupSlice(gpa: std.mem.Allocator, src: []const u8) std.mem.Allocator.Error![]u8 {
    return gpa.dupe(u8, src);
}

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
    const kNewFile4: u32 = 100;
};

/// kNewFile4 custom-field sub-tags (RocksDB version_edit.cc CustomTag enum).
/// Only kTerminate is emitted by us; the rest are recognised for safe skipping
/// when reading a real RocksDB MANIFEST.
const CustomTag = struct {
    const kTerminate: u32 = 1;
    /// Custom tags with this mask bit set MUST be understood; an unknown one is
    /// a Corruption (a forward-incompatible field we cannot safely ignore).
    const kSafeIgnoreMask: u32 = 0x40;
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
    /// Smallest sequence number in the file (RocksDB kNewFile4 field).
    /// Defaults to 0 for the legacy kNewFile=7 path, which carries no seqnos.
    smallest_seqno: u64 = 0,
    /// Largest sequence number in the file (RocksDB kNewFile4 field).
    largest_seqno: u64 = 0,
    // allowed_seeks, refs etc. can be added later (M5.1+).
};

// ---------------------------------------------------------------------------
// VersionEdit
// ---------------------------------------------------------------------------

pub const VersionEdit = struct {
    // Optional scalar fields — only emitted when non-null.
    comparator_name: ?[]const u8 = null, // owned (duped)
    log_number: ?u64 = null,
    prev_log_number: ?u64 = null,
    next_file_number: ?u64 = null,
    last_sequence: ?u64 = null,

    // File mutation lists.
    deleted_files: std.ArrayListUnmanaged(DeletedFile) = .empty,
    new_files: std.ArrayListUnmanaged(NewFileEntry) = .empty,

    // TODO(m5.1): compact_pointers (kCompactPointer tag=5) skipped for now.

    pub const DeletedFile = struct { level: u32, number: u64 };
    /// `is_v4` selects the wire format on encode: kNewFile4 (tag=100, carries
    /// seqnos + custom-field terminator) when true, else legacy kNewFile (tag=7).
    pub const NewFileEntry = struct { level: u32, meta: FileMetaData, is_v4: bool = false };

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn init() VersionEdit {
        return .{};
    }

    /// Free all owned memory.  Must be called exactly once.
    pub fn deinit(self: *VersionEdit, gpa: std.mem.Allocator) void {
        if (self.comparator_name) |s| gpa.free(s);
        for (self.new_files.items) |entry| {
            gpa.free(entry.meta.smallest);
            gpa.free(entry.meta.largest);
        }
        self.deleted_files.deinit(gpa);
        self.new_files.deinit(gpa);
    }

    // -----------------------------------------------------------------------
    // Setters
    // -----------------------------------------------------------------------

    pub fn setComparatorName(self: *VersionEdit, gpa: std.mem.Allocator, name: []const u8) !void {
        if (self.comparator_name) |old| gpa.free(old);
        self.comparator_name = try dupSlice(gpa, name);
    }

    pub fn setLogNumber(self: *VersionEdit, v: u64) void {
        self.log_number = v;
    }

    pub fn setPrevLogNumber(self: *VersionEdit, v: u64) void {
        self.prev_log_number = v;
    }

    pub fn setNextFileNumber(self: *VersionEdit, v: u64) void {
        self.next_file_number = v;
    }

    pub fn setLastSequence(self: *VersionEdit, v: u64) void {
        self.last_sequence = v;
    }

    /// Add a new SSTable file.  `smallest` and `largest` are duped into
    /// edit-owned memory; the caller may release their copies afterwards.
    pub fn addFile(
        self: *VersionEdit,
        gpa: std.mem.Allocator,
        level: u32,
        number: u64,
        file_size: u64,
        smallest: []const u8,
        largest: []const u8,
    ) !void {
        const s = try dupSlice(gpa, smallest);
        errdefer gpa.free(s);
        const l = try dupSlice(gpa, largest);
        errdefer gpa.free(l);
        try self.new_files.append(gpa, .{
            .level = level,
            .meta = .{
                .number = number,
                .file_size = file_size,
                .smallest = s,
                .largest = l,
            },
        });
    }

    /// Add a new SSTable file in RocksDB kNewFile4 format (tag=100), carrying
    /// the smallest/largest sequence numbers.  `smallest`/`largest` key bytes
    /// are duped into edit-owned memory; the caller may release their copies.
    pub fn addFile4(
        self: *VersionEdit,
        gpa: std.mem.Allocator,
        level: u32,
        number: u64,
        file_size: u64,
        smallest: []const u8,
        largest: []const u8,
        smallest_seqno: u64,
        largest_seqno: u64,
    ) !void {
        const s = try dupSlice(gpa, smallest);
        errdefer gpa.free(s);
        const l = try dupSlice(gpa, largest);
        errdefer gpa.free(l);
        try self.new_files.append(gpa, .{
            .level = level,
            .is_v4 = true,
            .meta = .{
                .number = number,
                .file_size = file_size,
                .smallest = s,
                .largest = l,
                .smallest_seqno = smallest_seqno,
                .largest_seqno = largest_seqno,
            },
        });
    }

    /// Mark a file as deleted.
    pub fn removeFile(
        self: *VersionEdit,
        gpa: std.mem.Allocator,
        level: u32,
        number: u64,
    ) !void {
        try self.deleted_files.append(gpa, .{ .level = level, .number = number });
    }

    // -----------------------------------------------------------------------
    // Encoding
    // -----------------------------------------------------------------------

    /// Append the binary representation of this VersionEdit to `buf`.
    /// Only non-null optional scalars are emitted.
    pub fn encodeTo(
        self: *const VersionEdit,
        buf: *std.ArrayListUnmanaged(u8),
        gpa: std.mem.Allocator,
    ) !void {
        // kComparator
        if (self.comparator_name) |name| {
            try coding.putVarint32(buf, gpa, Tag.kComparator);
            try coding.putLengthPrefixedSlice(buf, gpa, name);
        }
        // kLogNumber
        if (self.log_number) |v| {
            try coding.putVarint32(buf, gpa, Tag.kLogNumber);
            try coding.putVarint64(buf, gpa, v);
        }
        // kPrevLogNumber
        if (self.prev_log_number) |v| {
            try coding.putVarint32(buf, gpa, Tag.kPrevLogNumber);
            try coding.putVarint64(buf, gpa, v);
        }
        // kNextFileNumber
        if (self.next_file_number) |v| {
            try coding.putVarint32(buf, gpa, Tag.kNextFileNumber);
            try coding.putVarint64(buf, gpa, v);
        }
        // kLastSequence
        if (self.last_sequence) |v| {
            try coding.putVarint32(buf, gpa, Tag.kLastSequence);
            try coding.putVarint64(buf, gpa, v);
        }
        // kDeletedFile entries
        for (self.deleted_files.items) |df| {
            try coding.putVarint32(buf, gpa, Tag.kDeletedFile);
            try coding.putVarint32(buf, gpa, df.level);
            try coding.putVarint64(buf, gpa, df.number);
        }
        // New-file entries: kNewFile4 (tag=100, with seqnos) or legacy kNewFile.
        for (self.new_files.items) |nf| {
            if (nf.is_v4) {
                try coding.putVarint32(buf, gpa, Tag.kNewFile4);
                try coding.putVarint32(buf, gpa, nf.level);
                try coding.putVarint64(buf, gpa, nf.meta.number);
                try coding.putVarint64(buf, gpa, nf.meta.file_size);
                try coding.putLengthPrefixedSlice(buf, gpa, nf.meta.smallest);
                try coding.putLengthPrefixedSlice(buf, gpa, nf.meta.largest);
                try coding.putVarint64(buf, gpa, nf.meta.smallest_seqno);
                try coding.putVarint64(buf, gpa, nf.meta.largest_seqno);
                // We emit no custom fields beyond the terminator.
                try coding.putVarint32(buf, gpa, CustomTag.kTerminate);
            } else {
                try coding.putVarint32(buf, gpa, Tag.kNewFile);
                try coding.putVarint32(buf, gpa, nf.level);
                try coding.putVarint64(buf, gpa, nf.meta.number);
                try coding.putVarint64(buf, gpa, nf.meta.file_size);
                try coding.putLengthPrefixedSlice(buf, gpa, nf.meta.smallest);
                try coding.putLengthPrefixedSlice(buf, gpa, nf.meta.largest);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Decoding
    // -----------------------------------------------------------------------

    /// Parse a VersionEdit from `data`.  All variable-length byte slices
    /// (comparator_name, smallest, largest) are duped into edit-owned memory.
    /// Returns error.Corruption on truncation or unknown tag.
    pub fn decodeFrom(gpa: std.mem.Allocator, data: []const u8) Error!VersionEdit {
        var edit = VersionEdit.init();
        errdefer edit.deinit(gpa);

        var input: []const u8 = data;
        while (input.len > 0) {
            const tag = try coding.getVarint32(&input);
            switch (tag) {
                Tag.kComparator => {
                    const raw = try coding.getLengthPrefixedSlice(&input);
                    if (edit.comparator_name) |old| gpa.free(old);
                    edit.comparator_name = try dupSlice(gpa, raw);
                },
                Tag.kLogNumber => {
                    edit.log_number = try coding.getVarint64(&input);
                },
                Tag.kPrevLogNumber => {
                    edit.prev_log_number = try coding.getVarint64(&input);
                },
                Tag.kNextFileNumber => {
                    edit.next_file_number = try coding.getVarint64(&input);
                },
                Tag.kLastSequence => {
                    edit.last_sequence = try coding.getVarint64(&input);
                },
                Tag.kCompactPointer => {
                    // TODO(m5.1): store compact pointers when needed.
                    // For now consume the fields so we don't error on them.
                    _ = try coding.getVarint32(&input); // level
                    _ = try coding.getLengthPrefixedSlice(&input); // internal key
                },
                Tag.kDeletedFile => {
                    const level = try coding.getVarint32(&input);
                    const number = try coding.getVarint64(&input);
                    try edit.deleted_files.append(gpa, .{ .level = level, .number = number });
                },
                Tag.kNewFile => {
                    const level = try coding.getVarint32(&input);
                    const number = try coding.getVarint64(&input);
                    const file_size = try coding.getVarint64(&input);
                    const smallest_raw = try coding.getLengthPrefixedSlice(&input);
                    const largest_raw = try coding.getLengthPrefixedSlice(&input);
                    const smallest = try dupSlice(gpa, smallest_raw);
                    errdefer gpa.free(smallest);
                    const largest = try dupSlice(gpa, largest_raw);
                    errdefer gpa.free(largest);
                    try edit.new_files.append(gpa, .{
                        .level = level,
                        .meta = .{
                            .number = number,
                            .file_size = file_size,
                            .smallest = smallest,
                            .largest = largest,
                        },
                    });
                },
                Tag.kNewFile4 => {
                    const level = try coding.getVarint32(&input);
                    const number = try coding.getVarint64(&input);
                    const file_size = try coding.getVarint64(&input);
                    const smallest_raw = try coding.getLengthPrefixedSlice(&input);
                    const largest_raw = try coding.getLengthPrefixedSlice(&input);
                    const smallest_seqno = try coding.getVarint64(&input);
                    const largest_seqno = try coding.getVarint64(&input);
                    // Custom-field loop: read (tag, length-prefixed value) pairs
                    // until kTerminate.  Unknown tags with the non-safe-ignore
                    // mask bit set are a Corruption; others are skipped.
                    while (true) {
                        const ctag = try coding.getVarint32(&input);
                        if (ctag == CustomTag.kTerminate) break;
                        _ = try coding.getLengthPrefixedSlice(&input); // skip value
                        if (ctag & CustomTag.kSafeIgnoreMask != 0) return error.Corruption;
                    }
                    const smallest = try dupSlice(gpa, smallest_raw);
                    errdefer gpa.free(smallest);
                    const largest = try dupSlice(gpa, largest_raw);
                    errdefer gpa.free(largest);
                    try edit.new_files.append(gpa, .{
                        .level = level,
                        .is_v4 = true,
                        .meta = .{
                            .number = number,
                            .file_size = file_size,
                            .smallest = smallest,
                            .largest = largest,
                            .smallest_seqno = smallest_seqno,
                            .largest_seqno = largest_seqno,
                        },
                    });
                },
                else => return error.Corruption,
            }
        }
        return edit;
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

    // Internal keys: user key + 8-byte sequence-number/type trailer.
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

    // Scalars
    try std.testing.expectEqualStrings("leveldb.BytewiseComparator", edit2.comparator_name.?);
    try std.testing.expectEqual(@as(u64, 5), edit2.log_number.?);
    try std.testing.expectEqual(@as(u64, 4), edit2.prev_log_number.?);
    try std.testing.expectEqual(@as(u64, 10), edit2.next_file_number.?);
    try std.testing.expectEqual(@as(u64, 100), edit2.last_sequence.?);

    // New file
    try std.testing.expectEqual(@as(usize, 1), edit2.new_files.items.len);
    const nf = edit2.new_files.items[0];
    try std.testing.expectEqual(@as(u32, 1), nf.level);
    try std.testing.expectEqual(@as(u64, 7), nf.meta.number);
    try std.testing.expectEqual(@as(u64, 1000), nf.meta.file_size);
    try std.testing.expectEqualSlices(u8, smallest, nf.meta.smallest);
    try std.testing.expectEqualSlices(u8, largest, nf.meta.largest);

    // Deleted file
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
    // Tag 2 (kLogNumber) present but no varint64 following it.
    const data = [_]u8{0x02};
    try std.testing.expectError(error.Corruption, VersionEdit.decodeFrom(gpa, &data));
}

test "corruption: truncated length-prefixed string" {
    const gpa = std.testing.allocator;
    // Tag 1 (kComparator) + length prefix 10 + only 3 bytes of payload.
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try coding.putVarint32(&buf, gpa, Tag.kComparator);
    try coding.putVarint32(&buf, gpa, 10); // claims 10 bytes
    try buf.appendSlice(gpa, "abc"); // only 3 bytes
    try std.testing.expectError(error.Corruption, VersionEdit.decodeFrom(gpa, buf.items));
}

test "corruption: unknown tag" {
    const gpa = std.testing.allocator;
    // Tag 42 is not defined.
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try coding.putVarint32(&buf, gpa, 42);
    try std.testing.expectError(error.Corruption, VersionEdit.decodeFrom(gpa, buf.items));
}

// ---------------------------------------------------------------------------
// kNewFile4 (RocksDB tag=100) tests
// ---------------------------------------------------------------------------

test "newfile4: golden byte prefix + terminate (no custom fields)" {
    const gpa = std.testing.allocator;
    const smallest = "a" ++ [_]u8{0} ** 8; // 9 bytes
    const largest = "z" ++ [_]u8{0} ** 8; // 9 bytes

    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    try edit.addFile4(gpa, 1, 7, 1000, smallest, largest, 42, 99);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try edit.encodeTo(&buf, gpa);

    // tag=100 (0x64), level=1, number=7, file_size=1000 (0xe8,0x07),
    // smallest len=9 + 9 bytes, largest len=9 + 9 bytes,
    // smallest_seqno=42 (0x2a), largest_seqno=99 (0x63), kTerminate=1 (0x01).
    var expected: std.ArrayListUnmanaged(u8) = .empty;
    defer expected.deinit(gpa);
    try expected.appendSlice(gpa, &[_]u8{ 0x64, 0x01, 0x07, 0xe8, 0x07 });
    try expected.append(gpa, 0x09);
    try expected.appendSlice(gpa, smallest);
    try expected.append(gpa, 0x09);
    try expected.appendSlice(gpa, largest);
    try expected.appendSlice(gpa, &[_]u8{ 0x2a, 0x63, 0x01 });

    try std.testing.expectEqualSlices(u8, expected.items, buf.items);
}

test "newfile4: full round-trip preserves seqnos" {
    const gpa = std.testing.allocator;
    const smallest = "abc" ++ [_]u8{0} ** 8;
    const largest = "xyz" ++ [_]u8{0} ** 8;

    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    edit.setLogNumber(5);
    try edit.addFile4(gpa, 2, 33, 4096, smallest, largest, 1000, 2000);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try edit.encodeTo(&buf, gpa);

    var edit2 = try VersionEdit.decodeFrom(gpa, buf.items);
    defer edit2.deinit(gpa);

    try std.testing.expectEqual(@as(u64, 5), edit2.log_number.?);
    try std.testing.expectEqual(@as(usize, 1), edit2.new_files.items.len);
    const nf = edit2.new_files.items[0];
    try std.testing.expectEqual(@as(u32, 2), nf.level);
    try std.testing.expectEqual(@as(u64, 33), nf.meta.number);
    try std.testing.expectEqual(@as(u64, 4096), nf.meta.file_size);
    try std.testing.expectEqualSlices(u8, smallest, nf.meta.smallest);
    try std.testing.expectEqualSlices(u8, largest, nf.meta.largest);
    try std.testing.expectEqual(@as(u64, 1000), nf.meta.smallest_seqno);
    try std.testing.expectEqual(@as(u64, 2000), nf.meta.largest_seqno);
}

test "newfile4: decode skips safe-to-ignore custom fields" {
    const gpa = std.testing.allocator;
    const smallest = "a" ++ [_]u8{0} ** 8;
    const largest = "b" ++ [_]u8{0} ** 8;

    // Build a kNewFile4 record by hand including an unknown safe-to-ignore
    // custom tag (kFileCreationTime=6) with a length-prefixed payload, then
    // kTerminate=1.  Decode must skip the custom field and succeed.
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try coding.putVarint32(&buf, gpa, 100); // kNewFile4
    try coding.putVarint32(&buf, gpa, 0); // level
    try coding.putVarint64(&buf, gpa, 9); // number
    try coding.putVarint64(&buf, gpa, 512); // file_size
    try coding.putLengthPrefixedSlice(&buf, gpa, smallest);
    try coding.putLengthPrefixedSlice(&buf, gpa, largest);
    try coding.putVarint64(&buf, gpa, 7); // smallest_seqno
    try coding.putVarint64(&buf, gpa, 8); // largest_seqno
    try coding.putVarint32(&buf, gpa, 6); // kFileCreationTime (safe to ignore)
    try coding.putLengthPrefixedSlice(&buf, gpa, &[_]u8{ 0xde, 0xad });
    try coding.putVarint32(&buf, gpa, 1); // kTerminate

    var edit = try VersionEdit.decodeFrom(gpa, buf.items);
    defer edit.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 1), edit.new_files.items.len);
    const nf = edit.new_files.items[0];
    try std.testing.expectEqual(@as(u64, 9), nf.meta.number);
    try std.testing.expectEqual(@as(u64, 7), nf.meta.smallest_seqno);
    try std.testing.expectEqual(@as(u64, 8), nf.meta.largest_seqno);
}

test "newfile4: decode rejects unknown non-safe-to-ignore custom field" {
    const gpa = std.testing.allocator;
    const smallest = "a" ++ [_]u8{0} ** 8;
    const largest = "b" ++ [_]u8{0} ** 8;

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try coding.putVarint32(&buf, gpa, 100); // kNewFile4
    try coding.putVarint32(&buf, gpa, 0); // level
    try coding.putVarint64(&buf, gpa, 9); // number
    try coding.putVarint64(&buf, gpa, 512); // file_size
    try coding.putLengthPrefixedSlice(&buf, gpa, smallest);
    try coding.putLengthPrefixedSlice(&buf, gpa, largest);
    try coding.putVarint64(&buf, gpa, 7); // smallest_seqno
    try coding.putVarint64(&buf, gpa, 8); // largest_seqno
    // 0x40 (64) sets the non-safe-to-ignore mask bit → must error.
    try coding.putVarint32(&buf, gpa, 0x40);
    try coding.putLengthPrefixedSlice(&buf, gpa, &[_]u8{0x01});

    try std.testing.expectError(error.Corruption, VersionEdit.decodeFrom(gpa, buf.items));
}

test "newfile4 and newfile (v7) coexist; default seqnos are zero" {
    const gpa = std.testing.allocator;
    const ka = "ka" ++ [_]u8{0} ** 8;
    const kb = "kb" ++ [_]u8{0} ** 8;
    const kc = "kc" ++ [_]u8{0} ** 8;
    const kd = "kd" ++ [_]u8{0} ** 8;

    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    try edit.addFile(gpa, 0, 10, 100, ka, kb); // v7, seqnos default to 0
    try edit.addFile4(gpa, 1, 20, 200, kc, kd, 5, 6); // v4

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try edit.encodeTo(&buf, gpa);

    var edit2 = try VersionEdit.decodeFrom(gpa, buf.items);
    defer edit2.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 2), edit2.new_files.items.len);
    // v7 file: seqnos zero.
    try std.testing.expectEqual(@as(u64, 10), edit2.new_files.items[0].meta.number);
    try std.testing.expectEqual(@as(u64, 0), edit2.new_files.items[0].meta.smallest_seqno);
    try std.testing.expectEqual(@as(u64, 0), edit2.new_files.items[0].meta.largest_seqno);
    // v4 file: seqnos preserved.
    try std.testing.expectEqual(@as(u64, 20), edit2.new_files.items[1].meta.number);
    try std.testing.expectEqual(@as(u64, 5), edit2.new_files.items[1].meta.smallest_seqno);
    try std.testing.expectEqual(@as(u64, 6), edit2.new_files.items[1].meta.largest_seqno);
}
