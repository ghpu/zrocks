//! log_reader.zig — WAL/MANIFEST log record reader (legacy LevelDB format).
//!
//! See `log_format.zig` for the byte layout. The reader pulls physical blocks
//! from a `SequentialFile`, verifies each fragment's masked CRC32C, and
//! reassembles `first`/`middle`/`last` fragment chains into a single logical
//! record. `full` fragments are returned directly.
//!
//! Truncated-tail policy (matches LevelDB's default, non-checksum-strict
//! behavior): a partial/truncated record at the physical end of the file —
//! including a short final block, a header that is cut off, or a payload that
//! the file does not actually contain — is treated as a clean end-of-stream
//! and reported as `null` rather than `error.Corruption`. A bad CRC, a bad
//! record length within a fully-present block, or an out-of-order fragment
//! chain that is NOT at the physical tail is reported as `error.Corruption`.

const std = @import("std");
const env = @import("../env/env.zig");
const format = @import("log_format.zig");
const crc32c = @import("../util/crc32c.zig");

pub const Error = error{Corruption} || env.Error;

const kBlockSize = format.kBlockSize;
const kHeaderSize = format.kHeaderSize;

/// Outcome of parsing one physical record from the current block buffer.
const Fragment = union(enum) {
    /// A well-formed fragment with a verified checksum.
    ok: struct { record_type: format.RecordType, payload: []const u8 },
    /// The current block has no more parseable records (trailer/short tail);
    /// the caller should load the next block.
    end_of_block,
    /// Clean end of stream — no more data and nothing partial to report.
    eof,
};

pub const Reader = struct {
    file: env.SequentialFile,
    /// Backing store for the current physical block.
    buf: [kBlockSize]u8 = undefined,
    /// Number of valid bytes currently in `buf`.
    buf_len: usize = 0,
    /// Read cursor within `buf`.
    pos: usize = 0,
    /// True once a short (sub-block) read has been observed: the file has no
    /// further blocks, so a fragment that runs past `buf_len` is a truncated
    /// tail (clean EOF) rather than corruption.
    eof_seen: bool = false,

    pub fn init(file: env.SequentialFile) Reader {
        return .{ .file = file };
    }

    /// Read the next logical record. For a `full` fragment the returned slice
    /// points into the internal block buffer and is valid until the next
    /// `readRecord` call; for a reassembled record the slice points into
    /// `scratch`. Returns `null` at clean end-of-stream.
    pub fn readRecord(
        self: *Reader,
        gpa: std.mem.Allocator,
        scratch: *std.ArrayList(u8),
    ) Error!?[]const u8 {
        scratch.clearRetainingCapacity();
        // `in_fragmented` tracks whether we are mid-way through a
        // first/middle/last chain so we can detect out-of-order fragments.
        var in_fragmented = false;

        while (true) {
            switch (try self.readPhysicalRecord()) {
                .eof => {
                    // Clean EOF. If we were mid-chain, the tail was truncated:
                    // still a clean EOF under the default LevelDB policy.
                    return null;
                },
                .end_of_block => {
                    if (!try self.loadBlock()) return null;
                },
                .ok => |frag| switch (frag.record_type) {
                    .full => {
                        if (in_fragmented) return error.Corruption;
                        return frag.payload;
                    },
                    .first => {
                        if (in_fragmented) return error.Corruption;
                        in_fragmented = true;
                        try scratch.appendSlice(gpa, frag.payload);
                    },
                    .middle => {
                        if (!in_fragmented) return error.Corruption;
                        try scratch.appendSlice(gpa, frag.payload);
                    },
                    .last => {
                        if (!in_fragmented) return error.Corruption;
                        try scratch.appendSlice(gpa, frag.payload);
                        return scratch.items;
                    },
                    .zero => return error.Corruption,
                },
            }
        }
    }

    /// Fill `buf` with the next physical block (up to `kBlockSize` bytes).
    /// Returns `false` if no bytes were available (clean EOF). A short read
    /// marks `eof_seen` so the parser treats a later run-past-end as a
    /// truncated tail rather than corruption.
    fn loadBlock(self: *Reader) Error!bool {
        self.pos = 0;
        self.buf_len = 0;
        // `SequentialFile.read` may return short; loop until the block is full
        // or the file ends.
        while (self.buf_len < kBlockSize) {
            const n = try self.file.read(self.buf[self.buf_len..]);
            if (n == 0) {
                self.eof_seen = true;
                break;
            }
            self.buf_len += n;
        }
        return self.buf_len > 0;
    }

    /// Parse one physical record at `pos` within the current block buffer.
    fn readPhysicalRecord(self: *Reader) Error!Fragment {
        // Lazily load the first block.
        if (self.buf_len == 0 and self.pos == 0 and !self.eof_seen) {
            if (!try self.loadBlock()) return .eof;
        }

        const remaining = self.buf_len - self.pos;
        if (remaining < kHeaderSize) {
            // Trailer zero-padding (or a short tail that can't hold a header).
            if (self.eof_seen) {
                // No further blocks. Any leftover < header is a clean tail.
                return .eof;
            }
            // More blocks follow: skip this block's trailer.
            return .end_of_block;
        }

        const header = self.buf[self.pos .. self.pos + kHeaderSize];
        const stored_crc = std.mem.readInt(u32, header[0..4], .little);
        const length: usize = std.mem.readInt(u16, header[4..6], .little);
        const type_byte = header[6];

        // A zero type with zero length at end-of-data is the canonical trailer
        // marker; treat as end-of-block padding.
        if (type_byte == @intFromEnum(format.RecordType.zero) and length == 0) {
            if (self.eof_seen) return .eof;
            return .end_of_block;
        }

        if (kHeaderSize + length > remaining) {
            // The record claims more bytes than the block holds.
            if (self.eof_seen) {
                // Physical end of file in the middle of a record -> truncated
                // tail -> clean EOF (default LevelDB behavior).
                return .eof;
            }
            // A well-formed writer never lets a record's length exceed the
            // block, so this is real corruption.
            return error.Corruption;
        }

        if (type_byte > format.kMaxRecordType) return error.Corruption;
        const record_type: format.RecordType = @enumFromInt(type_byte);

        const payload = self.buf[self.pos + kHeaderSize .. self.pos + kHeaderSize + length];

        // Verify checksum: unmask, recompute over [type] ++ payload, compare.
        const want = crc32c.unmask(stored_crc);
        const got = crc32c.extend(crc32c.value(&[_]u8{type_byte}), payload);
        if (want != got) return error.Corruption;

        self.pos += kHeaderSize + length;
        return .{ .ok = .{ .record_type = record_type, .payload = payload } };
    }
};

// ===========================================================================
// Tests — writer <-> reader round-trips and byte-level golden vectors.
// ===========================================================================

const log_writer = @import("log_writer.zig");
const Writer = log_writer.Writer;

/// Read the entire committed contents of `path` into a freshly-allocated
/// buffer (caller frees). Used by the golden-header test to inspect raw bytes.
fn readAllBytes(e: env.Env, gpa: std.mem.Allocator, path: []const u8) ![]u8 {
    var sf = try e.newSequentialFile(gpa, path);
    defer sf.close() catch {};
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var chunk: [4096]u8 = undefined;
    while (true) {
        const n = try sf.read(&chunk);
        if (n == 0) break;
        try out.appendSlice(gpa, chunk[0..n]);
    }
    return out.toOwnedSlice(gpa);
}

test "golden header: single full record \"hello\"" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        var wf = try e.newWritableFile(gpa, "wal");
        var w = Writer.init(wf);
        errdefer wf.close() catch {};
        try w.addRecord(gpa, "hello");
        try wf.flush();
        try wf.close();
    }

    const bytes = try readAllBytes(e, gpa, "wal");
    defer gpa.free(bytes);

    // 7-byte header + 5-byte payload.
    try std.testing.expectEqual(@as(usize, format.kHeaderSize + 5), bytes.len);

    // checksum: mask(crc32c({1} ++ "hello")), LE in bytes[0..4].
    const expected_crc = crc32c.mask(crc32c.value(&[_]u8{1} ++ "hello".*));
    const stored_crc = std.mem.readInt(u32, bytes[0..4], .little);
    try std.testing.expectEqual(expected_crc, stored_crc);

    // length = 5, LE in bytes[4..6].
    const stored_len = std.mem.readInt(u16, bytes[4..6], .little);
    try std.testing.expectEqual(@as(u16, 5), stored_len);

    // type = full (1).
    try std.testing.expectEqual(@as(u8, @intFromEnum(format.RecordType.full)), bytes[6]);

    // payload.
    try std.testing.expectEqualStrings("hello", bytes[7..]);
}

/// Helper: write `records` to a MemEnv WAL, then read them back and assert the
/// read sequence equals the written sequence exactly.
fn roundTrip(records: []const []const u8) !void {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        var wf = try e.newWritableFile(gpa, "wal");
        var w = Writer.init(wf);
        errdefer wf.close() catch {};
        for (records) |rec| try w.addRecord(gpa, rec);
        try wf.flush();
        try wf.close();
    }

    var sf = try e.newSequentialFile(gpa, "wal");
    var r = Reader.init(sf);
    defer sf.close() catch {};

    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(gpa);

    var i: usize = 0;
    while (try r.readRecord(gpa, &scratch)) |rec| : (i += 1) {
        try std.testing.expect(i < records.len);
        try std.testing.expectEqualSlices(u8, records[i], rec);
    }
    try std.testing.expectEqual(records.len, i);
}

test "round-trip: single small record" {
    try roundTrip(&[_][]const u8{"hello world"});
}

test "round-trip: empty payload" {
    try roundTrip(&[_][]const u8{""});
}

test "round-trip: several records of varying sizes" {
    try roundTrip(&[_][]const u8{
        "",
        "a",
        "bb",
        "the quick brown fox",
        "x" ** 100,
        "y" ** 1000,
    });
}

test "round-trip: record straddling a block boundary" {
    const gpa = std.testing.allocator;
    // First record consumes most of block 0 so the next record's header lands
    // right at / across the boundary, forcing fragmentation.
    // Block 0 capacity for payload after one header = kBlockSize - kHeaderSize.
    const filler_len = format.kBlockSize - format.kHeaderSize - 3; // leave 3 bytes < header
    const filler = try gpa.alloc(u8, filler_len);
    defer gpa.free(filler);
    @memset(filler, 'A');

    // A second record that must start in a fresh block (only 3 bytes < 7 left).
    const second = "second record after padding";
    try roundTrip(&[_][]const u8{ filler, second });
}

test "round-trip: fragment lands exactly at the 32KB edge" {
    const gpa = std.testing.allocator;
    // Make a single record whose first fragment exactly fills block 0:
    // payload size = kBlockSize - kHeaderSize fills block 0 completely as a
    // `first` fragment, then a `last` fragment of size 1 in block 1.
    const big_len = (format.kBlockSize - format.kHeaderSize) + 1;
    const big = try gpa.alloc(u8, big_len);
    defer gpa.free(big);
    for (big, 0..) |*b, idx| b.* = @intCast(idx & 0xff);
    try roundTrip(&[_][]const u8{big});
}

test "round-trip: large record fragments first/middle/last across 3 blocks" {
    const gpa = std.testing.allocator;
    // > 2 * kBlockSize so we get first + middle + last.
    const big_len = 2 * format.kBlockSize + 1234;
    const big = try gpa.alloc(u8, big_len);
    defer gpa.free(big);
    var seed: u32 = 12345;
    for (big) |*b| {
        // simple LCG for deterministic varied bytes
        seed = seed *% 1664525 +% 1013904223;
        b.* = @intCast((seed >> 24) & 0xff);
    }
    try roundTrip(&[_][]const u8{big});
}

test "round-trip: remaining block space < 7 bytes forces zero padding" {
    const gpa = std.testing.allocator;
    // Leave exactly 6 bytes (< kHeaderSize) at the end of block 0.
    const filler_len = format.kBlockSize - format.kHeaderSize - 6;
    const filler = try gpa.alloc(u8, filler_len);
    defer gpa.free(filler);
    @memset(filler, 'Z');
    try roundTrip(&[_][]const u8{ filler, "next block please", "and another" });
}

test "round-trip: many records crossing several blocks" {
    const gpa = std.testing.allocator;
    var records: std.ArrayList([]const u8) = .empty;
    defer {
        for (records.items) |rec| gpa.free(@constCast(rec));
        records.deinit(gpa);
    }
    var i: usize = 0;
    while (i < 200) : (i += 1) {
        const len = 100 + (i * 37) % 5000;
        const buf = try gpa.alloc(u8, len);
        @memset(buf, @intCast('a' + (i % 26)));
        try records.append(gpa, buf);
    }
    try roundTrip(records.items);
}

test "corruption: flipping a payload byte yields error.Corruption" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        var wf = try e.newWritableFile(gpa, "wal");
        var w = Writer.init(wf);
        errdefer wf.close() catch {};
        try w.addRecord(gpa, "corrupt me please");
        try wf.flush();
        try wf.close();
    }

    // Read raw bytes, flip a payload byte, write them back.
    const bytes = try readAllBytes(e, gpa, "wal");
    defer gpa.free(bytes);
    bytes[format.kHeaderSize + 3] ^= 0xff; // flip a payload byte

    {
        var wf = try e.newWritableFile(gpa, "wal"); // truncates
        errdefer wf.close() catch {};
        try wf.append(bytes);
        try wf.flush();
        try wf.close();
    }

    var sf = try e.newSequentialFile(gpa, "wal");
    var r = Reader.init(sf);
    defer sf.close() catch {};
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(gpa);
    try std.testing.expectError(error.Corruption, r.readRecord(gpa, &scratch));
}

test "truncated tail: truncating mid-record yields clean EOF (null)" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        var wf = try e.newWritableFile(gpa, "wal");
        var w = Writer.init(wf);
        errdefer wf.close() catch {};
        try w.addRecord(gpa, "first record ok");
        try w.addRecord(gpa, "this second record will be truncated away");
        try wf.flush();
        try wf.close();
    }

    const bytes = try readAllBytes(e, gpa, "wal");
    defer gpa.free(bytes);

    // First record occupies kHeaderSize + len("first record ok") bytes.
    const first_total = format.kHeaderSize + "first record ok".len;
    // Keep the first whole record plus a few bytes of the second record's
    // header/payload, then chop the rest off.
    const truncated_len = first_total + 4;

    {
        var wf = try e.newWritableFile(gpa, "wal");
        errdefer wf.close() catch {};
        try wf.append(bytes[0..truncated_len]);
        try wf.flush();
        try wf.close();
    }

    var sf = try e.newSequentialFile(gpa, "wal");
    var r = Reader.init(sf);
    defer sf.close() catch {};
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(gpa);

    // First record reads fine.
    const rec0 = try r.readRecord(gpa, &scratch);
    try std.testing.expect(rec0 != null);
    try std.testing.expectEqualSlices(u8, "first record ok", rec0.?);

    // Truncated second record -> clean EOF, NOT an error.
    const rec1 = try r.readRecord(gpa, &scratch);
    try std.testing.expect(rec1 == null);
}

test "truncated tail: truncating in the middle of a header yields clean EOF" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        var wf = try e.newWritableFile(gpa, "wal");
        var w = Writer.init(wf);
        errdefer wf.close() catch {};
        try w.addRecord(gpa, "only record");
        try wf.flush();
        try wf.close();
    }

    const bytes = try readAllBytes(e, gpa, "wal");
    defer gpa.free(bytes);

    {
        var wf = try e.newWritableFile(gpa, "wal");
        errdefer wf.close() catch {};
        try wf.append(bytes[0..3]); // only 3 of 7 header bytes
        try wf.flush();
        try wf.close();
    }

    var sf = try e.newSequentialFile(gpa, "wal");
    var r = Reader.init(sf);
    defer sf.close() catch {};
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(gpa);
    try std.testing.expect((try r.readRecord(gpa, &scratch)) == null);
}
