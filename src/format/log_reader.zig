//! log_reader.zig — WAL/MANIFEST log record reader.
//!
//! See `log_format.zig` for the byte layout. The reader pulls physical blocks
//! from a `SequentialFile`, verifies each fragment's masked CRC32C, and
//! reassembles `first`/`middle`/`last` fragment chains into a single logical
//! record. `full` fragments are returned directly.
//!
//! Both on-disk formats are understood transparently. The reader inspects each
//! record's type byte to decide whether it is a legacy record (types 1-4,
//! 7-byte header) or a recyclable record (types 5-8, 11-byte header carrying a
//! log_number). For recyclable records the reader optionally enforces an
//! expected log_number: a record stamped with a different log number is a stale
//! leftover in a recycled file and is treated as a clean end-of-stream. Set the
//! expected log number with `initRecyclable`; the default reader (`init`)
//! accepts any log number.
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
const kRecyclableHeaderSize = format.kRecyclableHeaderSize;

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
    /// False until the first physical block has been loaded.
    started: bool = false,
    /// When set, recyclable records (types 5-8) whose stamped log_number does
    /// not equal this value are treated as stale leftovers from a recycled
    /// file and reported as a clean end-of-stream. `null` accepts any log
    /// number (and never rejects on this basis).
    expected_log_number: ?u32 = null,

    pub fn init(file: env.SequentialFile) Reader {
        return .{ .file = file };
    }

    /// Construct a reader that enforces `log_number` on recyclable records: any
    /// recyclable fragment carrying a different log number is treated as a
    /// clean end-of-stream (a stale record from the file's previous life).
    /// Legacy records (types 1-4) are unaffected.
    pub fn initRecyclable(file: env.SequentialFile, log_number: u32) Reader {
        return .{ .file = file, .expected_log_number = log_number };
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
                    .full, .recyclable_full => {
                        if (in_fragmented) return error.Corruption;
                        return frag.payload;
                    },
                    .first, .recyclable_first => {
                        if (in_fragmented) return error.Corruption;
                        in_fragmented = true;
                        try scratch.appendSlice(gpa, frag.payload);
                    },
                    .middle, .recyclable_middle => {
                        if (!in_fragmented) return error.Corruption;
                        try scratch.appendSlice(gpa, frag.payload);
                    },
                    .last, .recyclable_last => {
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
        self.started = true;
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
        // Lazily load the first block on the very first call.
        if (!self.started) {
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

        if (type_byte > format.kMaxRecyclableRecordType) return error.Corruption;
        const record_type: format.RecordType = @enumFromInt(type_byte);
        const recyclable = format.isRecyclable(record_type);
        // Recyclable records carry a 4-byte log_number after the legacy header.
        const header_size: usize = if (recyclable) kRecyclableHeaderSize else kHeaderSize;

        if (header_size + length > remaining) {
            // The record claims more bytes than the block holds (or the
            // recyclable log_number field itself runs past the block).
            if (self.eof_seen) {
                // Physical end of file in the middle of a record -> truncated
                // tail -> clean EOF (default LevelDB behavior).
                return .eof;
            }
            // A well-formed writer never lets a record's length exceed the
            // block, so this is real corruption.
            return error.Corruption;
        }

        const payload = self.buf[self.pos + header_size .. self.pos + header_size + length];

        if (recyclable) {
            const log_number = std.mem.readInt(u32, self.buf[self.pos + 7 .. self.pos + 11][0..4], .little);
            // Verify checksum over [type] ++ log_number_LE ++ payload. Both
            // `stored_crc` and `recyclableChecksum` are masked CRC32C values.
            const expected_crc = format.recyclableChecksum(record_type, log_number, payload);
            if (stored_crc != expected_crc) return error.Corruption;
            // A recyclable record whose log_number does not match the expected
            // one is a stale leftover from a recycled file: clean end-of-stream.
            if (self.expected_log_number) |expected| {
                if (log_number != expected) return .eof;
            }
        } else {
            // Verify checksum: unmask, recompute over [type] ++ payload, compare.
            const want = crc32c.unmask(stored_crc);
            const got = crc32c.extend(crc32c.value(&[_]u8{type_byte}), payload);
            if (want != got) return error.Corruption;
        }

        self.pos += header_size + length;
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

/// Helper: write `records` to a MemEnv WAL in recyclable format stamped with
/// `log_number`, then read them back and assert the read sequence matches. The
/// reader is constructed via `init` (accepts any log number).
fn roundTripRecyclable(log_number: u32, records: []const []const u8) !void {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        var wf = try e.newWritableFile(gpa, "wal");
        var w = Writer.initRecyclable(wf, log_number);
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

/// Encode one physical record (valid CRC) into `out`.
fn encodeRecord(
    out: *std.ArrayList(u8),
    gpa: std.mem.Allocator,
    record_type: format.RecordType,
    payload: []const u8,
) !void {
    var header: [format.kHeaderSize]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], format.checksum(record_type, payload), .little);
    std.mem.writeInt(u16, header[4..6], @intCast(payload.len), .little);
    header[6] = @intFromEnum(record_type);
    try out.appendSlice(gpa, &header);
    try out.appendSlice(gpa, payload);
}

test "corruption: stray `last` fragment with no preceding `first`" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(gpa);
    // A lone `last` fragment (valid CRC) that was never preceded by a `first`.
    try encodeRecord(&raw, gpa, .last, "orphan tail");

    {
        var wf = try e.newWritableFile(gpa, "wal");
        errdefer wf.close() catch {};
        try wf.append(raw.items);
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

test "corruption: bad record length within a fully-present block (not the tail)" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Build a full 32 KiB block 0 whose first record header claims a length
    // larger than the bytes remaining in the block. Because block 0 is fully
    // present (a second block follows), `eof_seen` is false while parsing it,
    // so the over-long length is corruption — not a truncated tail.
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(gpa);

    // Header claiming a payload that overruns the block remainder.
    const bogus_payload_claim: u16 = @intCast(format.kBlockSize); // >> remainder
    var header: [format.kHeaderSize]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], format.checksum(.full, ""), .little);
    std.mem.writeInt(u16, header[4..6], bogus_payload_claim, .little);
    header[6] = @intFromEnum(format.RecordType.full);
    try raw.appendSlice(gpa, &header);

    // Pad block 0 out to exactly kBlockSize with zeros (acts as a trailer-ish
    // filler; the parser never reaches it because the header already faults).
    try raw.appendNTimes(gpa, 0, format.kBlockSize - raw.items.len);
    // A valid record in block 1 so the file genuinely has a second block.
    try encodeRecord(&raw, gpa, .full, "block one record");
    try std.testing.expectEqual(@as(usize, format.kBlockSize + format.kHeaderSize + "block one record".len), raw.items.len);

    {
        var wf = try e.newWritableFile(gpa, "wal");
        errdefer wf.close() catch {};
        try wf.append(raw.items);
        try wf.flush();
        try wf.close();
    }

    var sf = try e.newSequentialFile(gpa, "wal");
    var r = Reader.init(sf);
    defer sf.close() catch {};
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(gpa);

    // Bad length in a fully-present (non-tail) block -> Corruption.
    try std.testing.expectError(error.Corruption, r.readRecord(gpa, &scratch));
}

// ===========================================================================
// Recyclable-log format tests (record types 5-8, 11-byte header).
// ===========================================================================

test "recyclable golden header: single full record \"hello\"" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const log_number: u32 = 0x11223344;
    {
        var wf = try e.newWritableFile(gpa, "wal");
        var w = Writer.initRecyclable(wf, log_number);
        errdefer wf.close() catch {};
        try w.addRecord(gpa, "hello");
        try wf.flush();
        try wf.close();
    }

    const bytes = try readAllBytes(e, gpa, "wal");
    defer gpa.free(bytes);

    // 11-byte recyclable header + 5-byte payload.
    try std.testing.expectEqual(@as(usize, format.kRecyclableHeaderSize + 5), bytes.len);

    // checksum: mask(crc32c({5} ++ log_number_LE ++ "hello")), LE in bytes[0..4].
    const expected_crc = format.recyclableChecksum(.recyclable_full, log_number, "hello");
    const stored_crc = std.mem.readInt(u32, bytes[0..4], .little);
    try std.testing.expectEqual(expected_crc, stored_crc);

    // length = 5, LE in bytes[4..6].
    const stored_len = std.mem.readInt(u16, bytes[4..6], .little);
    try std.testing.expectEqual(@as(u16, 5), stored_len);

    // type = recyclable_full (5).
    try std.testing.expectEqual(
        @as(u8, @intFromEnum(format.RecordType.recyclable_full)),
        bytes[6],
    );

    // log_number, LE in bytes[7..11].
    const stored_log = std.mem.readInt(u32, bytes[7..11], .little);
    try std.testing.expectEqual(log_number, stored_log);

    // payload.
    try std.testing.expectEqualStrings("hello", bytes[11..]);
}

test "recyclable round-trip: small record" {
    try roundTripRecyclable(7, &[_][]const u8{"hello world"});
}

test "recyclable round-trip: empty payload" {
    try roundTripRecyclable(1, &[_][]const u8{""});
}

test "recyclable round-trip: several records of varying sizes" {
    try roundTripRecyclable(99, &[_][]const u8{
        "",
        "a",
        "bb",
        "the quick brown fox",
        "x" ** 100,
        "y" ** 1000,
    });
}

test "recyclable round-trip: large record fragments first/middle/last across blocks" {
    const gpa = std.testing.allocator;
    const big_len = 2 * format.kBlockSize + 1234;
    const big = try gpa.alloc(u8, big_len);
    defer gpa.free(big);
    var seed: u32 = 777;
    for (big) |*b| {
        seed = seed *% 1664525 +% 1013904223;
        b.* = @intCast((seed >> 24) & 0xff);
    }
    try roundTripRecyclable(12345, &[_][]const u8{big});
}

test "recyclable round-trip: many records crossing several blocks" {
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
    try roundTripRecyclable(0xABCD, records.items);
}

test "recyclable round-trip: trailer padding when < 11 bytes remain" {
    const gpa = std.testing.allocator;
    // Leave exactly 9 bytes (< kRecyclableHeaderSize, >= kHeaderSize) at the end
    // of block 0 so the trailer is longer than a legacy header.
    const filler_len = format.kBlockSize - format.kRecyclableHeaderSize - 9;
    const filler = try gpa.alloc(u8, filler_len);
    defer gpa.free(filler);
    @memset(filler, 'Z');
    try roundTripRecyclable(5, &[_][]const u8{ filler, "next block please", "and another" });
}

test "recyclable reader enforces expected log_number (stale record -> clean EOF)" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Write two records with log_number 7.
    {
        var wf = try e.newWritableFile(gpa, "wal");
        var w = Writer.initRecyclable(wf, 7);
        errdefer wf.close() catch {};
        try w.addRecord(gpa, "record one");
        try w.addRecord(gpa, "record two");
        try wf.flush();
        try wf.close();
    }

    // A reader expecting log_number 7 reads both records.
    {
        var sf = try e.newSequentialFile(gpa, "wal");
        var r = Reader.initRecyclable(sf, 7);
        defer sf.close() catch {};
        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(gpa);
        const a = try r.readRecord(gpa, &scratch);
        try std.testing.expectEqualSlices(u8, "record one", a.?);
        const b = try r.readRecord(gpa, &scratch);
        try std.testing.expectEqualSlices(u8, "record two", b.?);
        try std.testing.expect((try r.readRecord(gpa, &scratch)) == null);
    }

    // A reader expecting a different log_number sees the first record as stale
    // and stops immediately (clean EOF, no error).
    {
        var sf = try e.newSequentialFile(gpa, "wal");
        var r = Reader.initRecyclable(sf, 8);
        defer sf.close() catch {};
        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(gpa);
        try std.testing.expect((try r.readRecord(gpa, &scratch)) == null);
    }
}

test "recyclable: recycled block — fresh record then stale record from prior life" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Build a file whose block 0 begins with one fresh log-2 record immediately
    // followed by a stale log-1 record (as if the block were recycled and only
    // the head rewritten). Both records are individually well-formed; the
    // reader rejects the stale one purely on the log_number mismatch — without
    // this check it would otherwise parse and return it.
    var fresh: std.ArrayList(u8) = .empty;
    defer fresh.deinit(gpa);
    var stale: std.ArrayList(u8) = .empty;
    defer stale.deinit(gpa);
    {
        var w1 = try e.newWritableFile(gpa, "fresh_tmp");
        var fw = Writer.initRecyclable(w1, 2);
        errdefer w1.close() catch {};
        try fw.addRecord(gpa, "fresh log-2 record");
        try w1.flush();
        try w1.close();
        const f = try readAllBytes(e, gpa, "fresh_tmp");
        defer gpa.free(f);
        try fresh.appendSlice(gpa, f);

        var w2 = try e.newWritableFile(gpa, "stale_tmp");
        var sw = Writer.initRecyclable(w2, 1);
        errdefer w2.close() catch {};
        try sw.addRecord(gpa, "stale log-1 record");
        try w2.flush();
        try w2.close();
        const s = try readAllBytes(e, gpa, "stale_tmp");
        defer gpa.free(s);
        try stale.appendSlice(gpa, s);
    }

    {
        var wf = try e.newWritableFile(gpa, "wal");
        errdefer wf.close() catch {};
        try wf.append(fresh.items);
        try wf.append(stale.items);
        try wf.flush();
        try wf.close();
    }

    // Reader expecting log_number 2: reads the fresh record, then sees the
    // stale log-1 record and stops cleanly (null), not returning it.
    {
        var sf = try e.newSequentialFile(gpa, "wal");
        var r = Reader.initRecyclable(sf, 2);
        defer sf.close() catch {};
        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(gpa);
        const rec0 = try r.readRecord(gpa, &scratch);
        try std.testing.expectEqualSlices(u8, "fresh log-2 record", rec0.?);
        try std.testing.expect((try r.readRecord(gpa, &scratch)) == null);
    }

    // Sanity: a permissive reader (no expected log_number) returns BOTH, proving
    // the stale record is otherwise well-formed and only the log_number check
    // suppresses it.
    {
        var sf = try e.newSequentialFile(gpa, "wal");
        var r = Reader.init(sf);
        defer sf.close() catch {};
        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(gpa);
        const rec0 = try r.readRecord(gpa, &scratch);
        try std.testing.expectEqualSlices(u8, "fresh log-2 record", rec0.?);
        const rec1 = try r.readRecord(gpa, &scratch);
        try std.testing.expectEqualSlices(u8, "stale log-1 record", rec1.?);
        try std.testing.expect((try r.readRecord(gpa, &scratch)) == null);
    }
}

test "recyclable corruption: flipping a payload byte yields error.Corruption" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        var wf = try e.newWritableFile(gpa, "wal");
        var w = Writer.initRecyclable(wf, 3);
        errdefer wf.close() catch {};
        try w.addRecord(gpa, "corrupt me please");
        try wf.flush();
        try wf.close();
    }

    const bytes = try readAllBytes(e, gpa, "wal");
    defer gpa.free(bytes);
    bytes[format.kRecyclableHeaderSize + 3] ^= 0xff; // flip a payload byte

    {
        var wf = try e.newWritableFile(gpa, "wal");
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

test "default reader reads recyclable records (accepts any log_number)" {
    // The plain `init` reader has no expected log_number and must transparently
    // read recyclable records regardless of their stamped log number.
    try roundTripRecyclable(0xDEADBEEF, &[_][]const u8{ "alpha", "beta", "gamma" });
}

test "mixed: legacy then recyclable records in the same file" {
    const gpa = std.testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // The default reader handles both formats; write a legacy record, then a
    // recyclable record, in one file and read both back.
    {
        var wf = try e.newWritableFile(gpa, "wal");
        errdefer wf.close() catch {};
        var legacy = Writer.init(wf);
        try legacy.addRecord(gpa, "legacy record");
        // Continue at the same offset but in recyclable mode.
        var recy = Writer.initRecyclableWithOffset(wf, legacy.log_number, legacy.block_offset);
        recy.log_number = 9;
        try recy.addRecord(gpa, "recyclable record");
        try wf.flush();
        try wf.close();
    }

    var sf = try e.newSequentialFile(gpa, "wal");
    var r = Reader.init(sf);
    defer sf.close() catch {};
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(gpa);

    const a = try r.readRecord(gpa, &scratch);
    try std.testing.expectEqualSlices(u8, "legacy record", a.?);
    const b = try r.readRecord(gpa, &scratch);
    try std.testing.expectEqualSlices(u8, "recyclable record", b.?);
    try std.testing.expect((try r.readRecord(gpa, &scratch)) == null);
}
