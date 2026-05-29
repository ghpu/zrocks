//! snappy.zig — Pure-Zig Snappy block-format codec.
//!
//! Implements the Snappy block format as described in:
//!   https://github.com/google/snappy/blob/main/format_description.txt
//!
//! The "block" format (not the framing format):
//!   - Preamble: uncompressed length as a Snappy varint (7-bit groups, LE,
//!     continuation bit = high bit of each byte).
//!   - Body: stream of literals and back-references (copies).
//!
//! This is the format used internally by RocksDB for block compression.
//!
//! API:
//!   compress(gpa, src) ![]u8          — caller owns returned slice
//!   decompress(gpa, src) ![]u8        — caller owns returned slice
//!   maxCompressedLength(uncompressed_len) usize  — safe output buffer bound

const std = @import("std");

// ---------------------------------------------------------------------------
// Public errors
// ---------------------------------------------------------------------------

pub const Error = error{
    /// Input is not valid Snappy-compressed data.
    Corrupt,
    /// Output would exceed an internal safety limit (>4 GiB uncompressed).
    InputTooLarge,
};

// ---------------------------------------------------------------------------
// Varint encoding (Snappy uses the same 7-bit-group encoding as LevelDB/protobuf)
// ---------------------------------------------------------------------------

/// Write a 32-bit value as a Snappy varint into `buf`. Returns bytes written.
/// buf must have at least 5 bytes available.
fn writeVarint32(buf: []u8, v: u32) usize {
    var val = v;
    var i: usize = 0;
    while (val >= 0x80) {
        buf[i] = @as(u8, @intCast(val & 0x7f)) | 0x80;
        val >>= 7;
        i += 1;
    }
    buf[i] = @as(u8, @intCast(val));
    return i + 1;
}

/// Read a Snappy varint from `src[pos..]`. Advances *pos past the varint.
/// Returns error.Corrupt if the varint is malformed or truncated.
fn readVarint32(src: []const u8, pos: *usize) Error!u32 {
    var result: u64 = 0;
    var shift: u6 = 0;
    var i = pos.*;
    while (i < src.len) {
        const byte = src[i];
        i += 1;
        result |= @as(u64, byte & 0x7f) << shift;
        if (byte & 0x80 == 0) {
            // Guard against overflow: Snappy limits uncompressed to 2^32-1 bytes.
            if (result > 0xffff_ffff) return error.Corrupt;
            pos.* = i;
            return @as(u32, @intCast(result));
        }
        if (shift >= 35) return error.Corrupt; // 5 bytes max for a 32-bit varint
        shift += 7;
    }
    return error.Corrupt;
}

// ---------------------------------------------------------------------------
// Snappy tag-type constants
// ---------------------------------------------------------------------------
const TAG_LITERAL: u8 = 0b00;
const TAG_COPY1: u8 = 0b01;
const TAG_COPY2: u8 = 0b10;
const TAG_COPY4: u8 = 0b11;

// ---------------------------------------------------------------------------
// Compress
// ---------------------------------------------------------------------------

/// Upper bound on the compressed length of `uncompressed_len` bytes.
/// Guaranteed to be >= any output produced by compress().
pub fn maxCompressedLength(uncompressed_len: usize) usize {
    // Snappy formula: 32 + src_len + src_len/6
    // We add a 5-byte varint overhead for the preamble.
    return 32 + uncompressed_len + uncompressed_len / 6 + 5;
}

/// Compress `src` using the Snappy block format.
/// Caller owns the returned slice; free with `gpa.free(result)`.
pub fn compress(gpa: std.mem.Allocator, src: []const u8) (std.mem.Allocator.Error || Error)![]u8 {
    if (src.len > 0xffff_ffff) return error.InputTooLarge;

    const max_len = maxCompressedLength(src.len);
    var dst = try gpa.alloc(u8, max_len);
    errdefer gpa.free(dst);

    var pos: usize = 0;

    // Write uncompressed length preamble.
    pos += writeVarint32(dst[pos..], @as(u32, @intCast(src.len)));

    // Compress body.
    pos = compressBody(src, dst, pos);

    // Shrink to actual size.
    dst = try gpa.realloc(dst, pos);
    return dst;
}

// ---------------------------------------------------------------------------
// Compression body — hash-table based LZ77
// ---------------------------------------------------------------------------

/// Hash-table size (power of 2).  We use a 16-bit-indexed table (65536 entries)
/// for inputs up to 65536 bytes, and step up for larger inputs.
const HASH_TABLE_BITS: u5 = 14; // 16 384 entries — good for most block sizes
const HASH_TABLE_SIZE: usize = 1 << HASH_TABLE_BITS;
const HASH_TABLE_MASK: u32 = HASH_TABLE_SIZE - 1;

/// Emit a literal run starting at src[lit_start..src_pos] into dst[dpos..].
/// Returns the new dpos.
fn emitLiteral(src: []const u8, lit_start: usize, lit_end: usize, dst: []u8, dpos: usize) usize {
    const length = lit_end - lit_start;
    if (length == 0) return dpos;
    var dp = dpos;
    const len_minus1 = length - 1;
    if (len_minus1 < 60) {
        dst[dp] = @as(u8, @intCast(len_minus1 << 2)) | TAG_LITERAL;
        dp += 1;
    } else if (len_minus1 <= 0xff) {
        dst[dp] = (60 << 2) | TAG_LITERAL;
        dst[dp + 1] = @as(u8, @intCast(len_minus1));
        dp += 2;
    } else if (len_minus1 <= 0xffff) {
        dst[dp] = (61 << 2) | TAG_LITERAL;
        dst[dp + 1] = @as(u8, @intCast(len_minus1 & 0xff));
        dst[dp + 2] = @as(u8, @intCast(len_minus1 >> 8));
        dp += 3;
    } else if (len_minus1 <= 0xff_ffff) {
        dst[dp] = (62 << 2) | TAG_LITERAL;
        dst[dp + 1] = @as(u8, @intCast(len_minus1 & 0xff));
        dst[dp + 2] = @as(u8, @intCast((len_minus1 >> 8) & 0xff));
        dst[dp + 3] = @as(u8, @intCast(len_minus1 >> 16));
        dp += 4;
    } else {
        dst[dp] = (63 << 2) | TAG_LITERAL;
        dst[dp + 1] = @as(u8, @intCast(len_minus1 & 0xff));
        dst[dp + 2] = @as(u8, @intCast((len_minus1 >> 8) & 0xff));
        dst[dp + 3] = @as(u8, @intCast((len_minus1 >> 16) & 0xff));
        dst[dp + 4] = @as(u8, @intCast(len_minus1 >> 24));
        dp += 5;
    }
    @memcpy(dst[dp .. dp + length], src[lit_start..lit_end]);
    return dp + length;
}

/// Emit a copy (back-reference) of `length` bytes at `offset` back.
fn emitCopy(length: usize, offset: usize, dst: []u8, dpos: usize) usize {
    var dp = dpos;
    var len = length;

    // Emit in chunks; Copy1 covers length 4..11, Copy2 covers 1..64.
    // We prefer Copy2 for simplicity and wider offset range.
    if (offset < 65536) {
        // Can use Copy1 (offset must fit in 11 bits = max 2047).
        // Copy2: length bits are tag[7:2] = (length - 1), max 63 bytes.
        while (len >= 68) {
            // Emit a max-length Copy2 (64 bytes).
            dst[dp] = @as(u8, @intCast(((64 - 1) << 2) | TAG_COPY2));
            dst[dp + 1] = @as(u8, @intCast(offset & 0xff));
            dst[dp + 2] = @as(u8, @intCast(offset >> 8));
            dp += 3;
            len -= 64;
        }
        if (len > 64) {
            // Split: emit (len - 60) then 60, both as Copy2.
            const first = len - 60;
            dst[dp] = @as(u8, @intCast(((first - 1) << 2) | TAG_COPY2));
            dst[dp + 1] = @as(u8, @intCast(offset & 0xff));
            dst[dp + 2] = @as(u8, @intCast(offset >> 8));
            dp += 3;
            len = 60;
        }
        dst[dp] = @as(u8, @intCast(((len - 1) << 2) | TAG_COPY2));
        dst[dp + 1] = @as(u8, @intCast(offset & 0xff));
        dst[dp + 2] = @as(u8, @intCast(offset >> 8));
        dp += 3;
    } else {
        // Copy4
        while (len >= 68) {
            dst[dp] = @as(u8, @intCast(((64 - 1) << 2) | TAG_COPY4));
            dst[dp + 1] = @as(u8, @intCast(offset & 0xff));
            dst[dp + 2] = @as(u8, @intCast((offset >> 8) & 0xff));
            dst[dp + 3] = @as(u8, @intCast((offset >> 16) & 0xff));
            dst[dp + 4] = @as(u8, @intCast(offset >> 24));
            dp += 5;
            len -= 64;
        }
        if (len > 64) {
            const first = len - 60;
            dst[dp] = @as(u8, @intCast(((first - 1) << 2) | TAG_COPY4));
            dst[dp + 1] = @as(u8, @intCast(offset & 0xff));
            dst[dp + 2] = @as(u8, @intCast((offset >> 8) & 0xff));
            dst[dp + 3] = @as(u8, @intCast((offset >> 16) & 0xff));
            dst[dp + 4] = @as(u8, @intCast(offset >> 24));
            dp += 5;
            len = 60;
        }
        dst[dp] = @as(u8, @intCast(((len - 1) << 2) | TAG_COPY4));
        dst[dp + 1] = @as(u8, @intCast(offset & 0xff));
        dst[dp + 2] = @as(u8, @intCast((offset >> 8) & 0xff));
        dst[dp + 3] = @as(u8, @intCast((offset >> 16) & 0xff));
        dst[dp + 4] = @as(u8, @intCast(offset >> 24));
        dp += 5;
    }
    return dp;
}

/// Quick 4-byte hash for match finding.
inline fn hash4(data: []const u8, pos: usize) u32 {
    const v = std.mem.readInt(u32, data[pos..][0..4], .little);
    // Knuth's multiplicative hash, shifted to HASH_TABLE_BITS.
    // HASH_TABLE_BITS=14, shift=18, fits in u5 (max 31).
    const shift: u5 = @intCast(32 - @as(u6, HASH_TABLE_BITS));
    return (v *% 0x1e35a7bd) >> shift;
}

/// Compress src into dst[start_pos..] and return the new position.
/// dst must have at least maxCompressedLength(src.len) bytes available.
fn compressBody(src: []const u8, dst: []u8, start_pos: usize) usize {
    const n = src.len;
    var dp = start_pos;

    if (n == 0) return dp;

    // For very short inputs, just emit a literal.
    if (n < 4) {
        dp = emitLiteral(src, 0, n, dst, dp);
        return dp;
    }

    // Hash table: maps hash -> source offset (u32).
    // Stack-allocate for small inputs; heap would require allocator threading.
    // We use a fixed-size table; for very large inputs collisions reduce ratio
    // but correctness is preserved (missed matches → extra literals).
    var table: [HASH_TABLE_SIZE]u32 = @splat(0);

    var ip: usize = 0; // input pointer
    var lit_start: usize = 0; // start of pending literal run

    // We stop searching for matches 8 bytes before the end to simplify
    // boundary checks in the inner loop.
    const ip_limit = n - 4;

    while (ip < ip_limit) {
        const h = hash4(src, ip);
        const candidate = table[h];
        table[h] = @as(u32, @intCast(ip));

        // Check for a match: candidate offset is valid and the 4 bytes match.
        if (candidate > 0 and ip > candidate) {
            const offset = ip - candidate;
            if (offset < 0x1_0000 // 16-bit offset for Copy2
            and std.mem.eql(u8, src[ip .. ip + 4], src[candidate .. candidate + 4]))
            {
                // Extend the match as far as possible.
                var match_len: usize = 4;
                const max_match = @min(n - ip, 64 + 4); // cap at max Copy2 chain length
                while (match_len < max_match and
                    src[ip + match_len] == src[candidate + match_len])
                {
                    match_len += 1;
                }

                // Emit the pending literal run.
                dp = emitLiteral(src, lit_start, ip, dst, dp);

                // Emit the copy.
                dp = emitCopy(match_len, offset, dst, dp);

                ip += match_len;
                lit_start = ip;
                continue;
            }
        }

        ip += 1;
    }

    // Emit any remaining literal bytes.
    dp = emitLiteral(src, lit_start, n, dst, dp);
    return dp;
}

// ---------------------------------------------------------------------------
// Decompress
// ---------------------------------------------------------------------------

/// Decompress a Snappy block-format byte slice.
/// Caller owns the returned slice; free with `gpa.free(result)`.
pub fn decompress(gpa: std.mem.Allocator, src: []const u8) (std.mem.Allocator.Error || Error)![]u8 {
    if (src.len == 0) return error.Corrupt;

    var pos: usize = 0;

    // Read preamble: uncompressed length.
    const uncompressed_len = try readVarint32(src, &pos);
    if (uncompressed_len > 0x7fff_ffff) return error.InputTooLarge; // sanity cap at 2 GiB

    const dst = try gpa.alloc(u8, uncompressed_len);
    errdefer gpa.free(dst);

    var dpos: usize = 0;

    while (pos < src.len) {
        const tag_byte = src[pos];
        pos += 1;
        const tag_type = tag_byte & 0x03;

        switch (tag_type) {
            TAG_LITERAL => {
                const len_field = tag_byte >> 2;
                const length: usize = blk: {
                    if (len_field < 60) {
                        break :blk @as(usize, len_field) + 1;
                    }
                    const extra_bytes: usize = @as(usize, len_field) - 59; // 1..4
                    if (pos + extra_bytes > src.len) return error.Corrupt;
                    var v: u32 = 0;
                    for (0..extra_bytes) |i| {
                        v |= @as(u32, src[pos + i]) << @as(u5, @intCast(i * 8));
                    }
                    pos += extra_bytes;
                    break :blk @as(usize, v) + 1;
                };
                if (pos + length > src.len) return error.Corrupt;
                if (dpos + length > dst.len) return error.Corrupt;
                @memcpy(dst[dpos .. dpos + length], src[pos .. pos + length]);
                pos += length;
                dpos += length;
            },

            TAG_COPY1 => {
                // 1 extra byte: tag[4:2] = length-4; tag[7:5] = offset[10:8]; next = offset[7:0]
                if (pos + 1 > src.len) return error.Corrupt;
                const length: usize = @as(usize, (tag_byte >> 2) & 0x7) + 4;
                const offset: usize = (@as(usize, tag_byte & 0xe0) << 3) | @as(usize, src[pos]);
                pos += 1;
                if (offset == 0 or offset > dpos) return error.Corrupt;
                if (dpos + length > dst.len) return error.Corrupt;
                copyBytes(dst, dpos, offset, length);
                dpos += length;
            },

            TAG_COPY2 => {
                // 2 extra bytes: tag[7:2] = length-1; next 2 bytes = LE offset
                if (pos + 2 > src.len) return error.Corrupt;
                const length: usize = @as(usize, tag_byte >> 2) + 1;
                const offset: usize = @as(usize, src[pos]) | (@as(usize, src[pos + 1]) << 8);
                pos += 2;
                if (offset == 0 or offset > dpos) return error.Corrupt;
                if (dpos + length > dst.len) return error.Corrupt;
                copyBytes(dst, dpos, offset, length);
                dpos += length;
            },

            TAG_COPY4 => {
                // 4 extra bytes: tag[7:2] = length-1; next 4 bytes = LE offset
                if (pos + 4 > src.len) return error.Corrupt;
                const length: usize = @as(usize, tag_byte >> 2) + 1;
                const offset: usize = @as(usize, src[pos]) |
                    (@as(usize, src[pos + 1]) << 8) |
                    (@as(usize, src[pos + 2]) << 16) |
                    (@as(usize, src[pos + 3]) << 24);
                pos += 4;
                if (offset == 0 or offset > dpos) return error.Corrupt;
                if (dpos + length > dst.len) return error.Corrupt;
                copyBytes(dst, dpos, offset, length);
                dpos += length;
            },

            else => unreachable, // only 2 bits, covered above
        }
    }

    if (dpos != dst.len) return error.Corrupt;
    return dst;
}

/// Copy `length` bytes from dst[dpos - offset .. dpos - offset + length] to dst[dpos..].
/// Handles overlapping (run-length-encoding) copies where offset < length.
inline fn copyBytes(dst: []u8, dpos: usize, offset: usize, length: usize) void {
    var i: usize = 0;
    while (i < length) : (i += 1) {
        dst[dpos + i] = dst[dpos - offset + i];
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "varint32 round-trip" {
    var buf: [5]u8 = undefined;
    const cases = [_]u32{ 0, 1, 127, 128, 255, 16383, 16384, 0xffff, 0x7fff_ffff, 0xffff_ffff };
    for (cases) |v| {
        const written = writeVarint32(&buf, v);
        var pos: usize = 0;
        const read = try readVarint32(&buf, &pos);
        try std.testing.expectEqual(v, read);
        try std.testing.expectEqual(written, pos);
    }
}

test "compress empty input" {
    const gpa = std.testing.allocator;
    const compressed = try compress(gpa, "");
    defer gpa.free(compressed);
    // Empty: just the preamble varint(0) = 0x00
    try std.testing.expectEqualSlices(u8, &[_]u8{0x00}, compressed);
}

test "decompress empty" {
    const gpa = std.testing.allocator;
    const decompressed = try decompress(gpa, &[_]u8{0x00});
    defer gpa.free(decompressed);
    try std.testing.expectEqualSlices(u8, "", decompressed);
}

test "golden vector — single byte 'a'" {
    // Snappy encoding of "a":
    //   varint(1) = 0x01
    //   literal tag: (0 << 2) | 0 = 0x00   (length-1 = 0, so length = 1)
    //   literal data: 0x61 ('a')
    const gpa = std.testing.allocator;
    const golden = &[_]u8{ 0x01, 0x00, 0x61 };

    // Decompress the golden vector.
    const dec = try decompress(gpa, golden);
    defer gpa.free(dec);
    try std.testing.expectEqualSlices(u8, "a", dec);

    // Compress "a" and verify we can decompress the result.
    const enc = try compress(gpa, "a");
    defer gpa.free(enc);
    const dec2 = try decompress(gpa, enc);
    defer gpa.free(dec2);
    try std.testing.expectEqualSlices(u8, "a", dec2);
}

test "golden vector — 5-byte literal 'abcde'" {
    // varint(5) = 0x05
    // literal tag: (4 << 2) | 0 = 0x10   (length-1 = 4)
    // literal data: 'a','b','c','d','e'
    const gpa = std.testing.allocator;
    const golden = &[_]u8{ 0x05, 0x10, 0x61, 0x62, 0x63, 0x64, 0x65 };

    const dec = try decompress(gpa, golden);
    defer gpa.free(dec);
    try std.testing.expectEqualSlices(u8, "abcde", dec);
}

test "golden vector — copy2 back-reference" {
    // Encode "abcabc" (6 bytes):
    //   varint(6) = 0x06
    //   literal "abc": tag = (2 << 2) | 0 = 0x08, 'a','b','c'  (4 bytes)
    //   copy2 of length=3, offset=3:
    //     tag = ((3-1) << 2) | 0x02 = 0x0a
    //     offset LE16 = 0x03, 0x00
    const gpa = std.testing.allocator;
    const golden = &[_]u8{ 0x06, 0x08, 0x61, 0x62, 0x63, 0x0a, 0x03, 0x00 };

    const dec = try decompress(gpa, golden);
    defer gpa.free(dec);
    try std.testing.expectEqualSlices(u8, "abcabc", dec);
}

test "round-trip short strings" {
    const gpa = std.testing.allocator;
    const inputs = [_][]const u8{
        "",
        "a",
        "hello",
        "hello world",
        "abcdefghijklmnopqrstuvwxyz",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "the quick brown fox jumps over the lazy dog",
    };
    for (inputs) |input| {
        const enc = try compress(gpa, input);
        defer gpa.free(enc);
        const dec = try decompress(gpa, enc);
        defer gpa.free(dec);
        try std.testing.expectEqualSlices(u8, input, dec);
    }
}

test "round-trip repetitive data (good compression)" {
    const gpa = std.testing.allocator;
    // 1024 'x' bytes compresses very well.
    const input = "x" ** 1024;
    const enc = try compress(gpa, input);
    defer gpa.free(enc);
    // Verify compressed is smaller.
    try std.testing.expect(enc.len < input.len);
    const dec = try decompress(gpa, enc);
    defer gpa.free(dec);
    try std.testing.expectEqualSlices(u8, input, dec);
}

test "round-trip binary data" {
    const gpa = std.testing.allocator;
    // Generate pseudo-random bytes.
    var buf: [4096]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(0xdeadbeef);
    prng.fill(&buf);

    const enc = try compress(gpa, &buf);
    defer gpa.free(enc);
    const dec = try decompress(gpa, enc);
    defer gpa.free(dec);
    try std.testing.expectEqualSlices(u8, &buf, dec);
}

test "round-trip 64 KiB all-zeros" {
    const gpa = std.testing.allocator;
    const input = [_]u8{0} ** (64 * 1024);
    const enc = try compress(gpa, &input);
    defer gpa.free(enc);
    try std.testing.expect(enc.len < input.len / 4);
    const dec = try decompress(gpa, enc);
    defer gpa.free(dec);
    try std.testing.expectEqualSlices(u8, &input, dec);
}

test "decompress corrupt: empty input" {
    const gpa = std.testing.allocator;
    try std.testing.expectError(error.Corrupt, decompress(gpa, ""));
}

test "decompress corrupt: truncated literal" {
    const gpa = std.testing.allocator;
    // varint(5) then literal tag for 5 bytes but only 2 data bytes provided
    const bad = &[_]u8{ 0x05, 0x10, 0x61, 0x62 };
    try std.testing.expectError(error.Corrupt, decompress(gpa, bad));
}

test "decompress corrupt: copy with zero offset" {
    const gpa = std.testing.allocator;
    // varint(2): we'll try a copy2 with offset=0 which is illegal
    // literal "a": 0x00, 0x61
    // copy2 length=1, offset=0: tag = ((1-1)<<2)|0x02 = 0x02, offset LE16 = 0x00, 0x00
    const bad = &[_]u8{ 0x02, 0x00, 0x61, 0x02, 0x00, 0x00 };
    try std.testing.expectError(error.Corrupt, decompress(gpa, bad));
}

test "decompress corrupt: copy offset beyond output" {
    const gpa = std.testing.allocator;
    // varint(2): literal "a" (1 byte), copy2 of offset=5 (beyond 1 byte output)
    // After literal, dpos=1; copy2 offset=5 > 1 → Corrupt
    const bad = &[_]u8{ 0x02, 0x00, 0x61, 0x02, 0x05, 0x00 };
    try std.testing.expectError(error.Corrupt, decompress(gpa, bad));
}

test "maxCompressedLength is always >= compressed size" {
    const gpa = std.testing.allocator;
    const inputs = [_][]const u8{
        "",
        "a",
        "hello world",
        "x" ** 1024,
    };
    for (inputs) |input| {
        const bound = maxCompressedLength(input.len);
        const enc = try compress(gpa, input);
        defer gpa.free(enc);
        try std.testing.expect(enc.len <= bound);
    }
}

test "round-trip overlap copy (run-length fill)" {
    // "aaaaaa...a" (repetition) triggers overlap copies in decompress.
    const gpa = std.testing.allocator;
    const input = "a" ** 256;
    const enc = try compress(gpa, input);
    defer gpa.free(enc);
    const dec = try decompress(gpa, enc);
    defer gpa.free(dec);
    try std.testing.expectEqualSlices(u8, input, dec);
}
