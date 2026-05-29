//! xxph3.zig — RocksDB XXPH3 64-bit hash (the XXH3-derived "pseudo-hash").
//!
//! Byte-exact port of RocksDB's vendored `util/xxph3.h` `XXPH3_64bits(data, len)`
//! with the default 192-byte secret and seed 0 — the function RocksDB exposes as
//! `Hash64` / `GetSliceHash64` and uses to seed `FastLocalBloom` probes.
//!
//! IMPORTANT: XXPH3 is an *early experimental snapshot* of xxHash's XXH3, NOT the
//! final standardized XXH3_64bits. They diverge for every length. The clearest
//! distinguishing property: `XXPH3_64bits("", 0) == 0` (final XXH3 returns the
//! constant 0x2d06800538d394c2). RocksDB froze this snapshot for on-disk format
//! stability, so a faithful reimplementation must reproduce the snapshot, not
//! `std.hash.XxHash3` (which is final XXH3). The golden vectors at the bottom of
//! this file were derived from an independent scalar reconstruction of
//! `util/xxph3.h` (see the milestone notes); they pin every code path.
//!
//! Capability style: pure function over a byte slice, no allocator/Io/globals.
//!
//! ---------------------------------------------------------------------------
//! FastLocalBloom probe algorithm (documented here for the wave-7 full-filter
//! milestone; NOT implemented in this file).
//! ---------------------------------------------------------------------------
//! RocksDB's `FastLocalBloomImpl` (util/bloom_impl.h) is the SST "full filter"
//! (filter name "rocksdb.BuiltinBloomFilter2" / format_version >= 5). Given a
//! 64-bit key hash `h = XXPH3_64bits(key)` and a bit array organized as
//! cache-line-aligned 512-bit (64-byte) blocks:
//!
//!   1. CACHE-LINE SELECT (FastRange32): take the HIGH 32 bits of `h` as the
//!      selector and map it into `[0, num_lines)` without a modulo:
//!          line = (u64(hi32) * u64(num_lines)) >> 32
//!      The cache-line BYTE offset is `line * 64`. (`FastRange32` is the
//!      Lemire "multiply-shift" reduction — uniform, division-free.)
//!   2. PROBE WALK (rotate-7): the LOW 32 bits of `h` are the rolling probe
//!      word `h32`. For each of `num_probes` probes:
//!          bit  = h32 & 511                 // bit within the 512-bit line
//!          set/test  block[line*64 + bit/8] bit (bit & 7)
//!          h32 = rotl32(h32, 7)             // rotate LEFT by 7 between probes
//!      (Some RocksDB builds add a fixed odd delta; the canonical scalar form
//!      is the plain rotate-left-7 shown here.)
//!   3. TRAILER (5 bytes): the filter block is followed by a 5-byte trailer:
//!          byte[0]     = filter type marker (0 == "not collapsed"/full filter)
//!          byte[1..5]  = u32 LE num_probes
//!      i.e. the metadata lives in the LAST 5 bytes of the filter payload, with
//!      the bit array occupying everything before it (a whole number of 64-byte
//!      cache lines). The reader recovers `num_probes` from the trailer.
//!
//! Helper `fastRange32` below is provided now so the wave-7 milestone can reuse
//! it; the probe loop itself belongs in the future `full_filter.zig`.

const std = @import("std");

// ---------------------------------------------------------------------------
// Primes (identical to xxph3.h).
// ---------------------------------------------------------------------------
const PRIME32_1: u32 = 0x9E3779B1;
const PRIME32_2: u32 = 0x85EBCA77;
const PRIME32_3: u32 = 0xC2B2AE3D;

const PRIME64_1: u64 = 0x9E3779B185EBCA87;
const PRIME64_2: u64 = 0xC2B2AE3D27D4EB4F;
const PRIME64_3: u64 = 0x165667B19E3779F9;
const PRIME64_4: u64 = 0x85EBCA77C2B2AE63;
const PRIME64_5: u64 = 0x27D4EB2F165667C5;

const SECRET_DEFAULT_SIZE: usize = 192;
const STRIPE_LEN: usize = 64;
const SECRET_CONSUME_RATE: usize = 8;
const ACC_NB: usize = STRIPE_LEN / 8; // 8 accumulators
const SECRET_LASTACC_START: usize = 7;
const SECRET_MERGEACCS_START: usize = 11;
const MIDSIZE_MAX: usize = 240;
const MIDSIZE_STARTOFFSET: usize = 3;
const MIDSIZE_LASTOFFSET: usize = 17;

/// The default 192-byte XXPH3 secret (xxph3.h `kSecret`).
const kSecret = [SECRET_DEFAULT_SIZE]u8{
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
    0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
    0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
    0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
    0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
    0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
    0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
    0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
    0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
    0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
    0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
};

// ---------------------------------------------------------------------------
// Little-endian readers (XXPH3 always reads LE regardless of host endianness).
// ---------------------------------------------------------------------------
inline fn readLE64(p: []const u8) u64 {
    return std.mem.readInt(u64, p[0..8], .little);
}
inline fn readLE32(p: []const u8) u32 {
    return std.mem.readInt(u32, p[0..4], .little);
}

inline fn avalanche(h_in: u64) u64 {
    var h = h_in;
    h ^= h >> 37;
    h *%= PRIME64_3;
    h ^= h >> 32;
    return h;
}

/// 64-bit fold of a 128-bit product (mulh ^ mull).
inline fn mul128Fold64(lhs: u64, rhs: u64) u64 {
    const product: u128 = @as(u128, lhs) *% @as(u128, rhs);
    return @as(u64, @truncate(product)) ^ @as(u64, @truncate(product >> 64));
}

// ---------------------------------------------------------------------------
// Short-input paths (len <= 16).
// ---------------------------------------------------------------------------
fn len1to3(in: []const u8, secret: []const u8, seed: u64) u64 {
    const len = in.len;
    const c1: u32 = in[0];
    const c2: u32 = in[len >> 1];
    const c3: u32 = in[len - 1];
    const combined: u32 = c1 | (c2 << 8) | (c3 << 16) | (@as(u32, @intCast(len)) << 24);
    const keyed: u64 = @as(u64, combined) ^ (@as(u64, readLE32(secret)) +% seed);
    const mixed: u64 = keyed *% PRIME64_1;
    return avalanche(mixed);
}

fn len4to8(in: []const u8, secret: []const u8, seed: u64) u64 {
    const len = in.len;
    const lo: u64 = readLE32(in);
    const hi: u64 = readLE32(in[len - 4 ..]);
    const in64: u64 = lo | (hi << 32);
    const keyed: u64 = in64 ^ (readLE64(secret) +% seed);
    const mix64: u64 = @as(u64, len) +% ((keyed ^ (keyed >> 51)) *% PRIME32_1);
    return avalanche((mix64 ^ (mix64 >> 47)) *% PRIME64_2);
}

fn len9to16(in: []const u8, secret: []const u8, seed: u64) u64 {
    const len = in.len;
    const lo: u64 = readLE64(in) ^ (readLE64(secret) +% seed);
    const hi: u64 = readLE64(in[len - 8 ..]) ^ (readLE64(secret[8..]) -% seed);
    const acc: u64 = @as(u64, len) +% (lo +% hi) +% mul128Fold64(lo, hi);
    return avalanche(acc);
}

fn len0to16(in: []const u8, secret: []const u8, seed: u64) u64 {
    const len = in.len;
    if (len > 8) return len9to16(in, secret, seed);
    if (len >= 4) return len4to8(in, secret, seed);
    if (len > 0) return len1to3(in, secret, seed);
    return 0;
}

inline fn mix16B(in: []const u8, secret: []const u8, seed: u64) u64 {
    const lo: u64 = readLE64(in) ^ (readLE64(secret) +% seed);
    const hi: u64 = readLE64(in[8..]) ^ (readLE64(secret[8..]) -% seed);
    return mul128Fold64(lo, hi);
}

// ---------------------------------------------------------------------------
// Mid-size path (17..128).
// ---------------------------------------------------------------------------
fn len17to128(in: []const u8, secret: []const u8, seed: u64) u64 {
    const len = in.len;
    var acc: u64 = @as(u64, len) *% PRIME64_1;
    if (len > 32) {
        if (len > 64) {
            if (len > 96) {
                acc +%= mix16B(in[48..], secret[96..], seed);
                acc +%= mix16B(in[len - 64 ..], secret[112..], seed);
            }
            acc +%= mix16B(in[32..], secret[64..], seed);
            acc +%= mix16B(in[len - 48 ..], secret[80..], seed);
        }
        acc +%= mix16B(in[16..], secret[32..], seed);
        acc +%= mix16B(in[len - 32 ..], secret[48..], seed);
    }
    acc +%= mix16B(in[0..], secret[0..], seed);
    acc +%= mix16B(in[len - 16 ..], secret[16..], seed);
    return avalanche(acc);
}

// ---------------------------------------------------------------------------
// Mid-size path (129..240).
// ---------------------------------------------------------------------------
fn len129to240(in: []const u8, secret: []const u8, seed: u64) u64 {
    const len = in.len;
    var acc: u64 = @as(u64, len) *% PRIME64_1;
    const nb_rounds: usize = len / 16;
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        acc +%= mix16B(in[16 * i ..], secret[16 * i ..], seed);
    }
    acc = avalanche(acc);
    i = 8;
    while (i < nb_rounds) : (i += 1) {
        acc +%= mix16B(in[16 * i ..], secret[16 * (i - 8) + MIDSIZE_STARTOFFSET ..], seed);
    }
    acc +%= mix16B(
        in[len - 16 ..],
        secret[SECRET_DEFAULT_SIZE - STRIPE_LEN - MIDSIZE_LASTOFFSET ..],
        seed,
    );
    return avalanche(acc);
}

// ---------------------------------------------------------------------------
// Long path (>240).
// ---------------------------------------------------------------------------
fn accumulate512(acc: *[ACC_NB]u64, in: []const u8, secret: []const u8) void {
    var i: usize = 0;
    while (i < ACC_NB) : (i += 1) {
        const data_val: u64 = readLE64(in[8 * i ..]);
        const data_key: u64 = data_val ^ readLE64(secret[8 * i ..]);
        acc[i ^ 1] +%= data_val;
        const lo: u64 = @as(u32, @truncate(data_key));
        const hi: u64 = @as(u32, @truncate(data_key >> 32));
        acc[i] +%= lo *% hi;
    }
}

fn scrambleAcc(acc: *[ACC_NB]u64, secret: []const u8) void {
    var i: usize = 0;
    while (i < ACC_NB) : (i += 1) {
        const key64: u64 = readLE64(secret[8 * i ..]);
        var a: u64 = acc[i];
        a ^= a >> 47;
        a ^= key64;
        a *%= PRIME32_1;
        acc[i] = a;
    }
}

fn accumulate(acc: *[ACC_NB]u64, in: []const u8, secret: []const u8, nb_stripes: usize) void {
    var n: usize = 0;
    while (n < nb_stripes) : (n += 1) {
        accumulate512(acc, in[n * STRIPE_LEN ..], secret[n * SECRET_CONSUME_RATE ..]);
    }
}

fn hashLongLoop(acc: *[ACC_NB]u64, in: []const u8, secret: []const u8) void {
    const len = in.len;
    const secret_size = secret.len;
    const nb_rounds: usize = (secret_size - STRIPE_LEN) / SECRET_CONSUME_RATE;
    const block_len: usize = STRIPE_LEN * nb_rounds;
    const nb_blocks: usize = len / block_len;
    var n: usize = 0;
    while (n < nb_blocks) : (n += 1) {
        accumulate(acc, in[n * block_len ..], secret, nb_rounds);
        scrambleAcc(acc, secret[secret_size - STRIPE_LEN ..]);
    }
    const nb_stripes: usize = (len - (block_len * nb_blocks)) / STRIPE_LEN;
    accumulate(acc, in[nb_blocks * block_len ..], secret, nb_stripes);
    // last stripe
    accumulate512(acc, in[len - STRIPE_LEN ..], secret[secret_size - STRIPE_LEN - SECRET_LASTACC_START ..]);
}

inline fn mix2Accs(acc: []const u64, secret: []const u8) u64 {
    return mul128Fold64(acc[0] ^ readLE64(secret), acc[1] ^ readLE64(secret[8..]));
}

fn mergeAccs(acc: *const [ACC_NB]u64, secret: []const u8, start: u64) u64 {
    var r: u64 = start;
    r +%= mix2Accs(acc[0..2], secret[0..]);
    r +%= mix2Accs(acc[2..4], secret[16..]);
    r +%= mix2Accs(acc[4..6], secret[32..]);
    r +%= mix2Accs(acc[6..8], secret[48..]);
    return avalanche(r);
}

fn hashLong64b(in: []const u8, secret: []const u8) u64 {
    var acc = [ACC_NB]u64{
        PRIME32_3, PRIME64_1, PRIME64_2, PRIME64_3,
        PRIME64_4, PRIME32_2, PRIME64_5, PRIME32_1,
    };
    hashLongLoop(&acc, in, secret);
    return mergeAccs(&acc, secret[SECRET_MERGEACCS_START..], @as(u64, in.len) *% PRIME64_1);
}

// ---------------------------------------------------------------------------
// Public API.
// ---------------------------------------------------------------------------

/// RocksDB XXPH3 64-bit hash with the given seed and the default 192-byte secret.
/// Seed 0 reproduces `XXPH3_64bits(data, len)` (RocksDB's `Hash64`).
pub fn hash64Seed(data: []const u8, seed: u64) u64 {
    const len = data.len;
    if (len <= 16) return len0to16(data, &kSecret, seed);
    if (len <= 128) return len17to128(data, &kSecret, seed);
    if (len <= MIDSIZE_MAX) return len129to240(data, &kSecret, seed);
    // Long path is seed-independent in this experimental snapshot (the seed
    // only customizes the short/mid secret-derived terms; >240 uses the raw
    // default secret). Matches RocksDB's seedless `XXPH3_64bits`.
    return hashLong64b(data, &kSecret);
}

/// RocksDB `XXPH3_64bits(data, len)` — seed 0. This is `GetSliceHash64`.
pub fn hash64(data: []const u8) u64 {
    return hash64Seed(data, 0);
}

/// FastRange32 (Lemire multiply-shift) — map a 32-bit value uniformly into
/// `[0, n)` without division. Used by FastLocalBloom to pick a cache line from
/// the high 32 bits of the key hash. Provided here for the wave-7 full-filter
/// milestone; see the FastLocalBloom note at the top of this file.
pub inline fn fastRange32(hash_hi32: u32, n: u32) u32 {
    return @intCast((@as(u64, hash_hi32) *% @as(u64, n)) >> 32);
}

// ---------------------------------------------------------------------------
// Tests — golden vectors.
//
// Derived from an independent scalar reconstruction of RocksDB's util/xxph3.h
// `XXPH3_64bits` (seed 0, default 192-byte secret), cross-checked to DIFFER
// from final XXH3 (std.hash.XxHash3) at every length — confirming the snapshot
// is the experimental XXPH3 variant, not standardized XXH3. The empty-input
// value (0) is the canonical XXPH3 signature (final XXH3 returns a nonzero
// constant). Each length below targets a distinct code path.
// ---------------------------------------------------------------------------

test "XXPH3 empty input is 0 (the XXPH3 signature)" {
    try std.testing.expectEqual(@as(u64, 0), hash64(""));
}

test "XXPH3 golden vectors — short string keys (len <= 16)" {
    try std.testing.expectEqual(@as(u64, 0x88d868bf607681c7), hash64("a"));
    try std.testing.expectEqual(@as(u64, 0x885ddfe7af6310e5), hash64("ab"));
    try std.testing.expectEqual(@as(u64, 0xd39eeb71bb5342e8), hash64("abc"));
    try std.testing.expectEqual(@as(u64, 0x97a5ba1d02e8378c), hash64("abcd"));
    try std.testing.expectEqual(@as(u64, 0x203a3ebc4ec540b6), hash64("abcde"));
    try std.testing.expectEqual(@as(u64, 0x225fd8b927ff3669), hash64("abcdefg"));
    try std.testing.expectEqual(@as(u64, 0x591716e1ea463467), hash64("abcdefgh"));
    try std.testing.expectEqual(@as(u64, 0xac4f8ba14fb14987), hash64("123456789"));
    try std.testing.expectEqual(@as(u64, 0x4b6f47358ab1c286), hash64("abcdefghi"));
    try std.testing.expectEqual(@as(u64, 0x01beaa84a43d9c4f), hash64("abcdefghijklmnop"));
}

test "XXPH3 golden vectors — mid/long string keys" {
    try std.testing.expectEqual(@as(u64, 0xa75edb31b266248f), hash64("abcdefghijklmnopq"));
    try std.testing.expectEqual(
        @as(u64, 0x7a1331a5f5e19b65),
        hash64("The quick brown fox jumps over the lazy dog"),
    );
    try std.testing.expectEqual(
        @as(u64, 0x2c0dc8e3b5fb5f60),
        hash64("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!!"),
    );
}

test "XXPH3 golden vectors — every length path (sequential buffer)" {
    // buf[i] = (i*7 + 3) mod 256, same generator as the C oracle.
    var buf: [300]u8 = undefined;
    for (0..300) |i| buf[i] = @truncate(i * 7 + 3);

    const Case = struct { len: usize, want: u64 };
    const cases = [_]Case{
        .{ .len = 1, .want = 0x81b78b21c2ce4f18 }, // len_1to3
        .{ .len = 2, .want = 0x6d7eec54f8754ce6 },
        .{ .len = 3, .want = 0x6731a716813ee5f1 },
        .{ .len = 4, .want = 0x3a5afd967acaf5c3 }, // len_4to8
        .{ .len = 5, .want = 0x4e6ef0d0cfdaa400 },
        .{ .len = 8, .want = 0xbcb56b6f8d4b12da },
        .{ .len = 9, .want = 0x03787b4eec57bf4b }, // len_9to16
        .{ .len = 16, .want = 0x81720cc0702edd73 },
        .{ .len = 17, .want = 0x0b515520f462e96f }, // len_17to128 (<=32)
        .{ .len = 32, .want = 0x48f0396187b56dd5 },
        .{ .len = 33, .want = 0xd020587a3c72b988 }, // (33..64)
        .{ .len = 64, .want = 0xc71b2c5a712e7f61 },
        .{ .len = 65, .want = 0xd40143de1beadd46 }, // (65..96)
        .{ .len = 96, .want = 0x0d71e54f419745c3 },
        .{ .len = 97, .want = 0x19e9c1871ecd39dc }, // (97..128)
        .{ .len = 128, .want = 0x8ea76d838ce7563f },
        .{ .len = 129, .want = 0x5e764d6baa102b9c }, // len_129to240
        .{ .len = 200, .want = 0x9c6414c14610ee0e },
        .{ .len = 240, .want = 0xc379092d4999c98b },
        .{ .len = 241, .want = 0x367ed7da35e0f4f7 }, // hashLong (>240)
        .{ .len = 256, .want = 0x72e6f9f26d656703 },
        .{ .len = 300, .want = 0x97518ad0bce52807 },
    };
    for (cases) |c| {
        try std.testing.expectEqual(c.want, hash64(buf[0..c.len]));
    }
}

test "XXPH3 is deterministic" {
    const k = "some-sstable-key-0042";
    try std.testing.expectEqual(hash64(k), hash64(k));
}

test "XXPH3 differs from final XXH3 (confirms experimental snapshot)" {
    // std.hash.XxHash3 is standardized XXH3; XXPH3 must NOT match it.
    try std.testing.expect(hash64("abc") != std.hash.XxHash3.hash(0, "abc"));
    try std.testing.expect(hash64("") != std.hash.XxHash3.hash(0, ""));
}

test "fastRange32 maps into [0, n) and is uniform-ish" {
    // Boundary values.
    try std.testing.expectEqual(@as(u32, 0), fastRange32(0, 100));
    try std.testing.expectEqual(@as(u32, 99), fastRange32(0xFFFFFFFF, 100));
    // Always in range.
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const r = fastRange32(@truncate(hash64(std.mem.asBytes(&i))), 137);
        try std.testing.expect(r < 137);
    }
}
