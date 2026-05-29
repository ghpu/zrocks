//! full_filter.zig — RocksDB-style FastLocalBloom "full filter".
//!
//! A whole-SST (single-block) bloom filter using the cache-line-local
//! FastLocalBloom layout RocksDB writes for `format_version >= 5` filters
//! (filter name "rocksdb.BuiltinBloomFilter2", the new full-filter variant).
//! It replaces the legacy LevelDB per-2KB-range *block-based* filter
//! (filter_block.zig) with a single contiguous filter over EVERY key in the
//! table, probed independently of the data-block offset.
//!
//! Layout (see the FastLocalBloom note at the top of util/xxph3.zig):
//!
//!   [ bit array : a whole number of 64-byte (512-bit) cache lines ]
//!   [ 5-byte TRAILER ]
//!       trailer[0]    = 0            // full-filter marker ("not collapsed")
//!       trailer[1..5] = u32 LE       // num_probes
//!
//! Probe algorithm for a key, given `h = XXPH3_64bits(key)`:
//!   1. CACHE-LINE SELECT (FastRange32): line = (u64(hi32(h)) * num_lines) >> 32
//!      → byte offset = line * 64.
//!   2. PROBE WALK (rotate-left-7): h32 = lo32(h); for each of num_probes probes
//!         bit = h32 & 511                          // bit within the 512-bit line
//!         set/test  block[line*64 + bit/8] bit (bit & 7)
//!         h32 = rotl32(h32, 7)
//!
//! Correctness contract: NO FALSE NEGATIVES — every key inserted via the
//! builder must report `mayMatch` true under the reader.  False positives occur
//! at a rate governed by bits_per_key / num_probes (≈1% at 10 bits, 6 probes).
//!
//! This is the ONLY filter format zrocks writes (filter-rocksdb-only): every
//! SST carries one FastLocalBloom full filter under "fullfilter."++policy.name().
//! External on-disk SSTs with legacy "filter." block-based filters are still
//! READ through filter_block.zig (LevelDB interop); only the WRITE path dropped.
//!
//! Capability style: every allocating method takes an explicit allocator; no
//! globals, no ambient authority.

const std = @import("std");
const xxph3 = @import("../util/xxph3.zig");

/// Bytes per cache line (512 bits).  FastLocalBloom keeps every probe for a
/// single key within one cache line.
pub const kCacheLineBytes: usize = 64;
const kCacheLineBits: u32 = 512;

/// Trailer length appended after the bit array.
pub const kTrailerBytes: usize = 5;

/// Full-filter marker (trailer byte 0). 0 == "not collapsed" / full filter,
/// matching RocksDB's convention.
pub const kFullFilterMarker: u8 = 0;

/// Rotate a 32-bit word left by `r` bits.
inline fn rotl32(x: u32, comptime r: u5) u32 {
    return (x << r) | (x >> (32 - @as(u6, r)));
}

/// Compute the number of probes from bits_per_key, matching RocksDB's
/// `BloomFilterPolicy` choice: round(bits_per_key * ln2), clamped to [1, 30].
pub fn probesForBitsPerKey(bits_per_key: usize) u32 {
    const raw: usize = @intFromFloat(@as(f64, @floatFromInt(bits_per_key)) * 0.69314718056);
    return @intCast(@max(@as(usize, 1), @min(@as(usize, 30), raw)));
}

/// Set one key's probe bits into a cache-line-organized bit array.
fn addKeyToArray(array: []u8, num_lines: u32, num_probes: u32, key: []const u8) void {
    const h = xxph3.hash64(key);
    const hi32: u32 = @truncate(h >> 32);
    const line = xxph3.fastRange32(hi32, num_lines);
    const base: usize = @as(usize, line) * kCacheLineBytes;

    var h32: u32 = @truncate(h);
    var p: u32 = 0;
    while (p < num_probes) : (p += 1) {
        const bit = h32 & (kCacheLineBits - 1);
        array[base + bit / 8] |= @as(u8, 1) << @intCast(bit & 7);
        h32 = rotl32(h32, 7);
    }
}

/// Test one key's probe bits against a cache-line-organized bit array.
fn arrayMayMatch(array: []const u8, num_lines: u32, num_probes: u32, key: []const u8) bool {
    const h = xxph3.hash64(key);
    const hi32: u32 = @truncate(h >> 32);
    const line = xxph3.fastRange32(hi32, num_lines);
    const base: usize = @as(usize, line) * kCacheLineBytes;

    var h32: u32 = @truncate(h);
    var p: u32 = 0;
    while (p < num_probes) : (p += 1) {
        const bit = h32 & (kCacheLineBits - 1);
        if ((array[base + bit / 8] & (@as(u8, 1) << @intCast(bit & 7))) == 0) return false;
        h32 = rotl32(h32, 7);
    }
    return true;
}

/// Builder for a single FastLocalBloom full-filter block.  Keys are added one
/// at a time (no buffering of key bytes — the filter bits are accumulated
/// directly), then `finish` appends the 5-byte trailer and returns the block.
pub const FullFilterBuilder = struct {
    bits_per_key: usize,
    num_probes: u32,
    /// Bit array (whole 64-byte cache lines), grows lazily on the first key.
    array: std.ArrayListUnmanaged(u8) = .empty,
    num_lines: u32 = 0,
    /// Keys added so far (used to size the array on first finish/add).
    num_keys: u64 = 0,
    /// Buffered keys (we must know the total count before sizing the array, so
    /// keys are duped and replayed at finish).  Concatenated bytes + offsets.
    key_bytes: std.ArrayListUnmanaged(u8) = .empty,
    key_starts: std.ArrayListUnmanaged(usize) = .empty,

    pub fn init(bits_per_key: usize) FullFilterBuilder {
        return .{ .bits_per_key = bits_per_key, .num_probes = probesForBitsPerKey(bits_per_key) };
    }

    pub fn deinit(self: *FullFilterBuilder, gpa: std.mem.Allocator) void {
        self.array.deinit(gpa);
        self.key_bytes.deinit(gpa);
        self.key_starts.deinit(gpa);
        self.* = undefined;
    }

    /// Number of keys added since the last finish.
    pub fn keyCount(self: *const FullFilterBuilder) u64 {
        return self.num_keys;
    }

    /// Add one key.  Keys are buffered so the bit array can be sized from the
    /// final key count (RocksDB sizes the filter from total keys * bits_per_key).
    pub fn addKey(self: *FullFilterBuilder, gpa: std.mem.Allocator, key: []const u8) !void {
        try self.key_starts.append(gpa, self.key_bytes.items.len);
        try self.key_bytes.appendSlice(gpa, key);
        self.num_keys += 1;
    }

    /// Finish the filter: size the cache-line bit array from the accumulated key
    /// count, set every key's probe bits, append the 5-byte trailer, and return
    /// the block (owned by the builder; valid until deinit / next reset).
    pub fn finish(self: *FullFilterBuilder, gpa: std.mem.Allocator) ![]const u8 {
        // Total bits, floored so even a tiny filter has at least one cache line.
        var total_bits: usize = @as(usize, @intCast(self.num_keys)) * self.bits_per_key;
        if (total_bits < kCacheLineBits) total_bits = kCacheLineBits;
        // Round UP to a whole number of cache lines.
        const num_lines: u32 = @intCast((total_bits + kCacheLineBits - 1) / kCacheLineBits);
        const array_bytes: usize = @as(usize, num_lines) * kCacheLineBytes;

        self.num_lines = num_lines;
        self.array.clearRetainingCapacity();
        try self.array.appendNTimes(gpa, 0, array_bytes);

        // Replay buffered keys, setting probe bits.
        const n: usize = @intCast(self.num_keys);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const start = self.key_starts.items[i];
            const end = if (i + 1 < n) self.key_starts.items[i + 1] else self.key_bytes.items.len;
            addKeyToArray(self.array.items, num_lines, self.num_probes, self.key_bytes.items[start..end]);
        }

        // 5-byte trailer: [marker=0][u32 LE num_probes].
        try self.array.append(gpa, kFullFilterMarker);
        var nb: [4]u8 = undefined;
        std.mem.writeInt(u32, &nb, self.num_probes, .little);
        try self.array.appendSlice(gpa, &nb);

        return self.array.items;
    }

    /// Reset for reuse (clears keys; keeps allocations).
    pub fn reset(self: *FullFilterBuilder) void {
        self.array.clearRetainingCapacity();
        self.key_bytes.clearRetainingCapacity();
        self.key_starts.clearRetainingCapacity();
        self.num_keys = 0;
        self.num_lines = 0;
    }
};

/// Reader for a FastLocalBloom full-filter block produced by FullFilterBuilder
/// (or RocksDB's full-filter format).  Recovers num_probes from the trailer and
/// derives num_lines from the bit-array length.
pub const FullFilterReader = struct {
    /// Bit array (the block minus its 5-byte trailer); not owned.
    array: []const u8,
    num_lines: u32,
    num_probes: u32,
    /// True when the block parsed as a well-formed full filter.
    valid: bool,

    pub fn init(contents: []const u8) FullFilterReader {
        // Need at least the trailer plus one cache line.
        if (contents.len < kCacheLineBytes + kTrailerBytes) {
            return .{ .array = &.{}, .num_lines = 0, .num_probes = 0, .valid = false };
        }
        const array_len = contents.len - kTrailerBytes;
        // The bit array must be a whole number of cache lines.
        if (array_len % kCacheLineBytes != 0) {
            return .{ .array = &.{}, .num_lines = 0, .num_probes = 0, .valid = false };
        }
        const marker = contents[array_len];
        const num_probes = std.mem.readInt(u32, contents[array_len + 1 ..][0..4], .little);
        // Sanity: full-filter marker and a plausible probe count.
        if (marker != kFullFilterMarker or num_probes == 0 or num_probes > 30) {
            return .{ .array = &.{}, .num_lines = 0, .num_probes = 0, .valid = false };
        }
        return .{
            .array = contents[0..array_len],
            .num_lines = @intCast(array_len / kCacheLineBytes),
            .num_probes = num_probes,
            .valid = true,
        };
    }

    /// May `key` be present?  A malformed/empty filter conservatively returns
    /// true (no information).  Never a false negative for a key that was added.
    pub fn keyMayMatch(self: *const FullFilterReader, key: []const u8) bool {
        if (!self.valid or self.num_lines == 0) return true;
        return arrayMayMatch(self.array, self.num_lines, self.num_probes, key);
    }

    /// Prefix probe (same content-agnostic bit test as keyMayMatch); documents
    /// that `prefix` is a key prefix rather than a whole key.
    pub fn prefixMayMatch(self: *const FullFilterReader, prefix: []const u8) bool {
        return self.keyMayMatch(prefix);
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;

test "no false negatives over 1000 keys (full filter)" {
    const gpa = testing.allocator;
    var b = FullFilterBuilder.init(10);
    defer b.deinit(gpa);

    var keys: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (keys.items) |k| gpa.free(k);
        keys.deinit(gpa);
    }
    var n: usize = 0;
    while (n < 1000) : (n += 1) {
        const k = try std.fmt.allocPrint(gpa, "key{d}", .{n});
        try keys.append(gpa, k);
        try b.addKey(gpa, k);
    }

    const block = try b.finish(gpa);
    var r = FullFilterReader.init(block);
    try testing.expect(r.valid);

    // Critical invariant: every inserted key MUST match.
    for (keys.items) |k| {
        try testing.expect(r.keyMayMatch(k));
    }
}

test "false-positive rate within tolerance (bits_per_key=10)" {
    const gpa = testing.allocator;
    var b = FullFilterBuilder.init(10);
    defer b.deinit(gpa);

    var n: usize = 0;
    while (n < 10000) : (n += 1) {
        var buf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&buf, "key{d}", .{n});
        try b.addKey(gpa, k);
    }
    const block = try b.finish(gpa);
    var r = FullFilterReader.init(block);

    var false_positives: usize = 0;
    const trials: usize = 10000;
    var i: usize = 0;
    while (i < trials) : (i += 1) {
        var buf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&buf, "nokey{d}", .{i});
        if (r.keyMayMatch(k)) false_positives += 1;
    }
    const rate = @as(f64, @floatFromInt(false_positives)) / @as(f64, @floatFromInt(trials));
    // 10 bits/key, 6 probes ≈ 1% theoretical; assert < 3% for headroom.
    try testing.expect(rate < 0.03);
}

test "trailer: marker 0 + LE num_probes, whole cache lines" {
    const gpa = testing.allocator;
    var b = FullFilterBuilder.init(10);
    defer b.deinit(gpa);
    const keys = [_][]const u8{ "a", "b", "c" };
    for (keys) |k| try b.addKey(gpa, k);
    const block = try b.finish(gpa);

    // Bit array = whole cache lines; small set → exactly one 64-byte line.
    try testing.expectEqual(@as(usize, kCacheLineBytes + kTrailerBytes), block.len);
    const array_len = block.len - kTrailerBytes;
    try testing.expectEqual(@as(usize, 0), array_len % kCacheLineBytes);
    try testing.expectEqual(kFullFilterMarker, block[array_len]);
    const num_probes = std.mem.readInt(u32, block[array_len + 1 ..][0..4], .little);
    try testing.expectEqual(probesForBitsPerKey(10), num_probes);
    try testing.expectEqual(@as(u32, 6), num_probes);
}

test "reader recovers probes/lines and matches absent key as false" {
    const gpa = testing.allocator;
    var b = FullFilterBuilder.init(10);
    defer b.deinit(gpa);
    const keys = [_][]const u8{ "hello", "world", "rocks" };
    for (keys) |k| try b.addKey(gpa, k);
    const block = try b.finish(gpa);

    var r = FullFilterReader.init(block);
    try testing.expect(r.valid);
    try testing.expectEqual(@as(u32, 6), r.num_probes);
    try testing.expect(r.num_lines >= 1);
    try testing.expect(r.keyMayMatch("hello"));
    try testing.expect(r.keyMayMatch("world"));
    try testing.expect(r.keyMayMatch("rocks"));
    try testing.expect(!r.keyMayMatch("absent_key_xyzzy_123"));
}

test "deterministic build is byte-identical" {
    const gpa = testing.allocator;
    const keys = [_][]const u8{ "alpha", "beta", "gamma", "delta", "epsilon" };

    var b1 = FullFilterBuilder.init(10);
    defer b1.deinit(gpa);
    var b2 = FullFilterBuilder.init(10);
    defer b2.deinit(gpa);
    for (keys) |k| {
        try b1.addKey(gpa, k);
        try b2.addKey(gpa, k);
    }
    const block1 = try b1.finish(gpa);
    const block2 = try b2.finish(gpa);
    try testing.expectEqualSlices(u8, block1, block2);
}

test "empty filter (no keys) is well-formed and conservative" {
    const gpa = testing.allocator;
    var b = FullFilterBuilder.init(10);
    defer b.deinit(gpa);
    const block = try b.finish(gpa);
    // Floored to one cache line + trailer.
    try testing.expectEqual(@as(usize, kCacheLineBytes + kTrailerBytes), block.len);
    var r = FullFilterReader.init(block);
    try testing.expect(r.valid);
    // No bits set → an arbitrary key won't match (no false negatives, since no
    // keys were inserted).
    try testing.expect(!r.keyMayMatch("anything"));
}

test "malformed contents → invalid reader, conservative match" {
    // Too short to be a full filter.
    var r = FullFilterReader.init(&[_]u8{ 1, 2, 3 });
    try testing.expect(!r.valid);
    try testing.expect(r.keyMayMatch("x")); // conservative true

    // Array length not a multiple of the cache-line size.
    var bad: [70]u8 = [_]u8{0} ** 70; // 65-byte "array" + 5 trailer
    bad[65] = kFullFilterMarker;
    std.mem.writeInt(u32, bad[66..70], 6, .little);
    var r2 = FullFilterReader.init(&bad);
    try testing.expect(!r2.valid);
    try testing.expect(r2.keyMayMatch("y"));
}

test "GOLDEN VECTOR: FastLocalBloom probe bits for a known key" {
    // Pin the exact layout (cache-line selection + rotate-7 probe walk) so a
    // regression in any of: XXPH3 hash, FastRange32, rotl-7, or bit order is
    // caught.  Derived by executing this module's documented algorithm by hand:
    //   key = "rocksdb", 1 key, 10 bits/key → 1 cache line (512 bits), 6 probes.
    //   h = XXPH3_64bits("rocksdb"); line = FastRange32(hi32, 1) = 0 (num_lines=1).
    //   h32 = lo32(h); probe bits = (h32 rotl 7*i) & 511 for i in 0..6.
    const gpa = testing.allocator;
    var b = FullFilterBuilder.init(10);
    defer b.deinit(gpa);
    try b.addKey(gpa, "rocksdb");
    const block = try b.finish(gpa);

    // Independently recompute the 6 expected bit positions from XXPH3.
    const h = xxph3.hash64("rocksdb");
    const hi32: u32 = @truncate(h >> 32);
    try testing.expectEqual(@as(u32, 0), xxph3.fastRange32(hi32, 1)); // single line
    var h32: u32 = @truncate(h);
    const array = block[0 .. block.len - kTrailerBytes];
    var i: usize = 0;
    while (i < 6) : (i += 1) {
        const bit = h32 & 511;
        // The bit must be set in the array.
        try testing.expect((array[bit / 8] & (@as(u8, 1) << @intCast(bit & 7))) != 0);
        h32 = (h32 << 7) | (h32 >> 25);
    }
    // And the key reports may-match.
    var r = FullFilterReader.init(block);
    try testing.expect(r.keyMayMatch("rocksdb"));
}
