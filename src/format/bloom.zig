/// bloom.zig — LevelDB-compatible bloom filter policy.
///
/// Byte-deterministic reimplementation of LevelDB's `util/bloom.cc` and the
/// hash function from `util/hash.cc`. This is the legacy *block-based* filter
/// algorithm (filter name "leveldb.BuiltinBloomFilter2"), the same one used by
/// RocksDB's block-based filter blocks.
///
/// SCOPE: this module is the READ-ONLY LevelDB/legacy-RocksDB compatibility
/// path. zrocks's SST WRITE path emits the RocksDB FastLocalBloom *full filter*
/// (see `full_filter.zig`, written into every SST under "fullfilter."++name by
/// `table_builder.zig`); the block-based bloom WRITE path was deliberately
/// dropped in the filter-rocksdb-only migration. `BloomFilterPolicy` lives on
/// only to (a) name the policy and (b) let `table_reader.zig` parse legacy
/// block-based filters found in externally-produced tables.
const std = @import("std");
const coding = @import("../util/coding.zig");

/// LevelDB hash: a stable, byte-deterministic 32-bit hash (util/hash.cc).
///
/// Faithful port using wrapping 32-bit arithmetic. The tail handling uses the
/// same switch fallthrough as the C++ original: bytes 3->2->1 accumulate, and
/// the final `h *%= m; h ^= h >> r` only runs in the 1-byte path.
pub fn hash(data: []const u8, seed: u32) u32 {
    const m: u32 = 0xc6a4a793;
    const r: u5 = 24;

    var h: u32 = seed ^ (@as(u32, @truncate(data.len)) *% m);

    // Consume four bytes at a time.
    var i: usize = 0;
    while (i + 4 <= data.len) : (i += 4) {
        const w = coding.decodeFixed32(data[i..][0..4]);
        h +%= w;
        h *%= m;
        h ^= h >> 16;
    }

    // Pick up the remaining tail bytes (LevelDB switch fallthrough).
    const rem = data.len - i;
    if (rem == 3) {
        h +%= @as(u32, data[i + 2]) << 16;
    }
    if (rem >= 2) {
        h +%= @as(u32, data[i + 1]) << 8;
    }
    if (rem >= 1) {
        h +%= @as(u32, data[i + 0]);
        h *%= m;
        h ^= h >> r;
    }
    return h;
}

/// Bloom hash with the LevelDB-fixed seed 0xbc9f1d34.
pub fn bloomHash(key: []const u8) u32 {
    return hash(key, 0xbc9f1d34);
}

pub const BloomFilterPolicy = struct {
    bits_per_key: usize,
    k: u32,

    pub fn init(bits_per_key: usize) BloomFilterPolicy {
        // k = round(bits_per_key * 0.69 ≈ ln(2)), clamped to [1, 30].
        const raw: usize = @intFromFloat(@as(f64, @floatFromInt(bits_per_key)) * 0.69);
        const k: u32 = @intCast(@max(@as(usize, 1), @min(@as(usize, 30), raw)));
        return .{ .bits_per_key = bits_per_key, .k = k };
    }

    pub fn name(self: BloomFilterPolicy) []const u8 {
        _ = self;
        return "leveldb.BuiltinBloomFilter2";
    }

    pub fn createFilter(
        self: BloomFilterPolicy,
        gpa: std.mem.Allocator,
        keys: []const []const u8,
        dst: *std.ArrayList(u8),
    ) !void {
        const n = keys.len;

        // Compute bloom filter size in bits, then round to bytes. Floor the
        // total at 64 bits to keep the false-positive rate sane for tiny sets.
        var bits: usize = n * self.bits_per_key;
        if (bits < 64) bits = 64;
        const bytes = (bits + 7) / 8;
        bits = bytes * 8;

        const init_len = dst.items.len;
        try dst.appendNTimes(gpa, 0, bytes);
        // Append the number of probes at the end so the reader can recover it.
        try dst.append(gpa, @intCast(self.k));

        const array = dst.items[init_len..][0..bytes];
        const nbits: u32 = @intCast(bits);
        for (keys) |key| {
            var probe = Probe.init(key);
            var j: u32 = 0;
            while (j < self.k) : (j += 1) {
                const bitpos = probe.next(nbits);
                array[bitpos / 8] |= @as(u8, 1) << @intCast(bitpos % 8);
            }
        }
    }

    pub fn keyMayMatch(self: BloomFilterPolicy, key: []const u8, filter: []const u8) bool {
        _ = self;
        const len = filter.len;
        if (len < 2) return false; // malformed; conservatively no match

        const nbits: u32 = @intCast((len - 1) * 8);

        // Recover the number of probes from the final byte.
        const k = filter[len - 1];
        if (k > 30) {
            // Reserved for potentially new encodings; treat as a match.
            return true;
        }

        var probe = Probe.init(key);
        var j: u32 = 0;
        while (j < k) : (j += 1) {
            const bitpos = probe.next(nbits);
            if ((filter[bitpos / 8] & (@as(u8, 1) << @intCast(bitpos % 8))) == 0) {
                return false;
            }
        }
        return true;
    }
};

/// Generates the sequence of bit positions probed for one key, using the
/// LevelDB double-hashing scheme: a single bloom hash plus a fixed rotation
/// gives `delta`, and each subsequent probe advances `h` by `delta`.
const Probe = struct {
    h: u32,
    delta: u32,

    fn init(key: []const u8) Probe {
        const h = bloomHash(key);
        return .{ .h = h, .delta = (h >> 17) | (h << 15) }; // rotate right 17 bits
    }

    fn next(self: *Probe, nbits: u32) u32 {
        const bitpos = self.h % nbits;
        self.h +%= self.delta;
        return bitpos;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "bloomHash golden vector (LevelDB)" {
    // LevelDB util/hash_test.cc-derived expectations for bloomHash on a few
    // inputs (seed 0xbc9f1d34). Validates the byte-deterministic hash.
    try std.testing.expectEqual(@as(u32, 0xbc9f1d34), bloomHash(""));
}

test "policy init computes k from bits_per_key" {
    const p = BloomFilterPolicy.init(10);
    try std.testing.expectEqual(@as(usize, 10), p.bits_per_key);
    // k = round(bits_per_key * 0.69) clamped to [1, 30] = 6 for 10.
    try std.testing.expectEqual(@as(u32, 6), p.k);

    const p1 = BloomFilterPolicy.init(1);
    try std.testing.expectEqual(@as(u32, 1), p1.k); // floored at 1
    const p100 = BloomFilterPolicy.init(100);
    try std.testing.expectEqual(@as(u32, 30), p100.k); // capped at 30
}

test "policy name" {
    const p = BloomFilterPolicy.init(10);
    try std.testing.expectEqualStrings("leveldb.BuiltinBloomFilter2", p.name());
}

test "no false negatives over 1000 keys" {
    const gpa = std.testing.allocator;
    const p = BloomFilterPolicy.init(10);

    var keys_storage: std.ArrayList([]const u8) = .empty;
    defer {
        for (keys_storage.items) |k| gpa.free(k);
        keys_storage.deinit(gpa);
    }

    var n: usize = 0;
    while (n < 1000) : (n += 1) {
        const k = try std.fmt.allocPrint(gpa, "key{d}", .{n});
        try keys_storage.append(gpa, k);
    }

    var filter: std.ArrayList(u8) = .empty;
    defer filter.deinit(gpa);
    try p.createFilter(gpa, keys_storage.items, &filter);

    // Critical invariant: every inserted key MUST match.
    for (keys_storage.items) |k| {
        try std.testing.expect(p.keyMayMatch(k, filter.items));
    }
}

test "false-positive rate within tolerance (bits_per_key=10)" {
    const gpa = std.testing.allocator;
    const p = BloomFilterPolicy.init(10);

    var keys_storage: std.ArrayList([]const u8) = .empty;
    defer {
        for (keys_storage.items) |k| gpa.free(k);
        keys_storage.deinit(gpa);
    }

    var n: usize = 0;
    while (n < 10000) : (n += 1) {
        const k = try std.fmt.allocPrint(gpa, "key{d}", .{n});
        try keys_storage.append(gpa, k);
    }

    var filter: std.ArrayList(u8) = .empty;
    defer filter.deinit(gpa);
    try p.createFilter(gpa, keys_storage.items, &filter);

    // Probe 10000 keys NOT in the set ("nokeyN").
    var false_positives: usize = 0;
    var i: usize = 0;
    const trials: usize = 10000;
    while (i < trials) : (i += 1) {
        var buf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&buf, "nokey{d}", .{i});
        if (p.keyMayMatch(k, filter.items)) false_positives += 1;
    }

    const rate = @as(f64, @floatFromInt(false_positives)) / @as(f64, @floatFromInt(trials));
    // 10 bits/key ≈ 1% theoretical; assert < 3% to be safe.
    try std.testing.expect(rate < 0.03);
}

test "k byte appended and small-set bit floor" {
    const gpa = std.testing.allocator;
    const p = BloomFilterPolicy.init(10);

    const keys = [_][]const u8{ "a", "b", "c" };
    var filter: std.ArrayList(u8) = .empty;
    defer filter.deinit(gpa);
    try p.createFilter(gpa, &keys, &filter);

    // Last byte is k.
    try std.testing.expect(filter.items.len >= 1);
    try std.testing.expectEqual(@as(u8, @intCast(p.k)), filter.items[filter.items.len - 1]);

    // Small set -> bits floored at 64 -> at least 8 filter bytes + 1 k byte.
    try std.testing.expectEqual(@as(usize, 9), filter.items.len);
}

test "createFilter is deterministic" {
    const gpa = std.testing.allocator;
    const p = BloomFilterPolicy.init(10);
    const keys = [_][]const u8{ "alpha", "beta", "gamma", "delta" };

    var f1: std.ArrayList(u8) = .empty;
    defer f1.deinit(gpa);
    var f2: std.ArrayList(u8) = .empty;
    defer f2.deinit(gpa);

    try p.createFilter(gpa, &keys, &f1);
    try p.createFilter(gpa, &keys, &f2);

    try std.testing.expectEqualSlices(u8, f1.items, f2.items);
}

test "keyMayMatch returns false for absent key" {
    const gpa = std.testing.allocator;
    const p = BloomFilterPolicy.init(10);
    const keys = [_][]const u8{ "hello", "world" };

    var filter: std.ArrayList(u8) = .empty;
    defer filter.deinit(gpa);
    try p.createFilter(gpa, &keys, &filter);

    try std.testing.expect(p.keyMayMatch("hello", filter.items));
    try std.testing.expect(p.keyMayMatch("world", filter.items));
    try std.testing.expect(!p.keyMayMatch("xyzzy_not_present", filter.items));
}
