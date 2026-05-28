/// bloom.zig — LevelDB-compatible bloom filter policy.
///
/// Byte-deterministic reimplementation of LevelDB's `util/bloom.cc` and the
/// hash function from `util/hash.cc`. This is the legacy *block-based* filter
/// algorithm (filter name "leveldb.BuiltinBloomFilter2"), the same one used by
/// RocksDB's block-based filter blocks.
///
/// TODO(m3.x): RocksDB also supports a "full filter" / partitioned-filter
/// format with a different in-block layout; that is intentionally out of scope
/// here and will be added when wiring up SST-table interop.
const std = @import("std");

// RED: stub types — implemented in the GREEN phase.

/// LevelDB hash: a stable, byte-deterministic 32-bit hash (util/hash.cc).
pub fn hash(data: []const u8, seed: u32) u32 {
    _ = data;
    _ = seed;
    return 0;
}

/// Bloom hash with the LevelDB-fixed seed 0xbc9f1d34.
pub fn bloomHash(key: []const u8) u32 {
    _ = key;
    return 0;
}

pub const BloomFilterPolicy = struct {
    bits_per_key: usize,
    k: u32,

    pub fn init(bits_per_key: usize) BloomFilterPolicy {
        _ = bits_per_key;
        return .{ .bits_per_key = 0, .k = 0 };
    }

    pub fn name(self: BloomFilterPolicy) []const u8 {
        _ = self;
        return "";
    }

    pub fn createFilter(
        self: BloomFilterPolicy,
        gpa: std.mem.Allocator,
        keys: []const []const u8,
        dst: *std.ArrayList(u8),
    ) !void {
        _ = self;
        _ = gpa;
        _ = keys;
        _ = dst;
    }

    pub fn keyMayMatch(self: BloomFilterPolicy, key: []const u8, filter: []const u8) bool {
        _ = self;
        _ = key;
        _ = filter;
        return false;
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
