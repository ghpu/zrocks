/// filter_block.zig — LevelDB block-based filter block (per 2KB range).
///
/// Byte-deterministic reimplementation of LevelDB's `table/filter_block.cc`.
/// A filter block holds one bloom filter per `kFilterBase` (2KB) range of the
/// data block region, followed by an array of fixed32 offsets, a fixed32
/// pointing at the start of that array, and a final base-lg byte.
///
/// TODO(m3.x): RocksDB's newer partitioned/full filter block formats differ;
/// only the legacy block-based layout is implemented here for now. SST-table
/// integration is out of scope for this milestone.
const std = @import("std");
const coding = @import("../util/coding.zig");
const bloom = @import("bloom.zig");

/// log2 of the per-filter byte range. LevelDB uses 11 (2KB).
pub const kFilterBaseLg: u6 = 11;
pub const kFilterBase: u64 = 1 << kFilterBaseLg;

// RED: stub types — implemented in the GREEN phase.

pub const FilterBlockBuilder = struct {
    pub fn init(gpa: std.mem.Allocator, policy: bloom.BloomFilterPolicy) FilterBlockBuilder {
        _ = gpa;
        _ = policy;
        return .{};
    }

    pub fn deinit(self: *FilterBlockBuilder, gpa: std.mem.Allocator) void {
        _ = self;
        _ = gpa;
    }

    pub fn startBlock(self: *FilterBlockBuilder, gpa: std.mem.Allocator, block_offset: u64) !void {
        _ = self;
        _ = gpa;
        _ = block_offset;
    }

    pub fn addKey(self: *FilterBlockBuilder, gpa: std.mem.Allocator, key: []const u8) !void {
        _ = self;
        _ = gpa;
        _ = key;
    }

    pub fn finish(self: *FilterBlockBuilder, gpa: std.mem.Allocator) ![]const u8 {
        _ = self;
        _ = gpa;
        return &.{};
    }
};

pub const FilterBlockReader = struct {
    pub fn init(policy: bloom.BloomFilterPolicy, contents: []const u8) FilterBlockReader {
        _ = policy;
        _ = contents;
        return .{};
    }

    pub fn keyMayMatch(self: *const FilterBlockReader, block_offset: u64, key: []const u8) bool {
        _ = self;
        _ = block_offset;
        _ = key;
        return false;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "build/read round-trip with keys at various block offsets" {
    const gpa = std.testing.allocator;
    const policy = bloom.BloomFilterPolicy.init(10);

    var builder = FilterBlockBuilder.init(gpa, policy);
    defer builder.deinit(gpa);

    // Block at offset 0.
    try builder.startBlock(gpa, 0);
    try builder.addKey(gpa, "foo");
    try builder.addKey(gpa, "bar");
    try builder.addKey(gpa, "box");

    // Block at offset 1000 (still within the first 2KB base range).
    try builder.startBlock(gpa, 1000);
    try builder.addKey(gpa, "hello");

    // Block well past the first base range -> generates intervening filters.
    try builder.startBlock(gpa, 10000);
    try builder.addKey(gpa, "world");

    const contents = try builder.finish(gpa);
    // contents is owned by the builder; freed on deinit.

    var reader = FilterBlockReader.init(policy, contents);

    // Inserted keys match at their block offset.
    try std.testing.expect(reader.keyMayMatch(0, "foo"));
    try std.testing.expect(reader.keyMayMatch(0, "bar"));
    try std.testing.expect(reader.keyMayMatch(0, "box"));
    try std.testing.expect(reader.keyMayMatch(1000, "hello"));
    try std.testing.expect(reader.keyMayMatch(10000, "world"));

    // An absent key at offset 0 should (almost surely) not match.
    try std.testing.expect(!reader.keyMayMatch(0, "missing_key_xyz"));
}

test "empty filter range returns conservative match" {
    const gpa = std.testing.allocator;
    const policy = bloom.BloomFilterPolicy.init(10);

    var builder = FilterBlockBuilder.init(gpa, policy);
    defer builder.deinit(gpa);

    try builder.startBlock(gpa, 0);
    try builder.addKey(gpa, "single");

    const contents = try builder.finish(gpa);
    var reader = FilterBlockReader.init(policy, contents);

    // A very large offset maps to a filter index with no keys (empty range).
    // LevelDB returns true (no information => potential match).
    try std.testing.expect(reader.keyMayMatch(9_000_000, "anything"));
}

test "builder finish with no keys is well-formed" {
    const gpa = std.testing.allocator;
    const policy = bloom.BloomFilterPolicy.init(10);

    var builder = FilterBlockBuilder.init(gpa, policy);
    defer builder.deinit(gpa);

    const contents = try builder.finish(gpa);
    var reader = FilterBlockReader.init(policy, contents);
    // No filters at all -> every probe is a conservative match.
    try std.testing.expect(reader.keyMayMatch(0, "x"));
}
