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

/// Builds a block-based filter block: one bloom filter per `kFilterBase`
/// (2KB) range of the data-block region, mirroring LevelDB's
/// `FilterBlockBuilder`.
pub const FilterBlockBuilder = struct {
    policy: bloom.BloomFilterPolicy,

    /// Flattened concatenation of all keys added since the last filter emit.
    keys: std.ArrayList(u8) = .empty,
    /// Start offsets of each key within `keys` (plus a sentinel end offset).
    start: std.ArrayList(usize) = .empty,
    /// Accumulated filter result bytes (one bloom filter per range).
    result: std.ArrayList(u8) = .empty,
    /// Offset of each emitted filter within `result`.
    filter_offsets: std.ArrayList(u32) = .empty,
    /// Scratch slice list reused by generateFilter.
    tmp_keys: std.ArrayList([]const u8) = .empty,

    pub fn init(gpa: std.mem.Allocator, policy: bloom.BloomFilterPolicy) FilterBlockBuilder {
        _ = gpa;
        return .{ .policy = policy };
    }

    pub fn deinit(self: *FilterBlockBuilder, gpa: std.mem.Allocator) void {
        self.keys.deinit(gpa);
        self.start.deinit(gpa);
        self.result.deinit(gpa);
        self.filter_offsets.deinit(gpa);
        self.tmp_keys.deinit(gpa);
    }

    pub fn startBlock(self: *FilterBlockBuilder, gpa: std.mem.Allocator, block_offset: u64) !void {
        const filter_index: u64 = block_offset / kFilterBase;
        std.debug.assert(filter_index >= self.filter_offsets.items.len);
        while (filter_index > self.filter_offsets.items.len) {
            try self.generateFilter(gpa);
        }
    }

    pub fn addKey(self: *FilterBlockBuilder, gpa: std.mem.Allocator, key: []const u8) !void {
        try self.start.append(gpa, self.keys.items.len);
        try self.keys.appendSlice(gpa, key);
    }

    pub fn finish(self: *FilterBlockBuilder, gpa: std.mem.Allocator) ![]const u8 {
        // Flush any pending keys into a final filter.
        if (self.start.items.len != 0) {
            try self.generateFilter(gpa);
        }

        // Append array of per-filter offsets.
        const array_offset: u32 = @intCast(self.result.items.len);
        for (self.filter_offsets.items) |off| {
            try coding.putFixed32(&self.result, gpa, off);
        }
        // Offset where the offset-array begins.
        try coding.putFixed32(&self.result, gpa, array_offset);
        // Save the base-lg in the final byte.
        try self.result.append(gpa, kFilterBaseLg);

        return self.result.items;
    }

    /// Generate a single bloom filter covering all currently pending keys and
    /// record its starting offset, then clear the pending-key buffers.
    fn generateFilter(self: *FilterBlockBuilder, gpa: std.mem.Allocator) !void {
        const num_keys = self.start.items.len;
        if (num_keys == 0) {
            // No keys for this range: reuse the previous filter offset (empty
            // filter), matching LevelDB.
            try self.filter_offsets.append(gpa, @intCast(self.result.items.len));
            return;
        }

        // Materialize pointers to each pending key as a slice-of-slices.
        // Sentinel end offset so the last key's length is computable.
        try self.start.append(gpa, self.keys.items.len);
        self.tmp_keys.clearRetainingCapacity();
        try self.tmp_keys.ensureTotalCapacity(gpa, num_keys);
        var i: usize = 0;
        while (i < num_keys) : (i += 1) {
            const base = self.start.items[i];
            const length = self.start.items[i + 1] - base;
            self.tmp_keys.appendAssumeCapacity(self.keys.items[base..][0..length]);
        }

        // Record offset and generate the filter for this range of keys.
        try self.filter_offsets.append(gpa, @intCast(self.result.items.len));
        try self.policy.createFilter(gpa, self.tmp_keys.items, &self.result);

        // Reset pending-key state for the next range.
        self.tmp_keys.clearRetainingCapacity();
        self.keys.clearRetainingCapacity();
        self.start.clearRetainingCapacity();
    }
};

/// Reads a block-based filter block produced by `FilterBlockBuilder`,
/// mirroring LevelDB's `FilterBlockReader`.
pub const FilterBlockReader = struct {
    policy: bloom.BloomFilterPolicy,
    /// Full filter-block contents (not owned).
    data: []const u8,
    /// Pointer to the start of the offset array within `data`.
    offset_array: usize,
    /// Number of filters (length of the offset array).
    num: usize,
    /// log2 of the per-filter range; recovered from the trailing byte.
    base_lg: u6,

    pub fn init(policy: bloom.BloomFilterPolicy, contents: []const u8) FilterBlockReader {
        var reader = FilterBlockReader{
            .policy = policy,
            .data = contents,
            .offset_array = 0,
            .num = 0,
            .base_lg = kFilterBaseLg,
        };

        const n = contents.len;
        // Need at least the 1-byte base-lg and the 4-byte array offset.
        if (n < 5) return reader;

        reader.base_lg = @intCast(contents[n - 1]);
        const last_word = coding.decodeFixed32(contents[n - 5 ..][0..4]);
        if (last_word > n - 5) return reader; // malformed
        reader.offset_array = last_word;
        reader.num = (n - 5 - last_word) / 4;
        return reader;
    }

    /// Prefix-filter probe (M7.2).  The filter block is content-agnostic — it
    /// hashes raw bytes — so a prefix-keyed filter is consulted exactly like a
    /// whole-key one; this is a named alias of `keyMayMatch` documenting that
    /// `prefix` is a key prefix (from a PrefixExtractor) rather than a full key.
    /// Returns false ONLY when the filter proves the prefix absent in the range;
    /// a conservative true (no/empty filter, malformed) is always safe.
    pub fn prefixMayMatch(self: *const FilterBlockReader, block_offset: u64, prefix: []const u8) bool {
        return self.keyMayMatch(block_offset, prefix);
    }

    pub fn keyMayMatch(self: *const FilterBlockReader, block_offset: u64, key: []const u8) bool {
        const index: u64 = block_offset >> self.base_lg;
        if (index >= self.num) {
            // No filter for this range -> conservative match.
            return true;
        }

        const off_pos = self.offset_array + index * 4;
        const start = coding.decodeFixed32(self.data[off_pos..][0..4]);
        const limit = coding.decodeFixed32(self.data[off_pos + 4 ..][0..4]);
        if (start <= limit and limit <= self.offset_array) {
            const filter = self.data[start..limit];
            // An empty filter range carries no information -> conservative
            // match; otherwise consult the bloom filter.
            if (filter.len == 0) return true;
            return self.policy.keyMayMatch(key, filter);
        }
        // Malformed offsets are treated conservatively as a potential match.
        return true;
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

test "prefix filter: no false negatives for present prefixes, prunes absent" {
    // M7.2: a filter built over key PREFIXES is probed via `prefixMayMatch`.
    // Keys "abc1","abc2","xyz1" with a 3-byte fixed prefix → prefixes
    // {"abc","xyz"}.  Every present prefix must report may-match (no false
    // negatives); an absent prefix ("qqq") must be pruned (no match).
    const gpa = std.testing.allocator;
    const policy = bloom.BloomFilterPolicy.init(10);

    var builder = FilterBlockBuilder.init(gpa, policy);
    defer builder.deinit(gpa);

    try builder.startBlock(gpa, 0);
    // Add prefixes (simulating TableBuilder prefix mode).
    try builder.addKey(gpa, "abc");
    try builder.addKey(gpa, "abc");
    try builder.addKey(gpa, "xyz");

    const contents = try builder.finish(gpa);
    var reader = FilterBlockReader.init(policy, contents);

    // Present prefixes → may-match (NEVER a false negative).
    try std.testing.expect(reader.prefixMayMatch(0, "abc"));
    try std.testing.expect(reader.prefixMayMatch(0, "xyz"));
    // Absent prefix → pruned.
    try std.testing.expect(!reader.prefixMayMatch(0, "qqq"));
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
