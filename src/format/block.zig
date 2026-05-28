/// block.zig — LevelDB/RocksDB-compatible data/index block builder and reader.
///
/// Byte-exact with the LevelDB/RocksDB block format (table/block_builder.cc and
/// table/block.cc). The same layout is used for both data blocks and index blocks.
///
/// Layout:
///   block          := entry* restart_offsets restart_count
///   entry          := varint32(shared) varint32(non_shared) varint32(value_len)
///                     key_delta[non_shared] value[value_len]
///   restart_offsets := fixed32_LE * num_restarts  (byte offset of each restart entry)
///   restart_count   := fixed32_LE                 (number of restarts; last 4 bytes)
///
/// `shared` is the length of the prefix the entry's key shares with the PREVIOUS
/// key; at a restart point shared=0 (the full key is stored). A restart point is
/// emitted for the first entry and then every `restart_interval` entries. Keys
/// MUST be added in non-decreasing sorted order.
const std = @import("std");
const coding = @import("../util/coding.zig");
const comparator = @import("../util/comparator.zig");

pub const Error = error{Corruption};

// ---------------------------------------------------------------------------
// BlockBuilder
// ---------------------------------------------------------------------------

pub const BlockBuilder = struct {
    gpa: std.mem.Allocator,
    restart_interval: usize,
    /// Accumulated block bytes (entries; restart array appended on finish()).
    buffer: std.ArrayListUnmanaged(u8),
    /// Byte offsets (within buffer) of each restart entry.
    restarts: std.ArrayListUnmanaged(u32),
    /// Number of entries emitted since the last restart point.
    counter: usize,
    /// Whether finish() has been called (no further add() until reset()).
    finished: bool,
    /// The previous key added, kept to compute shared prefixes.
    last_key: std.ArrayListUnmanaged(u8),

    pub fn init(gpa: std.mem.Allocator, restart_interval: usize) BlockBuilder {
        std.debug.assert(restart_interval >= 1);
        var restarts: std.ArrayListUnmanaged(u32) = .empty;
        // The first entry is always a restart point at offset 0.
        restarts.append(gpa, 0) catch @panic("OOM appending initial restart");
        return .{
            .gpa = gpa,
            .restart_interval = restart_interval,
            .buffer = .empty,
            .restarts = restarts,
            .counter = 0,
            .finished = false,
            .last_key = .empty,
        };
    }

    pub fn deinit(self: *BlockBuilder) void {
        self.buffer.deinit(self.gpa);
        self.restarts.deinit(self.gpa);
        self.last_key.deinit(self.gpa);
    }

    /// Reset the builder to its just-initialized state, ready to build a new block.
    pub fn reset(self: *BlockBuilder) void {
        self.buffer.clearRetainingCapacity();
        self.restarts.clearRetainingCapacity();
        self.restarts.append(self.gpa, 0) catch @panic("OOM appending initial restart");
        self.counter = 0;
        self.finished = false;
        self.last_key.clearRetainingCapacity();
    }

    pub fn isEmpty(self: *const BlockBuilder) bool {
        return self.buffer.items.len == 0;
    }

    /// Estimate of the size of the block if finish() were called now: the entry
    /// bytes plus the restart array (one fixed32 per restart) plus the count.
    pub fn currentSizeEstimate(self: *const BlockBuilder) usize {
        return self.buffer.items.len // entry data
        + self.restarts.items.len * @sizeOf(u32) // restart offsets
        + @sizeOf(u32); // restart count
    }

    /// Append (key, value). Keys must arrive in non-decreasing sorted order.
    pub fn add(self: *BlockBuilder, key: []const u8, value: []const u8) !void {
        std.debug.assert(!self.finished);
        std.debug.assert(self.counter <= self.restart_interval);
        // Sorted-order invariant: key >= last_key (when not the very first key).
        std.debug.assert(self.buffer.items.len == 0 or
            std.mem.order(u8, self.last_key.items, key) != .gt);

        var shared: usize = 0;
        if (self.counter < self.restart_interval) {
            // Compute the shared prefix length against the previous key.
            const min_len = @min(self.last_key.items.len, key.len);
            while (shared < min_len and self.last_key.items[shared] == key[shared]) {
                shared += 1;
            }
        } else {
            // Restart point: store the full key and record its offset.
            try self.restarts.append(self.gpa, @intCast(self.buffer.items.len));
            self.counter = 0;
        }

        const non_shared = key.len - shared;

        // entry header: varint32(shared) varint32(non_shared) varint32(value_len)
        try coding.putVarint32(&self.buffer, self.gpa, @intCast(shared));
        try coding.putVarint32(&self.buffer, self.gpa, @intCast(non_shared));
        try coding.putVarint32(&self.buffer, self.gpa, @intCast(value.len));
        // key delta (non_shared bytes) + value
        try self.buffer.appendSlice(self.gpa, key[shared..]);
        try self.buffer.appendSlice(self.gpa, value);

        // Update last_key = key (reuse the shared prefix already stored).
        self.last_key.shrinkRetainingCapacity(shared);
        try self.last_key.appendSlice(self.gpa, key[shared..]);
        std.debug.assert(std.mem.eql(u8, self.last_key.items, key));

        self.counter += 1;
    }

    /// Append the restart array + count and return the complete block bytes.
    /// The returned slice is owned by the builder until reset()/deinit().
    pub fn finish(self: *BlockBuilder) []const u8 {
        for (self.restarts.items) |off| {
            coding.putFixed32(&self.buffer, self.gpa, off) catch @panic("OOM in finish");
        }
        coding.putFixed32(&self.buffer, self.gpa, @intCast(self.restarts.items.len)) catch @panic("OOM in finish");
        self.finished = true;
        return self.buffer.items;
    }
};

// ---------------------------------------------------------------------------
// Block (reader)
// ---------------------------------------------------------------------------

pub const Block = struct {
    data: []const u8,
    /// Byte offset where the restart array begins.
    restart_offset: usize,
    num_restarts: u32,

    pub fn init(data: []const u8) !Block {
        // Minimum block: at least the trailing fixed32 restart count.
        if (data.len < @sizeOf(u32)) return error.Corruption;
        const count_bytes: *const [4]u8 = data[data.len - 4 ..][0..4];
        const num_restarts = coding.decodeFixed32(count_bytes);

        // The restart array occupies num_restarts*4 bytes immediately before the
        // count. Validate the block is large enough to hold it.
        const max_restarts_bytes = (data.len - @sizeOf(u32)) / @sizeOf(u32);
        if (num_restarts > max_restarts_bytes) return error.Corruption;

        const restart_offset = data.len - (@as(usize, 1) + num_restarts) * @sizeOf(u32);
        return .{
            .data = data,
            .restart_offset = restart_offset,
            .num_restarts = num_restarts,
        };
    }

    /// Read the byte offset of restart point `index` from the restart array.
    fn restartPoint(self: *const Block, index: u32) u32 {
        std.debug.assert(index < self.num_restarts);
        const off = self.restart_offset + @as(usize, index) * @sizeOf(u32);
        const bytes: *const [4]u8 = self.data[off..][0..4];
        return coding.decodeFixed32(bytes);
    }

    pub fn iterator(self: *const Block, cmp: comparator.Comparator) Iter {
        return Iter.init(self, cmp);
    }

    // -----------------------------------------------------------------------
    // Iterator
    // -----------------------------------------------------------------------

    pub const Iter = struct {
        block: *const Block,
        cmp: comparator.Comparator,
        /// Offset of the current entry within [0, restart_offset). When
        /// current == restart_offset the iterator is invalid (past end).
        current: usize,
        /// Offset of the entry that opened the restart region we are scanning.
        restart_index: u32,
        /// Reconstructed full key of the current entry.
        key_buf: std.ArrayListUnmanaged(u8),
        /// Value slice of the current entry (points into block.data).
        value_slice: []const u8,
        /// An allocation/parse error encountered while iterating, if any.
        err: ?Error,
        gpa: std.mem.Allocator,

        pub fn init(block: *const Block, cmp: comparator.Comparator) Iter {
            return .{
                .block = block,
                .cmp = cmp,
                .current = block.restart_offset, // invalid until positioned
                .restart_index = 0,
                .key_buf = .empty,
                .value_slice = &.{},
                .err = null,
                .gpa = std.testing.allocator,
            };
        }

        pub fn deinit(self: *Iter) void {
            self.key_buf.deinit(self.gpa);
        }

        pub fn valid(self: *const Iter) bool {
            return self.err == null and self.current < self.block.restart_offset;
        }

        pub fn key(self: *const Iter) []const u8 {
            std.debug.assert(self.valid());
            return self.key_buf.items;
        }

        pub fn value(self: *const Iter) []const u8 {
            std.debug.assert(self.valid());
            return self.value_slice;
        }

        pub fn seekToFirst(self: *Iter) void {
            _ = self;
            @panic("unimplemented");
        }

        pub fn seekToLast(self: *Iter) void {
            _ = self;
            @panic("unimplemented");
        }

        pub fn seek(self: *Iter, target: []const u8) void {
            _ = self;
            _ = target;
            @panic("unimplemented");
        }

        pub fn next(self: *Iter) void {
            _ = self;
            @panic("unimplemented");
        }
    };
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;

/// Collect (key,value) pairs by walking seekToFirst -> next until !valid.
fn collect(
    iter: *Block.Iter,
    out_keys: *std.ArrayListUnmanaged([]const u8),
    out_vals: *std.ArrayListUnmanaged([]const u8),
    gpa: std.mem.Allocator,
) !void {
    iter.seekToFirst();
    while (iter.valid()) : (iter.next()) {
        try out_keys.append(gpa, try gpa.dupe(u8, iter.key()));
        try out_vals.append(gpa, try gpa.dupe(u8, iter.value()));
    }
}

const KV = struct { k: []const u8, v: []const u8 };

fn buildBlock(gpa: std.mem.Allocator, restart_interval: usize, pairs: []const KV) ![]const u8 {
    var b = BlockBuilder.init(gpa, restart_interval);
    defer b.deinit();
    for (pairs) |p| try b.add(p.k, p.v);
    const block_bytes = b.finish();
    // The builder owns block_bytes; dupe so it outlives the builder.
    return gpa.dupe(u8, block_bytes);
}

test "round-trip: build then iterate reproduces added pairs in order" {
    const gpa = testing.allocator;
    const pairs = [_]KV{
        .{ .k = "app", .v = "1" },
        .{ .k = "apple", .v = "22" },
        .{ .k = "application", .v = "333" },
        .{ .k = "banana", .v = "4444" },
        .{ .k = "band", .v = "55555" },
    };

    const data = try buildBlock(gpa, 2, &pairs);
    defer gpa.free(data);

    const block = try Block.init(data);
    var iter = block.iterator(comparator.bytewise);
    defer iter.deinit();

    var i: usize = 0;
    iter.seekToFirst();
    while (iter.valid()) : (iter.next()) {
        try testing.expectEqualStrings(pairs[i].k, iter.key());
        try testing.expectEqualStrings(pairs[i].v, iter.value());
        i += 1;
    }
    try testing.expectEqual(pairs.len, i);
}

test "shared-prefix reconstruction: keys equal originals" {
    const gpa = testing.allocator;
    const pairs = [_]KV{
        .{ .k = "app", .v = "x" },
        .{ .k = "apple", .v = "x" },
        .{ .k = "application", .v = "x" },
        .{ .k = "applicationsuffix", .v = "x" },
        .{ .k = "banana", .v = "x" },
        .{ .k = "band", .v = "x" },
        .{ .k = "bandana", .v = "x" },
    };
    const data = try buildBlock(gpa, 3, &pairs);
    defer gpa.free(data);

    const block = try Block.init(data);
    var iter = block.iterator(comparator.bytewise);
    defer iter.deinit();

    var keys: std.ArrayListUnmanaged([]const u8) = .empty;
    var vals: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (keys.items) |k| gpa.free(k);
        for (vals.items) |v| gpa.free(v);
        keys.deinit(gpa);
        vals.deinit(gpa);
    }
    try collect(&iter, &keys, &vals, gpa);

    try testing.expectEqual(pairs.len, keys.items.len);
    for (pairs, 0..) |p, i| {
        try testing.expectEqualStrings(p.k, keys.items[i]);
    }
}

test "seek: present key, between key, past end, before first" {
    const gpa = testing.allocator;
    const pairs = [_]KV{
        .{ .k = "c", .v = "1" },
        .{ .k = "e", .v = "2" },
        .{ .k = "g", .v = "3" },
        .{ .k = "i", .v = "4" },
        .{ .k = "k", .v = "5" },
    };
    const data = try buildBlock(gpa, 2, &pairs);
    defer gpa.free(data);

    const block = try Block.init(data);
    var iter = block.iterator(comparator.bytewise);
    defer iter.deinit();

    // Present key lands on it.
    iter.seek("g");
    try testing.expect(iter.valid());
    try testing.expectEqualStrings("g", iter.key());
    try testing.expectEqualStrings("3", iter.value());

    // Between key -> next greater.
    iter.seek("f");
    try testing.expect(iter.valid());
    try testing.expectEqualStrings("g", iter.key());

    // Before first -> first entry.
    iter.seek("a");
    try testing.expect(iter.valid());
    try testing.expectEqualStrings("c", iter.key());

    // Exactly first.
    iter.seek("c");
    try testing.expect(iter.valid());
    try testing.expectEqualStrings("c", iter.key());

    // Past end -> invalid.
    iter.seek("z");
    try testing.expect(!iter.valid());

    // Exactly last.
    iter.seek("k");
    try testing.expect(iter.valid());
    try testing.expectEqualStrings("k", iter.key());
}

test "seek: binary search lands correctly around restart boundaries" {
    const gpa = testing.allocator;
    // restart_interval=2 with 7 entries -> restarts at entries 0,2,4,6.
    const pairs = [_]KV{
        .{ .k = "key00", .v = "v0" },
        .{ .k = "key01", .v = "v1" },
        .{ .k = "key02", .v = "v2" }, // restart
        .{ .k = "key03", .v = "v3" },
        .{ .k = "key04", .v = "v4" }, // restart
        .{ .k = "key05", .v = "v5" },
        .{ .k = "key06", .v = "v6" }, // restart
    };
    const data = try buildBlock(gpa, 2, &pairs);
    defer gpa.free(data);

    const block = try Block.init(data);
    try testing.expect(block.num_restarts >= 3);

    var iter = block.iterator(comparator.bytewise);
    defer iter.deinit();

    // Seek to each present key (at and around restart points).
    for (pairs) |p| {
        iter.seek(p.k);
        try testing.expect(iter.valid());
        try testing.expectEqualStrings(p.k, iter.key());
        try testing.expectEqualStrings(p.v, iter.value());
    }

    // Between two restart points -> next greater present key.
    iter.seek("key035"); // between key03 and key04
    try testing.expect(iter.valid());
    try testing.expectEqualStrings("key04", iter.key());
}

test "seekToLast lands on the final entry" {
    const gpa = testing.allocator;
    const pairs = [_]KV{
        .{ .k = "a", .v = "1" },
        .{ .k = "bb", .v = "2" },
        .{ .k = "ccc", .v = "3" },
        .{ .k = "dddd", .v = "4" },
    };
    const data = try buildBlock(gpa, 2, &pairs);
    defer gpa.free(data);

    const block = try Block.init(data);
    var iter = block.iterator(comparator.bytewise);
    defer iter.deinit();

    iter.seekToLast();
    try testing.expect(iter.valid());
    try testing.expectEqualStrings("dddd", iter.key());
    try testing.expectEqualStrings("4", iter.value());
}

test "single-entry block" {
    const gpa = testing.allocator;
    const pairs = [_]KV{.{ .k = "solo", .v = "value" }};
    const data = try buildBlock(gpa, 2, &pairs);
    defer gpa.free(data);

    const block = try Block.init(data);
    var iter = block.iterator(comparator.bytewise);
    defer iter.deinit();

    iter.seekToFirst();
    try testing.expect(iter.valid());
    try testing.expectEqualStrings("solo", iter.key());
    try testing.expectEqualStrings("value", iter.value());
    iter.next();
    try testing.expect(!iter.valid());
}

test "empty block: finish with no adds, iterate is invalid" {
    const gpa = testing.allocator;
    var b = BlockBuilder.init(gpa, 2);
    defer b.deinit();
    try testing.expect(b.isEmpty());

    const block_bytes = b.finish();
    const data = try gpa.dupe(u8, block_bytes);
    defer gpa.free(data);

    const block = try Block.init(data);
    var iter = block.iterator(comparator.bytewise);
    defer iter.deinit();

    iter.seekToFirst();
    try testing.expect(!iter.valid());

    iter.seek("anything");
    try testing.expect(!iter.valid());
}

test "BlockBuilder reset reuses the builder" {
    const gpa = testing.allocator;
    var b = BlockBuilder.init(gpa, 2);
    defer b.deinit();

    try b.add("x", "1");
    try b.add("y", "2");
    _ = b.finish();
    try testing.expect(!b.isEmpty());

    b.reset();
    try testing.expect(b.isEmpty());

    try b.add("z", "3");
    const data = try gpa.dupe(u8, b.finish());
    defer gpa.free(data);

    const block = try Block.init(data);
    var iter = block.iterator(comparator.bytewise);
    defer iter.deinit();
    iter.seekToFirst();
    try testing.expect(iter.valid());
    try testing.expectEqualStrings("z", iter.key());
    iter.next();
    try testing.expect(!iter.valid());
}

test "currentSizeEstimate grows with entries" {
    const gpa = testing.allocator;
    var b = BlockBuilder.init(gpa, 2);
    defer b.deinit();

    const empty_est = b.currentSizeEstimate();
    try b.add("aaaa", "bbbb");
    try testing.expect(b.currentSizeEstimate() > empty_est);
}

test "Block.init rejects too-small data" {
    try testing.expectError(error.Corruption, Block.init(&.{ 0x00, 0x01 }));
}
