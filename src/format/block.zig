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
    /// Allocator used by iterators to grow their reconstructed-key buffer.
    gpa: std.mem.Allocator,
    /// Byte offset where the restart array begins.
    restart_offset: usize,
    num_restarts: u32,

    /// Parse a block from raw bytes. Iterators over this block allocate their
    /// reconstructed-key buffer from `gpa`.
    pub fn init(gpa: std.mem.Allocator, data: []const u8) !Block {
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
            .gpa = gpa,
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
        gpa: std.mem.Allocator,
        /// Offset of the CURRENT entry within [0, restart_offset). When
        /// current >= restart_offset the iterator is invalid (past end).
        current: usize,
        /// Offset just past the current entry (where the next entry begins).
        next_offset: usize,
        /// Index of the restart point at/below the current entry.
        restart_index: u32,
        /// Reconstructed full key of the current entry.
        key_buf: std.ArrayListUnmanaged(u8),
        /// Value slice of the current entry (points into block.data).
        value_slice: []const u8,
        /// A parse/allocation error encountered while iterating, if any.
        err: ?Error,

        /// Decoded entry header at a given offset.
        const EntryHeader = struct {
            shared: u32,
            non_shared: u32,
            value_len: u32,
            /// Offset of the key-delta bytes (immediately after the header).
            key_delta_offset: usize,
        };

        pub fn init(block: *const Block, cmp: comparator.Comparator) Iter {
            return .{
                .block = block,
                .cmp = cmp,
                .gpa = block.gpa,
                .current = invalidOffset(block), // invalid until positioned
                .next_offset = 0,
                .restart_index = 0,
                .key_buf = .empty,
                .value_slice = &.{},
                .err = null,
            };
        }

        pub fn deinit(self: *Iter) void {
            self.key_buf.deinit(self.gpa);
        }

        /// A sentinel "invalid" offset: anything >= restart_offset.
        fn invalidOffset(block: *const Block) usize {
            return block.restart_offset;
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

        // -------------------------------------------------------------------
        // Entry decoding
        // -------------------------------------------------------------------

        /// Decode the entry header starting at `offset` (must be < restart_offset).
        fn decodeHeader(self: *Iter, offset: usize) Error!EntryHeader {
            var input: []const u8 = self.block.data[offset..self.block.restart_offset];
            const shared = try coding.getVarint32(&input);
            const non_shared = try coding.getVarint32(&input);
            const value_len = try coding.getVarint32(&input);
            // input now points just past the header; compute its absolute offset.
            const consumed = self.block.restart_offset - offset - input.len;
            const key_delta_offset = offset + consumed;
            // Bounds: key delta + value must fit before the restart array.
            const total = @as(usize, non_shared) + @as(usize, value_len);
            if (key_delta_offset + total > self.block.restart_offset) {
                return error.Corruption;
            }
            return .{
                .shared = shared,
                .non_shared = non_shared,
                .value_len = value_len,
                .key_delta_offset = key_delta_offset,
            };
        }

        /// Parse the entry at `offset`, reconstruct its full key into key_buf
        /// (reusing the `shared` prefix already present), and set value_slice.
        /// Advances next_offset past this entry. On error, sets self.err.
        fn parseEntryAt(self: *Iter, offset: usize) void {
            const h = self.decodeHeader(offset) catch |e| {
                self.err = e;
                self.current = invalidOffset(self.block);
                return;
            };
            // `shared` must not exceed the previously reconstructed key length.
            if (h.shared > self.key_buf.items.len) {
                self.err = error.Corruption;
                self.current = invalidOffset(self.block);
                return;
            }
            // key = key_buf[0..shared] ++ delta
            self.key_buf.shrinkRetainingCapacity(h.shared);
            const delta = self.block.data[h.key_delta_offset..][0..h.non_shared];
            self.key_buf.appendSlice(self.gpa, delta) catch {
                self.err = error.Corruption;
                self.current = invalidOffset(self.block);
                return;
            };
            const val_offset = h.key_delta_offset + h.non_shared;
            self.value_slice = self.block.data[val_offset..][0..h.value_len];
            self.current = offset;
            self.next_offset = val_offset + h.value_len;
        }

        /// Reset key reconstruction state to the start of restart point `index`
        /// and parse its first entry.
        fn seekToRestartPoint(self: *Iter, index: u32) void {
            self.key_buf.clearRetainingCapacity();
            self.restart_index = index;
            const off = self.block.restartPoint(index);
            self.parseEntryAt(off);
        }

        // -------------------------------------------------------------------
        // Positioning
        // -------------------------------------------------------------------

        pub fn seekToFirst(self: *Iter) void {
            self.err = null;
            if (self.block.num_restarts == 0 or self.block.restart_offset == 0) {
                // Empty block: nothing to iterate.
                self.current = invalidOffset(self.block);
                return;
            }
            self.seekToRestartPoint(0);
        }

        pub fn seekToLast(self: *Iter) void {
            self.err = null;
            if (self.block.num_restarts == 0 or self.block.restart_offset == 0) {
                self.current = invalidOffset(self.block);
                return;
            }
            // Position at the last restart point, then scan to the final entry.
            self.seekToRestartPoint(self.block.num_restarts - 1);
            while (self.err == null and self.next_offset < self.block.restart_offset) {
                self.parseEntryAt(self.next_offset);
            }
        }

        pub fn next(self: *Iter) void {
            std.debug.assert(self.valid());
            if (self.next_offset >= self.block.restart_offset) {
                // Past the last entry.
                self.current = invalidOffset(self.block);
                return;
            }
            // Advance restart_index if we crossed into the next restart region.
            const next_off = self.next_offset;
            while (self.restart_index + 1 < self.block.num_restarts and
                self.block.restartPoint(self.restart_index + 1) <= next_off)
            {
                self.restart_index += 1;
            }
            self.parseEntryAt(next_off);
        }

        /// Binary-search the restart array for the last restart whose first key
        /// is <= target, then linear-scan forward to the first entry whose key
        /// is >= target.
        pub fn seek(self: *Iter, target: []const u8) void {
            self.err = null;
            if (self.block.num_restarts == 0 or self.block.restart_offset == 0) {
                self.current = invalidOffset(self.block);
                return;
            }

            // Binary search: find the largest restart index whose first key < target.
            // Invariant: keys at restart[left].. are all < target after the loop is
            // resolved; we seek to `left` then linear-scan. Use LevelDB's variant:
            // find the last restart with key < target.
            var left: u32 = 0;
            var right: u32 = self.block.num_restarts - 1;
            while (left < right) {
                // Bias the midpoint upward so the loop makes progress.
                const mid = (left + right + 1) / 2;
                const region_key = self.firstKeyAtRestart(mid) catch {
                    // Corrupt: bail out invalid.
                    self.current = invalidOffset(self.block);
                    return;
                };
                if (self.cmp.compare(region_key, target) == .lt) {
                    // region_key < target -> answer is in [mid, right].
                    left = mid;
                } else {
                    // region_key >= target -> answer is in [left, mid-1].
                    right = mid - 1;
                }
            }

            // Linear scan from restart point `left`.
            self.seekToRestartPoint(left);
            while (self.err == null and self.valid()) {
                if (self.cmp.compare(self.key_buf.items, target) != .lt) {
                    // key >= target -> found.
                    return;
                }
                if (self.next_offset >= self.block.restart_offset) {
                    // Reached the end without finding key >= target.
                    self.current = invalidOffset(self.block);
                    return;
                }
                self.next();
            }
        }

        /// Reconstruct the first (full) key stored at restart point `index`.
        /// Restart points always store shared=0, so the delta is the full key.
        fn firstKeyAtRestart(self: *Iter, index: u32) Error![]const u8 {
            const off = self.block.restartPoint(index);
            const h = try self.decodeHeader(off);
            // A restart entry stores the full key (shared == 0).
            if (h.shared != 0) return error.Corruption;
            return self.block.data[h.key_delta_offset..][0..h.non_shared];
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

    const block = try Block.init(gpa, data);
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

    const block = try Block.init(gpa, data);
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

    const block = try Block.init(gpa, data);
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

    const block = try Block.init(gpa, data);
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

    const block = try Block.init(gpa, data);
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

    const block = try Block.init(gpa, data);
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

    const block = try Block.init(gpa, data);
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

    const block = try Block.init(gpa, data);
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
    try testing.expectError(error.Corruption, Block.init(std.testing.allocator, &.{ 0x00, 0x01 }));
}
