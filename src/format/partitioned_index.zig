/// partitioned_index.zig — zrocks's OWN clean two-level (partitioned) SST index.
///
/// SCOPE / FORMAT NOTE (read this first):
/// This is zrocks's OWN clean two-level index format.  It is NOT byte-compatible
/// with RocksDB's `format_version >= 4` partitioned index (which embeds delta-
/// encoded value sizes and a different top-level index value encoding tied to a
/// rework of the single-level index value format).  Making it RocksDB-byte-exact
/// would require an out-of-scope rewrite of the base single-level index value
/// encoding.  Following the same pragmatic choice already taken in this project
/// for the prefix filter, range-del block, and the CF manifest ("our own clean
/// format; TODO: RocksDB byte-exact later"), this milestone delivers a fully
/// self-consistent partitioned index that zrocks builds and reads itself.
/// TODO(wave-9.x): RocksDB-byte-exact partitioned index (fv4/fv5).
///
/// SHAPE
/// A normal single-level index is one flat Block whose entries are
///   (separator-key -> data-block BlockHandle).
/// The partitioned (two-level) index splits those entries across several INDEX
/// PARTITION blocks (each a normal index Block, same entry encoding as the flat
/// index), and adds a TOP-LEVEL index Block whose entries are
///   (last-key-of-partition -> that partition's BlockHandle).
/// The footer's `index_handle` then points at the TOP-LEVEL block.  A reader
/// performs a 3-level descent: top-index -> partition-index -> data block.
///
/// Each index partition block and the top-level block are written to the file
/// with the standard 5-byte CRC trailer, exactly like data/metaindex blocks
/// (kNoCompression).  The bytes inside every block are produced by the SAME
/// `block.BlockBuilder` used everywhere else, so partition/top-level blocks are
/// read with the ordinary `block.Block` iterator.
///
/// On-disk DETECTION: the table builder records the index type as a 1-byte meta
/// entry under `"rocksdb.index_type"` in the metaindex (0 = single_level,
/// 1 = two_level).  The reader reads it back; a missing entry means single_level
/// (so every pre-existing SST keeps reading as before).  This is zrocks's own
/// clean detection convention, NOT RocksDB's properties block.
const std = @import("std");

const block = @import("block.zig");
const footer_mod = @import("footer.zig");
const comparator = @import("../util/comparator.zig");

const BlockBuilder = block.BlockBuilder;
const Block = block.Block;
const BlockHandle = footer_mod.BlockHandle;

/// Metaindex key under which the index type is recorded (zrocks clean format).
pub const kIndexTypeMetaKey: []const u8 = "rocksdb.index_type";

/// 1-byte index-type tags stored in the `kIndexTypeMetaKey` meta entry.
pub const kSingleLevelTag: u8 = 0;
pub const kTwoLevelTag: u8 = 1;

/// One accumulated index entry: a separator key in [last_key_of_block, first_key
/// _of_next_block) and the encoded BlockHandle of the data block it covers.  The
/// builder owns both buffers until `deinit`.
const IndexEntry = struct {
    sep: []u8,
    handle_enc: []u8,
};

/// PartitionedIndexBuilder — accumulates the SST's index entries during the
/// build, then (at flush time) partitions them into per-partition index blocks
/// and a single top-level index block.
///
/// USAGE (driven by TableBuilder when `index_type == .two_level`):
///   1. `init(gpa, cmp, restart_interval, metadata_block_size)`.
///   2. For each data block, call `addEntry(sep_key, handle_encoding)` — exactly
///      where the single-level path would `index_block.add(sep, handle)`.
///   3. At table finish, call `finish(...)`:
///        - it groups the accumulated entries into partitions (a new partition is
///          started once the current partition's estimated block size would
///          exceed `metadata_block_size`),
///        - it asks the caller (via `write_partition`) to write each finished
///          partition block to the file and hand back its BlockHandle,
///        - it builds the TOP-LEVEL index block (last-key-of-partition -> handle)
///          and returns its finished bytes (caller writes it + points the footer
///          at it).
pub const PartitionedIndexBuilder = struct {
    gpa: std.mem.Allocator,
    cmp: comparator.Comparator,
    restart_interval: usize,
    metadata_block_size: usize,
    entries: std.ArrayListUnmanaged(IndexEntry),

    pub fn init(
        gpa: std.mem.Allocator,
        cmp: comparator.Comparator,
        restart_interval: usize,
        metadata_block_size: usize,
    ) PartitionedIndexBuilder {
        return .{
            .gpa = gpa,
            .cmp = cmp,
            .restart_interval = restart_interval,
            .metadata_block_size = metadata_block_size,
            .entries = .empty,
        };
    }

    pub fn deinit(self: *PartitionedIndexBuilder) void {
        for (self.entries.items) |e| {
            self.gpa.free(e.sep);
            self.gpa.free(e.handle_enc);
        }
        self.entries.deinit(self.gpa);
    }

    /// Record one index entry (separator -> data-block handle).  Copies both
    /// slices (the caller's buffers are reused), so the inputs need not outlive
    /// this call.
    pub fn addEntry(self: *PartitionedIndexBuilder, sep: []const u8, handle_enc: []const u8) !void {
        const sep_copy = try self.gpa.dupe(u8, sep);
        errdefer self.gpa.free(sep_copy);
        const handle_copy = try self.gpa.dupe(u8, handle_enc);
        errdefer self.gpa.free(handle_copy);
        try self.entries.append(self.gpa, .{ .sep = sep_copy, .handle_enc = handle_copy });
    }

    pub fn isEmpty(self: *const PartitionedIndexBuilder) bool {
        return self.entries.items.len == 0;
    }

    /// Signature of the caller-supplied callback that writes ONE finished index
    /// partition block to the file (with the standard CRC trailer) and returns
    /// its BlockHandle.  `ctx` is an opaque pointer to the caller (the
    /// TableBuilder), `contents` are the finished partition block bytes (valid
    /// only for the duration of the call — the builder resets the underlying
    /// BlockBuilder right after).
    pub const WritePartitionFn = *const fn (ctx: *anyopaque, contents: []const u8) anyerror!BlockHandle;

    /// Partition the accumulated entries, write each partition block via
    /// `write_partition`, build the TOP-LEVEL index block, and return its FINISHED
    /// bytes (owned by `out_top_level` BlockBuilder, which the caller must keep
    /// alive until it has written the bytes; the caller deinits it).
    ///
    /// Returns the number of partitions produced (always >= 1 when there is at
    /// least one entry).  `out_top_level` must be a freshly-init'd BlockBuilder
    /// (using the same comparator / a metaindex restart interval); on return it
    /// holds one entry per partition and is NOT yet finished — the caller calls
    /// `out_top_level.finish()` to obtain the top-level block bytes.
    pub fn finish(
        self: *PartitionedIndexBuilder,
        ctx: *anyopaque,
        write_partition: WritePartitionFn,
        out_top_level: *BlockBuilder,
    ) !usize {
        std.debug.assert(self.entries.items.len > 0);

        // A reusable builder for the CURRENT partition block.
        var part = BlockBuilder.init(self.gpa, self.cmp, self.restart_interval);
        defer part.deinit();

        var num_partitions: usize = 0;
        // The last separator added to the CURRENT partition (its "max key"), used
        // as the top-level index key for that partition.  Copied because `part`
        // is reset between partitions.
        var last_sep: std.ArrayListUnmanaged(u8) = .empty;
        defer last_sep.deinit(self.gpa);

        var i: usize = 0;
        while (i < self.entries.items.len) : (i += 1) {
            const e = self.entries.items[i];
            try part.add(e.sep, e.handle_enc);
            last_sep.clearRetainingCapacity();
            try last_sep.appendSlice(self.gpa, e.sep);

            // Close the partition once it has grown past the threshold (but never
            // an empty partition; `add` above guarantees at least one entry).  The
            // final partition is closed after the loop.
            const is_last = (i + 1 == self.entries.items.len);
            if (is_last or part.currentSizeEstimate() >= self.metadata_block_size) {
                num_partitions += 1;
                const handle = try self.flushPartition(&part, &last_sep, ctx, write_partition, out_top_level);
                _ = handle;
            }
        }

        return num_partitions;
    }

    /// Finish the current partition block, hand it to `write_partition`, add a
    /// top-level entry (last_sep -> partition handle), and reset `part`.
    fn flushPartition(
        self: *PartitionedIndexBuilder,
        part: *BlockBuilder,
        last_sep: *std.ArrayListUnmanaged(u8),
        ctx: *anyopaque,
        write_partition: WritePartitionFn,
        out_top_level: *BlockBuilder,
    ) !BlockHandle {
        const contents = part.finish();
        const handle = try write_partition(ctx, contents);
        part.reset();

        var handle_enc: std.ArrayListUnmanaged(u8) = .empty;
        defer handle_enc.deinit(self.gpa);
        try handle.encodeTo(&handle_enc, self.gpa);
        try out_top_level.add(last_sep.items, handle_enc.items);
        return handle;
    }
};

// ===========================================================================
// Tests — the partitioned index module in isolation (its own clean format).
// The TableBuilder/TableReader integration tests live in those modules.
// ===========================================================================

const testing = std.testing;

/// A tiny in-memory "file" the test write_partition callback appends to, so the
/// test can read partition blocks back by handle (offset into the buffer).
const FakeFile = struct {
    gpa: std.mem.Allocator,
    buf: std.ArrayListUnmanaged(u8) = .empty,

    fn deinit(self: *FakeFile) void {
        self.buf.deinit(self.gpa);
    }

    /// Append `contents` (no trailer here; the test only needs offset/size) and
    /// return a handle into the buffer.
    fn writePartition(ctx: *anyopaque, contents: []const u8) anyerror!BlockHandle {
        const self: *FakeFile = @ptrCast(@alignCast(ctx));
        const off = self.buf.items.len;
        try self.buf.appendSlice(self.gpa, contents);
        return .{ .offset = off, .size = contents.len };
    }
};

test "partitioned index: a small threshold forces MULTIPLE partitions" {
    const gpa = testing.allocator;

    var ff = FakeFile{ .gpa = gpa };
    defer ff.deinit();

    // restart_interval 1 + a tiny 128-byte threshold so a few dozen entries form
    // several partitions.
    var pib = PartitionedIndexBuilder.init(gpa, comparator.bytewise, 1, 128);
    defer pib.deinit();

    // 40 entries of separator -> handle.  Use a real BlockHandle encoding so the
    // partition size estimate is realistic.
    var sep_handles: std.ArrayListUnmanaged(BlockHandle) = .empty;
    defer sep_handles.deinit(gpa);
    var i: usize = 0;
    while (i < 40) : (i += 1) {
        const sep = try std.fmt.allocPrint(gpa, "sep{d:0>5}", .{i});
        defer gpa.free(sep);
        const h = BlockHandle{ .offset = i * 100, .size = 80 };
        try sep_handles.append(gpa, h);
        var enc: std.ArrayListUnmanaged(u8) = .empty;
        defer enc.deinit(gpa);
        try h.encodeTo(&enc, gpa);
        try pib.addEntry(sep, enc.items);
    }

    var top = BlockBuilder.init(gpa, comparator.bytewise, 1);
    defer top.deinit();

    const n = try pib.finish(@ptrCast(&ff), FakeFile.writePartition, &top);
    try testing.expect(n > 1); // MULTIPLE partitions

    // The top-level index has exactly `n` entries (one per partition).
    const top_bytes = try gpa.dupe(u8, top.finish());
    defer gpa.free(top_bytes);
    const top_block = try Block.init(gpa, top_bytes);
    var top_it = top_block.iterator(comparator.bytewise);
    defer top_it.deinit();
    var top_count: usize = 0;
    top_it.seekToFirst();
    while (top_it.valid()) : (top_it.next()) top_count += 1;
    try testing.expectEqual(n, top_count);
}

test "partitioned index: 3-level descent reconstructs every entry in order" {
    const gpa = testing.allocator;

    var ff = FakeFile{ .gpa = gpa };
    defer ff.deinit();

    var pib = PartitionedIndexBuilder.init(gpa, comparator.bytewise, 1, 96);
    defer pib.deinit();

    const N = 50;
    // Keep the expected (sep, handle) pairs to verify the round-trip.
    var exp_seps: std.ArrayListUnmanaged([]u8) = .empty;
    var exp_handles: std.ArrayListUnmanaged(BlockHandle) = .empty;
    defer {
        for (exp_seps.items) |s| gpa.free(s);
        exp_seps.deinit(gpa);
        exp_handles.deinit(gpa);
    }
    var i: usize = 0;
    while (i < N) : (i += 1) {
        const sep = try std.fmt.allocPrint(gpa, "k{d:0>4}", .{i});
        try exp_seps.append(gpa, sep);
        const h = BlockHandle{ .offset = i * 17 + 3, .size = i + 1 };
        try exp_handles.append(gpa, h);
        var enc: std.ArrayListUnmanaged(u8) = .empty;
        defer enc.deinit(gpa);
        try h.encodeTo(&enc, gpa);
        try pib.addEntry(sep, enc.items);
    }

    var top = BlockBuilder.init(gpa, comparator.bytewise, 1);
    defer top.deinit();
    const n = try pib.finish(@ptrCast(&ff), FakeFile.writePartition, &top);
    try testing.expect(n > 1);

    const top_bytes = try gpa.dupe(u8, top.finish());
    defer gpa.free(top_bytes);
    const top_block = try Block.init(gpa, top_bytes);

    // Walk the top-level index; for each partition handle, parse that partition
    // block (from the fake file buffer) and collect its (sep, handle) entries.
    var got_seps: std.ArrayListUnmanaged([]u8) = .empty;
    var got_handles: std.ArrayListUnmanaged(BlockHandle) = .empty;
    defer {
        for (got_seps.items) |s| gpa.free(s);
        got_seps.deinit(gpa);
        got_handles.deinit(gpa);
    }

    var top_it = top_block.iterator(comparator.bytewise);
    defer top_it.deinit();
    top_it.seekToFirst();
    while (top_it.valid()) : (top_it.next()) {
        var hv: []const u8 = top_it.value();
        const ph = try BlockHandle.decodeFrom(&hv);
        const part_bytes = ff.buf.items[@intCast(ph.offset)..][0..@intCast(ph.size)];
        const part_block = try Block.init(gpa, part_bytes);
        var part_it = part_block.iterator(comparator.bytewise);
        defer part_it.deinit();
        part_it.seekToFirst();
        while (part_it.valid()) : (part_it.next()) {
            try got_seps.append(gpa, try gpa.dupe(u8, part_it.key()));
            var dv: []const u8 = part_it.value();
            try got_handles.append(gpa, try BlockHandle.decodeFrom(&dv));
        }
    }

    // Every entry recovered, in order, with the exact handle.
    try testing.expectEqual(@as(usize, N), got_seps.items.len);
    for (exp_seps.items, exp_handles.items, got_seps.items, got_handles.items) |es, eh, gs, gh| {
        try testing.expectEqualStrings(es, gs);
        try testing.expectEqual(eh.offset, gh.offset);
        try testing.expectEqual(eh.size, gh.size);
    }
}

test "partitioned index: single entry yields exactly one partition" {
    const gpa = testing.allocator;
    var ff = FakeFile{ .gpa = gpa };
    defer ff.deinit();

    var pib = PartitionedIndexBuilder.init(gpa, comparator.bytewise, 1, 4096);
    defer pib.deinit();

    const h = BlockHandle{ .offset = 0, .size = 10 };
    var enc: std.ArrayListUnmanaged(u8) = .empty;
    defer enc.deinit(gpa);
    try h.encodeTo(&enc, gpa);
    try pib.addEntry("only", enc.items);

    var top = BlockBuilder.init(gpa, comparator.bytewise, 1);
    defer top.deinit();
    const n = try pib.finish(@ptrCast(&ff), FakeFile.writePartition, &top);
    try testing.expectEqual(@as(usize, 1), n);
}
