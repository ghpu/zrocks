/// table_builder.zig — LevelDB/RocksDB block-based SST table builder.
///
/// Writes a complete block-based table file, byte-compatible with the
/// LevelDB/RocksDB block-based table layout (kNoCompression, format_version 5
/// footer). Mirrors LevelDB's `table/table_builder.cc`.
///
/// File layout (write order):
///   [data block]*           one BlockBuilder per ~block_size run of entries
///   [filter block]          one block-based bloom filter block (LevelDB layout)
///   [metaindex block]       single entry: "filter."++policy.name() -> filter handle
///   [index block]           one entry per data block (separator key -> data handle)
///   [footer]                53 bytes (fv=5, crc32c), no trailer
///
/// Every block (data/filter/metaindex/index) is written via writeRawBlock:
/// the block contents, then a 5-byte trailer = [compression_type] ++
/// fixed32_LE(mask(extend(value(contents), &[compression_type]))).
///
/// M7.2 prefix filter (mode): when `options.prefix_extractor` is set, the
/// SST's filter block is built over key PREFIXES rather than whole keys — for
/// each added internal key we extract the user key and, if it is in the
/// extractor's domain, add `transform(user_key)` to the filter; out-of-domain
/// keys are not added.  When no prefix extractor is set we keep the original
/// whole-(internal-)key filter.  This is a single-mode choice (prefix XOR
/// whole-key) rather than RocksDB's combined `whole_key_filtering` + prefix
/// filter; the reader's pruning matches whichever mode the table was built in
/// (driven by the same `options.prefix_extractor`).
/// TODO(m7.x): RocksDB's exact prefix-filter on-block layout differs; this is
/// our own clean prefix filter over the existing block-based layout.
const std = @import("std");

const block = @import("block.zig");
const partitioned_index = @import("partitioned_index.zig");
const filter_block = @import("filter_block.zig");
const full_filter = @import("full_filter.zig");
const bloom = @import("bloom.zig");
const internal_key = @import("internal_key.zig");
const delete_range = @import("../rocks/delete_range.zig");
const footer_mod = @import("footer.zig");
const crc32c = @import("../util/crc32c.zig");
const coding = @import("../util/coding.zig");
const comparator = @import("../util/comparator.zig");
const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const snappy = @import("../util/snappy.zig");

const BlockBuilder = block.BlockBuilder;
const BlockHandle = footer_mod.BlockHandle;
const Footer = footer_mod.Footer;
const FilterBlockBuilder = filter_block.FilterBlockBuilder;

/// kNoCompression — block stored verbatim (compression type byte 0).
pub const kNoCompression: u8 = 0;
/// kSnappyCompression — block stored as a Snappy block-format payload (byte 1).
pub const kSnappyCompression: u8 = 1;

/// Restart interval used for the index and metaindex blocks (LevelDB uses 1).
const kMetaIndexRestartInterval: usize = 1;

/// Prefix of the metaindex key naming the table's (legacy block-based) filter.
const kFilterMetaKeyPrefix: []const u8 = "filter.";
/// Prefix of the metaindex key naming the table's FastLocalBloom full filter
/// (fulllocalbloom).  Distinct from `kFilterMetaKeyPrefix` so the two formats
/// never collide on disk and an old reader cannot mistake a full filter for a
/// block-based one (it simply finds no "filter." entry).
const kFullFilterMetaKeyPrefix: []const u8 = "fullfilter.";

/// Metaindex key naming the table's range-del block (M7.5).  Our own clean
/// format (see delete_range.zig), NOT RocksDB byte-compatible.
/// TODO(m7.x): RocksDB stores fragmented range tombstones in a dedicated
/// "rocksdb.deletion_data" / range_del block with a different layout.
const kRangeDelMetaKey: []const u8 = "rocksdb.range_del";

/// Metaindex key recording the SST's index shape (partitioned-idx), in zrocks's
/// own clean format: a 1-byte tag (0=single_level, 1=two_level).  Written only
/// for two-level tables; a missing entry means single_level so every existing
/// SST keeps reading unchanged.  See partitioned_index.zig.
const kIndexTypeMetaKey: []const u8 = partitioned_index.kIndexTypeMetaKey;

pub const TableBuilder = struct {
    gpa: std.mem.Allocator,
    options: options_mod.Options,
    file: env.WritableFile,
    policy: bloom.BloomFilterPolicy,

    /// Builder for the current data block (entries flushed at ~block_size).
    data_block: BlockBuilder,
    /// Builder for the index block (one entry per flushed data block).  Used
    /// directly as the flat index under `.single_level`; under `.two_level` it is
    /// the reusable builder for the TOP-LEVEL index block (see `index_partitions`).
    index_block: BlockBuilder,
    /// Partitioned (two-level) index accumulator (partitioned-idx).  Non-null only
    /// when `options.index_type == .two_level`: index entries are routed here
    /// during the build, then partitioned + written at `finish`.  zrocks's OWN
    /// clean two-level format (see partitioned_index.zig), NOT RocksDB byte-exact.
    index_partitions: ?partitioned_index.PartitionedIndexBuilder,
    /// Block-based bloom filter builder (one filter per 2KB data range).
    /// Used only when `options.filter_mode == .block_based` (the default).
    filter: FilterBlockBuilder,
    /// FastLocalBloom full-filter builder (one filter over EVERY key).
    /// Used only when `options.filter_mode == .full` (fulllocalbloom).
    full_filter: full_filter.FullFilterBuilder,

    /// The most recently added key (used for separators and the sorted assert).
    last_key: std.ArrayListUnmanaged(u8),
    /// Running file offset (bytes written so far).
    offset: u64,
    /// Number of (key,value) entries added.
    num_entries: u64,

    /// Whether a data block has been flushed whose index entry is not yet
    /// emitted. The index entry is deferred until the first key of the NEXT
    /// data block is known (so a short separator can be chosen).
    pending_index_entry: bool,
    /// Handle of the just-flushed data block awaiting an index entry.
    pending_handle: BlockHandle,

    /// True once finish() has run.
    finished: bool,

    /// Scratch buffer reused for block handle encodings (index/metaindex).
    handle_encoding: std.ArrayListUnmanaged(u8),

    /// Range tombstones to embed in this SST's range-del meta block (M7.5).
    /// Added via `addRangeTombstone` before `finish`; serialized into a dedicated
    /// meta block registered under `kRangeDelMetaKey`.
    range_tombstones: delete_range.RangeTombstoneList,

    pub fn init(
        gpa: std.mem.Allocator,
        options: options_mod.Options,
        file: env.WritableFile,
        policy: bloom.BloomFilterPolicy,
    ) !TableBuilder {
        var filter = FilterBlockBuilder.init(gpa, policy);
        // LevelDB starts the first filter range at offset 0.
        try filter.startBlock(gpa, 0);
        return .{
            .gpa = gpa,
            .options = options,
            .file = file,
            .policy = policy,
            // Data + index blocks hold keys ordered by the table's comparator
            // (an InternalKeyComparator for DB SSTs), so build them with it.
            .data_block = BlockBuilder.init(gpa, options.comparator, options.block_restart_interval),
            .index_block = BlockBuilder.init(gpa, options.comparator, kMetaIndexRestartInterval),
            .index_partitions = switch (options.index_type) {
                .single_level => null,
                .two_level => partitioned_index.PartitionedIndexBuilder.init(
                    gpa,
                    options.comparator,
                    kMetaIndexRestartInterval,
                    options.metadata_block_size,
                ),
            },
            .filter = filter,
            .full_filter = full_filter.FullFilterBuilder.init(policy.bits_per_key),
            .last_key = .empty,
            .offset = 0,
            .num_entries = 0,
            .pending_index_entry = false,
            .pending_handle = .{ .offset = 0, .size = 0 },
            .finished = false,
            .handle_encoding = .empty,
            .range_tombstones = delete_range.RangeTombstoneList.init(gpa),
        };
    }

    pub fn deinit(self: *TableBuilder) void {
        self.data_block.deinit();
        self.index_block.deinit();
        if (self.index_partitions) |*pib| pib.deinit();
        self.filter.deinit(self.gpa);
        self.full_filter.deinit(self.gpa);
        self.last_key.deinit(self.gpa);
        self.handle_encoding.deinit(self.gpa);
        self.range_tombstones.deinit();
        self.* = undefined;
    }

    /// Record a range tombstone `[begin, end)` @ `seq` to embed in this SST's
    /// range-del meta block (M7.5).  Must be called before `finish`.
    pub fn addRangeTombstone(self: *TableBuilder, begin: []const u8, end: []const u8, seq: u64) !void {
        std.debug.assert(!self.finished);
        try self.range_tombstones.add(begin, end, seq);
    }

    pub fn numEntries(self: *const TableBuilder) u64 {
        return self.num_entries;
    }

    pub fn fileSize(self: *const TableBuilder) u64 {
        return self.offset;
    }

    /// Add (key, value). Keys MUST arrive in non-decreasing sorted order.
    pub fn add(self: *TableBuilder, key: []const u8, value: []const u8) !void {
        std.debug.assert(!self.finished);
        // Sorted-order invariant (non-decreasing) against the previous key.
        std.debug.assert(self.num_entries == 0 or
            self.options.comparator.compare(self.last_key.items, key) != .gt);

        // If a data block was just flushed, emit its deferred index entry now
        // that we know the first key of the next block.
        if (self.pending_index_entry) {
            std.debug.assert(self.data_block.isEmpty());
            // separator in [last_key, key); shortens last_key in place.
            self.options.comparator.findShortestSeparator(&self.last_key, key);
            try self.appendIndexEntry();
        }

        // Record the key in the filter.  M7.2: when a prefix_extractor is
        // configured, the filter is built over key PREFIXES — extract the user
        // key from the internal key and, if it is in the extractor's domain, add
        // its prefix; out-of-domain keys are simply not added (so they cannot be
        // pruned, and the reader must never prune them either).  Without a prefix
        // extractor the filter is built over the whole (internal) key as before.
        if (self.options.prefix_extractor) |pe| {
            const user_key = internal_key.extractUserKey(key);
            if (pe.inDomain(user_key)) {
                try self.addFilterKey(pe.transform(user_key));
            }
        } else {
            try self.addFilterKey(key);
        }

        // Remember last_key = key.
        self.last_key.clearRetainingCapacity();
        try self.last_key.appendSlice(self.gpa, key);

        self.num_entries += 1;
        try self.data_block.add(key, value);

        if (self.data_block.currentSizeEstimate() >= self.options.block_size) {
            try self.flush();
        }
    }

    /// Route a filter key to the active filter format (fulllocalbloom gate).
    /// Block-based mode accumulates per-2KB-range; full mode accumulates every
    /// key into a single FastLocalBloom filter.
    fn addFilterKey(self: *TableBuilder, key: []const u8) !void {
        switch (self.options.filter_mode) {
            .block_based => try self.filter.addKey(self.gpa, key),
            .full => try self.full_filter.addKey(self.gpa, key),
        }
    }

    /// Emit the deferred index entry for the just-flushed data block:
    /// key = `last_key` (already narrowed to a short separator/successor by the
    /// caller), value = the pending data block's encoded BlockHandle. Clears
    /// `pending_index_entry`.
    fn appendIndexEntry(self: *TableBuilder) !void {
        std.debug.assert(self.pending_index_entry);
        self.handle_encoding.clearRetainingCapacity();
        try self.pending_handle.encodeTo(&self.handle_encoding, self.gpa);
        // Single-level: add straight to the flat index block.  Two-level: route to
        // the partitioned-index accumulator (partitioned-idx); the partition/top
        // blocks are produced at `finish`.
        if (self.index_partitions) |*pib| {
            try pib.addEntry(self.last_key.items, self.handle_encoding.items);
        } else {
            try self.index_block.add(self.last_key.items, self.handle_encoding.items);
        }
        self.pending_index_entry = false;
    }

    /// Flush the current data block to the file and arm a deferred index entry.
    fn flush(self: *TableBuilder) !void {
        std.debug.assert(!self.finished);
        if (self.data_block.isEmpty()) return;
        std.debug.assert(!self.pending_index_entry);

        self.pending_handle = try self.writeDataBlock(&self.data_block);
        self.pending_index_entry = true;
        try self.file.flush();

        // Begin the next filter range at the new file offset (block-based only;
        // the full filter is offset-agnostic — one filter over all keys).
        if (self.options.filter_mode == .block_based) {
            try self.filter.startBlock(self.gpa, self.offset);
        }
    }

    /// Finish a BlockBuilder, write it UNCOMPRESSED (with trailer) to the file,
    /// reset the builder, and return its BlockHandle.  Used for the
    /// index/metaindex blocks, which are always stored verbatim.
    fn writeBlock(self: *TableBuilder, builder: *BlockBuilder) !BlockHandle {
        const contents = builder.finish();
        const handle = try self.writeRawBlock(contents, kNoCompression);
        builder.reset();
        return handle;
    }

    /// Partition the accumulated index entries (partitioned-idx), write each
    /// index PARTITION block (uncompressed, with the standard CRC trailer) to the
    /// file, build the TOP-LEVEL index block, write it the same way, and return
    /// its BlockHandle (the footer's `index_handle`).  Only called in two-level
    /// mode (`index_partitions != null`).
    fn writePartitionedIndex(self: *TableBuilder) !BlockHandle {
        const pib = &self.index_partitions.?;
        std.debug.assert(!pib.isEmpty());

        // The TOP-LEVEL index block reuses `self.index_block` (already a fresh
        // BlockBuilder with the table comparator + meta restart interval).
        const num_parts = try pib.finish(
            @ptrCast(self),
            writePartitionCb,
            &self.index_block,
        );
        std.debug.assert(num_parts >= 1);

        // Write the finished top-level block (its bytes are produced by `finish`
        // on `self.index_block`; `writeBlock` finishes+writes+resets it).
        return try self.writeBlock(&self.index_block);
    }

    /// `PartitionedIndexBuilder.WritePartitionFn` adapter: writes one finished
    /// index partition block to the file (uncompressed, with CRC trailer) and
    /// returns its BlockHandle.  `ctx` is the `*TableBuilder`.
    fn writePartitionCb(ctx: *anyopaque, contents: []const u8) anyerror!BlockHandle {
        const self: *TableBuilder = @ptrCast(@alignCast(ctx));
        return self.writeRawBlock(contents, kNoCompression);
    }

    /// Finish a data block, optionally Snappy-compress it (when
    /// `options.compression == .snappy`), write it (with trailer) to the file,
    /// reset the builder, and return its BlockHandle.
    ///
    /// Compression is applied only when it actually shrinks the block; otherwise
    /// the block is stored verbatim with `kNoCompression` (mirrors RocksDB's
    /// "GoodCompressionRatio" guard so a non-compressible block never grows).
    /// The on-disk (post-compression) payload is what the trailer CRC covers and
    /// what `BlockHandle.size` records, so the reader sizes its read off it.
    fn writeDataBlock(self: *TableBuilder, builder: *BlockBuilder) !BlockHandle {
        const contents = builder.finish();

        if (self.options.compression == .snappy) {
            const compressed = try snappy.compress(self.gpa, contents);
            defer self.gpa.free(compressed);
            // Only keep the compressed form if it is actually smaller.
            if (compressed.len < contents.len) {
                const handle = try self.writeRawBlock(compressed, kSnappyCompression);
                builder.reset();
                return handle;
            }
        }

        const handle = try self.writeRawBlock(contents, kNoCompression);
        builder.reset();
        return handle;
    }

    /// Append block contents + 5-byte trailer to the file at the running
    /// offset, returning a handle whose size is the contents length (the
    /// trailer is NOT part of the handle size). Advances offset.
    fn writeRawBlock(self: *TableBuilder, contents: []const u8, compression_type: u8) !BlockHandle {
        const handle = BlockHandle{ .offset = self.offset, .size = contents.len };

        try self.file.append(contents);

        // trailer[0] = compression type
        // trailer[1..5] = fixed32_LE(mask(extend(crc(contents), &[type])))
        var trailer: [5]u8 = undefined;
        trailer[0] = compression_type;
        const crc = crc32c.extend(crc32c.value(contents), &[_]u8{compression_type});
        coding.encodeFixed32(trailer[1..5], crc32c.mask(crc));
        try self.file.append(&trailer);

        self.offset += contents.len + trailer.len;
        return handle;
    }

    /// Flush the remaining data, write the filter/metaindex/index blocks and
    /// the footer, then flush the file. Does NOT close the file (caller owns).
    pub fn finish(self: *TableBuilder) !void {
        std.debug.assert(!self.finished);
        try self.flush();
        self.finished = true;

        // 1. Filter block.  The active format (fulllocalbloom gate) decides both
        //    the on-disk filter layout and the metaindex key it is registered
        //    under — block-based under "filter."++name, full under
        //    "fullfilter."++name — so the two never collide on disk.
        const filter_contents = switch (self.options.filter_mode) {
            .block_based => try self.filter.finish(self.gpa),
            .full => try self.full_filter.finish(self.gpa),
        };
        const filter_handle = try self.writeRawBlock(filter_contents, kNoCompression);

        // 1b. Range-del block (M7.5): a serialized RangeTombstoneList.  Written
        //     only when the table carries tombstones (a table without them has no
        //     range-del entry in the metaindex, and the reader treats its absence
        //     as "no tombstones").
        var range_del_handle: ?BlockHandle = null;
        if (!self.range_tombstones.isEmpty()) {
            var rd_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer rd_buf.deinit(self.gpa);
            try self.range_tombstones.encode(&rd_buf, self.gpa);
            range_del_handle = try self.writeRawBlock(rd_buf.items, kNoCompression);
        }

        // 2. Metaindex block: "filter."++name -> filter handle, and (when present)
        //    "rocksdb.range_del" -> range-del handle.  Its keys are plain bytewise
        //    meta keys (NOT internal keys), ordered/searched with the bytewise
        //    comparator (matching the reader), so entries MUST be added in
        //    ascending bytewise key order: "filter." < "rocksdb.range_del".
        var metaindex_block = BlockBuilder.init(self.gpa, comparator.bytewise, kMetaIndexRestartInterval);
        defer metaindex_block.deinit();
        {
            var key_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer key_buf.deinit(self.gpa);
            const meta_prefix = switch (self.options.filter_mode) {
                .block_based => kFilterMetaKeyPrefix,
                .full => kFullFilterMetaKeyPrefix,
            };
            try key_buf.appendSlice(self.gpa, meta_prefix);
            try key_buf.appendSlice(self.gpa, self.policy.name());

            self.handle_encoding.clearRetainingCapacity();
            try filter_handle.encodeTo(&self.handle_encoding, self.gpa);
            try metaindex_block.add(key_buf.items, self.handle_encoding.items);
        }
        // Index-type tag (partitioned-idx): record a 1-byte tag ONLY for
        // two-level tables so the reader can auto-detect the index shape.  Added
        // before "rocksdb.range_del" to keep the metaindex bytewise-sorted
        // ("rocksdb.index_type" < "rocksdb.range_del").  A single-level table
        // writes no such entry (its absence means single_level).
        if (self.index_partitions != null) {
            try metaindex_block.add(kIndexTypeMetaKey, &[_]u8{partitioned_index.kTwoLevelTag});
        }
        if (range_del_handle) |rdh| {
            self.handle_encoding.clearRetainingCapacity();
            try rdh.encodeTo(&self.handle_encoding, self.gpa);
            try metaindex_block.add(kRangeDelMetaKey, self.handle_encoding.items);
        }
        const metaindex_handle = try self.writeBlock(&metaindex_block);

        // 3. Final pending index entry (use a short successor of the last key).
        if (self.pending_index_entry) {
            self.options.comparator.findShortSuccessor(&self.last_key);
            try self.appendIndexEntry();
        }

        // 4. Index block.  Single-level: write the one flat index block.
        //    Two-level (partitioned-idx): partition the accumulated index entries,
        //    write each partition block, build the TOP-LEVEL index block, and
        //    point the footer at the top-level block.
        const index_handle = if (self.index_partitions != null)
            try self.writePartitionedIndex()
        else
            try self.writeBlock(&self.index_block);

        // 5. Footer (no trailer).
        const footer = Footer{
            .metaindex_handle = metaindex_handle,
            .index_handle = index_handle,
            .format_version = 5,
            .checksum_type = .crc32c,
        };
        var footer_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer footer_buf.deinit(self.gpa);
        try footer.encodeTo(&footer_buf, self.gpa);
        try self.file.append(footer_buf.items);
        self.offset += footer_buf.items.len;

        try self.file.flush();
    }
};

// ===========================================================================
// Tests — byte-compat gate. Build a multi-data-block table, then parse it back
// with the existing footer/block/filter modules (a mini-reader) and verify.
// ===========================================================================

const testing = std.testing;
const FilterBlockReader = filter_block.FilterBlockReader;

const KV = struct { k: []const u8, v: []const u8 };

/// Read the entire contents of `path` out of an Env into a freshly allocated
/// buffer (caller frees). Uses positional reads via a RandomAccessFile.
fn readAllFile(e: env.Env, gpa: std.mem.Allocator, path: []const u8) ![]u8 {
    const size = try e.getFileSize(path);
    const buf = try gpa.alloc(u8, @intCast(size));
    errdefer gpa.free(buf);
    var raf = try e.newRandomAccessFile(gpa, path);
    defer raf.close() catch {};
    var total: usize = 0;
    while (total < buf.len) {
        const n = try raf.readAt(total, buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try testing.expectEqual(buf.len, total);
    return buf;
}

/// Slice an UNCOMPRESSED block's on-disk payload out of the file bytes given its
/// handle, validate the 5-byte trailer CRC, assert it is stored verbatim, and
/// return the contents slice (into `file`).  Use this for blocks the builder
/// never compresses (index/metaindex/filter/range-del).
fn readVerifiedBlock(file: []const u8, handle: BlockHandle) ![]const u8 {
    const start: usize = @intCast(handle.offset);
    const size: usize = @intCast(handle.size);
    try testing.expect(start + size + 5 <= file.len);
    const payload = file[start .. start + size];

    const trailer = file[start + size .. start + size + 5];
    const compression_type = trailer[0];
    try testing.expectEqual(@as(u8, kNoCompression), compression_type);
    const stored_masked = coding.decodeFixed32(trailer[1..5]);
    const expected = crc32c.extend(crc32c.value(payload), &[_]u8{compression_type});
    try testing.expectEqual(expected, crc32c.unmask(stored_masked));
    return payload;
}

/// Compression-aware verifier: slice a block's ON-DISK payload (whose length is
/// `handle.size`) out of `file`, validate the 5-byte trailer CRC over the
/// payload (the trailer CRC always covers the on-disk/compressed bytes), then
/// decompress it when the trailer marks it Snappy.  Returns an OWNED buffer the
/// caller frees with `gpa` (so an uncompressed block is duped too, for a uniform
/// ownership contract).
fn readVerifiedBlockC(gpa: std.mem.Allocator, file: []const u8, handle: BlockHandle) ![]u8 {
    const start: usize = @intCast(handle.offset);
    const size: usize = @intCast(handle.size);
    try testing.expect(start + size + 5 <= file.len);
    const payload = file[start .. start + size];

    const trailer = file[start + size .. start + size + 5];
    const compression_type = trailer[0];
    const stored_masked = coding.decodeFixed32(trailer[1..5]);
    const expected = crc32c.extend(crc32c.value(payload), &[_]u8{compression_type});
    try testing.expectEqual(expected, crc32c.unmask(stored_masked));

    return switch (compression_type) {
        kNoCompression => try gpa.dupe(u8, payload),
        kSnappyCompression => try snappy.decompress(gpa, payload),
        else => error.NotSupported,
    };
}

/// Build a sorted set of ~`n` entries with small keys/values.
fn makeSortedEntries(gpa: std.mem.Allocator, n: usize) !std.ArrayListUnmanaged(KV) {
    var list: std.ArrayListUnmanaged(KV) = .empty;
    errdefer {
        for (list.items) |kv| {
            gpa.free(kv.k);
            gpa.free(kv.v);
        }
        list.deinit(gpa);
    }
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const k = try std.fmt.allocPrint(gpa, "key{d:0>5}", .{i});
        const v = try std.fmt.allocPrint(gpa, "value-{d:0>5}-payload", .{i});
        try list.append(gpa, .{ .k = k, .v = v });
    }
    return list;
}

fn freeEntries(gpa: std.mem.Allocator, list: *std.ArrayListUnmanaged(KV)) void {
    for (list.items) |kv| {
        gpa.free(kv.k);
        gpa.free(kv.v);
    }
    list.deinit(gpa);
}

test "table builder: multi-block round-trip, trailer CRCs, filter matches" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Small block_size so ~50 entries form several data blocks.
    const opts = options_mod.Options{
        .block_size = 200,
        .block_restart_interval = 4,
    };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 50);
    defer freeEntries(gpa, &entries);

    // ---- Build the table -------------------------------------------------
    {
        var wf = try e.newWritableFile(gpa, "test.sst");
        errdefer wf.close() catch {};

        var tb = try TableBuilder.init(gpa, opts, wf, policy);
        defer tb.deinit();

        for (entries.items) |kv| {
            try tb.add(kv.k, kv.v);
        }
        try tb.finish();

        try testing.expectEqual(@as(u64, entries.items.len), tb.numEntries());
        try testing.expect(tb.fileSize() > 0);

        try wf.close();
    }

    // ---- Read the file back ---------------------------------------------
    const file = try readAllFile(e, gpa, "test.sst");
    defer gpa.free(file);

    // ---- Footer ----------------------------------------------------------
    try testing.expect(file.len >= footer_mod.kEncodedLength);
    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);
    try testing.expectEqual(@as(u32, 5), footer.format_version);
    try testing.expectEqual(footer_mod.ChecksumType.crc32c, footer.checksum_type);

    // ---- Index block -----------------------------------------------------
    const index_contents = try readVerifiedBlock(file, footer.index_handle);
    const index_block = try block.Block.init(gpa, index_contents);

    // Collect data-block handles from the index entries.
    var data_handles: std.ArrayListUnmanaged(BlockHandle) = .empty;
    defer data_handles.deinit(gpa);
    {
        var it = index_block.iterator(opts.comparator);
        defer it.deinit();
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            var hv: []const u8 = it.value();
            const h = try BlockHandle.decodeFrom(&hv);
            try data_handles.append(gpa, h);
        }
    }

    // Multiple data blocks must have been produced.
    try testing.expect(data_handles.items.len >= 2);

    // ---- Data blocks: concatenated entries reproduce the input exactly ---
    {
        var idx: usize = 0;
        for (data_handles.items) |h| {
            const data_contents = try readVerifiedBlock(file, h);
            const data_blk = try block.Block.init(gpa, data_contents);
            var it = data_blk.iterator(opts.comparator);
            defer it.deinit();
            it.seekToFirst();
            while (it.valid()) : (it.next()) {
                try testing.expect(idx < entries.items.len);
                try testing.expectEqualStrings(entries.items[idx].k, it.key());
                try testing.expectEqualStrings(entries.items[idx].v, it.value());
                idx += 1;
            }
        }
        // Every input entry was reproduced, in order.
        try testing.expectEqual(entries.items.len, idx);
    }

    // ---- Metaindex block: find the filter entry, read+verify filter ------
    {
        const meta_contents = try readVerifiedBlock(file, footer.metaindex_handle);
        const meta_blk = try block.Block.init(gpa, meta_contents);

        // Expected key: "filter." ++ policy.name().
        var want_key: std.ArrayListUnmanaged(u8) = .empty;
        defer want_key.deinit(gpa);
        try want_key.appendSlice(gpa, "filter.");
        try want_key.appendSlice(gpa, policy.name());
        try testing.expectEqualStrings("filter.leveldb.BuiltinBloomFilter2", want_key.items);

        var it = meta_blk.iterator(opts.comparator);
        defer it.deinit();
        it.seek(want_key.items);
        try testing.expect(it.valid());
        try testing.expectEqualStrings(want_key.items, it.key());

        var hv: []const u8 = it.value();
        const filter_handle = try BlockHandle.decodeFrom(&hv);

        const filter_contents = try readVerifiedBlock(file, filter_handle);
        var fr = FilterBlockReader.init(policy, filter_contents);
        // Several input keys should be reported as possibly-present.
        try testing.expect(fr.keyMayMatch(0, entries.items[0].k));
        try testing.expect(fr.keyMayMatch(0, entries.items[1].k));
        try testing.expect(fr.keyMayMatch(0, entries.items[2].k));
    }
}

test "table builder: snappy data blocks — kSnappy trailer, CRC over compressed, handle = compressed size, decompresses to entries" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Compression ON; small block_size so several data blocks form.
    const opts = options_mod.Options{
        .block_size = 256,
        .block_restart_interval = 4,
        .compression = .snappy,
    };
    const policy = bloom.BloomFilterPolicy.init(10);

    // Highly compressible values so each data block actually shrinks.
    var entries: std.ArrayListUnmanaged(KV) = .empty;
    defer freeEntries(gpa, &entries);
    {
        var i: usize = 0;
        while (i < 60) : (i += 1) {
            const k = try std.fmt.allocPrint(gpa, "key{d:0>5}", .{i});
            // Long run of repeated bytes → very compressible.
            const v = try std.fmt.allocPrint(gpa, "{s}", .{"A" ** 80});
            try entries.append(gpa, .{ .k = k, .v = v });
        }
    }

    {
        var wf = try e.newWritableFile(gpa, "snap.sst");
        errdefer wf.close() catch {};
        var tb = try TableBuilder.init(gpa, opts, wf, policy);
        defer tb.deinit();
        for (entries.items) |kv| try tb.add(kv.k, kv.v);
        try tb.finish();
        try wf.close();
    }

    const file = try readAllFile(e, gpa, "snap.sst");
    defer gpa.free(file);

    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);

    // Index/metaindex/filter remain uncompressed (verbatim).
    const index_contents = try readVerifiedBlock(file, footer.index_handle);
    const index_block = try block.Block.init(gpa, index_contents);

    var data_handles: std.ArrayListUnmanaged(BlockHandle) = .empty;
    defer data_handles.deinit(gpa);
    {
        var it = index_block.iterator(opts.comparator);
        defer it.deinit();
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            var hv: []const u8 = it.value();
            const h = try BlockHandle.decodeFrom(&hv);
            try data_handles.append(gpa, h);
        }
    }
    try testing.expect(data_handles.items.len >= 2);

    // Each data block on disk must carry the kSnappy trailer byte, its CRC must
    // cover the COMPRESSED bytes, handle.size must equal the compressed length,
    // and decompressing must reproduce the original entries in order.
    var any_snappy = false;
    var idx: usize = 0;
    for (data_handles.items) |h| {
        const start: usize = @intCast(h.offset);
        const size: usize = @intCast(h.size);
        const trailer_type = file[start + size];
        if (trailer_type == kSnappyCompression) {
            any_snappy = true;
            // handle.size is the compressed payload length: decompressing it must
            // yield MORE bytes than the on-disk size for this compressible data.
            const decompressed = try snappy.decompress(gpa, file[start .. start + size]);
            gpa.free(decompressed);
        }

        const data_contents = try readVerifiedBlockC(gpa, file, h);
        defer gpa.free(data_contents);
        const data_blk = try block.Block.init(gpa, data_contents);
        var it = data_blk.iterator(opts.comparator);
        defer it.deinit();
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            try testing.expect(idx < entries.items.len);
            try testing.expectEqualStrings(entries.items[idx].k, it.key());
            try testing.expectEqualStrings(entries.items[idx].v, it.value());
            idx += 1;
        }
    }
    try testing.expectEqual(entries.items.len, idx);
    // At least one data block was actually stored Snappy-compressed.
    try testing.expect(any_snappy);
}

test "table builder: single small block still well-formed" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{};
    const policy = bloom.BloomFilterPolicy.init(10);

    const pairs = [_]KV{
        .{ .k = "alpha", .v = "1" },
        .{ .k = "beta", .v = "2" },
        .{ .k = "gamma", .v = "3" },
    };

    {
        var wf = try e.newWritableFile(gpa, "small.sst");
        errdefer wf.close() catch {};
        var tb = try TableBuilder.init(gpa, opts, wf, policy);
        defer tb.deinit();
        for (pairs) |p| try tb.add(p.k, p.v);
        try tb.finish();
        try wf.close();
    }

    const file = try readAllFile(e, gpa, "small.sst");
    defer gpa.free(file);

    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);
    try testing.expectEqual(@as(u32, 5), footer.format_version);

    const index_contents = try readVerifiedBlock(file, footer.index_handle);
    const index_block = try block.Block.init(gpa, index_contents);

    // Exactly one data block -> exactly one index entry.
    var n_index: usize = 0;
    var data_handle: BlockHandle = undefined;
    {
        var it = index_block.iterator(opts.comparator);
        defer it.deinit();
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            var hv: []const u8 = it.value();
            data_handle = try BlockHandle.decodeFrom(&hv);
            n_index += 1;
        }
    }
    try testing.expectEqual(@as(usize, 1), n_index);

    const data_contents = try readVerifiedBlock(file, data_handle);
    const data_blk = try block.Block.init(gpa, data_contents);
    var it = data_blk.iterator(opts.comparator);
    defer it.deinit();
    var idx: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        try testing.expectEqualStrings(pairs[idx].k, it.key());
        try testing.expectEqualStrings(pairs[idx].v, it.value());
        idx += 1;
    }
    try testing.expectEqual(pairs.len, idx);
}

// ===========================================================================
// fulllocalbloom — FastLocalBloom full filter wired through builder + reader.
// ===========================================================================

const FullFilterReader = full_filter.FullFilterReader;

test "table builder: full-filter mode writes fullfilter. meta key + readable FastLocalBloom" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{
        .block_size = 200,
        .block_restart_interval = 4,
        .filter_mode = .full, // fulllocalbloom gate
    };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 50);
    defer freeEntries(gpa, &entries);

    {
        var wf = try e.newWritableFile(gpa, "full.sst");
        errdefer wf.close() catch {};
        var tb = try TableBuilder.init(gpa, opts, wf, policy);
        defer tb.deinit();
        for (entries.items) |kv| try tb.add(kv.k, kv.v);
        try tb.finish();
        try wf.close();
    }

    const file = try readAllFile(e, gpa, "full.sst");
    defer gpa.free(file);

    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);

    // Metaindex carries the FULL-filter key, NOT the block-based one.
    const meta_contents = try readVerifiedBlock(file, footer.metaindex_handle);
    const meta_blk = try block.Block.init(gpa, meta_contents);

    var full_key: std.ArrayListUnmanaged(u8) = .empty;
    defer full_key.deinit(gpa);
    try full_key.appendSlice(gpa, "fullfilter.");
    try full_key.appendSlice(gpa, policy.name());
    try testing.expectEqualStrings("fullfilter.leveldb.BuiltinBloomFilter2", full_key.items);

    var legacy_key: std.ArrayListUnmanaged(u8) = .empty;
    defer legacy_key.deinit(gpa);
    try legacy_key.appendSlice(gpa, "filter.");
    try legacy_key.appendSlice(gpa, policy.name());

    {
        var it = meta_blk.iterator(comparator.bytewise);
        defer it.deinit();
        it.seek(full_key.items);
        try testing.expect(it.valid());
        try testing.expectEqualStrings(full_key.items, it.key());

        var hv: []const u8 = it.value();
        const filter_handle = try BlockHandle.decodeFrom(&hv);
        const filter_contents = try readVerifiedBlock(file, filter_handle);

        var fr = FullFilterReader.init(filter_contents);
        try testing.expect(fr.valid);
        // No false negatives: every inserted internal key reports may-match.
        for (entries.items) |kv| try testing.expect(fr.keyMayMatch(kv.k));
        // An absent key is (almost surely) pruned.
        try testing.expect(!fr.keyMayMatch("definitely-absent-key-9999"));
    }

    {
        // The legacy "filter." block-based entry must be ABSENT in full mode.
        var it = meta_blk.iterator(comparator.bytewise);
        defer it.deinit();
        it.seek(legacy_key.items);
        const present = it.valid() and comparator.bytewise.compare(it.key(), legacy_key.items) == .eq;
        try testing.expect(!present);
    }
}

test "table reader: full-filter mode round-trips through Table.get (no data lost, absent pruned)" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Plain bytewise keys across several data blocks (small block_size), with
    // the FastLocalBloom full filter enabled.  This drives Table.get's bloom
    // fast-path through `full_filter_reader`: a present key must never be
    // pruned (no false negatives), an absent key is pruned/not-found.
    const opts = options_mod.Options{
        .block_size = 200,
        .block_restart_interval = 4,
        .filter_mode = .full,
    };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 50);
    defer freeEntries(gpa, &entries);

    {
        var wf = try e.newWritableFile(gpa, "fullget.sst");
        errdefer wf.close() catch {};
        var tb = try TableBuilder.init(gpa, opts, wf, policy);
        defer tb.deinit();
        for (entries.items) |kv| try tb.add(kv.k, kv.v);
        try tb.finish();
        try wf.close();
    }

    const size = try e.getFileSize("fullget.sst");
    var raf = try e.newRandomAccessFile(gpa, "fullget.sst");
    defer raf.close() catch {};

    const table_reader = @import("table_reader.zig");
    var tbl = try table_reader.Table.open(gpa, raf, size, opts, policy, null, 0);
    defer tbl.deinit();

    // The reader must have detected the FULL filter (not the legacy one).
    try testing.expect(tbl.full_filter_reader != null);
    try testing.expect(tbl.filter_reader == null);

    // 1. No data lost: every inserted key resolves to its value — proving the
    //    full filter never drops a present key.
    for (entries.items) |kv| {
        const got = try tbl.get(gpa, kv.k);
        try testing.expect(got != null);
        defer gpa.free(got.?);
        try testing.expectEqualStrings(kv.v, got.?);
    }

    // 2. Absent keys return null (the filter prunes most; any survivor is found
    //    absent in its data block).
    {
        const got = try tbl.get(gpa, "key99999-definitely-absent");
        try testing.expect(got == null);
    }
}
