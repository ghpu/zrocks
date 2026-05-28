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
const std = @import("std");

const block = @import("block.zig");
const filter_block = @import("filter_block.zig");
const bloom = @import("bloom.zig");
const internal_key = @import("internal_key.zig");
const footer_mod = @import("footer.zig");
const crc32c = @import("../util/crc32c.zig");
const coding = @import("../util/coding.zig");
const comparator = @import("../util/comparator.zig");
const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");

const BlockBuilder = block.BlockBuilder;
const BlockHandle = footer_mod.BlockHandle;
const Footer = footer_mod.Footer;
const FilterBlockBuilder = filter_block.FilterBlockBuilder;

/// kNoCompression — the only compression type supported here.
pub const kNoCompression: u8 = 0;

/// Restart interval used for the index and metaindex blocks (LevelDB uses 1).
const kMetaIndexRestartInterval: usize = 1;

/// Prefix of the metaindex key naming the table's filter block.
const kFilterMetaKeyPrefix: []const u8 = "filter.";

pub const TableBuilder = struct {
    gpa: std.mem.Allocator,
    options: options_mod.Options,
    file: env.WritableFile,
    policy: bloom.BloomFilterPolicy,

    /// Builder for the current data block (entries flushed at ~block_size).
    data_block: BlockBuilder,
    /// Builder for the index block (one entry per flushed data block).
    index_block: BlockBuilder,
    /// Block-based bloom filter builder (one filter per 2KB data range).
    filter: FilterBlockBuilder,

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
            .filter = filter,
            .last_key = .empty,
            .offset = 0,
            .num_entries = 0,
            .pending_index_entry = false,
            .pending_handle = .{ .offset = 0, .size = 0 },
            .finished = false,
            .handle_encoding = .empty,
        };
    }

    pub fn deinit(self: *TableBuilder) void {
        self.data_block.deinit();
        self.index_block.deinit();
        self.filter.deinit(self.gpa);
        self.last_key.deinit(self.gpa);
        self.handle_encoding.deinit(self.gpa);
        self.* = undefined;
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
                try self.filter.addKey(self.gpa, pe.transform(user_key));
            }
        } else {
            try self.filter.addKey(self.gpa, key);
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

    /// Emit the deferred index entry for the just-flushed data block:
    /// key = `last_key` (already narrowed to a short separator/successor by the
    /// caller), value = the pending data block's encoded BlockHandle. Clears
    /// `pending_index_entry`.
    fn appendIndexEntry(self: *TableBuilder) !void {
        std.debug.assert(self.pending_index_entry);
        self.handle_encoding.clearRetainingCapacity();
        try self.pending_handle.encodeTo(&self.handle_encoding, self.gpa);
        try self.index_block.add(self.last_key.items, self.handle_encoding.items);
        self.pending_index_entry = false;
    }

    /// Flush the current data block to the file and arm a deferred index entry.
    fn flush(self: *TableBuilder) !void {
        std.debug.assert(!self.finished);
        if (self.data_block.isEmpty()) return;
        std.debug.assert(!self.pending_index_entry);

        self.pending_handle = try self.writeBlock(&self.data_block);
        self.pending_index_entry = true;
        try self.file.flush();

        // Begin the next filter range at the new file offset.
        try self.filter.startBlock(self.gpa, self.offset);
    }

    /// Finish a BlockBuilder, write it (with trailer) to the file, reset the
    /// builder, and return its BlockHandle.
    fn writeBlock(self: *TableBuilder, builder: *BlockBuilder) !BlockHandle {
        const contents = builder.finish();
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

        // 1. Filter block.
        const filter_contents = try self.filter.finish(self.gpa);
        const filter_handle = try self.writeRawBlock(filter_contents, kNoCompression);

        // 2. Metaindex block: single entry "filter."++name -> filter handle.
        //    Its keys are plain bytewise meta keys (NOT internal keys), so it is
        //    ordered/searched with the bytewise comparator, matching the reader.
        var metaindex_block = BlockBuilder.init(self.gpa, comparator.bytewise, kMetaIndexRestartInterval);
        defer metaindex_block.deinit();
        {
            var key_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer key_buf.deinit(self.gpa);
            try key_buf.appendSlice(self.gpa, kFilterMetaKeyPrefix);
            try key_buf.appendSlice(self.gpa, self.policy.name());

            self.handle_encoding.clearRetainingCapacity();
            try filter_handle.encodeTo(&self.handle_encoding, self.gpa);
            try metaindex_block.add(key_buf.items, self.handle_encoding.items);
        }
        const metaindex_handle = try self.writeBlock(&metaindex_block);

        // 3. Final pending index entry (use a short successor of the last key).
        if (self.pending_index_entry) {
            self.options.comparator.findShortSuccessor(&self.last_key);
            try self.appendIndexEntry();
        }

        // 4. Index block.
        const index_handle = try self.writeBlock(&self.index_block);

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

/// Slice a block's contents out of the file bytes given its handle, validate
/// the 5-byte trailer CRC, and return the contents slice (into `file`).
fn readVerifiedBlock(file: []const u8, handle: BlockHandle) ![]const u8 {
    const start: usize = @intCast(handle.offset);
    const size: usize = @intCast(handle.size);
    try testing.expect(start + size + 5 <= file.len);
    const contents = file[start .. start + size];

    const trailer = file[start + size .. start + size + 5];
    const compression_type = trailer[0];
    try testing.expectEqual(@as(u8, kNoCompression), compression_type);
    const stored_masked = coding.decodeFixed32(trailer[1..5]);
    const expected = crc32c.extend(crc32c.value(contents), &[_]u8{compression_type});
    try testing.expectEqual(expected, crc32c.unmask(stored_masked));
    return contents;
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
