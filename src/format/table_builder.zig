/// table_builder.zig — LevelDB/RocksDB block-based SST table builder.
///
/// Writes a complete block-based table file, byte-compatible with the
/// LevelDB/RocksDB block-based table layout (kNoCompression, format_version 5
/// footer). Mirrors LevelDB's `table/table_builder.cc`.
///
/// File layout (write order):
///   [data block]*           one BlockBuilder per ~block_size run of entries
///   [full filter block]     one RocksDB FastLocalBloom full filter (full_filter.zig)
///   [metaindex block]       "fullfilter."++name -> filter, "rocksdb.properties", "rocksdb.range_del"
///   [index block]           one entry per data block (separator key -> data handle)
///   [footer]                53 bytes (fv=5, crc32c), no trailer
///
/// Every block (data/filter/metaindex/index) is written via writeRawBlock:
/// the block contents, then a 5-byte trailer = [compression_type] ++
/// fixed32_LE(mask(extend(value(contents), &[compression_type]))).
///
/// Filter: every SST carries a whole-key RocksDB FastLocalBloom full filter
/// (`full_filter.zig`), written under "fullfilter."++policy.name().  Because the
/// filter is always built over WHOLE internal keys, a present key can never
/// report a false negative regardless of any configured `prefix_extractor`.
/// Prefix seeks (M7.2) are served at READ time: `table_reader.zig` extracts the
/// user-key prefix and probes the same full filter — this matches RocksDB, which
/// also keeps a whole-SST FastLocalBloom rather than a per-block prefix filter.
const std = @import("std");

const block = @import("block.zig");
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

/// kNoCompression — block stored verbatim (compression type byte 0).
pub const kNoCompression: u8 = 0;
/// kSnappyCompression — block stored as a Snappy block-format payload (byte 1).
pub const kSnappyCompression: u8 = 1;

/// Restart interval used for the index and metaindex blocks (LevelDB uses 1).
const kMetaIndexRestartInterval: usize = 1;

/// Prefix of the metaindex key naming the table's FastLocalBloom full filter.
/// The legacy block-based filter ("filter."++name) WRITE path was dropped
/// (filter-rocksdb-only); only the full filter is written now.  The full-filter
/// prefix is distinct ('f'ull… vs 'f'ilter.) so no on-disk collision occurs.
const kFullFilterMetaKeyPrefix: []const u8 = "fullfilter.";

/// Metaindex key naming the table's range-del block — RocksDB's
/// `rocksdb.range_del` (range-del-rocksdb).  The block is the RocksDB on-disk
/// format: a data block of `InternalKey(begin, seq, kTypeRangeDeletion) -> end`
/// entries in InternalKeyComparator order (see delete_range.zig).
const kRangeDelMetaKey: []const u8 = "rocksdb.range_del";

/// Metaindex key naming the RocksDB table-properties block.  Every SST zrocks
/// writes now carries it (the RocksDB-only format flip); the reader recognises a
/// RocksDB-form index by its presence (see `table_reader`).
const kRocksDbPropertiesMetaKey: []const u8 = "rocksdb.properties";

pub const TableBuilder = struct {
    gpa: std.mem.Allocator,
    options: options_mod.Options,
    file: env.WritableFile,
    policy: bloom.BloomFilterPolicy,

    /// Builder for the current data block (entries flushed at ~block_size).
    data_block: BlockBuilder,
    /// FastLocalBloom full-filter builder (one filter over EVERY key).  Written
    /// into every SST under "fullfilter."++policy.name() (filter-rocksdb-only).
    /// The legacy block-based bloom WRITE path was dropped; only the read side
    /// (filter_block.zig FilterBlockReader) survives for LevelDB interop.
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

    // -- RocksDB-form index + properties accumulators -------------------------
    /// USER-key separators for the RocksDB-form index (one per data block).
    /// Each is the user-key portion of the chosen separator (no 8-byte trailer);
    /// RocksDB reads the index with `index.key.is.user.key=1`.  Owned (each entry
    /// freed in deinit).
    rdb_index_seps: std.ArrayListUnmanaged([]u8),
    /// Data-block handles paired with `rdb_index_seps` (same length/order).
    rdb_index_handles: std.ArrayListUnmanaged(BlockHandle),
    /// Sum of raw (uncompressed, undelta'd) internal-key bytes across all added
    /// entries — the `rocksdb.raw.key.size` property.
    rdb_raw_key_size: u64,
    /// Sum of raw value bytes — the `rocksdb.raw.value.size` property.
    rdb_raw_value_size: u64,
    /// Count of point-delete entries (kTypeDeletion / kTypeSingleDeletion) added
    /// — the `rocksdb.deleted.keys` property.
    rdb_deleted_keys: u64,
    /// Count of merge-operand entries (kTypeMerge) added — the
    /// `rocksdb.merge.operands` property.
    rdb_merge_operands: u64,
    /// Smallest / largest sequence numbers seen across added internal keys (for
    /// the `rocksdb.key.smallest.seqno` / `rocksdb.key.largest.seqno` props).
    /// `rdb_smallest_seqno` starts at max-u64 so the first key initialises it.
    rdb_smallest_seqno: u64,
    rdb_largest_seqno: u64,

    pub fn init(
        gpa: std.mem.Allocator,
        options: options_mod.Options,
        file: env.WritableFile,
        policy: bloom.BloomFilterPolicy,
    ) !TableBuilder {
        return .{
            .gpa = gpa,
            .options = options,
            .file = file,
            .policy = policy,
            // Data + index blocks hold keys ordered by the table's comparator
            // (an InternalKeyComparator for DB SSTs), so build them with it.
            .data_block = BlockBuilder.init(gpa, options.comparator, options.block_restart_interval),
            .full_filter = full_filter.FullFilterBuilder.init(policy.bits_per_key),
            .last_key = .empty,
            .offset = 0,
            .num_entries = 0,
            .pending_index_entry = false,
            .pending_handle = .{ .offset = 0, .size = 0 },
            .finished = false,
            .handle_encoding = .empty,
            .range_tombstones = delete_range.RangeTombstoneList.init(gpa),
            .rdb_index_seps = .empty,
            .rdb_index_handles = .empty,
            .rdb_raw_key_size = 0,
            .rdb_raw_value_size = 0,
            .rdb_deleted_keys = 0,
            .rdb_merge_operands = 0,
            .rdb_smallest_seqno = std.math.maxInt(u64),
            .rdb_largest_seqno = 0,
        };
    }

    pub fn deinit(self: *TableBuilder) void {
        self.data_block.deinit();
        self.full_filter.deinit(self.gpa);
        self.last_key.deinit(self.gpa);
        self.handle_encoding.deinit(self.gpa);
        self.range_tombstones.deinit();
        for (self.rdb_index_seps.items) |s| self.gpa.free(s);
        self.rdb_index_seps.deinit(self.gpa);
        self.rdb_index_handles.deinit(self.gpa);
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

        // Record the whole (internal) key in the FastLocalBloom full filter.
        // filter-rocksdb-only dropped the clean prefix-keyed filter; the full
        // filter is always built over whole keys and probed by whole key, so a
        // present key never reports a false negative regardless of any configured
        // prefix_extractor.
        try self.full_filter.addKey(self.gpa, key);

        // Remember last_key = key.
        self.last_key.clearRetainingCapacity();
        try self.last_key.appendSlice(self.gpa, key);

        self.num_entries += 1;
        // Accumulate the RocksDB table-properties statistics unconditionally so
        // the rocksdb finish path is always complete and correct regardless of
        // the discriminator (the flip milestone makes that path the only one):
        //   * raw (uncompressed) key/value byte totals;
        //   * point-delete / merge-operand counts;
        //   * the [smallest, largest] sequence-number span.
        // The seqno/type fields live in the 8-byte internal-key trailer; DB SSTs
        // are built with internal keys (>= 8 bytes).  Bare-bytewise SSTs (no
        // trailer) simply contribute no seqno/type stats.
        self.rdb_raw_key_size += key.len;
        self.rdb_raw_value_size += value.len;
        if (key.len >= 8) {
            const trailer = coding.decodeFixed64(key[key.len - 8 ..][0..8]);
            const seq = trailer >> 8;
            if (seq < self.rdb_smallest_seqno) self.rdb_smallest_seqno = seq;
            if (seq > self.rdb_largest_seqno) self.rdb_largest_seqno = seq;
            switch (@as(u8, @truncate(trailer))) {
                @intFromEnum(internal_key.ValueType.deletion),
                @intFromEnum(internal_key.ValueType.single_deletion),
                => self.rdb_deleted_keys += 1,
                @intFromEnum(internal_key.ValueType.merge) => self.rdb_merge_operands += 1,
                else => {},
            }
        }
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

        // Capture the USER-key separator + data-block handle for the RocksDB-form
        // index built at `finish`.  `self.last_key` is the chosen separator;
        // RocksDB's index stores only its user-key portion (index.key.is.user.key
        // =1).  DB SSTs are built with the InternalKeyComparator, so the separator
        // is an internal key whose 8-byte trailer must be stripped; a bare-bytewise
        // SST (no IKC) stores user keys directly, so use the separator as-is.
        const user_sep = if (std.mem.eql(u8, self.options.comparator.name(), "leveldb.InternalKeyComparator"))
            internal_key.extractUserKey(self.last_key.items)
        else
            self.last_key.items;
        try self.rdb_index_seps.append(self.gpa, try self.gpa.dupe(u8, user_sep));
        try self.rdb_index_handles.append(self.gpa, self.pending_handle);
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
        // The FastLocalBloom full filter is offset-agnostic (one filter over all
        // keys), so there is no per-data-block filter range to start here.
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

    /// Finish the SST in the (now unconditional) RocksDB-openable form.  Data
    /// blocks were already written (internal keys).  Emit, in order:
    ///   1. the final pending index entry (user-key short successor);
    ///   2. the RocksDB-form INDEX block — USER-key separators (no trailer) +
    ///      bare 2-varint handles, restart_interval=1 (every entry a restart);
    ///   3. (when the table carries tombstones) the RocksDB `rocksdb.range_del`
    ///      block — a data block of `InternalKey(begin,seq,kTypeRangeDeletion)
    ///      -> end` entries, fragmented + IKC-sorted (range-del-rocksdb);
    ///   4. the `rocksdb.properties` block — the table properties RocksDB
    ///      requires to open the file (num_entries, comparator name, the index
    ///      shape flags index.key.is.user.key=1 + index.value.is.delta.encoded=1
    ///      + block.based.table.index.type=kBinarySearch, raw sizes, …);
    ///   5. the metaindex block — `fullfilter.`++policy.name() -> filter handle,
    ///      `rocksdb.properties` -> handle and (when present) `rocksdb.range_del`
    ///      -> handle, in ascending bytewise key order
    ///      ("fullfilter." < "rocksdb.properties" < "rocksdb.range_del");
    ///   6. the fv5 / crc32c footer.
    /// A FastLocalBloom full filter is written in EVERY SST (filter-rocksdb-only):
    /// one whole-SST filter over every key, registered under
    /// "fullfilter."++policy.name().  Does NOT close the file (caller owns).
    pub fn finish(self: *TableBuilder) !void {
        std.debug.assert(!self.finished);
        try self.flush();
        self.finished = true;

        // 1. Final pending index entry — narrow the last key to a short user-key
        //    successor, then capture it into the index accumulators.
        if (self.pending_index_entry) {
            self.options.comparator.findShortSuccessor(&self.last_key);
            try self.appendIndexEntry();
        }

        // 2. RocksDB-form index block.
        const index_raw = try buildRocksDbIndexBlock(
            self.gpa,
            self.rdb_index_seps.items,
            self.rdb_index_handles.items,
        );
        defer self.gpa.free(index_raw);
        // Data blocks span [0, current offset); record it for the data.size
        // property BEFORE the index/range-del/properties/metaindex are written.
        const data_size = self.offset;
        const index_handle = try self.writeRawBlock(index_raw, kNoCompression);

        // 2b. FastLocalBloom full-filter block (filter-rocksdb-only): one whole-SST
        //     filter over every key, written uncompressed and registered in the
        //     metaindex under "fullfilter."++policy.name().
        const filter_raw = try self.full_filter.finish(self.gpa);
        const filter_handle = try self.writeRawBlock(filter_raw, kNoCompression);

        // 3. RocksDB `rocksdb.range_del` block (range-del-rocksdb): a data block
        //    of `InternalKey(begin,seq,kTypeRangeDeletion) -> end` entries,
        //    fragmented + IKC-sorted.  Written only when the table carries at
        //    least one non-degenerate tombstone; an absent meta entry => none.
        var range_del_handle: ?BlockHandle = null;
        if (!self.range_tombstones.isEmpty()) {
            var rd_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer rd_buf.deinit(self.gpa);
            try self.range_tombstones.encode(&rd_buf, self.gpa);
            // encode() yields no bytes when every tombstone is degenerate; only
            // register a meta-block entry when an actual block was produced.
            if (rd_buf.items.len > 0) {
                range_del_handle = try self.writeRawBlock(rd_buf.items, kNoCompression);
            }
        }

        // 4. Properties block.
        const props_raw = try self.buildRocksDbPropertiesBlock(
            data_size,
            index_handle.size,
            filter_handle.size,
        );
        defer self.gpa.free(props_raw);
        const props_handle = try self.writeRawBlock(props_raw, kNoCompression);

        // 5. Metaindex block: `fullfilter.`++name -> filter handle, then
        //    `rocksdb.properties` -> handle, then (when present)
        //    `rocksdb.range_del` -> handle.  Keys are plain bytewise meta keys
        //    ordered/searched with the bytewise comparator, so they MUST be added
        //    in ascending bytewise order
        //    ("fullfilter."++name < "rocksdb.properties" < "rocksdb.range_del";
        //     'f' < 'r').
        var metaindex_block = BlockBuilder.init(self.gpa, comparator.bytewise, kMetaIndexRestartInterval);
        defer metaindex_block.deinit();
        {
            var filter_key: std.ArrayListUnmanaged(u8) = .empty;
            defer filter_key.deinit(self.gpa);
            try filter_key.appendSlice(self.gpa, kFullFilterMetaKeyPrefix);
            try filter_key.appendSlice(self.gpa, self.policy.name());
            self.handle_encoding.clearRetainingCapacity();
            try filter_handle.encodeTo(&self.handle_encoding, self.gpa);
            try metaindex_block.add(filter_key.items, self.handle_encoding.items);
        }
        {
            self.handle_encoding.clearRetainingCapacity();
            try props_handle.encodeTo(&self.handle_encoding, self.gpa);
            try metaindex_block.add(kRocksDbPropertiesMetaKey, self.handle_encoding.items);
        }
        if (range_del_handle) |rdh| {
            self.handle_encoding.clearRetainingCapacity();
            try rdh.encodeTo(&self.handle_encoding, self.gpa);
            try metaindex_block.add(kRangeDelMetaKey, self.handle_encoding.items);
        }
        const metaindex_handle = try self.writeBlock(&metaindex_block);

        // 6. Footer.
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

    /// Build the `rocksdb.properties` block contents (caller frees with gpa).
    /// Properties are emitted in ascending bytewise key order (the block builder
    /// requires sorted keys) with restart_interval=1, matching RocksDB.
    fn buildRocksDbPropertiesBlock(self: *TableBuilder, data_size: u64, index_size: u64, filter_size: u64) ![]u8 {
        var pb = BlockBuilder.init(self.gpa, comparator.bytewise, kMetaIndexRestartInterval);
        defer pb.deinit();

        // Scratch for varint64 property values.
        var vbuf: std.ArrayListUnmanaged(u8) = .empty;
        defer vbuf.deinit(self.gpa);
        const putU64 = struct {
            fn go(b: *BlockBuilder, scratch: *std.ArrayListUnmanaged(u8), a: std.mem.Allocator, key: []const u8, v: u64) !void {
                scratch.clearRetainingCapacity();
                try coding.putVarint64(scratch, a, v);
                try b.add(key, scratch.items);
            }
        }.go;

        // RocksDB table-properties keys, emitted in STRICT ASCENDING bytewise
        // order (the block builder requires sorted keys).  This is the full field
        // set a real RocksDB v11 SST carries (verified byte-for-byte against the
        // tests/fixtures/rocksdb fixture written by librocksdb).  Values are
        // varint64 unless noted.  Fields whose source data zrocks does not track
        // (timestamps, host/db/session identity, the original file number) are
        // omitted — RocksDB treats every property as optional on read and opens
        // the file regardless; what we DO emit is always correct.
        //
        // smallest/largest seqno default to 0 when no internal-key trailers were
        // seen (e.g. a bare-bytewise SST), matching RocksDB's "no seqno" encoding.
        const smallest_seqno: u64 = if (self.rdb_smallest_seqno == std.math.maxInt(u64)) 0 else self.rdb_smallest_seqno;

        // index.type: a 4-byte fixed32 (kBinarySearch == 0) — RocksDB reads it
        // as a raw fixed32, NOT a varint.
        try pb.add("rocksdb.block.based.table.index.type", &[_]u8{ 0, 0, 0, 0 });
        // whole.key.filtering=1 / prefix.filtering=0: a whole-key (non-prefix)
        // table.  Stored as a single ASCII '0'/'1' byte, matching RocksDB.
        try pb.add("rocksdb.block.based.table.prefix.filtering", if (self.options.prefix_extractor != null) "1" else "0");
        try pb.add("rocksdb.block.based.table.whole.key.filtering", "1");
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.column.family.id", 0);
        // column.family.name: the default CF (zrocks emits a single shared CF).
        try pb.add("rocksdb.column.family.name", "default");
        try pb.add("rocksdb.comparator", rocksDbUserComparatorName(self.options.comparator));
        // compression: zrocks SSTs in the rocksdb path are written uncompressed.
        try pb.add("rocksdb.compression", "NoCompression");
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.data.block.restart.interval", self.options.block_restart_interval);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.data.size", data_size);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.deleted.keys", self.rdb_deleted_keys);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.filter.size", filter_size);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.fixed.key.length", 0);
        // format.version: the block-based-table format version (5).
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.format.version", 5);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.index.block.restart.interval", 1);
        // index.key.is.user.key=1: the index separators are USER keys (no seq).
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.index.key.is.user.key", 1);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.index.size", index_size);
        // index.value.is.delta.encoded=1: matches RocksDB's fv5 default decode
        // path; harmless here because restart_interval=1 makes every index entry
        // a restart, so each stores its full (offset,size) handle anyway.
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.index.value.is.delta.encoded", 1);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.key.largest.seqno", self.rdb_largest_seqno);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.key.smallest.seqno", smallest_seqno);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.merge.operands", self.rdb_merge_operands);
        // merge.operator name: "nullptr" when none is configured (RocksDB form).
        try pb.add("rocksdb.merge.operator", "nullptr");
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.num.data.blocks", self.rdb_index_handles.items.len);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.num.entries", self.num_entries);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.num.filter_entries", self.num_entries);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.num.range-deletions", self.range_tombstones.count());
        // prefix.extractor.name: the configured extractor's name, or "nullptr".
        try pb.add("rocksdb.prefix.extractor.name", if (self.options.prefix_extractor) |pe| pe.name() else "nullptr");
        // property.collectors: none configured — RocksDB encodes this as "[]".
        try pb.add("rocksdb.property.collectors", "[]");
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.raw.key.size", self.rdb_raw_key_size);
        try putU64(&pb, &vbuf, self.gpa, "rocksdb.raw.value.size", self.rdb_raw_value_size);

        const contents = pb.finish();
        return self.gpa.dupe(u8, contents);
    }
};

/// The USER comparator name for a SST built with `cmp`.  DB SSTs are built with
/// the InternalKeyComparator (which wraps a user comparator); RocksDB's
/// `rocksdb.comparator` property records the USER comparator name.  The IKC
/// always wraps the bytewise user comparator in this codebase's write paths, so
/// we map its name accordingly; any non-IKC comparator (e.g. a bare bytewise
/// SST) records its own name directly.
fn rocksDbUserComparatorName(cmp: comparator.Comparator) []const u8 {
    if (std.mem.eql(u8, cmp.name(), "leveldb.InternalKeyComparator")) {
        return "leveldb.BytewiseComparator";
    }
    return cmp.name();
}

/// Build a RocksDB block-based-table INDEX block: kBinarySearch with
/// restart_interval=1, USER-key separators (no 8-byte trailer), bare 2-varint
/// BlockHandle values (NO per-entry value length — the value runs to the next
/// entry, every entry being its own restart).  This is exactly the shape
/// `table_reader.transcodeRocksDbIndex` parses back, and the byte-shape a real
/// RocksDB reads.  Returns a freshly gpa-allocated buffer the caller frees.
fn buildRocksDbIndexBlock(
    gpa: std.mem.Allocator,
    seps: []const []const u8,
    handles: []const BlockHandle,
) ![]u8 {
    std.debug.assert(seps.len == handles.len);
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(gpa);
    var restarts: std.ArrayListUnmanaged(u32) = .empty;
    defer restarts.deinit(gpa);

    for (seps, handles) |sep, h| {
        try restarts.append(gpa, @intCast(buf.items.len));
        try coding.putVarint32(&buf, gpa, 0); // shared (restart: full key)
        try coding.putVarint32(&buf, gpa, @intCast(sep.len)); // non_shared
        try buf.appendSlice(gpa, sep); // user-key separator
        try h.encodeTo(&buf, gpa); // bare 2-varint handle, no value length
    }
    for (restarts.items) |r| try coding.putFixed32(&buf, gpa, r);
    try coding.putFixed32(&buf, gpa, @intCast(restarts.items.len));
    return buf.toOwnedSlice(gpa);
}

// ===========================================================================
// Tests — byte-compat gate. Build a multi-data-block table, then parse it back
// with the existing footer/block/filter modules (a mini-reader) and verify.
// ===========================================================================

const testing = std.testing;

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

test "table builder: multi-block round-trip via Table.open (RocksDB-form), trailer CRCs" {
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

    // ---- Footer: fv5 + crc32c, RocksDB-form metaindex carries properties --
    const file = try readAllFile(e, gpa, "test.sst");
    defer gpa.free(file);
    try testing.expect(file.len >= footer_mod.kEncodedLength);
    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);
    try testing.expectEqual(@as(u32, 5), footer.format_version);
    try testing.expectEqual(footer_mod.ChecksumType.crc32c, footer.checksum_type);
    {
        const meta_contents = try readVerifiedBlock(file, footer.metaindex_handle);
        const meta_blk = try block.Block.init(gpa, meta_contents);
        var it = meta_blk.iterator(comparator.bytewise);
        defer it.deinit();
        it.seek("rocksdb.properties");
        try testing.expect(it.valid());
        try testing.expectEqualStrings("rocksdb.properties", it.key());
        // No legacy block-based filter entry: the block-based bloom WRITE path
        // is dropped (filter-rocksdb-only).  A FastLocalBloom full filter is
        // written instead, under "fullfilter."++policy.name().
        it.seek("filter.leveldb.BuiltinBloomFilter2");
        const has_block_filter = it.valid() and comparator.bytewise.compare(it.key(), "filter.leveldb.BuiltinBloomFilter2") == .eq;
        try testing.expect(!has_block_filter);
        it.seek("fullfilter.leveldb.BuiltinBloomFilter2");
        const has_full_filter = it.valid() and comparator.bytewise.compare(it.key(), "fullfilter.leveldb.BuiltinBloomFilter2") == .eq;
        try testing.expect(has_full_filter);
    }

    // ---- Round-trip every entry + a full ordered scan through Table.open --
    const RdwTableT = @import("table_reader.zig").Table;
    const size = try e.getFileSize("test.sst");
    var raf = try e.newRandomAccessFile(gpa, "test.sst");
    defer raf.close() catch {};
    var tbl = try RdwTableT.open(gpa, raf, size, opts, policy, null, 0);
    defer tbl.deinit();

    for (entries.items) |kv| {
        const got = try tbl.get(gpa, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
    var it = tbl.iterator(gpa);
    defer it.deinit();
    var idx: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        try testing.expect(idx < entries.items.len);
        try testing.expectEqualStrings(entries.items[idx].k, it.key());
        try testing.expectEqualStrings(entries.items[idx].v, it.value());
        idx += 1;
    }
    try testing.expectEqual(entries.items.len, idx);
}

test "table builder: every SST carries a functional FastLocalBloom full filter (no false negatives)" {
    // filter-rocksdb-only: `finish` writes a FastLocalBloom full filter under
    // "fullfilter."++policy.name() in EVERY SST, regardless of the (now-removed)
    // filter_mode.  The filter must report may-match for every inserted internal
    // key (no false negatives) and prune at least one obviously-absent key.
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    var ikc = internal_key.InternalKeyComparator{ .user = comparator.bytewise };
    const opts = options_mod.Options{
        .comparator = ikc.comparatorInterface(),
        .block_size = 64, // several data blocks
        .block_restart_interval = 4,
    };
    const policy = bloom.BloomFilterPolicy.init(10);

    const users = [_][]const u8{ "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel" };
    var ikeys: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (ikeys.items) |k| gpa.free(k);
        ikeys.deinit(gpa);
    }
    {
        var wf = try e.newWritableFile(gpa, "ff.sst");
        errdefer wf.close() catch {};
        var tb = try TableBuilder.init(gpa, opts, wf, policy);
        defer tb.deinit();
        for (users) |u| {
            const ik = try gpa.alloc(u8, u.len + 8);
            @memcpy(ik[0..u.len], u);
            coding.encodeFixed64(ik[u.len..][0..8], internal_key.packSequenceAndType(1, .value));
            try ikeys.append(gpa, ik);
            try tb.add(ik, u);
        }
        try tb.finish();
        try wf.close();
    }

    const file = try readAllFile(e, gpa, "ff.sst");
    defer gpa.free(file);
    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);

    // Locate the "fullfilter."++name metaindex entry, read the filter block, and
    // verify the full-filter reader sees every inserted internal key.
    const meta_contents = try readVerifiedBlock(file, footer.metaindex_handle);
    const meta_blk = try block.Block.init(gpa, meta_contents);
    var it = meta_blk.iterator(comparator.bytewise);
    defer it.deinit();
    it.seek("fullfilter.leveldb.BuiltinBloomFilter2");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("fullfilter.leveldb.BuiltinBloomFilter2", it.key());

    var hv: []const u8 = it.value();
    const filter_handle = try BlockHandle.decodeFrom(&hv);
    const filter_payload = try readVerifiedBlock(file, filter_handle);
    var fr = full_filter.FullFilterReader.init(filter_payload);
    try testing.expect(fr.valid);

    for (ikeys.items) |ik| {
        try testing.expect(fr.keyMayMatch(ik)); // no false negatives
    }
    // An obviously-absent internal key is pruned (not a hard guarantee, but the
    // FP rate is ~1%, so a single distinctive key reliably prunes).
    var absent: [13]u8 = undefined;
    @memcpy(absent[0..5], "zzzzz");
    coding.encodeFixed64(absent[5..][0..8], internal_key.packSequenceAndType(1, .value));
    try testing.expect(!fr.keyMayMatch(&absent));
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

    // At least one data block on disk must carry the kSnappy trailer byte (proves
    // compression actually happened); each compressed payload must decompress.
    // The data blocks span the file prefix before the index/properties/metaindex/
    // footer; we scan trailers by walking blocks from offset 0 using the on-disk
    // index handles parsed from the RocksDB-form index.
    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);
    const index_contents = try readVerifiedBlock(file, footer.index_handle);
    // RocksDB-form index: restart_interval=1, USER-key sep + bare handle.  Decode
    // each restart entry to recover the data-block handles.
    var any_snappy = false;
    {
        const raw = index_contents;
        const num_restarts = coding.decodeFixed32(raw[raw.len - 4 ..][0..4]);
        try testing.expect(num_restarts >= 2); // several data blocks
        const restart_array_off = raw.len - (@as(usize, num_restarts) + 1) * @sizeOf(u32);
        for (0..num_restarts) |i| {
            const estart = coding.decodeFixed32(raw[restart_array_off + i * 4 ..][0..4]);
            const eend: usize = if (i + 1 < num_restarts)
                coding.decodeFixed32(raw[restart_array_off + (i + 1) * 4 ..][0..4])
            else
                restart_array_off;
            var entry: []const u8 = raw[estart..eend];
            const shared = try coding.getVarint32(&entry);
            const non_shared = try coding.getVarint32(&entry);
            try testing.expectEqual(@as(u32, 0), shared);
            entry = entry[non_shared..];
            const h = try BlockHandle.decodeFrom(&entry);
            const dstart: usize = @intCast(h.offset);
            const dsize: usize = @intCast(h.size);
            if (file[dstart + dsize] == kSnappyCompression) {
                any_snappy = true;
                const decompressed = try snappy.decompress(gpa, file[dstart .. dstart + dsize]);
                gpa.free(decompressed);
            }
        }
    }
    try testing.expect(any_snappy);

    // Round-trip every entry through Table.open (exercises on-read decompression).
    const RdwTableT = @import("table_reader.zig").Table;
    const size = try e.getFileSize("snap.sst");
    var raf = try e.newRandomAccessFile(gpa, "snap.sst");
    defer raf.close() catch {};
    var tbl = try RdwTableT.open(gpa, raf, size, opts, policy, null, 0);
    defer tbl.deinit();
    var idx: usize = 0;
    var it = tbl.iterator(gpa);
    defer it.deinit();
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        try testing.expect(idx < entries.items.len);
        try testing.expectEqualStrings(entries.items[idx].k, it.key());
        try testing.expectEqualStrings(entries.items[idx].v, it.value());
        idx += 1;
    }
    try testing.expectEqual(entries.items.len, idx);
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

    // Exactly one data block -> the RocksDB-form index has exactly one entry.
    const index_contents = try readVerifiedBlock(file, footer.index_handle);
    const num_restarts = coding.decodeFixed32(index_contents[index_contents.len - 4 ..][0..4]);
    try testing.expectEqual(@as(u32, 1), num_restarts);

    // Round-trip through Table.open.
    const RdwTableT = @import("table_reader.zig").Table;
    const size = try e.getFileSize("small.sst");
    var raf = try e.newRandomAccessFile(gpa, "small.sst");
    defer raf.close() catch {};
    var tbl = try RdwTableT.open(gpa, raf, size, opts, policy, null, 0);
    defer tbl.deinit();
    var it = tbl.iterator(gpa);
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
// rocksdb-write — SST emitted in real-RocksDB-openable form (now the ONLY form).
// CI-safe: zrocks writes the RocksDB-form SST, then RE-READS it via
// its own RocksDB-read path (Table.open auto-detects the form), proving the
// metaindex carries rocksdb.properties and the index parses as the RocksDB
// (user-key separator) shape.  The real-RocksDB authenticity gate lives in
// tests/rocksdb_write_interop_test.zig (shells out to verify_open).
// ===========================================================================

const internal_key_mod = @import("internal_key.zig");
const RdwTable = @import("table_reader.zig").Table;

/// Encode `user ++ fixed64(packSequenceAndType(seq, t))` (caller frees).
fn rdwEncodeIkey(gpa: std.mem.Allocator, user: []const u8, seq: u64, t: internal_key_mod.ValueType) ![]u8 {
    const out = try gpa.alloc(u8, user.len + 8);
    @memcpy(out[0..user.len], user);
    coding.encodeFixed64(out[user.len..][0..8], internal_key_mod.packSequenceAndType(seq, t));
    return out;
}

test "rocksdb-write: SST is RocksDB-form (metaindex has rocksdb.properties, index is user-key) and round-trips" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Build with the InternalKeyComparator (as the DB flush path does) and a
    // small block_size so several data blocks form.
    var ikc = internal_key_mod.InternalKeyComparator{ .user = comparator.bytewise };
    const opts = options_mod.Options{
        .comparator = ikc.comparatorInterface(),
        .block_size = 64,
        .block_restart_interval = 4,
    };
    const policy = bloom.BloomFilterPolicy.init(10);

    // 12 sorted user keys (internal keys, seq increasing), enough for >= 2 data
    // blocks at block_size=64.
    const users = [_][]const u8{ "k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10", "k11" };
    var ikeys: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (ikeys.items) |k| gpa.free(k);
        ikeys.deinit(gpa);
    }
    {
        var wf = try e.newWritableFile(gpa, "rdw.sst");
        errdefer wf.close() catch {};
        var tb = try TableBuilder.init(gpa, opts, wf, policy);
        defer tb.deinit();
        var seq: u64 = 1;
        for (users) |u| {
            const ik = try rdwEncodeIkey(gpa, u, seq, .value);
            try ikeys.append(gpa, ik);
            try tb.add(ik, u); // value = user key for easy verification
            seq += 1;
        }
        try tb.finish();
        try wf.close();
    }

    const file = try readAllFile(e, gpa, "rdw.sst");
    defer gpa.free(file);

    // ---- Footer: fv5, crc32c, RocksDB magic ----
    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);
    try testing.expectEqual(@as(u32, 5), footer.format_version);
    try testing.expectEqual(footer_mod.ChecksumType.crc32c, footer.checksum_type);

    // ---- Metaindex carries rocksdb.properties and NO filter entry ----
    {
        const meta_contents = try readVerifiedBlock(file, footer.metaindex_handle);
        const meta_blk = try block.Block.init(gpa, meta_contents);
        var it = meta_blk.iterator(comparator.bytewise);
        defer it.deinit();
        it.seek("rocksdb.properties");
        try testing.expect(it.valid());
        try testing.expectEqualStrings("rocksdb.properties", it.key());

        // No legacy "filter." entry in rocksdb-write mode.
        it.seek("filter.leveldb.BuiltinBloomFilter2");
        const has_filter = it.valid() and comparator.bytewise.compare(it.key(), "filter.leveldb.BuiltinBloomFilter2") == .eq;
        try testing.expect(!has_filter);
    }

    // ---- Properties block carries the required keys ----
    {
        const meta_contents = try readVerifiedBlock(file, footer.metaindex_handle);
        const meta_blk = try block.Block.init(gpa, meta_contents);
        var mit = meta_blk.iterator(comparator.bytewise);
        defer mit.deinit();
        mit.seek("rocksdb.properties");
        var hv: []const u8 = mit.value();
        const props_handle = try BlockHandle.decodeFrom(&hv);
        const props_contents = try readVerifiedBlock(file, props_handle);
        const props_blk = try block.Block.init(gpa, props_contents);

        const expect = struct {
            fn present(b: *const block.Block, key: []const u8) !void {
                var it = b.iterator(comparator.bytewise);
                defer it.deinit();
                it.seek(key);
                try testing.expect(it.valid());
                try testing.expectEqualStrings(key, it.key());
            }
        };
        // The FULL required field set a real RocksDB v11 SST carries (matches the
        // tests/fixtures/rocksdb fixture written by librocksdb).
        const required = [_][]const u8{
            "rocksdb.block.based.table.index.type",
            "rocksdb.block.based.table.prefix.filtering",
            "rocksdb.block.based.table.whole.key.filtering",
            "rocksdb.column.family.id",
            "rocksdb.column.family.name",
            "rocksdb.comparator",
            "rocksdb.compression",
            "rocksdb.data.block.restart.interval",
            "rocksdb.data.size",
            "rocksdb.deleted.keys",
            "rocksdb.filter.size",
            "rocksdb.fixed.key.length",
            "rocksdb.format.version",
            "rocksdb.index.block.restart.interval",
            "rocksdb.index.key.is.user.key",
            "rocksdb.index.size",
            "rocksdb.index.value.is.delta.encoded",
            "rocksdb.key.largest.seqno",
            "rocksdb.key.smallest.seqno",
            "rocksdb.merge.operands",
            "rocksdb.merge.operator",
            "rocksdb.num.data.blocks",
            "rocksdb.num.entries",
            "rocksdb.num.filter_entries",
            "rocksdb.num.range-deletions",
            "rocksdb.prefix.extractor.name",
            "rocksdb.property.collectors",
            "rocksdb.raw.key.size",
            "rocksdb.raw.value.size",
        };
        for (required) |k| try expect.present(&props_blk, k);

        // comparator property records the USER comparator name.
        var cit = props_blk.iterator(comparator.bytewise);
        defer cit.deinit();
        cit.seek("rocksdb.comparator");
        try testing.expectEqualStrings("leveldb.BytewiseComparator", cit.value());
        // column.family.name + merge.operator carry their RocksDB ASCII forms.
        cit.seek("rocksdb.column.family.name");
        try testing.expectEqualStrings("default", cit.value());
        cit.seek("rocksdb.merge.operator");
        try testing.expectEqualStrings("nullptr", cit.value());
        cit.seek("rocksdb.compression");
        try testing.expectEqualStrings("NoCompression", cit.value());
        // index.type is a 4-byte fixed32 (kBinarySearch == 0).
        cit.seek("rocksdb.block.based.table.index.type");
        try testing.expectEqualSlices(u8, &[_]u8{ 0, 0, 0, 0 }, cit.value());
        // num.entries == 12 (all 12 keys added).
        cit.seek("rocksdb.num.entries");
        var nv: []const u8 = cit.value();
        try testing.expectEqual(@as(u64, 12), try coding.getVarint64(&nv));
        // All 12 added keys are kTypeValue -> zero deletes / merges.
        cit.seek("rocksdb.deleted.keys");
        var dv: []const u8 = cit.value();
        try testing.expectEqual(@as(u64, 0), try coding.getVarint64(&dv));
    }

    // ---- Index is RocksDB form: user-key separators + bare handles ----
    // The RocksDB index is NOT a standard LevelDB block (its handle values have
    // no value-length prefix), so it must be parsed by restart points the way
    // `table_reader.transcodeRocksDbIndex` does.  Decode each restart entry and
    // assert: separator is a USER key (len 3, no 8-byte trailer) and the value
    // is a bare BlockHandle that runs exactly to the next entry boundary.
    {
        const index_contents = try readVerifiedBlock(file, footer.index_handle);
        const raw = index_contents;
        const num_restarts = coding.decodeFixed32(raw[raw.len - 4 ..][0..4]);
        try testing.expect(num_restarts >= 2); // >= 2 data blocks
        const restart_array_off = raw.len - (@as(usize, num_restarts) + 1) * @sizeOf(u32);
        for (0..num_restarts) |i| {
            const start = coding.decodeFixed32(raw[restart_array_off + i * 4 ..][0..4]);
            const end: usize = if (i + 1 < num_restarts)
                coding.decodeFixed32(raw[restart_array_off + (i + 1) * 4 ..][0..4])
            else
                restart_array_off;
            var entry: []const u8 = raw[start..end];
            const shared = try coding.getVarint32(&entry);
            const non_shared = try coding.getVarint32(&entry);
            try testing.expectEqual(@as(u32, 0), shared); // restart: full key
            // Separator is a USER key (a short separator/successor, 1..3 bytes —
            // never an internal key, which would be >= 9 bytes for these keys).
            try testing.expect(non_shared >= 1 and non_shared < 9);
            entry = entry[non_shared..];
            _ = try BlockHandle.decodeFrom(&entry);
            try testing.expectEqual(@as(usize, 0), entry.len); // bare handle, no trailer
        }
    }

    // ---- Round-trip: re-read via Table.open (auto-detects RocksDB form) ----
    {
        const size = try e.getFileSize("rdw.sst");
        var raf = try e.newRandomAccessFile(gpa, "rdw.sst");
        defer raf.close() catch {};
        var tbl = try RdwTable.open(gpa, raf, size, opts, policy, null, 0);
        defer tbl.deinit();

        for (users, ikeys.items) |u, ik| {
            const got = try tbl.get(gpa, ik) orelse return error.TestExpectedFound;
            defer gpa.free(got);
            try testing.expectEqualStrings(u, got);
        }
        // Full scan yields all 12 in order.
        var it = tbl.iterator(gpa);
        defer it.deinit();
        var idx: usize = 0;
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            try testing.expectEqualStrings(users[idx], it.value());
            idx += 1;
        }
        try testing.expectEqual(users.len, idx);
    }
}
