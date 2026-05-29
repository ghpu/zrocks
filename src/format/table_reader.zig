/// table_reader.zig — LevelDB/RocksDB block-based SST table reader.
///
/// Reads the block-based SST files produced by `table_builder.zig`
/// (format_version 5, kNoCompression, crc32c trailers). Mirrors LevelDB's
/// `table/table.cc` and `table/two_level_iterator.cc`.
///
/// On `open`, the footer (last 53 bytes) is decoded to locate the index block
/// and the metaindex block. The index block is read and kept resident. The
/// metaindex block is scanned for the `"filter." ++ policy.name()` entry; if
/// present its filter block is read and kept resident behind a
/// `FilterBlockReader` so point lookups can skip data-block reads for keys the
/// bloom filter proves absent.
///
/// Block reads validate the 5-byte trailer exactly like the builder writes it:
/// `compression_type` (must be kNoCompression) followed by
/// `fixed32_LE(mask(extend(crc(contents), &[compression_type])))`. A mismatch
/// is reported as `error.Corruption`.
///
/// Optional block cache (M3.5; zero-copy pinned-handle path): when a
/// `*cache_mod.Cache` is supplied to `open`, each block read first consults the
/// cache keyed by `cache_id (8 bytes LE) ++ handle.offset (8 bytes LE)`. On a
/// HIT the read returns a `BlockContents` that BORROWS the cached buffer behind
/// a pinned handle (no copy, no file read, no CRC verify); the handle is
/// released when the `BlockContents` is released (on iterator/handle deinit).
/// On a MISS the block is read, verified, and the freshly decoded buffer is
/// inserted into the cache; the returned `BlockContents` then borrows that same
/// cached buffer behind its pinned insert handle. With a null cache (or when a
/// best-effort cache insert fails) the `BlockContents` simply OWNS the decoded
/// buffer and frees it on release — reproducing the original behaviour. Either
/// way the round-trip bytes are identical; only the ownership/lifetime differs.
const std = @import("std");

const footer_mod = @import("footer.zig");
const block = @import("block.zig");
const filter_block = @import("filter_block.zig");
const full_filter = @import("full_filter.zig");
const bloom = @import("bloom.zig");
const crc32c = @import("../util/crc32c.zig");
const coding = @import("../util/coding.zig");
const comparator = @import("../util/comparator.zig");
const cache_mod = @import("../util/cache.zig");
const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const internal_key = @import("internal_key.zig");
const prefix = @import("../rocks/prefix.zig");
const delete_range = @import("../rocks/delete_range.zig");
const snappy = @import("../util/snappy.zig");

const BlockHandle = footer_mod.BlockHandle;
const Footer = footer_mod.Footer;
const Block = block.Block;
const FilterBlockReader = filter_block.FilterBlockReader;
const FullFilterReader = full_filter.FullFilterReader;

/// kNoCompression — block stored verbatim (compression type byte 0).
pub const kNoCompression: u8 = 0;
/// kSnappyCompression — block stored as a Snappy block-format payload (byte 1).
pub const kSnappyCompression: u8 = 1;

/// Length of the 5-byte block trailer (compression type + masked crc32c).
const kBlockTrailerSize: usize = 5;

pub const Table = struct {
    gpa: std.mem.Allocator,
    file: env.RandomAccessFile,
    comparator: comparator.Comparator,
    policy: bloom.BloomFilterPolicy,
    /// Optional prefix extractor (M7.2).  When set, the table's filter block is
    /// assumed to be built over key PREFIXES, so point lookups prune by prefix
    /// (computed from the lookup's user key) instead of by whole key.  Mirrors
    /// `options.prefix_extractor`; null reproduces whole-key bloom behaviour.
    prefix_extractor: ?prefix.PrefixExtractor,

    /// Contents of the index block (kept resident for the table's lifetime;
    /// owned outright or pinned in the block cache — released in `deinit`).
    index_contents: BlockContents,
    index_block: Block,

    /// Contents of the filter block, if the table carries one (shared by
    /// whichever filter format the table was built with; released in `deinit`).
    filter_contents: ?BlockContents,
    /// Legacy LevelDB block-based filter reader ("filter."++name entry).
    filter_reader: ?FilterBlockReader,
    /// FastLocalBloom full-filter reader ("fullfilter."++name entry).  At most
    /// one of `filter_reader` / `full_filter_reader` is set, chosen by which
    /// metaindex entry the table carries (fulllocalbloom gate, auto-detected).
    full_filter_reader: ?FullFilterReader,

    /// Optional shared block cache (caller-owned; not freed by `deinit`).
    block_cache: ?*cache_mod.Cache,
    /// Per-table id mixed into cache keys so blocks at the same offset in
    /// different tables do not collide in a shared cache.
    cache_id: u64,

    /// Handle of the metaindex block (kept so `rangeTombstones` can re-scan it
    /// on demand for the range-del entry, M7.5).
    metaindex_handle: BlockHandle,

    /// Open a table from a random-access file of `file_size` bytes. Reads and
    /// validates the footer, the index block, and (if present) the filter
    /// block. The caller retains ownership of `file` and must keep it alive for
    /// the lifetime of the returned Table; `deinit` does NOT close it.
    ///
    /// `block_cache` is an optional caller-owned block cache shared across
    /// tables; pass `null` for the original always-read-the-file behaviour.
    /// `cache_id` is a per-table id (e.g. its file number) mixed into cache
    /// keys so blocks do not collide across tables; it is ignored when
    /// `block_cache` is null.
    pub fn open(
        gpa: std.mem.Allocator,
        file: env.RandomAccessFile,
        file_size: u64,
        options: options_mod.Options,
        policy: bloom.BloomFilterPolicy,
        block_cache: ?*cache_mod.Cache,
        cache_id: u64,
    ) !Table {
        if (file_size < footer_mod.kEncodedLength) return error.Corruption;

        // ---- Footer (last 53 bytes) -------------------------------------
        var footer_buf: [footer_mod.kEncodedLength]u8 = undefined;
        try readFully(file, file_size - footer_mod.kEncodedLength, &footer_buf);
        const footer = try Footer.decodeFrom(&footer_buf);

        // ---- Index block ------------------------------------------------
        var index_contents = try readBlockCached(gpa, file, footer.index_handle, block_cache, cache_id);
        errdefer index_contents.release(gpa, block_cache);
        const index_block = try Block.init(gpa, index_contents.bytes);

        var self = Table{
            .gpa = gpa,
            .file = file,
            .comparator = options.comparator,
            .policy = policy,
            .prefix_extractor = options.prefix_extractor,
            .index_contents = index_contents,
            .index_block = index_block,
            .filter_contents = null,
            .filter_reader = null,
            .full_filter_reader = null,
            .block_cache = block_cache,
            .cache_id = cache_id,
            .metaindex_handle = footer.metaindex_handle,
        };

        // ---- Metaindex block -> filter block ----------------------------
        try self.readFilter(footer.metaindex_handle);
        return self;
    }

    pub fn deinit(self: *Table) void {
        if (self.filter_contents) |*fc| fc.release(self.gpa, self.block_cache);
        self.index_contents.release(self.gpa, self.block_cache);
        self.* = undefined;
    }

    /// Read the block at `handle`, consulting/populating this table's optional
    /// block cache. Returns a `BlockContents` that either owns the decoded
    /// buffer (no/failed cache) or borrows it behind a pinned cache handle. The
    /// caller MUST `release` the result with `self.gpa` and `self.block_cache`.
    fn readBlockContents(self: *Table, handle: BlockHandle) !BlockContents {
        return readBlockCached(self.gpa, self.file, handle, self.block_cache, self.cache_id);
    }

    /// Read the metaindex block, look for a filter entry, and if present read
    /// the filter block and install the matching reader.  Auto-detects the
    /// format (fulllocalbloom gate): a `"fullfilter."++name` entry installs a
    /// FastLocalBloom `FullFilterReader`; otherwise a `"filter."++name` entry
    /// installs the legacy block-based `FilterBlockReader`.  A missing filter
    /// entry leaves the table working without bloom filtering.  The reader's
    /// own `options.filter_mode` is irrelevant here — what is on disk wins, so
    /// reopening a DB with a different mode never misreads old SSTs.
    fn readFilter(self: *Table, metaindex_handle: BlockHandle) !void {
        var meta_contents = try self.readBlockContents(metaindex_handle);
        defer meta_contents.release(self.gpa, self.block_cache);
        const meta_block = try Block.init(self.gpa, meta_contents.bytes);

        // The metaindex is built with the BYTEWISE comparator over plain meta
        // keys (NOT internal keys), so it must be searched bytewise — using the
        // table's main comparator (an IKC for DB SSTs, which strips a trailer)
        // would mis-order/mis-match the meta keys.

        // 1. Prefer the FastLocalBloom full filter ("fullfilter."++name).
        {
            var key_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer key_buf.deinit(self.gpa);
            try key_buf.appendSlice(self.gpa, "fullfilter.");
            try key_buf.appendSlice(self.gpa, self.policy.name());

            var it = meta_block.iterator(comparator.bytewise);
            defer it.deinit();
            it.seek(key_buf.items);
            if (it.valid() and comparator.bytewise.compare(it.key(), key_buf.items) == .eq) {
                var hv: []const u8 = it.value();
                const filter_handle = try BlockHandle.decodeFrom(&hv);
                const filter_contents = try self.readBlockContents(filter_handle);
                self.filter_contents = filter_contents;
                self.full_filter_reader = FullFilterReader.init(filter_contents.bytes);
                return;
            }
        }

        // 2. Fall back to the legacy block-based filter ("filter."++name).
        {
            var key_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer key_buf.deinit(self.gpa);
            try key_buf.appendSlice(self.gpa, "filter.");
            try key_buf.appendSlice(self.gpa, self.policy.name());

            var it = meta_block.iterator(comparator.bytewise);
            defer it.deinit();
            it.seek(key_buf.items);
            if (!it.valid() or comparator.bytewise.compare(it.key(), key_buf.items) != .eq) {
                // No filter for this policy: reader works without bloom.
                return;
            }

            var hv: []const u8 = it.value();
            const filter_handle = try BlockHandle.decodeFrom(&hv);
            const filter_contents = try self.readBlockContents(filter_handle);
            self.filter_contents = filter_contents;
            self.filter_reader = FilterBlockReader.init(self.policy, filter_contents.bytes);
        }
    }

    /// Read this table's range tombstones (M7.5) by scanning the metaindex for
    /// the `"rocksdb.range_del"` entry and parsing its block.  Returns a freshly
    /// initialized `RangeTombstoneList` the CALLER OWNS (must `deinit`); an empty
    /// list when the table carries no range-del block.
    pub fn rangeTombstones(self: *Table, gpa: std.mem.Allocator) !delete_range.RangeTombstoneList {
        var meta_contents = try self.readBlockContents(self.metaindex_handle);
        defer meta_contents.release(self.gpa, self.block_cache);
        const meta_block = try Block.init(self.gpa, meta_contents.bytes);

        // Metaindex keys are plain bytewise meta keys; search bytewise.
        var it = meta_block.iterator(comparator.bytewise);
        defer it.deinit();
        it.seek("rocksdb.range_del");
        if (!it.valid() or comparator.bytewise.compare(it.key(), "rocksdb.range_del") != .eq) {
            // No range-del block: an empty tombstone list.
            return delete_range.RangeTombstoneList.init(gpa);
        }

        var hv: []const u8 = it.value();
        const handle = try BlockHandle.decodeFrom(&hv);
        var rd_contents = try self.readBlockContents(handle);
        defer rd_contents.release(self.gpa, self.block_cache);
        return delete_range.RangeTombstoneList.decode(gpa, rd_contents.bytes);
    }

    /// Point lookup. Returns a freshly allocated copy of the value (caller owns
    /// and must free with `gpa`) or null if the key is absent.
    pub fn get(self: *Table, gpa: std.mem.Allocator, key: []const u8) !?[]u8 {
        // Locate the first index entry whose key >= key: that data block is the
        // only one that may contain `key`.
        var index_it = self.index_block.iterator(self.comparator);
        defer index_it.deinit();
        index_it.seek(key);
        if (!index_it.valid()) return null;

        var hv: []const u8 = index_it.value();
        const handle = try BlockHandle.decodeFrom(&hv);

        // Bloom fast-path: if the filter proves the key absent, skip the read.
        // M7.2: a prefix-keyed filter is probed by the lookup key's PREFIX.  We
        // compute prefix = transform(extractUserKey(internal_key)) and prune via
        // prefixMayMatch — but ONLY when the user key is in the extractor's
        // domain (out-of-domain keys were never added to the filter, so pruning
        // them would be a false negative).  Without a prefix extractor, fall back
        // to the whole-key probe.
        if (self.filter_reader) |*fr| {
            if (self.prefix_extractor) |pe| {
                const user_key = internal_key.extractUserKey(key);
                if (pe.inDomain(user_key)) {
                    if (!fr.prefixMayMatch(handle.offset, pe.transform(user_key))) return null;
                }
                // out-of-domain: cannot prune; fall through to read the block.
            } else {
                if (!fr.keyMayMatch(handle.offset, key)) return null;
            }
        } else if (self.full_filter_reader) |*ffr| {
            // FastLocalBloom full filter (fulllocalbloom): a single filter over
            // every key, probed independently of the data-block offset.
            if (self.prefix_extractor) |pe| {
                const user_key = internal_key.extractUserKey(key);
                if (pe.inDomain(user_key)) {
                    if (!ffr.prefixMayMatch(pe.transform(user_key))) return null;
                }
                // out-of-domain: cannot prune; fall through to read the block.
            } else {
                if (!ffr.keyMayMatch(key)) return null;
            }
        }

        var data_contents = try self.readBlockContents(handle);
        defer data_contents.release(self.gpa, self.block_cache);
        const data_block = try Block.init(self.gpa, data_contents.bytes);
        var it = data_block.iterator(self.comparator);
        defer it.deinit();
        it.seek(key);
        if (it.valid() and self.comparator.compare(it.key(), key) == .eq) {
            return try gpa.dupe(u8, it.value());
        }
        return null;
    }

    pub fn iterator(self: *Table, gpa: std.mem.Allocator) Iterator {
        return Iterator.init(self, gpa);
    }

    /// Two-level forward/seek iterator over all (key, value) pairs in the table.
    ///
    /// VALUE-OWNERSHIP CONTRACT: `key()` and `value()` return slices that point
    /// into the CURRENT data block's buffer (and the inner iterator's
    /// reconstructed-key buffer). They are valid only until the next call to
    /// `next()`/`seek()`/`seekToFirst()` that crosses a data-block boundary, or
    /// until `deinit()`. Copy them if you need to retain them.
    pub const Iterator = struct {
        table: *Table,
        gpa: std.mem.Allocator,
        /// Outer iterator over the index block.
        index_it: Block.Iter,
        /// Contents of the current data block (null when not positioned); owned
        /// outright or pinned in the block cache, released via `releaseData`.
        data_contents: ?BlockContents,
        /// Parsed current data block (valid while data_contents != null).
        data_block: Block,
        /// Inner iterator over the current data block.
        data_it: ?Block.Iter,
        /// A read/parse error encountered while positioning, if any.
        err: ?anyerror,

        pub fn init(table: *Table, gpa: std.mem.Allocator) Iterator {
            return .{
                .table = table,
                .gpa = gpa,
                .index_it = table.index_block.iterator(table.comparator),
                .data_contents = null,
                .data_block = undefined,
                .data_it = null,
                .err = null,
            };
        }

        pub fn deinit(self: *Iterator) void {
            self.releaseData();
            self.index_it.deinit();
            self.* = undefined;
        }

        /// Free the current data block + inner iterator (if any).
        fn releaseData(self: *Iterator) void {
            if (self.data_it) |*di| {
                di.deinit();
                self.data_it = null;
            }
            if (self.data_contents) |*dc| {
                dc.release(self.table.gpa, self.table.block_cache);
                self.data_contents = null;
            }
        }

        /// Load the data block referenced by the current index entry and create
        /// its inner iterator (left unpositioned). Records an error on failure.
        fn loadDataBlock(self: *Iterator) void {
            self.releaseData();
            var hv: []const u8 = self.index_it.value();
            const handle = BlockHandle.decodeFrom(&hv) catch |e| {
                self.err = e;
                return;
            };
            var contents = self.table.readBlockContents(handle) catch |e| {
                self.err = e;
                return;
            };
            self.data_block = Block.init(self.table.gpa, contents.bytes) catch |e| {
                self.err = e;
                contents.release(self.table.gpa, self.table.block_cache);
                return;
            };
            self.data_contents = contents;
            self.data_it = self.data_block.iterator(self.table.comparator);
        }

        pub fn seekToFirst(self: *Iterator) void {
            self.err = null;
            self.index_it.seekToFirst();
            if (!self.index_it.valid()) {
                self.releaseData();
                return;
            }
            self.loadDataBlock();
            if (self.data_it) |*di| di.seekToFirst();
            self.skipEmptyDataBlocksForward();
        }

        pub fn seek(self: *Iterator, target: []const u8) void {
            self.err = null;
            self.index_it.seek(target);
            if (!self.index_it.valid()) {
                self.releaseData();
                return;
            }
            self.loadDataBlock();
            if (self.data_it) |*di| di.seek(target);
            self.skipEmptyDataBlocksForward();
        }

        pub fn next(self: *Iterator) void {
            std.debug.assert(self.valid());
            if (self.data_it) |*di| di.next();
            self.skipEmptyDataBlocksForward();
        }

        /// If the current inner iterator is exhausted, advance the outer index
        /// iterator to the next data block and position its inner iterator at
        /// the first entry. Repeats across empty blocks.
        fn skipEmptyDataBlocksForward(self: *Iterator) void {
            while (self.err == null) {
                if (self.data_it) |*di| {
                    if (di.valid()) return;
                }
                // Inner exhausted (or no data block): advance the outer.
                if (!self.index_it.valid()) {
                    self.releaseData();
                    return;
                }
                self.index_it.next();
                if (!self.index_it.valid()) {
                    self.releaseData();
                    return;
                }
                self.loadDataBlock();
                if (self.data_it) |*di| di.seekToFirst();
            }
        }

        pub fn valid(self: *const Iterator) bool {
            if (self.err != null) return false;
            if (self.data_it) |di| return di.valid();
            return false;
        }

        pub fn key(self: *const Iterator) []const u8 {
            std.debug.assert(self.valid());
            return self.data_it.?.key();
        }

        pub fn value(self: *const Iterator) []const u8 {
            std.debug.assert(self.valid());
            return self.data_it.?.value();
        }

        pub fn status(self: *const Iterator) ?anyerror {
            return self.err;
        }
    };
};

/// Read exactly `buf.len` bytes from `file` at `offset` (looping over short
/// reads). Returns error.Corruption if EOF is hit before the buffer is full.
fn readFully(file: env.RandomAccessFile, offset: u64, buf: []u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try file.readAt(offset + total, buf[total..]);
        if (n == 0) return error.Corruption; // unexpected EOF
        total += n;
    }
}

/// Build the 16-byte cache key for a block: cache_id ++ offset, both LE.
fn blockCacheKey(cache_id: u64, offset: u64) [16]u8 {
    var key: [16]u8 = undefined;
    coding.encodeFixed64(key[0..8], cache_id);
    coding.encodeFixed64(key[8..16], offset);
    return key;
}

/// Decoded contents of one block, plus the ownership token needed to release
/// them. `bytes` is always the (decompressed) block payload to parse; the
/// `owner` variant says how to free/release it:
///   - `.owned`: a `gpa.alloc`'d buffer this struct must `gpa.free` (no cache,
///     or a best-effort cache insert that failed);
///   - `.pinned`: a cache-owned buffer borrowed behind a pinned handle that
///     this struct must `cache.release` (zero-copy hit OR post-insert handle).
/// Callers MUST call `release` exactly once with the same gpa/cache used to
/// produce the contents.
pub const BlockContents = struct {
    bytes: []const u8,
    owner: union(enum) {
        owned: []u8,
        pinned: *cache_mod.Cache.Handle,
    },

    pub fn release(self: *BlockContents, gpa: std.mem.Allocator, block_cache: ?*cache_mod.Cache) void {
        switch (self.owner) {
            .owned => |buf| gpa.free(buf),
            .pinned => |h| block_cache.?.release(h),
        }
        self.* = undefined;
    }
};

/// Read the block at `handle` (contents + 5-byte trailer), verify the trailer's
/// compression type and masked crc32c, and return a `BlockContents` over the
/// decoded payload (consolidates the former D3c-1/D3a-M3 dup-on-hit paths).
///
/// Zero-copy pinned-handle cache: a HIT pins the cached entry and borrows its
/// buffer (no copy, no file read, no CRC verify). A MISS reads + verifies, then
/// inserts the freshly decoded buffer into the cache (the cache TAKES OWNERSHIP)
/// and borrows it back behind the pinned insert handle — so the returned bytes
/// and the cached bytes are the SAME memory. With a null cache (or if the
/// best-effort insert fails) the contents simply own the decoded buffer.
fn readBlockCached(
    gpa: std.mem.Allocator,
    file: env.RandomAccessFile,
    handle: BlockHandle,
    block_cache: ?*cache_mod.Cache,
    cache_id: u64,
) !BlockContents {
    if (block_cache) |bc| {
        const key = blockCacheKey(cache_id, handle.offset);
        if (bc.lookup(&key)) |h| {
            // Zero-copy hit: borrow the cached buffer behind the pinned handle.
            return .{ .bytes = bc.value(h), .owner = .{ .pinned = h } };
        }
    }

    const owned = try readBlockRaw(gpa, file, handle);
    if (block_cache) |bc| {
        // Hand the decoded buffer to the cache (it takes ownership), then borrow
        // it back behind the returned pinned handle — no copy on either side.
        const key = blockCacheKey(cache_id, handle.offset);
        const h = bc.insert(&key, owned, owned.len) catch {
            // Best-effort: cache full/OOM -> just own the buffer ourselves.
            return .{ .bytes = owned, .owner = .{ .owned = owned } };
        };
        return .{ .bytes = bc.value(h), .owner = .{ .pinned = h } };
    }
    return .{ .bytes = owned, .owner = .{ .owned = owned } };
}

/// Read the block at `handle` (on-disk payload + 5-byte trailer), verify the
/// trailer's masked crc32c (over the ON-DISK payload, exactly as the builder
/// wrote it), decompress when the trailer marks the block Snappy, and return the
/// OWNED uncompressed contents (caller frees with `gpa`). Always hits the file
/// (no cache).
///
/// `handle.size` is the on-disk payload length — for a compressed block that is
/// the COMPRESSED length, so the read window is sized off it directly.
fn readBlockRaw(gpa: std.mem.Allocator, file: env.RandomAccessFile, handle: BlockHandle) ![]u8 {
    const size: usize = @intCast(handle.size);

    // Read the on-disk payload plus the 5-byte trailer in one positional read.
    const raw = try gpa.alloc(u8, size + kBlockTrailerSize);
    defer gpa.free(raw);
    try readFully(file, handle.offset, raw);

    const payload = raw[0..size];
    const trailer = raw[size..][0..kBlockTrailerSize];

    // trailer[0] = compression type; trailer[1..5] = fixed32_LE(masked crc32c).
    // The CRC always covers the on-disk (possibly compressed) payload.
    const compression_type = trailer[0];
    const stored_masked = coding.decodeFixed32(trailer[1..5]);
    const expected = crc32c.extend(crc32c.value(payload), &[_]u8{compression_type});
    if (crc32c.unmask(stored_masked) != expected) return error.Corruption;

    return switch (compression_type) {
        kNoCompression => try gpa.dupe(u8, payload),
        kSnappyCompression => snappy.decompress(gpa, payload) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            // Malformed Snappy payload behaves like block corruption.
            error.Corrupt, error.InputTooLarge => return error.Corruption,
        },
        else => error.NotSupported,
    };
}

// ===========================================================================
// Tests — the round-trip gate against TableBuilder via MemEnv.
// ===========================================================================

const testing = std.testing;
const table_builder = @import("table_builder.zig");
const TableBuilder = table_builder.TableBuilder;
const prefix_mod = @import("../rocks/prefix.zig");

const KV = struct { k: []const u8, v: []const u8 };

/// Build a sorted set of `n` entries with small keys/values.
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

/// Build a multi-data-block SST from `entries` into MemEnv at `path`.
fn buildTable(
    gpa: std.mem.Allocator,
    e: env.Env,
    path: []const u8,
    opts: options_mod.Options,
    policy: bloom.BloomFilterPolicy,
    entries: []const KV,
) !void {
    var wf = try e.newWritableFile(gpa, path);
    errdefer wf.close() catch {};
    var tb = try TableBuilder.init(gpa, opts, wf, policy);
    defer tb.deinit();
    for (entries) |kv| try tb.add(kv.k, kv.v);
    try tb.finish();
    try wf.close();
}

/// Read the whole file from the Env into an owned buffer (caller frees).
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

test "table reader: get round-trip for every key, absent keys return null" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{ .block_size = 200, .block_restart_interval = 4 };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 50);
    defer freeEntries(gpa, &entries);

    try buildTable(gpa, e, "rt.sst", opts, policy, entries.items);

    const file_size = try e.getFileSize("rt.sst");
    var raf = try e.newRandomAccessFile(gpa, "rt.sst");
    defer raf.close() catch {};

    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    // Every inserted key round-trips to its exact value.
    for (entries.items) |kv| {
        const got = try table.get(gpa, kv.k);
        try testing.expect(got != null);
        defer gpa.free(got.?);
        try testing.expectEqualStrings(kv.v, got.?);
    }

    // Absent keys return null (must never miss a present key, exercised above).
    const absent = [_][]const u8{ "key99999", "aardvark", "zzz", "key00050", "" };
    for (absent) |k| {
        const got = try table.get(gpa, k);
        if (got) |g| {
            gpa.free(g);
            try testing.expect(false); // should have been null
        }
    }
}

test "table reader: full scan yields all entries in sorted order" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{ .block_size = 200, .block_restart_interval = 4 };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 50);
    defer freeEntries(gpa, &entries);

    try buildTable(gpa, e, "scan.sst", opts, policy, entries.items);

    const file_size = try e.getFileSize("scan.sst");
    var raf = try e.newRandomAccessFile(gpa, "scan.sst");
    defer raf.close() catch {};

    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    var it = table.iterator(gpa);
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
    try testing.expect(it.status() == null);
}

test "table reader: seek present, between, past end" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{ .block_size = 200, .block_restart_interval = 4 };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 50);
    defer freeEntries(gpa, &entries);

    try buildTable(gpa, e, "seek.sst", opts, policy, entries.items);

    const file_size = try e.getFileSize("seek.sst");
    var raf = try e.newRandomAccessFile(gpa, "seek.sst");
    defer raf.close() catch {};

    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    var it = table.iterator(gpa);
    defer it.deinit();

    // Seek to a present key (one in a later data block).
    it.seek(entries.items[25].k);
    try testing.expect(it.valid());
    try testing.expectEqualStrings(entries.items[25].k, it.key());
    try testing.expectEqualStrings(entries.items[25].v, it.value());

    // Seek to a between-key -> next greater. "key00010x" sorts between
    // key00010 and key00011.
    it.seek("key00010x");
    try testing.expect(it.valid());
    try testing.expectEqualStrings(entries.items[11].k, it.key());

    // Seek before first -> first entry.
    it.seek("");
    try testing.expect(it.valid());
    try testing.expectEqualStrings(entries.items[0].k, it.key());

    // Seek past end -> invalid.
    it.seek("zzzzzzzz");
    try testing.expect(!it.valid());

    // Seek to exactly the last key.
    it.seek(entries.items[entries.items.len - 1].k);
    try testing.expect(it.valid());
    try testing.expectEqualStrings(entries.items[entries.items.len - 1].k, it.key());
    it.next();
    try testing.expect(!it.valid());
}

test "table reader: corruption inside a data block is detected" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{ .block_size = 200, .block_restart_interval = 4 };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 50);
    defer freeEntries(gpa, &entries);

    try buildTable(gpa, e, "good.sst", opts, policy, entries.items);

    // Slurp the file bytes, flip a byte near the start (inside the first data
    // block, which begins at offset 0), and write the result to a fresh file.
    const bytes = try readAllFile(e, gpa, "good.sst");
    defer gpa.free(bytes);
    bytes[5] ^= 0xFF; // corrupt a byte well inside the first data block

    {
        var wf = try e.newWritableFile(gpa, "bad.sst");
        errdefer wf.close() catch {};
        try wf.append(bytes);
        try wf.close();
    }

    const file_size = try e.getFileSize("bad.sst");
    var raf = try e.newRandomAccessFile(gpa, "bad.sst");
    defer raf.close() catch {};

    // Footer/index/filter are intact -> open succeeds.
    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    // Reading the corrupted first data block must surface error.Corruption,
    // never a wrong value or a crash. The first key lives in the first block;
    // its filter still reports may-match so the data block is actually read.
    const r = table.get(gpa, entries.items[0].k);
    try testing.expectError(error.Corruption, r);

    // Iterating into the corrupted block also reports the error (not valid,
    // status set), rather than crashing.
    var it = table.iterator(gpa);
    defer it.deinit();
    it.seekToFirst();
    try testing.expect(!it.valid());
    try testing.expect(it.status() != null);
}

test "table reader: single small block round-trips" {
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

    try buildTable(gpa, e, "one.sst", opts, policy, &pairs);

    const file_size = try e.getFileSize("one.sst");
    var raf = try e.newRandomAccessFile(gpa, "one.sst");
    defer raf.close() catch {};

    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    for (pairs) |p| {
        const got = try table.get(gpa, p.k);
        try testing.expect(got != null);
        defer gpa.free(got.?);
        try testing.expectEqualStrings(p.v, got.?);
    }

    var it = table.iterator(gpa);
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

test "table reader: snappy round-trip — get + full scan identical to uncompressed, blocks shrink on disk" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const policy = bloom.BloomFilterPolicy.init(10);

    // Highly compressible payloads so data blocks actually shrink.
    var entries: std.ArrayListUnmanaged(KV) = .empty;
    defer freeEntries(gpa, &entries);
    {
        var i: usize = 0;
        while (i < 60) : (i += 1) {
            const k = try std.fmt.allocPrint(gpa, "key{d:0>5}", .{i});
            const v = try std.fmt.allocPrint(gpa, "{s}", .{"Z" ** 90});
            try entries.append(gpa, .{ .k = k, .v = v });
        }
    }

    const base = options_mod.Options{ .block_size = 256, .block_restart_interval = 4 };
    const snap = options_mod.Options{ .block_size = 256, .block_restart_interval = 4, .compression = .snappy };

    try buildTable(gpa, e, "plain.sst", base, policy, entries.items);
    try buildTable(gpa, e, "snap.sst", snap, policy, entries.items);

    // The compressed SST is smaller on disk than the uncompressed one.
    try testing.expect((try e.getFileSize("snap.sst")) < (try e.getFileSize("plain.sst")));

    const snap_size = try e.getFileSize("snap.sst");
    var raf = try e.newRandomAccessFile(gpa, "snap.sst");
    defer raf.close() catch {};
    var table = try Table.open(gpa, raf, snap_size, snap, policy, null, 0);
    defer table.deinit();

    // get: every key round-trips to its exact value.
    for (entries.items) |kv| {
        const got = try table.get(gpa, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
    // Absent key returns null.
    {
        const got = try table.get(gpa, "key99999");
        if (got) |g| {
            gpa.free(g);
            return error.TestExpectedNull;
        }
    }

    // Full scan yields all entries in order.
    var it = table.iterator(gpa);
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
    try testing.expect(it.status() == null);
}

test "table reader: snappy round-trip through the block cache (hits on reread)" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const policy = bloom.BloomFilterPolicy.init(10);
    const opts = options_mod.Options{ .block_size = 256, .block_restart_interval = 4, .compression = .snappy };

    var entries: std.ArrayListUnmanaged(KV) = .empty;
    defer freeEntries(gpa, &entries);
    {
        var i: usize = 0;
        while (i < 40) : (i += 1) {
            const k = try std.fmt.allocPrint(gpa, "key{d:0>5}", .{i});
            const v = try std.fmt.allocPrint(gpa, "{s}", .{"Q" ** 64});
            try entries.append(gpa, .{ .k = k, .v = v });
        }
    }
    try buildTable(gpa, e, "snapc.sst", opts, policy, entries.items);

    const file_size = try e.getFileSize("snapc.sst");
    var cache = cache_mod.Cache.init(gpa, e.io(), 1 << 20);
    defer cache.deinit();

    var raf = try e.newRandomAccessFile(gpa, "snapc.sst");
    defer raf.close() catch {};
    var table = try Table.open(gpa, raf, file_size, opts, policy, &cache, 3);
    defer table.deinit();

    // First pass populates the cache with DECOMPRESSED blocks.
    for (entries.items) |kv| {
        const got = try table.get(gpa, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
    const hits_before = cache.hitCount();
    // Second pass: cache hits serve the decompressed blocks; values identical.
    for (entries.items) |kv| {
        const got = try table.get(gpa, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
    try testing.expect(cache.hitCount() > hits_before);
}

test "table reader: cache hit serves the SAME buffer (zero-copy pinned handle)" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{ .block_size = 200, .block_restart_interval = 4 };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 50);
    defer freeEntries(gpa, &entries);

    try buildTable(gpa, e, "pin.sst", opts, policy, entries.items);

    const file_size = try e.getFileSize("pin.sst");
    var cache = cache_mod.Cache.init(gpa, e.io(), 1 << 20);
    defer cache.deinit();

    var raf = try e.newRandomAccessFile(gpa, "pin.sst");
    defer raf.close() catch {};
    var table = try Table.open(gpa, raf, file_size, opts, policy, &cache, 11);
    defer table.deinit();

    // First read of a data block: a MISS that inserts into the cache.
    const handle = blk: {
        var index_it = table.index_block.iterator(table.comparator);
        defer index_it.deinit();
        index_it.seekToFirst();
        try testing.expect(index_it.valid());
        var hv: []const u8 = index_it.value();
        break :blk try BlockHandle.decodeFrom(&hv);
    };

    // Prime the cache by reading the block once (the bytes are owned/copied out).
    {
        var bc = try table.readBlockContents(handle);
        defer bc.release(table.gpa, table.block_cache);
        try testing.expect(bc.bytes.len > 0);
    }

    // Capture the pointer the cache itself stores for this block.
    const key = blockCacheKey(11, handle.offset);
    const cached_ptr = blk: {
        const h = cache.lookup(&key) orelse return error.TestExpectedFound;
        defer cache.release(h);
        break :blk cache.value(h).ptr;
    };

    // A subsequent read is a HIT: it must borrow the SAME buffer, not a copy.
    {
        var bc = try table.readBlockContents(handle);
        defer bc.release(table.gpa, table.block_cache);
        try testing.expectEqual(cached_ptr, bc.bytes.ptr); // zero-copy: same memory
    }
}

// ===========================================================================
// M7.2 — prefix bloom filter: build over key prefixes, prune by prefix.
// ===========================================================================

/// Encode `user ++ fixed64(packSequenceAndType(seq, .value))` (caller frees).
fn encodeIkey(gpa: std.mem.Allocator, user: []const u8, seq: u64) ![]u8 {
    const out = try gpa.alloc(u8, user.len + 8);
    @memcpy(out[0..user.len], user);
    coding.encodeFixed64(out[user.len..][0..8], internal_key.packSequenceAndType(seq, .value));
    return out;
}

test "table reader: prefix bloom — no false negatives, prunes absent prefixes, no data lost" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // SST stores INTERNAL keys; open/build with the InternalKeyComparator AND a
    // 3-byte fixed prefix extractor so the filter is built over key prefixes.
    var ikc = internal_key.InternalKeyComparator{ .user = comparator.bytewise };
    var fpe = prefix_mod.FixedPrefixExtractor.init(3);
    const opts = options_mod.Options{
        .comparator = ikc.comparatorInterface(),
        .prefix_extractor = fpe.extractor(),
    };
    const policy = bloom.BloomFilterPolicy.init(10);

    // User keys sharing two prefixes: "abc*" and "xyz*".  Internal-key order is
    // user asc then seq desc; build them sorted.
    const users = [_][]const u8{ "abc1", "abc2", "abc3", "xyz1", "xyz2" };
    var ikeys: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (ikeys.items) |k| gpa.free(k);
        ikeys.deinit(gpa);
    }

    {
        var wf = try e.newWritableFile(gpa, "pfx.sst");
        errdefer wf.close() catch {};
        var tb = try TableBuilder.init(gpa, opts, wf, policy);
        defer tb.deinit();
        for (users) |u| {
            const ik = try encodeIkey(gpa, u, 1);
            try ikeys.append(gpa, ik);
            try tb.add(ik, u); // value = the user key for easy verification
        }
        try tb.finish();
        try wf.close();
    }

    const file_size = try e.getFileSize("pfx.sst");
    var raf = try e.newRandomAccessFile(gpa, "pfx.sst");
    defer raf.close() catch {};

    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    // 1. NO DATA LOST: every inserted key round-trips (prefix filtering must
    //    never cause a false negative for a present key).
    for (users, ikeys.items) |u, ik| {
        const got = try table.get(gpa, ik) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(u, got);
    }

    // 2. Absent key WITH A PRESENT PREFIX ("abc9"): filter says maybe → block is
    //    read → not found → null, no error.
    {
        const ik = try encodeIkey(gpa, "abc9", 1);
        defer gpa.free(ik);
        const got = try table.get(gpa, ik);
        if (got) |g| {
            gpa.free(g);
            return error.TestExpectedNull;
        }
    }

    // 3. Absent key whose PREFIX is absent ("qqq9"): the prefix filter prunes it
    //    → null without error (no block read needed, but correctness holds).
    {
        const ik = try encodeIkey(gpa, "qqq9", 1);
        defer gpa.free(ik);
        const got = try table.get(gpa, ik);
        if (got) |g| {
            gpa.free(g);
            return error.TestExpectedNull;
        }
    }
}

// ===========================================================================
// M7.5 — range-del meta block: builder writes it, reader parses it back.
// ===========================================================================

test "M7.5: range tombstones round-trip through the SST range-del meta block" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    var ikc = internal_key.InternalKeyComparator{ .user = comparator.bytewise };
    const opts = options_mod.Options{ .comparator = ikc.comparatorInterface() };
    const policy = bloom.BloomFilterPolicy.init(10);

    // Build a table with a couple of point keys AND two range tombstones.
    {
        var wf = try e.newWritableFile(gpa, "rd.sst");
        errdefer wf.close() catch {};
        var tb = try TableBuilder.init(gpa, opts, wf, policy);
        defer tb.deinit();

        const a = try encodeIkey(gpa, "a", 1);
        defer gpa.free(a);
        const m = try encodeIkey(gpa, "m", 2);
        defer gpa.free(m);
        try tb.add(a, "av");
        try tb.add(m, "mv");

        try tb.addRangeTombstone("b", "d", 10);
        try tb.addRangeTombstone("f", "h", 20);

        try tb.finish();
        try wf.close();
    }

    const file_size = try e.getFileSize("rd.sst");
    var raf = try e.newRandomAccessFile(gpa, "rd.sst");
    defer raf.close() catch {};

    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    var rtl = try table.rangeTombstones(gpa);
    defer rtl.deinit();
    try testing.expectEqual(@as(usize, 2), rtl.count());
    try testing.expectEqualStrings("b", rtl.tombstones.items[0].begin);
    try testing.expectEqualStrings("d", rtl.tombstones.items[0].end);
    try testing.expectEqual(@as(u64, 10), rtl.tombstones.items[0].seq);
    try testing.expectEqualStrings("f", rtl.tombstones.items[1].begin);
    try testing.expectEqualStrings("h", rtl.tombstones.items[1].end);
    try testing.expectEqual(@as(u64, 20), rtl.tombstones.items[1].seq);

    // The point keys still read back normally.
    {
        const ik = try encodeIkey(gpa, "a", 1);
        defer gpa.free(ik);
        const got = try table.get(gpa, ik) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("av", got);
    }
}

test "M7.5: a table with no range tombstones returns an empty list" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{};
    const policy = bloom.BloomFilterPolicy.init(10);

    const pairs = [_]KV{ .{ .k = "alpha", .v = "1" }, .{ .k = "beta", .v = "2" } };
    try buildTable(gpa, e, "nordsst.sst", opts, policy, &pairs);

    const file_size = try e.getFileSize("nordsst.sst");
    var raf = try e.newRandomAccessFile(gpa, "nordsst.sst");
    defer raf.close() catch {};
    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    var rtl = try table.rangeTombstones(gpa);
    defer rtl.deinit();
    try testing.expect(rtl.isEmpty());
}

// ===========================================================================
// partitioned-idx — two-level (partitioned) SST index round-trip.
// zrocks's OWN clean two-level format (see format/partitioned_index.zig), NOT
// RocksDB byte-exact.  A small metadata_block_size forces MULTIPLE partitions;
// get/iterate/seek must cross partition boundaries correctly.
// ===========================================================================

const partitioned_index = @import("partitioned_index.zig");

/// Read the on-disk `rocksdb.index_type` meta tag from a table's metaindex; null
/// when the entry is absent (a single-level table).
fn readIndexTypeTag(gpa: std.mem.Allocator, file: []const u8) !?u8 {
    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);
    const mstart: usize = @intCast(footer.metaindex_handle.offset);
    const msize: usize = @intCast(footer.metaindex_handle.size);
    const meta_contents = file[mstart .. mstart + msize];
    const meta_block = try Block.init(gpa, meta_contents);
    var it = meta_block.iterator(comparator.bytewise);
    defer it.deinit();
    it.seek(partitioned_index.kIndexTypeMetaKey);
    if (!it.valid() or comparator.bytewise.compare(it.key(), partitioned_index.kIndexTypeMetaKey) != .eq) {
        return null;
    }
    const v = it.value();
    if (v.len != 1) return error.Corruption;
    return v[0];
}

/// Verify a two-level SST: the footer's index_handle must be a TOP-LEVEL block
/// whose entries point at index PARTITION blocks (each itself an index block of
/// data-block handles).  Returns the number of partitions (top-level entries),
/// after asserting the on-disk index-type tag is `kTwoLevelTag` and that every
/// partition block parses with a non-zero entry count.
fn countIndexPartitions(gpa: std.mem.Allocator, e: env.Env, path: []const u8) !usize {
    const file = try readAllFile(e, gpa, path);
    defer gpa.free(file);

    // The on-disk tag must mark this table two-level.
    const tag = try readIndexTypeTag(gpa, file);
    try testing.expectEqual(@as(?u8, partitioned_index.kTwoLevelTag), tag);

    const footer = try Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);
    const start: usize = @intCast(footer.index_handle.offset);
    const size: usize = @intCast(footer.index_handle.size);
    const top_contents = file[start .. start + size];
    const top_block = try Block.init(gpa, top_contents);
    var it = top_block.iterator(comparator.bytewise);
    defer it.deinit();
    var n: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        // Each top-level value is a partition BlockHandle; the partition block
        // must itself be a parseable index block with >= 1 entry.
        var hv: []const u8 = it.value();
        const ph = try BlockHandle.decodeFrom(&hv);
        const pstart: usize = @intCast(ph.offset);
        const psize: usize = @intCast(ph.size);
        const part_block = try Block.init(gpa, file[pstart .. pstart + psize]);
        var pit = part_block.iterator(comparator.bytewise);
        defer pit.deinit();
        var pcount: usize = 0;
        pit.seekToFirst();
        while (pit.valid()) : (pit.next()) pcount += 1;
        try testing.expect(pcount >= 1);
        n += 1;
    }
    return n;
}

test "partitioned-idx: two-level index round-trips get/scan/seek across MULTIPLE partitions" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Small block_size -> many data blocks -> many index entries; tiny
    // metadata_block_size (128) -> MULTIPLE index partitions.
    const opts = options_mod.Options{
        .block_size = 128,
        .block_restart_interval = 2,
        .index_type = .two_level,
        .metadata_block_size = 128,
    };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 200);
    defer freeEntries(gpa, &entries);

    try buildTable(gpa, e, "two.sst", opts, policy, entries.items);

    // MULTIPLE index partitions must have been produced.
    const n_parts = try countIndexPartitions(gpa, e, "two.sst");
    try testing.expect(n_parts > 1);

    const file_size = try e.getFileSize("two.sst");
    var raf = try e.newRandomAccessFile(gpa, "two.sst");
    defer raf.close() catch {};
    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    // 1. get: every key round-trips to its exact value (across all partitions).
    for (entries.items) |kv| {
        const got = try table.get(gpa, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
    // Absent key returns null.
    {
        const got = try table.get(gpa, "key99999-absent");
        if (got) |g| {
            gpa.free(g);
            return error.TestExpectedNull;
        }
    }

    // 2. full forward scan yields all entries in order.
    {
        var it = table.iterator(gpa);
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
        try testing.expect(it.status() == null);
    }

    // 3. seek: present / between / before-first / past-end / exactly-last,
    //    landing correctly across partition boundaries.
    {
        var it = table.iterator(gpa);
        defer it.deinit();

        // A present key deep in a later partition.
        it.seek(entries.items[137].k);
        try testing.expect(it.valid());
        try testing.expectEqualStrings(entries.items[137].k, it.key());
        try testing.expectEqualStrings(entries.items[137].v, it.value());

        // Between two keys -> next greater. "key00099x" sorts between 99 and 100.
        it.seek("key00099x");
        try testing.expect(it.valid());
        try testing.expectEqualStrings(entries.items[100].k, it.key());

        // Before first -> first entry.
        it.seek("");
        try testing.expect(it.valid());
        try testing.expectEqualStrings(entries.items[0].k, it.key());

        // Past end -> invalid.
        it.seek("zzzzzzzz");
        try testing.expect(!it.valid());

        // Exactly the last key, then next -> invalid.
        it.seek(entries.items[entries.items.len - 1].k);
        try testing.expect(it.valid());
        try testing.expectEqualStrings(entries.items[entries.items.len - 1].k, it.key());
        it.next();
        try testing.expect(!it.valid());
    }
}

test "partitioned-idx: single small two-level table still well-formed (one partition)" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{ .index_type = .two_level, .metadata_block_size = 4096 };
    const policy = bloom.BloomFilterPolicy.init(10);

    const pairs = [_]KV{
        .{ .k = "alpha", .v = "1" },
        .{ .k = "beta", .v = "2" },
        .{ .k = "gamma", .v = "3" },
    };
    try buildTable(gpa, e, "twosmall.sst", opts, policy, &pairs);

    const file_size = try e.getFileSize("twosmall.sst");
    var raf = try e.newRandomAccessFile(gpa, "twosmall.sst");
    defer raf.close() catch {};
    var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
    defer table.deinit();

    for (pairs) |p| {
        const got = try table.get(gpa, p.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(p.v, got);
    }
    var it = table.iterator(gpa);
    defer it.deinit();
    var idx: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        try testing.expectEqualStrings(pairs[idx].k, it.key());
        idx += 1;
    }
    try testing.expectEqual(pairs.len, idx);
}

test "partitioned-idx: two-level reader auto-detects regardless of open Options.index_type" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const build_opts = options_mod.Options{
        .block_size = 128,
        .block_restart_interval = 2,
        .index_type = .two_level,
        .metadata_block_size = 128,
    };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 120);
    defer freeEntries(gpa, &entries);
    try buildTable(gpa, e, "auto.sst", build_opts, policy, entries.items);

    // Open with the DEFAULT single_level Options: the reader must still detect the
    // on-disk two-level index from the metaindex and read every key correctly.
    const open_opts = options_mod.Options{ .block_size = 128, .block_restart_interval = 2 };
    const file_size = try e.getFileSize("auto.sst");
    var raf = try e.newRandomAccessFile(gpa, "auto.sst");
    defer raf.close() catch {};
    var table = try Table.open(gpa, raf, file_size, open_opts, policy, null, 0);
    defer table.deinit();

    for (entries.items) |kv| {
        const got = try table.get(gpa, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
}

test "table reader: optional block cache yields identical results and hits on reread" {
    const gpa = testing.allocator;

    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = options_mod.Options{ .block_size = 200, .block_restart_interval = 4 };
    const policy = bloom.BloomFilterPolicy.init(10);

    var entries = try makeSortedEntries(gpa, 50);
    defer freeEntries(gpa, &entries);

    try buildTable(gpa, e, "cache.sst", opts, policy, entries.items);

    const file_size = try e.getFileSize("cache.sst");

    // ---- Baseline: no-cache reference results (full scan) ----------------
    var ref: std.ArrayListUnmanaged(KV) = .empty;
    defer {
        for (ref.items) |kv| {
            gpa.free(kv.k);
            gpa.free(kv.v);
        }
        ref.deinit(gpa);
    }
    {
        var raf = try e.newRandomAccessFile(gpa, "cache.sst");
        defer raf.close() catch {};
        var table = try Table.open(gpa, raf, file_size, opts, policy, null, 0);
        defer table.deinit();
        var it = table.iterator(gpa);
        defer it.deinit();
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            try ref.append(gpa, .{
                .k = try gpa.dupe(u8, it.key()),
                .v = try gpa.dupe(u8, it.value()),
            });
        }
        try testing.expect(it.status() == null);
    }

    // ---- Cached: same results, and hits on reread ------------------------
    var cache = cache_mod.Cache.init(gpa, e.io(), 1 << 20);
    defer cache.deinit();

    var raf = try e.newRandomAccessFile(gpa, "cache.sst");
    defer raf.close() catch {};
    var table = try Table.open(gpa, raf, file_size, opts, policy, &cache, 7);
    defer table.deinit();

    // First scan: populates the cache (misses).
    {
        var it = table.iterator(gpa);
        defer it.deinit();
        var idx: usize = 0;
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            try testing.expect(idx < ref.items.len);
            try testing.expectEqualStrings(ref.items[idx].k, it.key());
            try testing.expectEqualStrings(ref.items[idx].v, it.value());
            idx += 1;
        }
        try testing.expectEqual(ref.items.len, idx);
        try testing.expect(it.status() == null);
    }
    try testing.expect(cache.totalCharge() > 0);

    const hits_before = cache.hitCount();

    // Second scan: same blocks -> cache hits, identical results.
    {
        var it = table.iterator(gpa);
        defer it.deinit();
        var idx: usize = 0;
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            try testing.expectEqualStrings(ref.items[idx].k, it.key());
            try testing.expectEqualStrings(ref.items[idx].v, it.value());
            idx += 1;
        }
        try testing.expectEqual(ref.items.len, idx);
    }
    try testing.expect(cache.hitCount() > hits_before);

    // Point gets from the same blocks also identical + serviced from cache.
    const hits_before_get = cache.hitCount();
    for (entries.items) |kv| {
        const got = try table.get(gpa, kv.k);
        try testing.expect(got != null);
        defer gpa.free(got.?);
        try testing.expectEqualStrings(kv.v, got.?);
    }
    try testing.expect(cache.hitCount() > hits_before_get);
}
