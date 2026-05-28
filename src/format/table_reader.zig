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
/// No block cache: every block read hits the file (M3.5 will add caching).
const std = @import("std");

const footer_mod = @import("footer.zig");
const block = @import("block.zig");
const filter_block = @import("filter_block.zig");
const bloom = @import("bloom.zig");
const crc32c = @import("../util/crc32c.zig");
const coding = @import("../util/coding.zig");
const comparator = @import("../util/comparator.zig");
const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");

const BlockHandle = footer_mod.BlockHandle;
const Footer = footer_mod.Footer;
const Block = block.Block;
const FilterBlockReader = filter_block.FilterBlockReader;

/// kNoCompression — the only compression type the reader accepts.
pub const kNoCompression: u8 = 0;

/// Length of the 5-byte block trailer (compression type + masked crc32c).
const kBlockTrailerSize: usize = 5;

pub const Table = struct {
    gpa: std.mem.Allocator,
    file: env.RandomAccessFile,
    comparator: comparator.Comparator,
    policy: bloom.BloomFilterPolicy,

    /// Owned contents of the index block (kept resident for its lifetime).
    index_contents: []u8,
    index_block: Block,

    /// Owned contents of the filter block, if the table carries one.
    filter_contents: ?[]u8,
    filter_reader: ?FilterBlockReader,

    /// Open a table from a random-access file of `file_size` bytes. Reads and
    /// validates the footer, the index block, and (if present) the filter
    /// block. The caller retains ownership of `file` and must keep it alive for
    /// the lifetime of the returned Table; `deinit` does NOT close it.
    pub fn open(
        gpa: std.mem.Allocator,
        file: env.RandomAccessFile,
        file_size: u64,
        options: options_mod.Options,
        policy: bloom.BloomFilterPolicy,
    ) !Table {
        if (file_size < footer_mod.kEncodedLength) return error.Corruption;

        // ---- Footer (last 53 bytes) -------------------------------------
        var footer_buf: [footer_mod.kEncodedLength]u8 = undefined;
        try readFully(file, file_size - footer_mod.kEncodedLength, &footer_buf);
        const footer = try Footer.decodeFrom(&footer_buf);

        // ---- Index block ------------------------------------------------
        const index_contents = try readBlock(gpa, file, footer.index_handle);
        errdefer gpa.free(index_contents);
        const index_block = try Block.init(gpa, index_contents);

        var self = Table{
            .gpa = gpa,
            .file = file,
            .comparator = options.comparator,
            .policy = policy,
            .index_contents = index_contents,
            .index_block = index_block,
            .filter_contents = null,
            .filter_reader = null,
        };

        // ---- Metaindex block -> filter block ----------------------------
        try self.readFilter(footer.metaindex_handle);
        return self;
    }

    pub fn deinit(self: *Table) void {
        if (self.filter_contents) |fc| self.gpa.free(fc);
        self.gpa.free(self.index_contents);
        self.* = undefined;
    }

    /// Read the metaindex block, look for the filter entry, and if present read
    /// the filter block and install a FilterBlockReader. A missing filter entry
    /// leaves the table working without bloom filtering.
    fn readFilter(self: *Table, metaindex_handle: BlockHandle) !void {
        const meta_contents = try readBlock(self.gpa, self.file, metaindex_handle);
        defer self.gpa.free(meta_contents);
        const meta_block = try Block.init(self.gpa, meta_contents);

        // Build the lookup key "filter." ++ policy.name().
        var key_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer key_buf.deinit(self.gpa);
        try key_buf.appendSlice(self.gpa, "filter.");
        try key_buf.appendSlice(self.gpa, self.policy.name());

        var it = meta_block.iterator(self.comparator);
        defer it.deinit();
        it.seek(key_buf.items);
        if (!it.valid() or self.comparator.compare(it.key(), key_buf.items) != .eq) {
            // No filter for this policy: reader works without bloom.
            return;
        }

        var hv: []const u8 = it.value();
        const filter_handle = try BlockHandle.decodeFrom(&hv);
        const filter_contents = try readBlock(self.gpa, self.file, filter_handle);
        self.filter_contents = filter_contents;
        self.filter_reader = FilterBlockReader.init(self.policy, filter_contents);
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
        if (self.filter_reader) |*fr| {
            if (!fr.keyMayMatch(handle.offset, key)) return null;
        }

        const data_contents = try readBlock(self.gpa, self.file, handle);
        defer self.gpa.free(data_contents);
        const data_block = try Block.init(self.gpa, data_contents);
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
        /// Owned contents of the current data block (null when not positioned).
        data_contents: ?[]u8,
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
            if (self.data_contents) |dc| {
                self.gpa.free(dc);
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
            const contents = readBlock(self.table.gpa, self.table.file, handle) catch |e| {
                self.err = e;
                return;
            };
            self.data_contents = contents;
            self.data_block = Block.init(self.table.gpa, contents) catch |e| {
                self.err = e;
                self.gpa.free(contents);
                self.data_contents = null;
                return;
            };
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

/// Read the block at `handle` (contents + 5-byte trailer), verify the trailer's
/// compression type and masked crc32c, and return an OWNED copy of the contents
/// (caller frees with `gpa`).
fn readBlock(gpa: std.mem.Allocator, file: env.RandomAccessFile, handle: BlockHandle) ![]u8 {
    const size: usize = @intCast(handle.size);

    // Read the contents plus the 5-byte trailer in one positional read.
    const raw = try gpa.alloc(u8, size + kBlockTrailerSize);
    defer gpa.free(raw);
    try readFully(file, handle.offset, raw);

    const contents = raw[0..size];
    const trailer = raw[size..][0..kBlockTrailerSize];

    // trailer[0] = compression type; trailer[1..5] = fixed32_LE(masked crc32c).
    const compression_type = trailer[0];
    if (compression_type != kNoCompression) return error.NotSupported;

    const stored_masked = coding.decodeFixed32(trailer[1..5]);
    const expected = crc32c.extend(crc32c.value(contents), &[_]u8{compression_type});
    if (crc32c.unmask(stored_masked) != expected) return error.Corruption;

    return gpa.dupe(u8, contents);
}

// ===========================================================================
// Tests — the round-trip gate against TableBuilder via MemEnv.
// ===========================================================================

const testing = std.testing;
const table_builder = @import("table_builder.zig");
const TableBuilder = table_builder.TableBuilder;

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

    var table = try Table.open(gpa, raf, file_size, opts, policy);
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

    var table = try Table.open(gpa, raf, file_size, opts, policy);
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

    var table = try Table.open(gpa, raf, file_size, opts, policy);
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
    var table = try Table.open(gpa, raf, file_size, opts, policy);
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

    var table = try Table.open(gpa, raf, file_size, opts, policy);
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

const cache_mod = @import("../util/cache.zig");

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
    var cache = cache_mod.Cache.init(gpa, 1 << 20);
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

    const hits_before = cache.hits;

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
    try testing.expect(cache.hits > hits_before);

    // Point gets from the same blocks also identical + serviced from cache.
    const hits_before_get = cache.hits;
    for (entries.items) |kv| {
        const got = try table.get(gpa, kv.k);
        try testing.expect(got != null);
        defer gpa.free(got.?);
        try testing.expectEqualStrings(kv.v, got.?);
    }
    try testing.expect(cache.hits > hits_before_get);
}
