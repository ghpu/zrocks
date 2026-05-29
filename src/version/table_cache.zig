//! table_cache.zig — opens and caches SST `Table` readers by file number.
//!
//! The DB read path (Version.get / Version.addIterators) needs a `Table` reader
//! for an on-disk SST identified by its file number.  Opening a table reads and
//! validates the footer + index (and filter) block, so the TableCache keeps
//! opened readers resident, keyed by file number, and hands them back on reuse.
//!
//! Ownership
//! ---------
//! Each cached `*Table` is heap-allocated and OWNED by the TableCache, together
//! with the `env.RandomAccessFile` the table reads through (Table.deinit does
//! NOT close the file, so the cache closes it).  `deinit` closes + frees every
//! cached table and its file.
//!
//! `dbname`, `env`, `options`, and the optional block cache are BORROWED — the
//! owning DB must outlive the TableCache.
//!
//! There is no eviction yet — every opened table stays resident.
//! TODO: bound by options.max_open_files (LRU eviction).

const std = @import("std");

const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const cache_mod = @import("../util/cache.zig");
const bloom = @import("../format/bloom.zig");
const table_reader = @import("../format/table_reader.zig");
const filename = @import("filename.zig");
const iterator = @import("../iterator/iterator.zig");
const internal_key = @import("../format/internal_key.zig");

const Table = table_reader.Table;

pub const TableCache = struct {
    gpa: std.mem.Allocator,
    env: env.Env,
    /// Borrowed DB directory name (the owning DB outlives this cache).
    dbname: []const u8,
    options: options_mod.Options,
    /// InternalKeyComparator wrapping `options.comparator`.  SST files store
    /// INTERNAL keys (user_key ++ trailer), so tables must be opened with this
    /// comparator — not the bare user comparator — for seeks to land correctly.
    /// Its address is taken into each opened Table's comparator, so it must live
    /// at a stable address: callers MUST keep the TableCache pinned (the DB
    /// embeds it in its heap-allocated struct; tests keep it on the stack for
    /// the test's duration and never move it after the first findTable).
    ikcmp: internal_key.InternalKeyComparator,
    /// Optional shared block cache passed through to each opened Table.
    block_cache: ?*cache_mod.Cache,
    /// Bloom policy used when opening tables (must match the builder's).
    policy: bloom.BloomFilterPolicy,
    /// file_number -> heap-allocated open Table reader.
    tables: std.AutoHashMapUnmanaged(u64, *Entry),

    /// One cached open table: the reader plus the file it reads through (the
    /// reader borrows the file and never closes it, so the cache owns both).
    const Entry = struct {
        table: Table,
        file: env.RandomAccessFile,
    };

    pub fn init(
        gpa: std.mem.Allocator,
        e: env.Env,
        dbname: []const u8,
        opts: options_mod.Options,
        block_cache: ?*cache_mod.Cache,
    ) TableCache {
        return .{
            .gpa = gpa,
            .env = e,
            .dbname = dbname,
            .options = opts,
            .ikcmp = .{ .user = opts.comparator },
            .block_cache = block_cache,
            // Bloom policy must match the table builder; M6.0 builders use 10
            // bits/key, so we open with the same policy.  Tables that carry no
            // filter for this policy simply work without bloom fast-paths.
            .policy = bloom.BloomFilterPolicy.init(10),
            .tables = .empty,
        };
    }

    /// Close + free every cached table reader and its open file.
    pub fn deinit(self: *TableCache) void {
        var it = self.tables.valueIterator();
        while (it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            entry.table.deinit();
            entry.file.close() catch {};
            self.gpa.destroy(entry);
        }
        self.tables.deinit(self.gpa);
        self.* = undefined;
    }

    /// Return the open `*Table` for `file_number`, opening + caching it on first
    /// use.  The returned pointer is owned by the cache and stays valid until
    /// `deinit`.
    pub fn findTable(self: *TableCache, file_number: u64, file_size: u64) !*Table {
        if (self.tables.get(file_number)) |entry| return &entry.table;

        const path = try filename.tableFileName(self.gpa, self.dbname, file_number);
        defer self.gpa.free(path);

        const file = try self.env.newRandomAccessFile(self.gpa, path);
        errdefer file.close() catch {};

        const entry = try self.gpa.create(Entry);
        errdefer self.gpa.destroy(entry);

        // Open with the InternalKeyComparator (SSTs store internal keys).  The
        // comparator's ctx points at `&self.ikcmp`, which is stable because the
        // TableCache is pinned for its lifetime.
        var table_opts = self.options;
        table_opts.comparator = self.ikcmp.comparatorInterface();

        entry.file = file;
        entry.table = try Table.open(
            self.gpa,
            file,
            file_size,
            table_opts,
            self.policy,
            self.block_cache,
            file_number, // cache_id — distinct per table
        );
        errdefer entry.table.deinit();

        try self.tables.put(self.gpa, file_number, entry);
        // TODO: bound by options.max_open_files (LRU eviction).
        return &entry.table;
    }

    /// Point lookup against the SST `file_number` by EXACT user-or-internal key
    /// match (delegates to Table.get).  Returns a gpa-owned value the caller
    /// frees, or null.  NOTE: this is exact-match, not LSM seek-semantics; the
    /// LSM point lookup is driven by `Version.get` via `newIterator`.
    pub fn get(self: *TableCache, file_number: u64, file_size: u64, key: []const u8) !?[]u8 {
        const table = try self.findTable(file_number, file_size);
        return table.get(self.gpa, key);
    }

    /// Evict the cached entry for `file_number` if present; no-op if not cached.
    /// Mirrors the `deinit` teardown order: table.deinit → file.close → gpa.destroy.
    pub fn evict(self: *TableCache, file_number: u64) void {
        const kv = self.tables.fetchRemove(file_number) orelse return;
        const entry = kv.value;
        entry.table.deinit();
        entry.file.close() catch {};
        self.gpa.destroy(entry);
    }

    /// Build a generic `iterator.Iterator` over the whole SST `file_number`.
    /// The returned iterator OWNS a heap-allocated adapter wrapping the Table's
    /// own iterator; its `deinit` frees the adapter and tears down the Table
    /// iterator.  The backing `*Table` stays owned by the cache.
    pub fn newIterator(self: *TableCache, gpa: std.mem.Allocator, file_number: u64, file_size: u64) !iterator.Iterator {
        const table = try self.findTable(file_number, file_size);

        const adapter = try gpa.create(TableIterAdapter);
        errdefer gpa.destroy(adapter);
        adapter.* = .{ .gpa = gpa, .it = table.iterator(gpa) };
        return adapter.genericIterator();
    }
};

// ---------------------------------------------------------------------------
// Table.Iterator -> generic iterator.Iterator adapter.
// ---------------------------------------------------------------------------
//
// Table.iterator returns Table's OWN iterator type (forward/seek, with its own
// `deinit`).  This adapter exposes it through the generic `Iterator` vtable and
// registers a `deinit` that tears the Table iterator down and frees the heap
// adapter, so a MergingIterator (or any owner) can release it uniformly.
const TableIterAdapter = struct {
    gpa: std.mem.Allocator,
    it: Table.Iterator,

    fn genericIterator(self: *TableIterAdapter) iterator.Iterator {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable = iterator.Iterator.VTable{
        .seekToFirst = vSeekToFirst,
        .seekToLast = vSeekToLast,
        .seek = vSeek,
        .next = vNext,
        .prev = vPrev,
        .valid = vValid,
        .key = vKey,
        .value = vValue,
        .status = vStatus,
        .deinit = vDeinit,
    };

    fn cast(ctx: *anyopaque) *TableIterAdapter {
        return @ptrCast(@alignCast(ctx));
    }

    fn vSeekToFirst(ctx: *anyopaque) void {
        cast(ctx).it.seekToFirst();
    }
    fn vSeekToLast(ctx: *anyopaque) void {
        // Table.Iterator is forward/seek-only; reverse is unsupported (the DB
        // read path only scans forward).  Leave the cursor invalid.
        _ = ctx;
    }
    fn vSeek(ctx: *anyopaque, target: []const u8) void {
        cast(ctx).it.seek(target);
    }
    fn vNext(ctx: *anyopaque) void {
        cast(ctx).it.next();
    }
    fn vPrev(ctx: *anyopaque) void {
        _ = ctx; // forward-only
    }
    fn vValid(ctx: *anyopaque) bool {
        return cast(ctx).it.valid();
    }
    fn vKey(ctx: *anyopaque) []const u8 {
        return cast(ctx).it.key();
    }
    fn vValue(ctx: *anyopaque) []const u8 {
        return cast(ctx).it.value();
    }
    fn vStatus(ctx: *anyopaque) ?anyerror {
        return cast(ctx).it.status();
    }
    fn vDeinit(ctx: *anyopaque) void {
        const self = cast(ctx);
        const gpa = self.gpa;
        self.it.deinit();
        gpa.destroy(self);
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const table_builder = @import("../format/table_builder.zig");
const comparator = @import("../util/comparator.zig");
const coding = @import("../util/coding.zig");

/// An internal-key entry: user key + sequence/type + value.
const IKV = struct { user: []const u8, seq: u64, t: internal_key.ValueType, v: []const u8 };

/// Encode `user ++ fixed64(packSequenceAndType(seq, t))` (caller frees).
fn encodeIkey(gpa: std.mem.Allocator, user: []const u8, seq: u64, t: internal_key.ValueType) ![]u8 {
    const out = try gpa.alloc(u8, user.len + 8);
    @memcpy(out[0..user.len], user);
    coding.encodeFixed64(out[user.len..][0..8], internal_key.packSequenceAndType(seq, t));
    return out;
}

/// Build an SST of INTERNAL keys (matching how TableCache opens tables — with
/// the InternalKeyComparator).  Entries MUST be in internal-key order.
fn buildSST(
    gpa: std.mem.Allocator,
    e: env.Env,
    dbname: []const u8,
    number: u64,
    policy: bloom.BloomFilterPolicy,
    entries: []const IKV,
) !void {
    const path = try filename.tableFileName(gpa, dbname, number);
    defer gpa.free(path);

    var ikc = internal_key.InternalKeyComparator{ .user = comparator.bytewise };
    const opts = options_mod.Options{ .comparator = ikc.comparatorInterface() };

    var wf = try e.newWritableFile(gpa, path);
    errdefer wf.close() catch {};
    var tb = try table_builder.TableBuilder.init(gpa, opts, wf, policy);
    defer tb.deinit();
    for (entries) |en| {
        const ik = try encodeIkey(gpa, en.user, en.seq, en.t);
        defer gpa.free(ik);
        try tb.add(ik, en.v);
    }
    try tb.finish();
    try wf.close();
}

test "TableCache: findTable opens, caches (same pointer), and scans" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const entries = [_]IKV{
        .{ .user = "alpha", .seq = 1, .t = .value, .v = "1" },
        .{ .user = "beta", .seq = 2, .t = .value, .v = "2" },
        .{ .user = "gamma", .seq = 3, .t = .value, .v = "3" },
    };
    try buildSST(gpa, e, "db", 7, policy, &entries);

    const path7 = try filename.tableFileName(gpa, "db", 7);
    defer gpa.free(path7);
    const size = try e.getFileSize(path7);

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    const t1 = try tc.findTable(7, size);
    const t2 = try tc.findTable(7, size); // cached: same pointer
    try testing.expectEqual(@intFromPtr(t1), @intFromPtr(t2));

    // newIterator scans all entries in order; keys are internal keys whose user
    // portion equals the user we wrote.
    var it = try tc.newIterator(gpa, 7, size);
    defer it.deinit();
    it.seekToFirst();
    var i: usize = 0;
    while (it.valid()) : (it.next()) {
        try testing.expect(i < entries.len);
        try testing.expectEqualStrings(entries[i].user, internal_key.extractUserKey(it.key()));
        try testing.expectEqualStrings(entries[i].v, it.value());
        i += 1;
    }
    try testing.expectEqual(entries.len, i);
    try testing.expect(it.status() == null);
}

test "TableCache: get returns the stored value, null for absent" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const entries = [_]IKV{
        .{ .user = "k1", .seq = 1, .t = .value, .v = "v1" },
        .{ .user = "k2", .seq = 2, .t = .value, .v = "v2" },
    };
    try buildSST(gpa, e, "db", 9, policy, &entries);

    const path = try filename.tableFileName(gpa, "db", 9);
    defer gpa.free(path);
    const size = try e.getFileSize(path);

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    // get does an EXACT internal-key match (TableCache.get delegates to
    // Table.get); seek to the exact internal key for k2@2.
    const k2_ikey = try encodeIkey(gpa, "k2", 2, .value);
    defer gpa.free(k2_ikey);
    const got = try tc.get(9, size, k2_ikey) orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("v2", got);

    const absent_ikey = try encodeIkey(gpa, "absent", 1, .value);
    defer gpa.free(absent_ikey);
    try testing.expect((try tc.get(9, size, absent_ikey)) == null);
}

test "TableCache.evict: evict-uncached is a no-op (zero leaks)" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    // Evicting a file that was never cached must be a no-op (no crash, no leak).
    tc.evict(42);
    tc.evict(0);
}

test "TableCache.evict: open two SSTs, evict one, count drops, re-open works" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const entries1 = [_]IKV{
        .{ .user = "a", .seq = 1, .t = .value, .v = "va" },
    };
    const entries2 = [_]IKV{
        .{ .user = "b", .seq = 2, .t = .value, .v = "vb" },
    };
    try buildSST(gpa, e, "db", 10, policy, &entries1);
    try buildSST(gpa, e, "db", 11, policy, &entries2);

    const path10 = try filename.tableFileName(gpa, "db", 10);
    defer gpa.free(path10);
    const path11 = try filename.tableFileName(gpa, "db", 11);
    defer gpa.free(path11);
    const size10 = try e.getFileSize(path10);
    const size11 = try e.getFileSize(path11);

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    // Open both tables into the cache.
    _ = try tc.findTable(10, size10);
    _ = try tc.findTable(11, size11);
    try testing.expectEqual(@as(usize, 2), tc.tables.count());

    // Evict SST 10; count drops to 1.
    tc.evict(10);
    try testing.expectEqual(@as(usize, 1), tc.tables.count());

    // SST 11 is still cached (same pointer as before).
    const t11a = try tc.findTable(11, size11);
    const t11b = try tc.findTable(11, size11);
    try testing.expectEqual(@intFromPtr(t11a), @intFromPtr(t11b));

    // Re-opening SST 10 succeeds (file still exists on disk) and re-caches it.
    const t10 = try tc.findTable(10, size10);
    try testing.expectEqual(@as(usize, 2), tc.tables.count());

    // The re-opened table is functional: scan its one entry.
    var it = try tc.newIterator(gpa, 10, size10);
    defer it.deinit();
    it.seekToFirst();
    try testing.expect(it.valid());
    try testing.expectEqualStrings("a", internal_key.extractUserKey(it.key()));
    try testing.expectEqualStrings("va", it.value());
    it.next();
    try testing.expect(!it.valid());
    // Returned pointer is fresh (different from the pre-evict pointer would have been),
    // but we can verify it's a valid, non-null pointer.
    try testing.expect(@intFromPtr(t10) != 0);
}

test "TableCache.evict: evict then deleteFile -> findTable returns FileNotFound" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const entries = [_]IKV{
        .{ .user = "x", .seq = 5, .t = .value, .v = "vx" },
    };
    try buildSST(gpa, e, "db", 20, policy, &entries);

    const path = try filename.tableFileName(gpa, "db", 20);
    defer gpa.free(path);
    const size = try e.getFileSize(path);

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    // Open the table so it's cached.
    _ = try tc.findTable(20, size);
    try testing.expectEqual(@as(usize, 1), tc.tables.count());

    // Evict first (releases the file handle), then delete the file.
    tc.evict(20);
    try testing.expectEqual(@as(usize, 0), tc.tables.count());
    try e.deleteFile(path);

    // findTable should now fail because the file is gone.
    const result = tc.findTable(20, size);
    try testing.expectError(error.NotFound, result);
}
