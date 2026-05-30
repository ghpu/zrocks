//! table_cache.zig — opens and caches SST `Table` readers by file number, with
//! a REFERENCE-COUNTED, LRU-BOUNDED resident set (bounded by
//! `options.max_open_files`) so sustained wide-fanout reads do not leak file
//! descriptors.
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
//! cached table and its file even if still pinned (terminal teardown).
//!
//! `dbname`, `env`, `options`, and the optional block cache are BORROWED — the
//! owning DB must outlive the TableCache.
//!
//! Refcount + LRU design (mirrors `util/cache.zig`, LevelDB/RocksDB semantics)
//! --------------------------------------------------------------------------
//! Iterators and rangeTombstone gathers BORROW cache-owned `*Table` pointers,
//! and many are pinned at once (one per overlapping SST during a merge), so
//! eviction MUST NOT free a table still in use → use-after-free.  We solve this
//! exactly the way LevelDB's `ShardedLRUCache` does, specialized to a single
//! (unsharded) file-handle cache:
//!
//!   * An `Entry` resident in the map holds ONE "in-cache" reference.  Each
//!     outstanding `Handle` adds one more.  A freshly opened+inserted entry
//!     starts at `refs == 2` (one in-cache, one for the returned handle).
//!   * An entry is on the LRU list ONLY while `in_cache and refs == 1` (cached
//!     but unpinned → evictable).  Pinned entries are OFF the list.
//!   * `evictToCapacity` walks the LRU tail freeing the oldest UNPINNED entries
//!     until the resident COUNT <= capacity.  It STOPS when the tail is pinned,
//!     so the resident set MAY exceed capacity when enough handles are pinned —
//!     this is correct and matches RocksDB; capacity is a SOFT target.
//!   * Capacity = number of resident table handles.  Each entry charges 1 and we
//!     compare the entry COUNT against `options.max_open_files`.  RocksDB
//!     semantics: a NEGATIVE `max_open_files` means UNLIMITED — never evict.
//!
//! Where pins are taken and dropped
//! --------------------------------
//!   * `acquire` pins (refs++ on hit, or insert at refs==2) and returns a
//!     `*Handle`; `release` drops the pin.
//!   * `get` is acquire → table.get → release (short-lived).
//!   * `newIterator` acquires and STORES the handle in the iterator adapter; the
//!     adapter's `deinit` releases it.  This makes the LONG-lived iterator paths
//!     (version_set addIterators / probeFile, compaction merge children) correct
//!     with ZERO caller changes — the handle lives exactly as long as the
//!     iterator that borrows the table.
//!   * `evict(file_number)` has ERASE semantics: remove from the map + drop the
//!     in-cache ref.  If handles are still outstanding the storage is freed when
//!     the last `release` runs (so an obsolete file pinned by a live reader stays
//!     alive until released; on POSIX the unlinked fd keeps working).
//!
//! Concurrency (D2a-2)
//! -------------------
//! All map / refcount / LRU mutation happens under `self.mutex`.  No file I/O is
//! done under the lock EXCEPT `acquire`'s open path, which opens the file under
//! the lock (kept from the original findTable; correct, though it serializes
//! opens).  The returned `*Table` / `*Handle` stay valid after the lock drops:
//! an `Entry`'s storage is freed only when its refcount hits zero, which cannot
//! happen while a caller still holds a handle.

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
    /// the test's duration and never move it after the first acquire).
    ikcmp: internal_key.InternalKeyComparator,
    /// Optional shared block cache passed through to each opened Table.
    block_cache: ?*cache_mod.Cache,
    /// Bloom policy used when opening tables (must match the builder's).
    policy: bloom.BloomFilterPolicy,
    /// file_number -> heap-allocated open Table Entry (refcounted; see Entry).
    tables: std.AutoHashMapUnmanaged(u64, *Entry),
    /// Number of resident entries currently allowed before `evictToCapacity`
    /// starts freeing unpinned tables.  Derived from `options.max_open_files`:
    /// a non-negative value caps the resident COUNT; a NEGATIVE value means
    /// UNLIMITED (recorded as `unbounded == true`, never evicts).
    capacity: usize,
    /// True iff `options.max_open_files < 0` (unlimited resident set).
    unbounded: bool,
    /// MRU head / LRU tail of the intrusive doubly-linked list of EVICTABLE
    /// entries (those with `in_cache and refs == 1`).  Pinned entries are not on
    /// the list.  `evictToCapacity` frees from the tail.
    lru_head: ?*Entry,
    lru_tail: ?*Entry,
    /// Concurrency capability for `mutex` (D2a-2): the SAME `std.Io` that owns
    /// the owning DB's filesystem authority — never an ambient/global io.
    io: std.Io,
    /// Serializes mutation of the `tables` map, the refcounts, and the LRU list
    /// (D2a-2).  A background flush or compaction worker calls `acquire`
    /// concurrently with the read path; both may INSERT (resizing the map) while
    /// another caller reads it — a data race on the hashmap's backing store.
    /// This mutex makes lookup-insert, refcount mutation, eviction, and `evict`
    /// atomic.  Returned `*Table` / `*Handle` pointers stay valid after the lock
    /// drops because an `Entry` is freed only when its refcount reaches zero.
    mutex: std.Io.Mutex,

    /// An opaque-ish handle pinning a cached entry against eviction.  Obtain one
    /// from `acquire` (and the iterator path); read the table via `h.table()`;
    /// drop the pin with `TableCache.release(h)`.  Internally a `*Entry`.
    pub const Handle = opaque {
        /// The pinned table reader (valid until this handle is released).
        pub fn table(h: *Handle) *Table {
            const e: *Entry = @ptrCast(@alignCast(h));
            return &e.table;
        }
    };

    /// One cached open table: the reader plus the file it reads through (the
    /// reader borrows the file and never closes it, so the cache owns both),
    /// plus the refcount/LRU bookkeeping mirrored from `util/cache.zig`.
    const Entry = struct {
        table: Table,
        file: env.RandomAccessFile,
        /// Map key (so the LRU eviction sweep can remove the entry by key).
        file_number: u64,
        /// Total references = 1 (in-cache) + number of outstanding handles.
        refs: u32,
        /// True while the entry is still present in the `tables` map.
        in_cache: bool,
        /// Intrusive LRU links.  Meaningful only while the entry is on the LRU
        /// list (`in_cache and refs == 1`); null otherwise.
        prev: ?*Entry,
        next: ?*Entry,
    };

    pub fn init(
        gpa: std.mem.Allocator,
        e: env.Env,
        dbname: []const u8,
        opts: options_mod.Options,
        block_cache: ?*cache_mod.Cache,
    ) TableCache {
        const unbounded = opts.max_open_files < 0;
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
            .capacity = if (unbounded) 0 else @intCast(opts.max_open_files),
            .unbounded = unbounded,
            .lru_head = null,
            .lru_tail = null,
            // The cache's `io` is the SAME one the Env owns (no ambient io).
            .io = e.io(),
            .mutex = .init,
        };
    }

    /// Terminal teardown: close + free every cached table reader and its open
    /// file even if still pinned (mirrors `util/cache.zig` Shard.deinit — drop
    /// the in-cache ref and free storage anyway).
    pub fn deinit(self: *TableCache) void {
        var it = self.tables.valueIterator();
        while (it.next()) |entry_ptr| {
            self.freeEntry(entry_ptr.*);
        }
        self.tables.deinit(self.gpa);
        self.* = undefined;
    }

    /// Free an entry's storage (table reader, file, node).  Does not touch the
    /// map or LRU list.
    fn freeEntry(self: *TableCache, e: *Entry) void {
        e.table.deinit();
        e.file.close() catch {};
        self.gpa.destroy(e);
    }

    // -- LRU list helpers (head = MRU, tail = LRU) ---------------------------

    fn lruRemove(self: *TableCache, e: *Entry) void {
        if (e.prev) |p| p.next = e.next else self.lru_head = e.next;
        if (e.next) |n| n.prev = e.prev else self.lru_tail = e.prev;
        e.prev = null;
        e.next = null;
    }

    /// Push `e` to the MRU (head) end of the LRU list.
    fn lruPushFront(self: *TableCache, e: *Entry) void {
        e.prev = null;
        e.next = self.lru_head;
        if (self.lru_head) |h| h.prev = e else self.lru_tail = e;
        self.lru_head = e;
    }

    /// Drop one reference.  When it reaches zero the entry is freed.  When it
    /// drops to exactly the in-cache ref (1) while still cached, the entry
    /// becomes evictable and is (re)inserted at the MRU end of the LRU list.
    fn unref(self: *TableCache, e: *Entry) void {
        std.debug.assert(e.refs > 0);
        e.refs -= 1;
        if (e.refs == 0) {
            // Last reference gone: the entry must already be out of the map.
            std.debug.assert(!e.in_cache);
            self.freeEntry(e);
        } else if (e.in_cache and e.refs == 1) {
            // No external handles left: becomes evictable → MRU of LRU list.
            self.lruPushFront(e);
        }
    }

    /// Finish detaching an entry that is ALREADY removed from the map: unlink it
    /// from the LRU list (if present), clear `in_cache`, and drop the in-cache
    /// reference.  The storage is freed once the last outstanding handle (if
    /// any) is released.
    fn detach(self: *TableCache, e: *Entry) void {
        std.debug.assert(e.in_cache);
        // On the LRU list only when evictable (refs == 1).
        if (e.refs == 1) self.lruRemove(e);
        e.in_cache = false;
        self.unref(e); // drop the in-cache reference
    }

    /// Remove `e` from the map and detach it.
    fn removeFromCache(self: *TableCache, e: *Entry) void {
        _ = self.tables.remove(e.file_number);
        self.detach(e);
    }

    /// Evict unpinned entries from the LRU tail until the resident COUNT is
    /// within capacity.  Stops when the tail is pinned (resident set may then
    /// exceed capacity — a SOFT target, matching RocksDB).  No-op when unbounded.
    fn evictToCapacity(self: *TableCache) void {
        if (self.unbounded) return;
        while (self.tables.count() > self.capacity) {
            const victim = self.lru_tail orelse break; // all remaining pinned
            self.removeFromCache(victim);
        }
    }

    /// Open the SST for `file_number` and INSERT it into the map at `refs == 2`
    /// (one in-cache, one for the returned handle).  Caller holds `self.mutex`.
    /// File I/O happens under the lock (kept from the original findTable).
    fn openAndInsert(self: *TableCache, file_number: u64, file_size: u64) !*Entry {
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

        entry.* = .{
            .table = undefined,
            .file = file,
            .file_number = file_number,
            .refs = 2, // one in-cache, one for the returned handle
            .in_cache = true,
            .prev = null,
            .next = null,
        };
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
        return entry;
    }

    /// Look up (or open) the SST for `file_number` and return a PINNED handle.
    /// On a hit the entry's refcount is bumped (and it is taken off the LRU list
    /// if it was evictable); on a miss the file is opened and inserted at
    /// `refs == 2`, then `evictToCapacity` runs.  The caller MUST `release` the
    /// returned handle exactly once.
    pub fn acquire(self: *TableCache, file_number: u64, file_size: u64) !*Handle {
        // Serialize map mutation against a concurrent background worker (D2a-2).
        // Held across the open + insert so a miss can never race a concurrent
        // insert of the same file number into the hashmap's backing arrays.
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        if (self.tables.get(file_number)) |entry| {
            // Pin: if it was evictable (refs == 1), take it off the LRU list.
            if (entry.refs == 1) self.lruRemove(entry);
            entry.refs += 1;
            return @ptrCast(entry);
        }

        const entry = try self.openAndInsert(file_number, file_size);
        // Bound the resident set after inserting the freshly opened entry.
        self.evictToCapacity();
        return @ptrCast(entry);
    }

    /// Drop the caller's pin on `handle`.  If this was the last reference to an
    /// already-evicted entry, its storage is freed here.  Infallible (mirrors
    /// `util/cache.zig` release): locks uncancelably.
    pub fn release(self: *TableCache, handle: *Handle) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        const entry: *Entry = @ptrCast(@alignCast(handle));
        self.unref(entry);
    }

    /// Read the table behind `handle` (convenience accessor mirroring
    /// `Handle.table`).
    pub fn tableOf(self: *TableCache, handle: *Handle) *Table {
        _ = self;
        return handle.table();
    }

    /// Point lookup against the SST `file_number` by EXACT user-or-internal key
    /// match (delegates to Table.get).  Returns a gpa-owned value the caller
    /// frees, or null.  NOTE: this is exact-match, not LSM seek-semantics; the
    /// LSM point lookup is driven by `Version.get` via `newIterator`.
    pub fn get(self: *TableCache, file_number: u64, file_size: u64, key: []const u8) !?[]u8 {
        const h = try self.acquire(file_number, file_size);
        defer self.release(h);
        return h.table().get(self.gpa, key);
    }

    /// ERASE the cached entry for `file_number` if present; no-op if not cached.
    /// Removes it from the map and drops the in-cache ref.  If handles are still
    /// outstanding the storage is freed when the last `release` runs (so an
    /// obsolete file pinned by a live reader stays alive until released; on
    /// POSIX the unlinked fd keeps working).
    pub fn evict(self: *TableCache, file_number: u64) void {
        // Serialize against acquire / a concurrent worker (D2a-2).  `evict`
        // returns void and cannot propagate error.Canceled, so lock uncancelably.
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.tables.get(file_number)) |entry| self.removeFromCache(entry);
    }

    /// Build a generic `iterator.Iterator` over the whole SST `file_number`.
    /// The returned iterator OWNS a heap-allocated adapter wrapping the Table's
    /// own iterator AND the cache handle pinning the backing table; its `deinit`
    /// tears down the Table iterator, then RELEASES the cache handle (so the
    /// pinned table can be evicted once no iterator borrows it).
    pub fn newIterator(self: *TableCache, gpa: std.mem.Allocator, file_number: u64, file_size: u64) !iterator.Iterator {
        const handle = try self.acquire(file_number, file_size);
        errdefer self.release(handle);

        const adapter = try gpa.create(TableIterAdapter);
        errdefer gpa.destroy(adapter);
        adapter.* = .{
            .gpa = gpa,
            .it = handle.table().iterator(gpa),
            .cache = self,
            .handle = handle,
        };
        return adapter.genericIterator();
    }

    /// Number of resident (in-map) entries.  Test/observability accessor.  Reads
    /// under the lock so it is consistent with concurrent mutation.
    pub fn residentCount(self: *TableCache) usize {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.tables.count();
    }
};

// ---------------------------------------------------------------------------
// Table.Iterator -> generic iterator.Iterator adapter.
// ---------------------------------------------------------------------------
//
// Table.iterator returns Table's OWN iterator type (forward/seek, with its own
// `deinit`).  This adapter exposes it through the generic `Iterator` vtable and
// registers a `deinit` that tears the Table iterator down, RELEASES the cache
// handle that pins the backing table, and frees the heap adapter, so a
// MergingIterator (or any owner) can release it uniformly.  Holding the handle
// here is the linchpin that keeps LONG-lived iterator paths correct with zero
// caller changes: the borrowed `*Table` cannot be evicted until the iterator is
// deinited.
const TableIterAdapter = struct {
    gpa: std.mem.Allocator,
    it: Table.Iterator,
    /// Cache + handle pinning the backing table for this iterator's lifetime.
    cache: *TableCache,
    handle: *TableCache.Handle,

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
        const cache = self.cache;
        const handle = self.handle;
        self.it.deinit();
        cache.release(handle); // drop the pin on the backing table
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

/// Build a single-entry SST numbered `number` with user key/value derived from
/// `number`, and return its on-disk size.
fn buildOne(gpa: std.mem.Allocator, e: env.Env, dbname: []const u8, number: u64, policy: bloom.BloomFilterPolicy) !u64 {
    var ubuf: [32]u8 = undefined;
    var vbuf: [32]u8 = undefined;
    const user = try std.fmt.bufPrint(&ubuf, "key{d}", .{number});
    const val = try std.fmt.bufPrint(&vbuf, "val{d}", .{number});
    const entries = [_]IKV{.{ .user = user, .seq = number, .t = .value, .v = val }};
    try buildSST(gpa, e, dbname, number, policy, &entries);
    const path = try filename.tableFileName(gpa, dbname, number);
    defer gpa.free(path);
    return e.getFileSize(path);
}

test "TableCache: acquire caches (same table pointer), and newIterator scans" {
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

    const h1 = try tc.acquire(7, size);
    const h2 = try tc.acquire(7, size); // cached: same table pointer
    try testing.expectEqual(@intFromPtr(h1.table()), @intFromPtr(h2.table()));
    try testing.expectEqual(@as(usize, 1), tc.residentCount());
    tc.release(h1);
    tc.release(h2);

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

    // get acquires + releases internally, so nothing stays pinned.
    try testing.expectEqual(@as(usize, 1), tc.residentCount());
}

test "TableCache: resident bound — N+K distinct tables, resident count stays <= N" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const N: usize = 3;
    const total: u64 = 7; // N + K, K = 4
    var sizes: [7]u64 = undefined;
    var n: u64 = 0;
    while (n < total) : (n += 1) sizes[n] = try buildOne(gpa, e, "db", 100 + n, policy);

    var tc = TableCache.init(gpa, e, "db", .{ .max_open_files = @intCast(N) }, null);
    defer tc.deinit();

    // Acquire+release each distinct table sequentially.  Because each is
    // released before the next acquire, every entry is unpinned and evictable,
    // so the resident set never exceeds N.
    n = 0;
    while (n < total) : (n += 1) {
        const h = try tc.acquire(100 + n, sizes[n]);
        tc.release(h);
        try testing.expect(tc.residentCount() <= N);
    }
    try testing.expectEqual(N, tc.residentCount());
}

test "TableCache: pinned entries exceed capacity, then shrink toward cap after release" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const N: usize = 2;
    const M: u64 = 5; // M > N
    var sizes: [5]u64 = undefined;
    var handles: [5]*TableCache.Handle = undefined;
    var n: u64 = 0;
    while (n < M) : (n += 1) sizes[n] = try buildOne(gpa, e, "db", 200 + n, policy);

    var tc = TableCache.init(gpa, e, "db", .{ .max_open_files = @intCast(N) }, null);
    defer tc.deinit();

    // Acquire and HOLD M > N handles.  Pinned entries are off the LRU list, so
    // evictToCapacity cannot free them: all M stay resident.
    n = 0;
    while (n < M) : (n += 1) handles[n] = try tc.acquire(200 + n, sizes[n]);
    try testing.expectEqual(@as(usize, M), tc.residentCount());

    // All M tables still read correctly (no pinned eviction).
    n = 0;
    while (n < M) : (n += 1) {
        var vbuf: [32]u8 = undefined;
        const want_val = try std.fmt.bufPrint(&vbuf, "val{d}", .{200 + n});
        const ikey = try encodeIkey(gpa, blk: {
            var ubuf: [32]u8 = undefined;
            const u = try std.fmt.bufPrint(&ubuf, "key{d}", .{200 + n});
            break :blk u;
        }, 200 + n, .value);
        defer gpa.free(ikey);
        const got = try handles[n].table().get(gpa, ikey) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(want_val, got);
    }

    // Release all M; the now-unpinned entries land on the LRU list but no
    // eviction runs yet (eviction only triggers on insert).
    n = 0;
    while (n < M) : (n += 1) tc.release(handles[n]);
    try testing.expectEqual(@as(usize, M), tc.residentCount());

    // Acquire one MORE distinct table: this insert runs evictToCapacity, which
    // frees unpinned LRU-tail entries until count <= N (so it shrinks toward N).
    const sz = try buildOne(gpa, e, "db", 999, policy);
    const h = try tc.acquire(999, sz);
    defer tc.release(h);
    try testing.expectEqual(N, tc.residentCount());
}

test "TableCache: reopen-after-evict — cap=1, A then B evicts A, re-acquire A reads correctly" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const sizeA = try buildOne(gpa, e, "db", 301, policy); // key301/val301
    const sizeB = try buildOne(gpa, e, "db", 302, policy); // key302/val302

    var tc = TableCache.init(gpa, e, "db", .{ .max_open_files = 1 }, null);
    defer tc.deinit();

    // Acquire+release A, then acquire+release B (evicts A since cap == 1).
    {
        const hA = try tc.acquire(301, sizeA);
        tc.release(hA);
    }
    {
        const hB = try tc.acquire(302, sizeB);
        tc.release(hB);
    }
    try testing.expectEqual(@as(usize, 1), tc.residentCount());

    // Re-acquire A: it was evicted, so this RE-OPENS the file.  Its data must
    // still read correctly.
    const hA2 = try tc.acquire(301, sizeA);
    defer tc.release(hA2);
    const ikey = try encodeIkey(gpa, "key301", 301, .value);
    defer gpa.free(ikey);
    const got = try hA2.table().get(gpa, ikey) orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("val301", got);
}

test "TableCache: unlimited when max_open_files < 0 — never evicts" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const total: u64 = 6;
    var sizes: [6]u64 = undefined;
    var n: u64 = 0;
    while (n < total) : (n += 1) sizes[n] = try buildOne(gpa, e, "db", 400 + n, policy);

    var tc = TableCache.init(gpa, e, "db", .{ .max_open_files = -1 }, null);
    defer tc.deinit();

    // Acquire+release every table; with unlimited capacity none are evicted.
    n = 0;
    while (n < total) : (n += 1) {
        const h = try tc.acquire(400 + n, sizes[n]);
        tc.release(h);
    }
    try testing.expectEqual(@as(usize, total), tc.residentCount());
}

test "TableCache.evict: with a live handle, handle still reads, then release frees (no leak)" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);
    const sizeX = try buildOne(gpa, e, "db", 500, policy); // key500/val500

    var tc = TableCache.init(gpa, e, "db", .{}, null);
    defer tc.deinit();

    // Acquire a handle to X, then ERASE X from the cache while the handle is
    // still outstanding.  The entry leaves the map (residentCount drops) but its
    // storage survives because the handle still pins it.
    const h = try tc.acquire(500, sizeX);
    try testing.expectEqual(@as(usize, 1), tc.residentCount());
    tc.evict(500);
    try testing.expectEqual(@as(usize, 0), tc.residentCount());

    // The handle still reads X correctly (POSIX-unlink semantics: the open table
    // keeps working).
    const ikey = try encodeIkey(gpa, "key500", 500, .value);
    defer gpa.free(ikey);
    const got = try h.table().get(gpa, ikey) orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("val500", got);

    // Releasing the last handle frees the storage (testing.allocator catches a
    // leak if not).
    tc.release(h);
    try testing.expectEqual(@as(usize, 0), tc.residentCount());
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
    try testing.expectEqual(@as(usize, 0), tc.residentCount());
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

    // Open both tables into the cache (acquire+release so they're unpinned).
    {
        const ha = try tc.acquire(10, size10);
        tc.release(ha);
        const hb = try tc.acquire(11, size11);
        tc.release(hb);
    }
    try testing.expectEqual(@as(usize, 2), tc.residentCount());

    // Evict SST 10; count drops to 1.
    tc.evict(10);
    try testing.expectEqual(@as(usize, 1), tc.residentCount());

    // SST 11 is still cached (same table pointer as before).
    const h11a = try tc.acquire(11, size11);
    const h11b = try tc.acquire(11, size11);
    try testing.expectEqual(@intFromPtr(h11a.table()), @intFromPtr(h11b.table()));
    tc.release(h11a);
    tc.release(h11b);

    // Re-opening SST 10 succeeds (file still exists on disk) and re-caches it.
    const h10 = try tc.acquire(10, size10);
    try testing.expectEqual(@as(usize, 2), tc.residentCount());
    tc.release(h10);

    // The re-opened table is functional: scan its one entry.
    var it = try tc.newIterator(gpa, 10, size10);
    defer it.deinit();
    it.seekToFirst();
    try testing.expect(it.valid());
    try testing.expectEqualStrings("a", internal_key.extractUserKey(it.key()));
    try testing.expectEqualStrings("va", it.value());
    it.next();
    try testing.expect(!it.valid());
}

test "TableCache.evict: evict then deleteFile -> acquire returns NotFound" {
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

    // Open the table so it's cached (and unpinned).
    {
        const h = try tc.acquire(20, size);
        tc.release(h);
    }
    try testing.expectEqual(@as(usize, 1), tc.residentCount());

    // Evict first (drops the in-cache ref → closes the file), then delete it.
    tc.evict(20);
    try testing.expectEqual(@as(usize, 0), tc.residentCount());
    try e.deleteFile(path);

    // acquire should now fail because the file is gone.
    const result = tc.acquire(20, size);
    try testing.expectError(error.NotFound, result);
}

test "TableCache: iterator path over more files than the cap; after deinit resident <= cap" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    const N: usize = 2;
    const nfiles: u64 = 5; // more files than the cap
    var sizes: [5]u64 = undefined;
    var n: u64 = 0;
    while (n < nfiles) : (n += 1) sizes[n] = try buildOne(gpa, e, "db", 600 + n, policy);

    var tc = TableCache.init(gpa, e, "db", .{ .max_open_files = @intCast(N) }, null);
    defer tc.deinit();

    // Open one iterator PER file and hold them all at once (mirrors a merging
    // iterator over every overlapping SST).  Each iterator pins its table, so
    // all nfiles stay resident even though nfiles > N (pinned exceeds cap).
    var iters: [5]iterator.Iterator = undefined;
    n = 0;
    while (n < nfiles) : (n += 1) iters[n] = try tc.newIterator(gpa, 600 + n, sizes[n]);
    try testing.expectEqual(@as(usize, nfiles), tc.residentCount());

    // Full scan of each is correct.
    n = 0;
    while (n < nfiles) : (n += 1) {
        var ubuf: [32]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const want_user = try std.fmt.bufPrint(&ubuf, "key{d}", .{600 + n});
        const want_val = try std.fmt.bufPrint(&vbuf, "val{d}", .{600 + n});
        iters[n].seekToFirst();
        try testing.expect(iters[n].valid());
        try testing.expectEqualStrings(want_user, internal_key.extractUserKey(iters[n].key()));
        try testing.expectEqualStrings(want_val, iters[n].value());
        iters[n].next();
        try testing.expect(!iters[n].valid());
    }

    // Deinit every iterator: each adapter releases its cache handle, unpinning
    // its table.  The releases themselves do not evict (eviction runs on
    // insert), so we acquire one more table to trigger evictToCapacity and
    // confirm the resident set collapses back to the cap.
    n = 0;
    while (n < nfiles) : (n += 1) iters[n].deinit();

    const sz = try buildOne(gpa, e, "db", 700, policy);
    const h = try tc.acquire(700, sz);
    defer tc.release(h);
    try testing.expect(tc.residentCount() <= N);
}

test "D2a-2: concurrent acquire from multiple fibers is serialized (no map corruption)" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);

    // Build a handful of distinct SSTs so concurrent acquirers insert different
    // file numbers into the cache map at the same time — the exact race the
    // D2a-2 mutex guards (a concurrent insert resizes the hashmap's backing
    // arrays while another caller reads/inserts them).
    const nfiles: u64 = 8;
    var fnum: u64 = 30;
    var sizes: [8]u64 = undefined;
    while (fnum < 30 + nfiles) : (fnum += 1) {
        const entries = [_]IKV{
            .{ .user = "k", .seq = fnum, .t = .value, .v = "v" },
        };
        try buildSST(gpa, e, "db", fnum, policy, &entries);
        const p = try filename.tableFileName(gpa, "db", fnum);
        defer gpa.free(p);
        sizes[fnum - 30] = try e.getFileSize(p);
    }

    // Unlimited cap so concurrent acquire+release leaves all files resident and
    // the final count is deterministic.
    var tc = TableCache.init(gpa, e, "db", .{ .max_open_files = -1 }, null);
    defer tc.deinit();

    // Each worker acquires + releases every file (each `acquire` may insert);
    // running them concurrently exercises the serialization.  A worker returns
    // the first error it hits (or null on success).
    const Worker = struct {
        fn run(cache: *TableCache, szs: []const u64, base: u64, count: u64) ?anyerror {
            var n: u64 = 0;
            while (n < count) : (n += 1) {
                const h = cache.acquire(base + n, szs[n]) catch |err| return err;
                cache.release(h);
            }
            return null;
        }
    };

    const io = std.testing.io;
    var f1 = std.Io.async(io, Worker.run, .{ &tc, sizes[0..nfiles], @as(u64, 30), nfiles });
    var f2 = std.Io.async(io, Worker.run, .{ &tc, sizes[0..nfiles], @as(u64, 30), nfiles });
    var f3 = std.Io.async(io, Worker.run, .{ &tc, sizes[0..nfiles], @as(u64, 30), nfiles });
    const r1 = f1.await(io);
    const r2 = f2.await(io);
    const r3 = f3.await(io);
    try testing.expect(r1 == null);
    try testing.expect(r2 == null);
    try testing.expect(r3 == null);

    // Exactly one entry per file ended up cached (no duplicate inserts, no
    // lost/corrupted entries).
    try testing.expectEqual(@as(usize, nfiles), tc.residentCount());
}

test "TableCache: zero leaks with an outstanding handle at deinit" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();
    try e.makeDir("db");

    const policy = bloom.BloomFilterPolicy.init(10);
    const size = try buildOne(gpa, e, "db", 800, policy);

    var tc = TableCache.init(gpa, e, "db", .{}, null);

    // Acquire and keep a handle outstanding; deinit must free gracefully even
    // though the entry is pinned (terminal teardown).
    const h = try tc.acquire(800, size);
    _ = h; // intentionally not released before deinit
    tc.deinit();
}
