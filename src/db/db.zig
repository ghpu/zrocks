//! db.zig — the embedded key/value store (M4.1 store + M5.2 durability +
//! M6.0 SST read path + M6.1 flush).
//!
//! Ties the building blocks into a usable DB: a MemTable behind a write-ahead
//! log plus the on-disk SSTs of the current Version, with snapshot-aware point
//! lookups and a tombstone-hiding forward iterator.  `open` recovers durable
//! state: a VersionSet reconstructs the MANIFEST/CURRENT and the active WAL is
//! replayed into the MemTable, then that SAME log is reused for new appends so
//! committed writes survive reopen (the "reuse-logs" design).
//!
//! Flush (M6.1): when the live MemTable exceeds `write_buffer_size`, `write`
//! rotates it into an immutable MemTable + a fresh WAL and synchronously writes
//! it to a new L0 SSTable (see flush.zig), recording the file + rotated log in
//! the MANIFEST.  No leveled compaction yet (M6.2) — flush only ever produces
//! L0 files; the flush is synchronous (TODO: background flush thread).
//!
//! Reads consult the live MemTable FIRST (newest writes), then the immutable
//! MemTable being flushed (if any), then the current Version's SSTs via a
//! `TableCache`.  `get` returns the newest value visible at the snapshot, or
//! null on a tombstone/absence.  `newIterator` merges the memtable iterator(s)
//! with one iterator per SST file into a `MergingIterator` (ordered by the
//! InternalKeyComparator) and wraps it in a `DBIterator` for user-facing
//! snapshot/tombstone semantics.
//!
//! Standalone test note (Zig 0.16): this file uses `../...` imports that only
//! resolve when compiled as part of the `src`-rooted module.  To run the suite:
//!   printf 'test { _ = @import("db/db.zig"); }' > src/_verify.zig \
//!     && zig test src/_verify.zig && rm src/_verify.zig

const std = @import("std");

const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const comparator = @import("../util/comparator.zig");
const coding = @import("../util/coding.zig");
const internal_key = @import("../format/internal_key.zig");
const memtable_mod = @import("../memtable/memtable.zig");
const write_batch = @import("../format/write_batch.zig");
const log_writer = @import("../format/log_writer.zig");
const iterator = @import("../iterator/iterator.zig");
const merging_iterator = @import("../iterator/merging_iterator.zig");

const version_set = @import("../version/version_set.zig");
const version_edit = @import("../version/version_edit.zig");
const table_cache_mod = @import("../version/table_cache.zig");
const filename = @import("../version/filename.zig");
const log_format = @import("../format/log_format.zig");

const write_path = @import("write_path.zig");
const db_iter = @import("db_iter.zig");
const snapshot_mod = @import("snapshot.zig");
const recovery = @import("recovery.zig");
const flush = @import("flush.zig");
const compaction = @import("compaction.zig");

const Options = options_mod.Options;
const ReadOptions = options_mod.ReadOptions;
const WriteOptions = options_mod.WriteOptions;
const MemTable = memtable_mod.MemTable;
const WriteBatch = write_batch.WriteBatch;

pub const Snapshot = snapshot_mod.Snapshot;
pub const SnapshotList = snapshot_mod.SnapshotList;
pub const DBIterator = db_iter.DBIterator;

pub const DB = struct {
    gpa: std.mem.Allocator,
    env: env.Env,
    options: Options,
    name: []u8,
    ikcmp: internal_key.InternalKeyComparator,
    mem: *MemTable,
    /// Memtable being flushed (set during a synchronous flush, otherwise null).
    /// Between writes it is always null (the flush in `write` is synchronous),
    /// but `get`/`newIterator` consult it so a future background flush stays
    /// correct.  TODO(perf): background flush thread keeps this set for longer.
    imm: ?*MemTable = null,
    versions: *version_set.VersionSet,
    /// Opens + caches SST `Table` readers for the current Version's files.  Its
    /// InternalKeyComparator's address is taken into opened tables, so the cache
    /// must stay pinned — it lives inline in this heap-allocated DB.
    table_cache: table_cache_mod.TableCache,
    wal_file: env.WritableFile,
    wal: log_writer.Writer,
    last_sequence: u64,
    /// Live point-in-time snapshots.  Their oldest sequence bounds what
    /// compaction may discard (M6.3 snapshot pinning).
    snapshots: SnapshotList,
    // TODO(concurrency): DB write mutex (single-threaded for M4.1/M5.2).

    /// Open a DB rooted at directory `name` on `e`, recovering durable state.
    ///
    /// If a `CURRENT` file exists, the VersionSet is recovered from the
    /// MANIFEST and the active WAL is replayed into the MemTable; the SAME log
    /// is reopened for appending so subsequent writes continue in it (the
    /// "reuse-logs" design — recovered data lives in the single MemTable kept
    /// durable by the reused log, since there is no flush layer yet, Phase 6).
    ///
    /// Otherwise a fresh DB is created: the directory is made, a MANIFEST +
    /// CURRENT are written via the VersionSet, and a new empty WAL is opened.
    ///
    /// Returns a heap-allocated *DB; the caller must eventually call `close`.
    pub fn open(gpa: std.mem.Allocator, e: env.Env, name: []const u8, options: Options) !*DB {
        const self = try gpa.create(DB);
        errdefer gpa.destroy(self);

        self.gpa = gpa;
        self.env = e;
        self.options = options;
        self.last_sequence = 0;
        // ikcmp must live at a stable address (the memtable's entry comparator
        // points at it); `self` is heap-allocated so &self.ikcmp is stable.
        self.ikcmp = .{ .user = options.comparator };

        self.name = try gpa.dupe(u8, name);
        errdefer gpa.free(self.name);

        // Directory (no-op success if it already exists on MemEnv).
        try e.makeDir(name);

        self.mem = try MemTable.init(gpa, options.comparator);
        errdefer self.mem.deinit();
        self.imm = null;

        const vs = try gpa.create(version_set.VersionSet);
        errdefer gpa.destroy(vs);
        vs.* = try version_set.VersionSet.init(gpa, e, name, options);
        errdefer vs.deinit();
        self.versions = vs;

        // SST reader cache for the current Version's files.  `self.name` is the
        // borrowed DB directory; `self` is pinned (heap-allocated) so the cache's
        // internal comparator address stays stable.  No block cache wired yet.
        self.table_cache = table_cache_mod.TableCache.init(gpa, e, self.name, options, null);
        errdefer self.table_cache.deinit();

        // Live snapshots start empty; populated by getSnapshot/releaseSnapshot.
        self.snapshots = SnapshotList.init(gpa);
        errdefer self.snapshots.deinit();

        const current_path = try filename.currentFileName(gpa, name);
        defer gpa.free(current_path);

        if (e.fileExists(current_path)) {
            // ----- recover an existing DB -----------------------------------
            try vs.recover();

            const log_number = vs.logNumber();
            const log_path = try filename.logFileName(gpa, name, log_number);
            defer gpa.free(log_path);

            // Replay the active WAL into the memtable.  Sequences are assigned
            // from each batch's own header; max_seq is the highest seen.
            const max_seq = try recovery.replayLog(
                gpa,
                e,
                log_path,
                self.mem,
                vs.lastSequence() + 1,
            );
            self.last_sequence = @max(vs.lastSequence(), max_seq);

            // Reuse the SAME log: reopen it for appending and resume the writer
            // mid-block so the next reopen replays everything (reuse-logs).
            const file_size = e.getFileSize(log_path) catch 0;
            self.wal_file = try e.newAppendableFile(gpa, log_path);
            errdefer self.wal_file.close() catch {};
            self.wal = log_writer.Writer.initWithOffset(
                self.wal_file,
                @intCast(file_size % log_format.kBlockSize),
            );
        } else {
            // ----- fresh DB --------------------------------------------------
            const log_number = vs.newFileNumber();

            var edit = version_edit.VersionEdit.init();
            defer edit.deinit(gpa);
            try edit.setComparatorName(gpa, options.comparator.name());
            edit.setLogNumber(log_number);
            edit.setNextFileNumber(vs.nextFileNumber());
            edit.setLastSequence(0);
            try vs.logAndApply(&edit);

            const log_path = try filename.logFileName(gpa, name, log_number);
            defer gpa.free(log_path);

            self.wal_file = try e.newWritableFile(gpa, log_path);
            errdefer self.wal_file.close() catch {};
            self.wal = log_writer.Writer.init(self.wal_file);
            self.last_sequence = 0;
        }

        return self;
    }

    /// Flush+close the WAL, deinit the table cache + VersionSet + MemTable,
    /// free the DB.
    pub fn close(self: *DB) void {
        const gpa = self.gpa;
        self.wal_file.close() catch {};
        // Free any snapshots the client never released.
        self.snapshots.deinit();
        self.table_cache.deinit();
        self.versions.deinit();
        gpa.destroy(self.versions);
        // Flush is synchronous, so `imm` is always null between writes; free it
        // defensively in case a flush ever leaves one pending (e.g. a future
        // background flush or an error path).  Its data is durable in either the
        // SST (if the flush finished) or the WAL (if it didn't), so freeing the
        // RAM copy here loses nothing.
        if (self.imm) |imm| imm.deinit();
        self.mem.deinit();
        gpa.free(self.name);
        gpa.destroy(self);
    }

    /// Put a single key/value (a one-op WriteBatch under the hood).
    pub fn put(self: *DB, wopts: WriteOptions, key: []const u8, value: []const u8) !void {
        var batch = try WriteBatch.init(self.gpa);
        defer batch.deinit(self.gpa);
        try batch.put(self.gpa, key, value);
        try self.write(wopts, &batch);
    }

    /// Delete a single key (a one-op WriteBatch under the hood).
    pub fn delete(self: *DB, wopts: WriteOptions, key: []const u8) !void {
        var batch = try WriteBatch.init(self.gpa);
        defer batch.deinit(self.gpa);
        try batch.delete(self.gpa, key);
        try self.write(wopts, &batch);
    }

    /// Atomically apply `batch`: stamp its sequence, append it to the WAL
    /// (unless disabled), insert its records into the MemTable, and advance the
    /// last sequence by the batch's record count.
    pub fn write(self: *DB, wopts: WriteOptions, batch: *WriteBatch) !void {
        // TODO(concurrency): acquire DB write mutex here (single-threaded now).
        const first_sequence = self.last_sequence + 1;
        batch.setSequence(first_sequence);

        if (!wopts.disable_wal) {
            try self.wal.addRecord(self.gpa, batch.contents());
            // Make the record visible/durable.  MemEnv only commits buffered
            // bytes on flush/sync, so flush after every record so the on-"disk"
            // WAL reflects the write (and getFileSize sees it).
            if (wopts.sync) {
                try self.wal_file.sync();
            } else {
                try self.wal_file.flush();
            }
        }

        try write_path.insertBatch(self.mem, batch, first_sequence);
        self.last_sequence += batch.count();

        // If the active memtable is now over budget, rotate it into an immutable
        // memtable + a fresh WAL and flush it to an L0 SST.
        try self.maybeFlush();

        // A flush may have pushed a level over its compaction threshold; run any
        // pending leveled compactions before returning.
        try self.maybeScheduleCompaction();
    }

    /// Run leveled compactions until no level wants one (or a guard trips).
    /// Synchronous + single-threaded.  The compaction's `smallest_snapshot` is
    /// the oldest LIVE snapshot's sequence (or the latest sequence if none is
    /// live), so versions/tombstones still visible to a snapshot are never
    /// dropped (M6.3 snapshot pinning).  TODO(perf): background compaction
    /// thread.
    fn maybeScheduleCompaction(self: *DB) !void {
        // Pin compaction to the oldest live snapshot so it cannot discard a
        // version (or a tombstone) that a snapshot read could still need.
        const smallest_snapshot = self.snapshots.oldest() orelse self.last_sequence;
        // Guard against a pathological loop: each compaction must make progress
        // (it reduces a level's score by moving files down), so bound the number
        // of iterations generously by the current file count.
        var budget: usize = 0;
        {
            const v = self.versions.currentVersion();
            for (&v.files) |level| budget += level.items.len;
            budget = budget * 2 + 16;
        }

        while (budget > 0) : (budget -= 1) {
            var c = (try compaction.pickCompaction(
                self.gpa,
                self.versions,
                self.options.comparator,
            )) orelse break;
            defer c.deinit(self.gpa);

            try compaction.doCompaction(
                self.gpa,
                self.env,
                self.name,
                self.options,
                self.ikcmp.comparatorInterface(),
                self.options.comparator,
                self.versions,
                &c,
                smallest_snapshot,
            );
        }
    }

    /// If the live memtable has exceeded `write_buffer_size`, rotate it out and
    /// flush it to an L0 SST.  Synchronous for M6.1 (single-threaded).
    /// TODO(perf): background flush thread (keep serving reads from `imm`).
    fn maybeFlush(self: *DB) !void {
        if (self.imm != null) return; // a flush is already pending.
        if (self.mem.approximateMemoryUsage() < self.options.write_buffer_size) return;

        // 1. Rotate the WAL: allocate a new log number and open a fresh WAL.
        const new_log_number = self.versions.newFileNumber();
        const new_log_path = try filename.logFileName(self.gpa, self.name, new_log_number);
        defer self.gpa.free(new_log_path);

        var new_wal_file = try self.env.newWritableFile(self.gpa, new_log_path);
        errdefer new_wal_file.close() catch {};

        // 2. Rotate the memtable: the full one becomes immutable; install a new
        //    empty one for subsequent writes.
        const new_mem = try MemTable.init(self.gpa, self.options.comparator);
        errdefer new_mem.deinit();

        self.imm = self.mem;
        self.mem = new_mem;

        // Swap in the new WAL (close the old one — its data is going into the
        // SST and will not be replayed once logAndApply records the new log).
        const old_wal_file = self.wal_file;
        self.wal_file = new_wal_file;
        self.wal = log_writer.Writer.init(self.wal_file);
        old_wal_file.close() catch {};

        // 3. Flush the immutable memtable to an L0 SST (records the SST + the
        //    rotated log number in a VersionEdit).
        try flush.flushMemTable(
            self.gpa,
            self.env,
            self.name,
            self.options,
            self.ikcmp.comparatorInterface(),
            self.versions,
            self.imm.?,
            new_log_number,
            self.last_sequence,
        );

        // 4. Free the flushed memtable; no pending flush remains.
        self.imm.?.deinit();
        self.imm = null;
    }

    /// Point lookup visible at the snapshot (`ropts.snapshot` or the latest
    /// sequence).  Returns a freshly duped value the CALLER OWNS and must free,
    /// or null if the key is absent or deleted at that snapshot.
    pub fn get(self: *DB, ropts: ReadOptions, key: []const u8) !?[]u8 {
        const seq = ropts.snapshot orelse self.last_sequence;
        var lookup = try memtable_mod.LookupKey.init(self.gpa, key, seq);
        defer lookup.deinit(self.gpa);

        // 1. MemTable first (it holds the newest writes).
        if (self.mem.get(lookup)) |r| switch (r) {
            .found => |v| return try self.gpa.dupe(u8, v),
            .deleted => return null,
        };

        // 1b. The immutable memtable being flushed (if any) is next-newest.
        if (self.imm) |imm| {
            if (imm.get(lookup)) |r| switch (r) {
                .found => |v| return try self.gpa.dupe(u8, v),
                .deleted => return null,
            };
        }

        // 2. Not in the memtable: consult the on-disk SSTs via the current
        //    Version (LSM point lookup with snapshot + tombstone semantics).
        const version = self.versions.currentVersion();
        if (try version.get(self.gpa, &self.table_cache, self.options.comparator, key, seq)) |r| {
            switch (r) {
                // The value is freshly gpa-allocated by Version.get; the caller
                // owns and frees it.  Drop const since it is uniquely owned.
                .found => |v| return @constCast(v),
                .deleted => return null,
            }
        }
        return null;
    }

    /// Forward, snapshot-aware, tombstone-hiding iterator over the live MemTable
    /// merged with every SST file in the current Version.  Caller must call
    /// `.deinit()` on the returned iterator.
    ///
    /// Builds a child list — [memtable adapter] ++ one table iterator per file —
    /// merges them with a heap-allocated MergingIterator (ordered by the
    /// InternalKeyComparator, so equal user keys are visited newest-sequence
    /// first), and wraps that in a DBIterator at the snapshot.  The DBIterator
    /// owns the MergingIterator via `owned_inner_destroy`, whose deinit tears
    /// down every child (freeing the memtable adapter + the wrapped table
    /// iterators) before freeing the merging iterator itself.
    pub fn newIterator(self: *DB, gpa: std.mem.Allocator, ropts: ReadOptions) !DBIterator {
        const seq = ropts.snapshot orelse self.last_sequence;

        // Collect child iterators; on any error, deinit whatever we built.
        var children: std.ArrayListUnmanaged(iterator.Iterator) = .empty;
        errdefer {
            for (children.items) |it| it.deinit();
            children.deinit(gpa);
        }

        // 1. MemTable adapter (its vtable.deinit frees the heap adapter).
        const adapter = try gpa.create(MemIterAdapter);
        {
            errdefer gpa.destroy(adapter);
            adapter.* = .{ .gpa = gpa, .it = MemTable.Iterator.init(self.mem) };
            try children.append(gpa, adapter.genericIterator());
        }

        // 1b. The immutable memtable being flushed (if any), next in recency.
        if (self.imm) |imm| {
            const imm_adapter = try gpa.create(MemIterAdapter);
            errdefer gpa.destroy(imm_adapter);
            imm_adapter.* = .{ .gpa = gpa, .it = MemTable.Iterator.init(imm) };
            try children.append(gpa, imm_adapter.genericIterator());
        }

        // 2. One table iterator per file in the current Version.
        try self.versions.currentVersion().addIterators(gpa, &self.table_cache, &children);

        // 3. Merge over the internal-key order.  The merging iterator is heap
        //    allocated so its address is stable behind the DBIterator.
        const merger = try gpa.create(merging_iterator.MergingIterator);
        errdefer gpa.destroy(merger);
        merger.* = try merging_iterator.MergingIterator.init(
            gpa,
            self.ikcmp.comparatorInterface(),
            children.items,
        );
        // MergingIterator.init copied the children slice into its own buffer, so
        // release our temporary list (the copies are now owned by the merger).
        children.deinit(gpa);

        var dbit = DBIterator.init(gpa, merger.iterator(), self.options.comparator, seq);
        dbit.owned_inner = merger;
        dbit.owned_inner_destroy = destroyMerger;
        // M7.2: thread the prefix extractor + prefix-bounded scan flag so a
        // `seek` can bound iteration to the seek target's prefix.
        dbit.prefix_extractor = self.options.prefix_extractor;
        dbit.prefix_same_as_start = ropts.prefix_same_as_start;
        return dbit;
    }

    /// DBIterator ownership hook: tear down the merging iterator (which deinits
    /// every child) and free its heap allocation.
    fn destroyMerger(gpa: std.mem.Allocator, ctx: *anyopaque) void {
        const merger: *merging_iterator.MergingIterator = @ptrCast(@alignCast(ctx));
        merger.deinit();
        gpa.destroy(merger);
    }

    /// Take a snapshot pinned at the current latest sequence.  The returned
    /// `*Snapshot` is owned by the DB's SnapshotList until `releaseSnapshot`;
    /// while it is live, compaction will not discard versions visible to it.
    pub fn getSnapshot(self: *DB) !*Snapshot {
        return self.snapshots.newSnapshot(self.last_sequence);
    }

    /// Release a snapshot taken with `getSnapshot`, unpinning its sequence so
    /// later compactions may reclaim versions it was holding.
    pub fn releaseSnapshot(self: *DB, snap: *Snapshot) void {
        self.snapshots.release(snap);
    }
};

// ---------------------------------------------------------------------------
// MemTable -> generic Iterator adapter.
// ---------------------------------------------------------------------------
//
// The MemTable.Iterator yields `internalKey()` / `value()` but its `seek` takes
// a length-prefixed *memtable key* (varint32(ikey_len) ++ internal_key).  The
// generic Iterator contract — and DBIterator — pass raw internal keys, so this
// adapter re-prefixes on seek.  An owned scratch buffer holds the wrapped key.
const MemIterAdapter = struct {
    gpa: std.mem.Allocator,
    it: MemTable.Iterator,
    seek_buf: std.ArrayListUnmanaged(u8) = .empty,

    fn genericIterator(self: *MemIterAdapter) iterator.Iterator {
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

    fn cast(ctx: *anyopaque) *MemIterAdapter {
        return @ptrCast(@alignCast(ctx));
    }

    /// Generic-Iterator destructor: free the scratch buffer + the heap adapter.
    /// Reached when the adapter is handed out as a generic `Iterator` child of a
    /// MergingIterator (newIterator); the adapter was created with `self.gpa`.
    fn vDeinit(ctx: *anyopaque) void {
        const self = cast(ctx);
        const gpa = self.gpa;
        self.seek_buf.deinit(gpa);
        gpa.destroy(self);
    }

    fn vSeekToFirst(ctx: *anyopaque) void {
        cast(ctx).it.seekToFirst();
    }

    fn vSeekToLast(ctx: *anyopaque) void {
        _ = ctx; // unused: DBIterator never seeks the inner source backward.
    }

    fn vSeek(ctx: *anyopaque, target: []const u8) void {
        // `target` is a raw internal key; wrap it as a memtable key
        // (varint32(len) ++ internal_key) into the owned scratch buffer.
        const self = cast(ctx);
        self.seek_buf.clearRetainingCapacity();
        coding.putVarint32(&self.seek_buf, self.gpa, @intCast(target.len)) catch return;
        self.seek_buf.appendSlice(self.gpa, target) catch return;
        self.it.seek(self.seek_buf.items);
    }

    fn vNext(ctx: *anyopaque) void {
        cast(ctx).it.next();
    }

    fn vPrev(ctx: *anyopaque) void {
        _ = ctx; // forward-only inner cursor for M4.1.
    }

    fn vValid(ctx: *anyopaque) bool {
        return cast(ctx).it.valid();
    }

    fn vKey(ctx: *anyopaque) []const u8 {
        return cast(ctx).it.internalKey();
    }

    fn vValue(ctx: *anyopaque) []const u8 {
        return cast(ctx).it.value();
    }

    fn vStatus(ctx: *anyopaque) ?anyerror {
        _ = ctx;
        return null;
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const MemEnv = env.MemEnv;

fn openTestDB(gpa: std.mem.Allocator, me: *MemEnv) !*DB {
    return DB.open(gpa, me.env(), "testdb", .{});
}

test "put/get basic" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v1");
    const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("v1", got);
}

test "put overwrite returns newest" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v1");
    try db.put(.{}, "k", "v2");
    const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("v2", got);
}

test "delete removes key" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v1");
    try db.delete(.{}, "k");
    try testing.expect((try db.get(.{}, "k")) == null);
}

test "get absent key is null" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try testing.expect((try db.get(.{}, "missing")) == null);
}

test "write applies multi-op batch atomically" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    // Seed a so the delete has something to remove.
    try db.put(.{}, "a", "a-old");

    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);
    try wb.put(gpa, "a", "a-new");
    try wb.put(gpa, "b", "b-val");
    try wb.delete(gpa, "a");
    try db.write(.{}, &wb);

    // a: put then delete in the same batch → gone.
    try testing.expect((try db.get(.{}, "a")) == null);
    // b present.
    const b = try db.get(.{}, "b") orelse return error.TestExpectedFound;
    defer gpa.free(b);
    try testing.expectEqualStrings("b-val", b);
}

test "snapshot isolation" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v1");
    try db.put(.{}, "k", "v2");
    const snap = try db.getSnapshot();
    defer db.releaseSnapshot(snap);

    try db.put(.{}, "k", "v3");

    // Read at the snapshot → v2 (the value as of the snapshot).
    const at_snap = try db.get(.{ .snapshot = snap.sequence }, "k") orelse return error.TestExpectedFound;
    defer gpa.free(at_snap);
    try testing.expectEqualStrings("v2", at_snap);

    // Read without snapshot → v3.
    const latest = try db.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(latest);
    try testing.expectEqualStrings("v3", latest);
}

test "newIterator: latest-only sorted scan with tombstone hidden, seek works" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    // Insert a..e with an overwrite of "b" and a delete of "d".
    try db.put(.{}, "a", "a1");
    try db.put(.{}, "b", "b1");
    try db.put(.{}, "c", "c1");
    try db.put(.{}, "d", "d1");
    try db.put(.{}, "e", "e1");
    try db.put(.{}, "b", "b2"); // overwrite
    try db.delete(.{}, "d"); // tombstone

    var it = try db.newIterator(gpa, .{});
    defer it.deinit();

    const exp_k = [_][]const u8{ "a", "b", "c", "e" };
    const exp_v = [_][]const u8{ "a1", "b2", "c1", "e1" };
    var i: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        try testing.expect(i < exp_k.len);
        try testing.expectEqualStrings(exp_k[i], it.key());
        try testing.expectEqualStrings(exp_v[i], it.value());
        i += 1;
    }
    try testing.expectEqual(exp_k.len, i);

    // seek("c") lands on c.
    it.seek("c");
    try testing.expect(it.valid());
    try testing.expectEqualStrings("c", it.key());
    try testing.expectEqualStrings("c1", it.value());
}

test "sequence numbers count single puts" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    const n: u64 = 5;
    var i: u64 = 0;
    while (i < n) : (i += 1) {
        try db.put(.{}, "k", "v");
    }
    try testing.expectEqual(n, db.last_sequence);
}

test "WAL file is non-empty after writes" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "v");

    // The active WAL is named by the VersionSet's log number; on a fresh DB
    // that is the first file number handed out.
    const log_path = try filename.logFileName(gpa, "testdb", db.versions.logNumber());
    defer gpa.free(log_path);
    const size = try me.env().getFileSize(log_path);
    try testing.expect(size > 0);
}

// ---------------------------------------------------------------------------
// M5.2 — durability / recovery
// ---------------------------------------------------------------------------

test "fresh DB creates CURRENT, MANIFEST, and a log file" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const db = try DB.open(gpa, e, "freshdb", .{});
    defer db.close();

    const cur = try filename.currentFileName(gpa, "freshdb");
    defer gpa.free(cur);
    try testing.expect(e.fileExists(cur));

    // CURRENT names a MANIFEST that exists.
    const manifest = try filename.manifestFileName(gpa, "freshdb", db.versions.manifestFileNumber());
    defer gpa.free(manifest);
    try testing.expect(e.fileExists(manifest));

    // The active log file exists.
    const log_path = try filename.logFileName(gpa, "freshdb", db.versions.logNumber());
    defer gpa.free(log_path);
    try testing.expect(e.fileExists(log_path));
}

test "reopen recovers data from the WAL" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        const db = try DB.open(gpa, e, "recdb", .{});
        defer db.close();
        try db.put(.{}, "a", "1");
        try db.put(.{}, "b", "2");
        try db.delete(.{}, "a");
    }

    // Reopen the SAME MemEnv + name.
    {
        const db = try DB.open(gpa, e, "recdb", .{});
        defer db.close();

        try testing.expect((try db.get(.{}, "a")) == null);

        const b = try db.get(.{}, "b") orelse return error.TestExpectedFound;
        defer gpa.free(b);
        try testing.expectEqualStrings("2", b);
    }
}

test "multi-session append: writes accumulate across reopens" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        const db = try DB.open(gpa, e, "msdb", .{});
        defer db.close();
        try db.put(.{}, "a", "1");
        try db.put(.{}, "b", "2");
        try db.delete(.{}, "a");
    }
    {
        const db = try DB.open(gpa, e, "msdb", .{});
        defer db.close();
        try db.put(.{}, "c", "3");
    }
    {
        const db = try DB.open(gpa, e, "msdb", .{});
        defer db.close();

        try testing.expect((try db.get(.{}, "a")) == null);

        const b = try db.get(.{}, "b") orelse return error.TestExpectedFound;
        defer gpa.free(b);
        try testing.expectEqualStrings("2", b);

        const c = try db.get(.{}, "c") orelse return error.TestExpectedFound;
        defer gpa.free(c);
        try testing.expectEqualStrings("3", c);
    }
}

test "sequence continuity: a new put outranks all recovered writes" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        const db = try DB.open(gpa, e, "seqdb", .{});
        defer db.close();
        try db.put(.{}, "x", "old1");
        try db.put(.{}, "x", "old2");
        try db.put(.{}, "y", "yv");
    }

    {
        const db = try DB.open(gpa, e, "seqdb", .{});
        defer db.close();

        // Recovered last_sequence covers all 3 prior writes.
        try testing.expect(db.last_sequence >= 3);
        const recovered_seq = db.last_sequence;

        // A snapshot taken right after reopen sees the recovered values.
        const snap = try db.getSnapshot();
        defer db.releaseSnapshot(snap);

        // A new write must get a strictly higher sequence (no regression).
        try db.put(.{}, "x", "new");
        try testing.expect(db.last_sequence > recovered_seq);

        // The pre-reopen value of x is still readable at the post-reopen snapshot.
        const at_snap = try db.get(.{ .snapshot = snap.sequence }, "x") orelse return error.TestExpectedFound;
        defer gpa.free(at_snap);
        try testing.expectEqualStrings("old2", at_snap);

        // The latest read sees the new write.
        const latest = try db.get(.{}, "x") orelse return error.TestExpectedFound;
        defer gpa.free(latest);
        try testing.expectEqualStrings("new", latest);
    }
}

test "crash recovery: data is present without a clean close" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Open, write, but deliberately do NOT close this handle in the usual way.
    // The WAL is flushed per write, so the bytes are committed to the MemEnv.
    const db1 = try DB.open(gpa, e, "crashdb", .{});
    try db1.put(.{}, "k1", "v1");
    try db1.put(.{}, "k2", "v2");

    // Simulate a crash + restart: open a SECOND handle on the same MemEnv.
    {
        const db2 = try DB.open(gpa, e, "crashdb", .{});
        defer db2.close();

        const v1 = try db2.get(.{}, "k1") orelse return error.TestExpectedFound;
        defer gpa.free(v1);
        try testing.expectEqualStrings("v1", v1);

        const v2 = try db2.get(.{}, "k2") orelse return error.TestExpectedFound;
        defer gpa.free(v2);
        try testing.expectEqualStrings("v2", v2);
    }

    // Tidy up the leaked-on-purpose first handle so the test is leak-free.
    db1.close();
}

test "corrupt WAL tail: committed prefix is recovered, no error" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    var log_path: []u8 = undefined;
    {
        const db = try DB.open(gpa, e, "corruptdb", .{});
        defer db.close();
        try db.put(.{}, "a", "1");
        try db.put(.{}, "b", "2");
        log_path = try filename.logFileName(gpa, "corruptdb", db.versions.logNumber());
    }
    defer gpa.free(log_path);

    // Corrupt the LAST few bytes of the committed WAL in the MemEnv.
    {
        const bytes = try readAllBytes(e, gpa, log_path);
        defer gpa.free(bytes);
        try testing.expect(bytes.len > 4);
        // Flip the trailing bytes (the last record's tail).
        var i: usize = bytes.len - 4;
        while (i < bytes.len) : (i += 1) bytes[i] ^= 0xff;

        var wf = try e.newWritableFile(gpa, log_path); // truncates + rewrites
        errdefer wf.close() catch {};
        try wf.append(bytes);
        try wf.flush();
        try wf.close();
    }

    // Reopen: the committed prefix ("a") must be recovered; no error thrown.
    {
        const db = try DB.open(gpa, e, "corruptdb", .{});
        defer db.close();

        const a = try db.get(.{}, "a") orelse return error.TestExpectedFound;
        defer gpa.free(a);
        try testing.expectEqualStrings("1", a);
    }
}

/// Read the entire committed contents of `path` (caller frees).
fn readAllBytes(e: env.Env, gpa: std.mem.Allocator, path: []const u8) ![]u8 {
    var sf = try e.newSequentialFile(gpa, path);
    defer sf.close() catch {};
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(gpa);
    var chunk: [4096]u8 = undefined;
    while (true) {
        const n = try sf.read(&chunk);
        if (n == 0) break;
        try out.appendSlice(gpa, chunk[0..n]);
    }
    return out.toOwnedSlice(gpa);
}

// ===========================================================================
// M6.0 — SST read path: reads consult the current Version's SSTs + memtable.
// ===========================================================================

const table_builder = @import("../format/table_builder.zig");
const bloom = @import("../format/bloom.zig");

/// Encode `user ++ fixed64(packSequenceAndType(seq, t))` (caller frees).
fn encodeIkey(gpa: std.mem.Allocator, user: []const u8, seq: u64, t: internal_key.ValueType) ![]u8 {
    const out = try gpa.alloc(u8, user.len + 8);
    @memcpy(out[0..user.len], user);
    coding.encodeFixed64(out[user.len..][0..8], internal_key.packSequenceAndType(seq, t));
    return out;
}

const M6Entry = struct { user: []const u8, seq: u64, t: internal_key.ValueType, value: []const u8 };

/// Build an SST of internal keys at `<dbname>/<number>.sst` (entries in
/// internal-key order), opened later with the InternalKeyComparator.  Writes the
/// smallest/largest internal keys (caller-owned) and returns the file size.
fn buildM6SST(
    gpa: std.mem.Allocator,
    e: env.Env,
    dbname: []const u8,
    number: u64,
    entries: []const M6Entry,
    smallest: *[]u8,
    largest: *[]u8,
) !u64 {
    const path = try filename.tableFileName(gpa, dbname, number);
    defer gpa.free(path);

    const policy = bloom.BloomFilterPolicy.init(10);
    var ikc = internal_key.InternalKeyComparator{ .user = comparator.bytewise };
    const opts = Options{ .comparator = ikc.comparatorInterface() };

    var first: ?[]u8 = null;
    var last: ?[]u8 = null;
    errdefer {
        if (first) |s| gpa.free(s);
        if (last) |l| gpa.free(l);
    }

    var wf = try e.newWritableFile(gpa, path);
    errdefer wf.close() catch {};
    var tb = try table_builder.TableBuilder.init(gpa, opts, wf, policy);
    defer tb.deinit();
    for (entries) |en| {
        const ik = try encodeIkey(gpa, en.user, en.seq, en.t);
        defer gpa.free(ik);
        try tb.add(ik, en.value);
        if (first == null) first = try gpa.dupe(u8, ik);
        if (last) |l| gpa.free(l);
        last = try gpa.dupe(u8, ik);
    }
    try tb.finish();
    try wf.close();

    smallest.* = first.?;
    largest.* = last.?;
    return e.getFileSize(path);
}

test "M6.0: get reads from SST; memtable shadows SST by sequence; scan merges both" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const db = try DB.open(gpa, e, "m6db", .{});
    defer db.close();

    // Build an SST with x@1="sst_x", y@2="sst_y" (internal-key order: x then y).
    const entries = [_]M6Entry{
        .{ .user = "x", .seq = 1, .t = .value, .value = "sst_x" },
        .{ .user = "y", .seq = 2, .t = .value, .value = "sst_y" },
    };
    var smallest: []u8 = undefined;
    var largest: []u8 = undefined;
    const file_number = db.versions.newFileNumber();
    const size = try buildM6SST(gpa, e, "m6db", file_number, &entries, &smallest, &largest);
    defer gpa.free(smallest);
    defer gpa.free(largest);

    // Add the SST to L0 via a VersionEdit (test reaches into private fields).
    {
        var edit = version_edit.VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.addFile(gpa, 0, file_number, size, smallest, largest);
        edit.setLastSequence(2);
        try db.versions.logAndApply(&edit);
    }

    // The SST used sequences 1,2; make the memtable write outrank them.
    db.last_sequence = 2;
    try db.put(.{}, "y", "mem_y"); // gets seq 3 → shadows y@2 in the SST

    // get("x") → from the SST (not in memtable).
    {
        const got = try db.get(.{}, "x") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("sst_x", got);
    }

    // get("y") → memtable shadows the SST (higher sequence).
    {
        const got = try db.get(.{}, "y") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("mem_y", got);
    }

    // get absent → null.
    try testing.expect((try db.get(.{}, "zzz")) == null);

    // Full scan merges memtable + SST: x→"sst_x", y→"mem_y" (memtable wins), in
    // user-key order.
    {
        var it = try db.newIterator(gpa, .{});
        defer it.deinit();
        const exp_k = [_][]const u8{ "x", "y" };
        const exp_v = [_][]const u8{ "sst_x", "mem_y" };
        var i: usize = 0;
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            try testing.expect(i < exp_k.len);
            try testing.expectEqualStrings(exp_k[i], it.key());
            try testing.expectEqualStrings(exp_v[i], it.value());
            i += 1;
        }
        try testing.expectEqual(exp_k.len, i);
        try testing.expect(it.status() == null);
    }
}

test "M6.0: SST tombstone hides an older memtable value at the right snapshot" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const db = try DB.open(gpa, e, "m6tomb", .{});
    defer db.close();

    // SST holds k = value@5 "kept".
    const entries = [_]M6Entry{.{ .user = "k", .seq = 5, .t = .value, .value = "kept" }};
    var smallest: []u8 = undefined;
    var largest: []u8 = undefined;
    const file_number = db.versions.newFileNumber();
    const size = try buildM6SST(gpa, e, "m6tomb", file_number, &entries, &smallest, &largest);
    defer gpa.free(smallest);
    defer gpa.free(largest);

    {
        var edit = version_edit.VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.addFile(gpa, 0, file_number, size, smallest, largest);
        edit.setLastSequence(5);
        try db.versions.logAndApply(&edit);
    }
    db.last_sequence = 5;

    // Latest read sees the SST value.
    {
        const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("kept", got);
    }

    // A memtable tombstone (seq 6) hides it.
    try db.delete(.{}, "k");
    try testing.expect((try db.get(.{}, "k")) == null);

    // But at a snapshot BEFORE the tombstone, the SST value is still visible.
    {
        const got = try db.get(.{ .snapshot = 5 }, "k") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("kept", got);
    }
}

// ===========================================================================
// M7.2 — prefix-bounded iteration.
// ===========================================================================

const prefix_mod = @import("../rocks/prefix.zig");

test "M7.2: prefix_same_as_start scan stops at the prefix boundary" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // A 2-byte fixed prefix extractor.  It must live for the DB's lifetime
    // (the Options copy holds a PrefixExtractor whose ctx points into it).
    var fpe = prefix_mod.FixedPrefixExtractor.init(2);
    const db = try DB.open(gpa, e, "pfxscan", .{ .prefix_extractor = fpe.extractor() });
    defer db.close();

    try db.put(.{}, "aa1", "1");
    try db.put(.{}, "aa2", "2");
    try db.put(.{}, "bb1", "3");

    // Prefix-bounded scan from "aa": only "aa1","aa2", then invalid (does NOT
    // continue into "bb1" whose prefix differs).
    var it = try db.newIterator(gpa, .{ .prefix_same_as_start = true });
    defer it.deinit();

    const exp_k = [_][]const u8{ "aa1", "aa2" };
    var i: usize = 0;
    it.seek("aa");
    while (it.valid()) : (it.next()) {
        try testing.expect(i < exp_k.len);
        try testing.expectEqualStrings(exp_k[i], it.key());
        i += 1;
    }
    try testing.expectEqual(exp_k.len, i);
    try testing.expect(it.status() == null);

    // Sanity: WITHOUT prefix_same_as_start, the scan crosses into "bb1".
    var it2 = try db.newIterator(gpa, .{});
    defer it2.deinit();
    var seen: usize = 0;
    it2.seek("aa");
    while (it2.valid()) : (it2.next()) seen += 1;
    try testing.expectEqual(@as(usize, 3), seen);
}

// ===========================================================================
// M6.1 — flush (memtable -> L0 SST): the durability-through-SST gate.
// ===========================================================================

/// Total number of SST files across every level of the current Version.
fn totalSSTFiles(db: *DB) usize {
    var n: usize = 0;
    const v = db.versions.currentVersion();
    for (&v.files) |level| n += level.items.len;
    return n;
}

test "M6.1 flush: small write_buffer triggers an L0 SST that serves reads" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // A tiny write buffer so a handful of puts overflows it and forces a flush.
    const db = try DB.open(gpa, e, "flushdb", .{ .write_buffer_size = 1024 });
    defer db.close();

    // Write enough distinct keys to exceed ~1KB of memtable arena.
    const n: usize = 64;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>5}", .{i});
        const v = try std.fmt.bufPrint(&vbuf, "value-{d:0>5}-payload", .{i});
        try db.put(.{}, k, v);
    }

    // A flush must have produced at least one L0 SST file on "disk".
    try testing.expect(totalSSTFiles(db) >= 1);
    {
        // Whatever the first SST's number is, its file must exist.
        const l0 = db.versions.currentVersion().files[0].items;
        try testing.expect(l0.len >= 1);
        const sst_path = try filename.tableFileName(gpa, "flushdb", l0[0].number);
        defer gpa.free(sst_path);
        try testing.expect(e.fileExists(sst_path));
    }

    // Every key remains readable (served from the SST and/or the live memtable).
    i = 0;
    while (i < n) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>5}", .{i});
        const want = try std.fmt.bufPrint(&vbuf, "value-{d:0>5}-payload", .{i});
        const got = try db.get(.{}, k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(want, got);
    }

    // A full forward scan returns all keys in order.
    {
        var it = try db.newIterator(gpa, .{});
        defer it.deinit();
        var seen: usize = 0;
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            var kbuf: [16]u8 = undefined;
            const want = try std.fmt.bufPrint(&kbuf, "key{d:0>5}", .{seen});
            try testing.expectEqualStrings(want, it.key());
            seen += 1;
        }
        try testing.expectEqual(n, seen);
        try testing.expect(it.status() == null);
    }
}

test "M6.1 flush: overwrite + delete across flushes with snapshot semantics" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Force a flush on essentially every put (buffer near zero).
    const db = try DB.open(gpa, e, "flushmv", .{ .write_buffer_size = 1 });
    defer db.close();

    try db.put(.{}, "k", "v1"); // -> flushed to an L0 SST
    const snap_after_v1 = try db.getSnapshot(); // sees v1
    defer db.releaseSnapshot(snap_after_v1);

    try db.put(.{}, "k", "v2"); // -> flushed to a second L0 SST (newer)

    // Latest read sees v2 (newer L0 file shadows the older one).
    {
        const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("v2", got);
    }

    // At the snapshot taken after v1 (before v2), the value is still v1 — this
    // exercises multiple versions of one user key living in distinct SSTs and
    // read back with snapshot semantics (the IKC block-builder fix).
    {
        const got = try db.get(.{ .snapshot = snap_after_v1.sequence }, "k") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("v1", got);
    }

    // Delete + flush: the tombstone in the newest L0 hides all older versions.
    try db.delete(.{}, "k");
    try testing.expect((try db.get(.{}, "k")) == null);

    // Several SST files must now exist (v1, v2, tombstone).
    try testing.expect(totalSSTFiles(db) >= 3);
}

test "M6.1 flush: reopen recovers SST data + unflushed WAL data" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        // Small buffer -> early keys flush to SSTs; the very last writes stay in
        // the active memtable / WAL (not yet flushed).
        const db = try DB.open(gpa, e, "reopendb", .{ .write_buffer_size = 512 });
        defer db.close();

        var i: usize = 0;
        while (i < 40) : (i += 1) {
            var kbuf: [16]u8 = undefined;
            var vbuf: [16]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "k{d:0>4}", .{i});
            const v = try std.fmt.bufPrint(&vbuf, "v{d:0>4}", .{i});
            try db.put(.{}, k, v);
        }
        // Some flush must have happened (SST present) and the memtable still
        // holds the most recent writes (only-in-WAL keys).
        try testing.expect(totalSSTFiles(db) >= 1);
    }

    // Reopen: SST data (via MANIFEST) + WAL replay must reconstruct everything.
    {
        const db = try DB.open(gpa, e, "reopendb", .{ .write_buffer_size = 512 });
        defer db.close();

        var i: usize = 0;
        while (i < 40) : (i += 1) {
            var kbuf: [16]u8 = undefined;
            var vbuf: [16]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "k{d:0>4}", .{i});
            const want = try std.fmt.bufPrint(&vbuf, "v{d:0>4}", .{i});
            const got = try db.get(.{}, k) orelse return error.TestExpectedFound;
            defer gpa.free(got);
            try testing.expectEqualStrings(want, got);
        }
    }
}

test "M6.1 flush: multiple L0 files merge newest-first" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const db = try DB.open(gpa, e, "l0merge", .{ .write_buffer_size = 1 });
    defer db.close();

    // Three flushes, each overwriting "k" and adding a fresh key.
    try db.put(.{}, "k", "first");
    try db.put(.{}, "a", "av"); // forces flush of {k=first}
    try db.put(.{}, "k", "second");
    try db.put(.{}, "b", "bv"); // forces flush of {k=second, a=av}
    try db.put(.{}, "k", "third");
    try db.put(.{}, "c", "cv"); // forces flush of {k=third, b=bv}

    // Multiple L0 files exist.
    try testing.expect(db.versions.currentVersion().files[0].items.len >= 2);

    // The newest version of "k" wins across the L0 files.
    {
        const got = try db.get(.{}, "k") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("third", got);
    }

    // Distinct keys from different flushes are all present.
    for ([_]struct { k: []const u8, v: []const u8 }{
        .{ .k = "a", .v = "av" },
        .{ .k = "b", .v = "bv" },
        .{ .k = "c", .v = "cv" },
    }) |kv| {
        const got = try db.get(.{}, kv.k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(kv.v, got);
    }
}
