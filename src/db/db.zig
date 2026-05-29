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
const delete_range = @import("../rocks/delete_range.zig");
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
    /// Whether this DB owns + manages its own WAL (M7.0).  A normal single-CF DB
    /// owns its WAL (true).  A per-column-family sub-LSM opened via `openCf` does
    /// NOT — the multi-CF `CfDB` owns ONE shared WAL across all families — so
    /// `wal_file`/`wal` are left undefined and `close`/`write` never touch them.
    owns_wal: bool = true,
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
        self.owns_wal = true;
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
        // internal comparator address stays stable.  Block cache is optional and
        // is taken from Options.block_cache (may be null).
        self.table_cache = table_cache_mod.TableCache.init(gpa, e, self.name, options, options.block_cache);
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

    /// Open a per-column-family sub-LSM rooted at directory `name` WITHOUT a WAL
    /// of its own (M7.0).  Used by `CfDB`: the multi-CF database owns ONE shared
    /// WAL across all families and replays it into each CF's memtable, so a
    /// ColumnFamily only needs {memtable, imm, VersionSet (own MANIFEST/CURRENT),
    /// table_cache, flush, compaction} — everything the existing `DB` already
    /// provides MINUS the WAL.
    ///
    /// On an existing CF directory the per-CF VersionSet is recovered from its
    /// MANIFEST (restoring its SST files + last_sequence); the memtable is left
    /// EMPTY (the caller replays the shared WAL into it).  On a fresh CF
    /// directory a MANIFEST/CURRENT is created.  No `.log` file is ever opened.
    ///
    /// The returned `*DB` has `owns_wal = false`: `close` will not touch the WAL,
    /// and `write` must NOT be called on it (use `applyBatchNoWal`).  Caller
    /// `close`s it.
    pub fn openCf(gpa: std.mem.Allocator, e: env.Env, name: []const u8, options: Options) !*DB {
        const self = try gpa.create(DB);
        errdefer gpa.destroy(self);

        self.gpa = gpa;
        self.env = e;
        self.options = options;
        self.last_sequence = 0;
        self.owns_wal = false;
        self.ikcmp = .{ .user = options.comparator };

        self.name = try gpa.dupe(u8, name);
        errdefer gpa.free(self.name);

        try e.makeDir(name);

        self.mem = try MemTable.init(gpa, options.comparator);
        errdefer self.mem.deinit();
        self.imm = null;

        const vs = try gpa.create(version_set.VersionSet);
        errdefer gpa.destroy(vs);
        vs.* = try version_set.VersionSet.init(gpa, e, name, options);
        errdefer vs.deinit();
        self.versions = vs;

        self.table_cache = table_cache_mod.TableCache.init(gpa, e, self.name, options, options.block_cache);
        errdefer self.table_cache.deinit();

        self.snapshots = SnapshotList.init(gpa);
        errdefer self.snapshots.deinit();

        const current_path = try filename.currentFileName(gpa, name);
        defer gpa.free(current_path);

        if (e.fileExists(current_path)) {
            // Recover the per-CF VersionSet (SST files + sequences).  The shared
            // WAL replay (done by CfDB) repopulates the memtable; we do NOT touch
            // any per-CF `.log` file (there is none in the CF design).
            try vs.recover();
            self.last_sequence = vs.lastSequence();
        } else {
            // Fresh CF: write an initial MANIFEST/CURRENT.  No log file.
            var edit = version_edit.VersionEdit.init();
            defer edit.deinit(gpa);
            try edit.setComparatorName(gpa, options.comparator.name());
            edit.setNextFileNumber(vs.nextFileNumber());
            edit.setLastSequence(0);
            try vs.logAndApply(&edit);
            self.last_sequence = 0;
        }

        // wal_file / wal are intentionally left undefined: owns_wal == false means
        // no code path on this DB reads them.
        return self;
    }

    /// Apply the records of `batch` that target column family `cf_id` into this
    /// CF's memtable WITHOUT any WAL append (M7.0).  The CfDB has already appended
    /// the (CF-tagged) batch to the SHARED WAL exactly once and stamped the shared
    /// sequence; here we insert only this CF's records (each consuming a slot in
    /// the shared sequence space starting at `first_sequence`) and advance
    /// flush/compaction.  `set_last_sequence` is the DB-wide last sequence after
    /// the batch, recorded onto this CF so its reads/snapshots and any flush use
    /// the shared sequence space.
    pub fn applyBatchNoWal(self: *DB, batch: *const WriteBatch, cf_id: u32, first_sequence: u64, set_last_sequence: u64) !void {
        try write_path.insertBatchForCf(self.mem, batch, cf_id, first_sequence);
        self.last_sequence = set_last_sequence;
        try self.maybeFlush();
        try self.maybeScheduleCompaction();
    }

    /// Flush+close the WAL, deinit the table cache + VersionSet + MemTable,
    /// free the DB.
    pub fn close(self: *DB) void {
        const gpa = self.gpa;
        // Only close the WAL we own.  A per-CF sub-LSM (openCf) shares the
        // CfDB's WAL and must not close it here.
        if (self.owns_wal) self.wal_file.close() catch {};
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

    /// Delete every key in `[begin, end)` (half-open, `end` exclusive) as of this
    /// op's sequence (a one-op range-del WriteBatch, M7.5).  Keys written AFTER
    /// this (higher sequence) are not affected; keys with a lower sequence covered
    /// by the range become invisible.  A degenerate range (`begin >= end`) deletes
    /// nothing.
    pub fn deleteRange(self: *DB, wopts: WriteOptions, begin: []const u8, end: []const u8) !void {
        var batch = try WriteBatch.init(self.gpa);
        defer batch.deinit(self.gpa);
        try batch.deleteRange(self.gpa, begin, end);
        try self.write(wopts, &batch);
    }

    /// Record a merge operand for `key` (a one-op WriteBatch, M7.1).  The operand
    /// is combined lazily — on read/compaction — with the existing value and any
    /// other pending operands via the configured `merge_operator`.  Returns
    /// `error.MergeOperatorNotConfigured` if no operator is configured (a usage
    /// error: an unmerged operand could never be interpreted on read).
    pub fn merge(self: *DB, wopts: WriteOptions, key: []const u8, value: []const u8) !void {
        if (self.options.merge_operator == null) return error.MergeOperatorNotConfigured;
        var batch = try WriteBatch.init(self.gpa);
        defer batch.deinit(self.gpa);
        try batch.merge(self.gpa, key, value);
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

    /// Run compactions for the configured style until nothing wants one (or a
    /// guard trips).  Synchronous + single-threaded.
    ///
    /// `smallest_snapshot` is the oldest LIVE snapshot's sequence (or the latest
    /// sequence if none is live), so versions/tombstones still visible to a
    /// snapshot are never dropped (M6.3 snapshot pinning).
    ///
    /// Style dispatch (M7.3):
    ///   * `.level`     — the classic leveled picker/merge loop.
    ///   * `.universal` — merge similarly-sized L0 runs (kept in L0) until none
    ///                    qualifies (the merged run lowers the L0 count/ratio).
    ///   * `.fifo`      — drop the oldest L0 files until under the byte budget.
    /// TODO(perf): background compaction thread.
    fn maybeScheduleCompaction(self: *DB) !void {
        // Pin compaction to the oldest live snapshot so it cannot discard a
        // version (or a tombstone) that a snapshot read could still need.  When no
        // snapshot is live we fall back to the latest sequence; `has_live_snapshot`
        // records which case we are in so the M7.4 compaction filter never
        // modifies an entry a live snapshot can still read.
        const oldest_snapshot = self.snapshots.oldest();
        const has_live_snapshot = oldest_snapshot != null;
        const smallest_snapshot = oldest_snapshot orelse self.last_sequence;
        // Guard against a pathological loop: each compaction must make progress
        // (it reduces a level's score by moving files down / merging runs / evicting
        // files), so bound the number of iterations generously by the file count.
        var budget: usize = 0;
        {
            const v = self.versions.currentVersion();
            for (&v.files) |level| budget += level.items.len;
            budget = budget * 2 + 16;
        }

        switch (self.options.compaction_style) {
            .level => {
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
                        &self.table_cache,
                        &c,
                        smallest_snapshot,
                        has_live_snapshot,
                    );
                }
            },
            .universal => {
                while (budget > 0) : (budget -= 1) {
                    var c = (try compaction.pickUniversalCompaction(
                        self.gpa,
                        self.versions,
                        self.options,
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
                        &self.table_cache,
                        &c,
                        smallest_snapshot,
                        has_live_snapshot,
                    );
                }
            },
            .fifo => {
                while (budget > 0) : (budget -= 1) {
                    const evicted = try compaction.runFifoEviction(
                        self.gpa,
                        self.env,
                        self.name,
                        self.versions,
                        &self.table_cache,
                        self.options.fifo_max_table_files_size,
                    );
                    if (!evicted) break;
                }
            },
        }
    }

    /// If the live memtable has exceeded `write_buffer_size`, rotate it out and
    /// flush it to an L0 SST.  Synchronous for M6.1 (single-threaded).
    /// TODO(perf): background flush thread (keep serving reads from `imm`).
    fn maybeFlush(self: *DB) !void {
        if (self.imm != null) return; // a flush is already pending.
        if (self.mem.approximateMemoryUsage() < self.options.write_buffer_size) return;

        // 1. Allocate a new log number.  When this DB owns its WAL, also open a
        //    fresh WAL and swap it in (the classic single-CF flush).  A per-CF
        //    sub-LSM (owns_wal == false) shares the CfDB's single WAL and must NOT
        //    touch any per-CF log file — the new_log_number is still recorded in
        //    its MANIFEST for consistency but no `.log` is created/rotated.
        const new_log_number = self.versions.newFileNumber();

        // 2. Rotate the memtable: the full one becomes immutable; install a new
        //    empty one for subsequent writes.
        const new_mem = try MemTable.init(self.gpa, self.options.comparator);
        errdefer new_mem.deinit();

        // Remember the old log number so we can delete the stale file after the
        // flush commits it to the MANIFEST.  Only meaningful when this DB owns its
        // WAL (single-CF); per-CF sub-LSMs (owns_wal == false) have no log files.
        const old_log_number: u64 = if (self.owns_wal) self.versions.logNumber() else 0;

        if (self.owns_wal) {
            const new_log_path = try filename.logFileName(self.gpa, self.name, new_log_number);
            defer self.gpa.free(new_log_path);

            var new_wal_file = try self.env.newWritableFile(self.gpa, new_log_path);
            errdefer new_wal_file.close() catch {};

            self.imm = self.mem;
            self.mem = new_mem;

            // Swap in the new WAL (close the old one — its data is going into the
            // SST and will not be replayed once logAndApply records the new log).
            const old_wal_file = self.wal_file;
            self.wal_file = new_wal_file;
            self.wal = log_writer.Writer.init(self.wal_file);
            old_wal_file.close() catch {};
        } else {
            self.imm = self.mem;
            self.mem = new_mem;
        }

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

        // 3b. WAL GC: the old log's data is now safely in the L0 SST (and the
        //     new log number is committed to the MANIFEST via logAndApply above).
        //     Delete the stale log file so the directory stays clean.  Ignore
        //     errors: a missing file is harmless (e.g. an empty-memtable no-op
        //     flush may have skipped creating one), and we never want a GC
        //     failure to abort a successful write.
        if (self.owns_wal and old_log_number != 0) {
            const old_log_path = try filename.logFileName(self.gpa, self.name, old_log_number);
            defer self.gpa.free(old_log_path);
            self.env.deleteFile(old_log_path) catch {};
        }

        // 4. Free the flushed memtable; no pending flush remains.
        self.imm.?.deinit();
        self.imm = null;
    }

    /// Point lookup visible at the snapshot (`ropts.snapshot` or the latest
    /// sequence).  Returns a freshly duped value the CALLER OWNS and must free,
    /// or null if the key is absent or deleted at that snapshot.
    pub fn get(self: *DB, ropts: ReadOptions, key: []const u8) !?[]u8 {
        const seq = ropts.snapshot orelse self.last_sequence;

        // With a merge operator configured, route through the merge-aware path:
        // the newest entries for the key may be a run of merge operands on top of
        // an optional Put/Delete that the fast point-lookup path cannot combine.
        // TODO(perf): GetContext operand accumulation instead of a full re-scan.
        if (self.options.merge_operator != null) {
            return self.mergeGet(key, seq);
        }

        // M7.5: the largest covering range-tombstone sequence visible at `seq`.
        // A surfaced value with sequence < this is deleted by the range tombstone;
        // a value with sequence >= it outranks the tombstone and is visible.  When
        // there are no covering tombstones this is 0 (no shadowing).
        const cover_seq = try self.maxCoveringTombstoneSeq(key, seq);

        var lookup = try memtable_mod.LookupKey.init(self.gpa, key, seq);
        defer lookup.deinit(self.gpa);

        // 1. MemTable first (it holds the newest writes).
        {
            var vseq: u64 = 0;
            if (self.mem.getWithSeq(lookup, &vseq)) |r| switch (r) {
                .found => |v| {
                    if (vseq < cover_seq) return null; // shadowed by a range tombstone
                    return try self.gpa.dupe(u8, v);
                },
                .deleted => return null,
            };
        }

        // 1b. The immutable memtable being flushed (if any) is next-newest.
        if (self.imm) |imm| {
            var vseq: u64 = 0;
            if (imm.getWithSeq(lookup, &vseq)) |r| switch (r) {
                .found => |v| {
                    if (vseq < cover_seq) return null;
                    return try self.gpa.dupe(u8, v);
                },
                .deleted => return null,
            };
        }

        // 2. Not in the memtable: consult the on-disk SSTs via the current
        //    Version (LSM point lookup with snapshot + tombstone semantics).
        const version = self.versions.currentVersion();
        var vseq: u64 = 0;
        if (try version.getWithSeq(self.gpa, &self.table_cache, self.options.comparator, key, seq, &vseq)) |r| {
            switch (r) {
                // The value is freshly gpa-allocated by Version.get; the caller
                // owns and frees it.  Drop const since it is uniquely owned.
                .found => |v| {
                    if (vseq < cover_seq) {
                        self.gpa.free(@constCast(v));
                        return null;
                    }
                    return @constCast(v);
                },
                .deleted => return null,
            }
        }
        return null;
    }

    /// The sequence of the NEWEST entry for `key` across the live MemTable, the
    /// immutable MemTable (if flushing), and the current Version's SSTs — of ANY
    /// kind (put, delete, or merge operand).  Returns 0 if the key has never
    /// appeared.  (M7.6 transaction conflict detection: an optimistic txn began at
    /// snapshot S; if `latestSequenceForKey(k) > S` then some commit touched `k`
    /// after the txn's snapshot and a write-write conflict exists.)
    ///
    /// Reuses the M7.5 `getWithSeq` machinery, probing layers newest-first.  Each
    /// layer's `getWithSeq` reports the single newest entry visible at the query
    /// sequence and writes its sequence into `seq_out` even for a `.merge` operand
    /// (where the function itself returns null), so a non-zero `seq_out` means the
    /// layer holds the key — the first such layer (newest) wins.
    pub fn latestSequenceForKey(self: *DB, key: []const u8) u64 {
        // Query at the latest sequence so the newest entry is visible.
        const seq = self.last_sequence;

        var lookup = memtable_mod.LookupKey.init(self.gpa, key, seq) catch return 0;
        defer lookup.deinit(self.gpa);

        // 1. Live MemTable (newest writes).
        {
            var vseq: u64 = 0;
            const r = self.mem.getWithSeq(lookup, &vseq);
            if (r != null or vseq != 0) return vseq;
        }

        // 2. The immutable MemTable being flushed (if any).
        if (self.imm) |imm| {
            var vseq: u64 = 0;
            const r = imm.getWithSeq(lookup, &vseq);
            if (r != null or vseq != 0) return vseq;
        }

        // 3. The current Version's SSTs.
        const version = self.versions.currentVersion();
        var vseq: u64 = 0;
        const r = version.getWithSeq(self.gpa, &self.table_cache, self.options.comparator, key, seq, &vseq) catch return 0;
        if (r) |res| {
            // A Version `.found` returns a freshly gpa-allocated value the caller
            // owns; we only need the sequence, so free it.
            switch (res) {
                .found => |v| self.gpa.free(@constCast(v)),
                .deleted => {},
            }
            return vseq;
        }
        if (vseq != 0) return vseq;

        return 0;
    }

    /// The largest range-tombstone sequence (visible at `snapshot`) that covers
    /// `key`, or 0 if none.  Builds the snapshot-scoped aggregator (live MemTable
    /// + imm + every SST's range-del block) and folds its covering tombstones
    /// into one effective deletion sequence.
    /// D3a-M1 fast path: skip the aggregator entirely when no source carries any
    /// range tombstone.
    /// TODO(perf): prune by file key-range overlap; cache per-Version aggregation.
    fn maxCoveringTombstoneSeq(self: *DB, key: []const u8, snapshot: u64) !u64 {
        if (!self.hasAnyRangeTombstones()) return 0;
        var agg = try self.buildRangeAggregator(self.gpa, snapshot);
        defer agg.deinit();
        return agg.maxCoveringSeq(key, snapshot, self.options.comparator);
    }

    /// Fast pre-check: returns true iff at least one of {live MemTable, immutable
    /// MemTable, current Version's SST files} carries at least one range tombstone
    /// visible at ANY snapshot.  A `false` result means the range-tombstone
    /// aggregator will always be empty, so it can be skipped entirely.
    ///
    /// D3a-M1 guard.  Correctness note: SST files use a conservative
    /// `has_range_tombstones = true` default (set at flush time to the actual
    /// value; after MANIFEST recovery the field defaults to `true`), so this
    /// never produces a false negative — it may produce a false positive (causing
    /// a wasted aggregator build), but never incorrectly skips a real tombstone.
    pub fn hasAnyRangeTombstones(self: *const DB) bool {
        // 1. Live MemTable.
        if (!self.mem.range_tombstones.isEmpty()) return true;
        // 2. Immutable MemTable being flushed (if any).
        if (self.imm) |imm| {
            if (!imm.range_tombstones.isEmpty()) return true;
        }
        // 3. Every SST file in the current Version.
        const v = self.versions.currentVersion();
        for (&v.files) |level| {
            for (level.items) |f| {
                if (f.has_range_tombstones) return true;
            }
        }
        return false;
    }

    /// Merge-aware point lookup (M7.1).  Builds an internal MergingIterator over
    /// memtable+imm+SSTs scoped by `seq`, seeks to `key`, and walks the entries
    /// for that user key in IKC order (newest first): it gathers `.merge`
    /// operands until it hits a `.value` base (stop, use as base), a `.deletion`
    /// (stop, no base), or the user key changes / the source ends.  The gathered
    /// operands are reversed to OLDEST-first and combined via the merge operator.
    ///
    /// Returns a freshly gpa-allocated value the CALLER OWNS, or null when the
    /// key is absent / a tombstone with no overlying operands / fullMerge fails.
    fn mergeGet(self: *DB, key: []const u8, seq: u64) !?[]u8 {
        const merge_op = self.options.merge_operator.?;

        const merger = try self.buildInternalIterator(self.gpa, seq);
        defer destroyMerger(self.gpa, merger);
        const it = merger.iterator();

        // Seek to the newest version of `key` at/below the snapshot.  Use a
        // max-type seek trailer so the seek lands at/before EVERY entry at this
        // sequence — including `.merge` (0x2), whose type byte exceeds the plain
        // `kValueTypeForSeek` (.value = 0x1) and would otherwise sort before the
        // seek key and be skipped.  The trailer is only ever compared (never
        // parsed), so a raw 0xFF type byte is safe.
        var lookup: std.ArrayListUnmanaged(u8) = .empty;
        defer lookup.deinit(self.gpa);
        try lookup.appendSlice(self.gpa, key);
        var tbuf: [8]u8 = undefined;
        coding.encodeFixed64(&tbuf, (seq << 8) | 0xFF);
        try lookup.appendSlice(self.gpa, &tbuf);
        it.seek(lookup.items);

        // Gather operands (newest-first) + an optional base for this user key.
        var operands: std.ArrayListUnmanaged([]u8) = .empty;
        defer {
            for (operands.items) |op| self.gpa.free(op);
            operands.deinit(self.gpa);
        }
        var base: ?[]u8 = null;
        defer if (base) |b| self.gpa.free(b);
        var have_base = false; // a Put base reached (vs Delete / end)

        while (it.valid()) : (it.next()) {
            if (it.status()) |err| return err;
            const ikey = try internal_key.parseInternalKey(it.key());
            if (self.options.comparator.compare(ikey.user_key, key) != .eq) break;
            if (ikey.sequence > seq) continue; // not visible at this snapshot
            switch (ikey.type) {
                .merge => try operands.append(self.gpa, try self.gpa.dupe(u8, it.value())),
                .value => {
                    base = try self.gpa.dupe(u8, it.value());
                    have_base = true;
                    break;
                },
                .deletion, .single_deletion, .range_deletion => break, // Delete stops the merge.
            }
        }

        if (operands.items.len == 0) {
            // No operands: ordinary point-lookup result.  Hand ownership of the
            // base to the caller (null it out so the `defer` does not free it).
            if (have_base) {
                const out = base.?;
                base = null;
                return out;
            }
            return null; // tombstone or absent
        }

        // Reverse operands to OLDEST-first for the operator.
        std.mem.reverse([]u8, operands.items);
        // Build a []const []const u8 view for the operator.
        const view = try self.gpa.alloc([]const u8, operands.items.len);
        defer self.gpa.free(view);
        for (operands.items, 0..) |op, i| view[i] = op;

        const existing: ?[]const u8 = if (have_base) base.? else null;
        const merged = (try merge_op.fullMerge(key, existing, view, self.gpa)) orelse {
            // Operator failed: fall back to the existing value (or not-found).
            if (have_base) {
                const out = base.?;
                base = null; // hand ownership to the caller
                return out;
            }
            return null;
        };
        return merged;
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

        const merger = try self.buildInternalIterator(gpa, seq);
        errdefer destroyMerger(gpa, merger);

        var dbit = DBIterator.init(gpa, merger.iterator(), self.options.comparator, seq);
        dbit.owned_inner = merger;
        dbit.owned_inner_destroy = destroyMerger;
        // M7.5: a snapshot-scoped range-tombstone aggregator so the scan skips
        // any surfaced user key whose value is covered by a visible tombstone.
        // The DBIterator owns it (deinits + frees it).
        // D3a-M1 fast path: skip building the aggregator when no source carries
        // any range tombstone — `range_aggregator` stays null and the DBIterator
        // treats a null aggregator as "no tombstones, nothing hidden".
        if (self.hasAnyRangeTombstones()) {
            const agg = try gpa.create(delete_range.RangeTombstoneList);
            errdefer gpa.destroy(agg);
            agg.* = try self.buildRangeAggregator(gpa, seq);
            dbit.range_aggregator = agg;
        }
        // M7.2: thread the prefix extractor + prefix-bounded scan flag so a
        // `seek` can bound iteration to the seek target's prefix.
        dbit.prefix_extractor = self.options.prefix_extractor;
        dbit.prefix_same_as_start = ropts.prefix_same_as_start;
        // M7.1: thread the merge operator so a merge-operand run surfaces its
        // combined value.
        dbit.merge_operator = self.options.merge_operator;
        return dbit;
    }

    /// Build the internal MergingIterator (keys = internal keys, values = user
    /// values) over the live MemTable, the immutable MemTable being flushed (if
    /// any), and one iterator per SST file in the current Version, all ordered by
    /// the InternalKeyComparator (equal user keys visited newest-sequence first).
    ///
    /// Returns a heap-allocated `*MergingIterator` whose address is stable for
    /// the caller; free it with `destroyMerger` (which tears down every child).
    /// Shared by `newIterator` (wraps it in a DBIterator) and `mergeGet`.
    fn buildInternalIterator(
        self: *DB,
        gpa: std.mem.Allocator,
        seq: u64,
    ) !*merging_iterator.MergingIterator {
        _ = seq; // snapshot scoping is applied by the DBIterator / mergeGet walk.

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
        //    allocated so its address is stable behind the DBIterator / mergeGet.
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
        return merger;
    }

    /// Build a snapshot-scoped range-tombstone aggregator (M7.5): collect every
    /// range tombstone visible at `snapshot` (`tomb.seq <= snapshot`) from the
    /// live MemTable, the immutable MemTable being flushed (if any), and every SST
    /// in the current Version.  The caller OWNS the returned list (`deinit`s it).
    ///
    /// Correctness-first: we gather tombstones from ALL SSTs (no range-overlap
    /// pruning) and re-scan them linearly per query.
    /// TODO(perf): prune by file key-range overlap + a fragmented tombstone iter.
    fn buildRangeAggregator(self: *DB, gpa: std.mem.Allocator, snapshot: u64) !delete_range.RangeTombstoneList {
        var agg = delete_range.RangeTombstoneList.init(gpa);
        errdefer agg.deinit();

        // 1. Live MemTable tombstones.
        for (self.mem.range_tombstones.tombstones.items) |t| {
            if (t.seq <= snapshot) try agg.add(t.begin, t.end, t.seq);
        }
        // 1b. Immutable MemTable being flushed (if any).
        if (self.imm) |imm| {
            for (imm.range_tombstones.tombstones.items) |t| {
                if (t.seq <= snapshot) try agg.add(t.begin, t.end, t.seq);
            }
        }
        // 2. Every SST in the current Version.
        const v = self.versions.currentVersion();
        for (&v.files) |level| {
            for (level.items) |f| {
                const table = try self.table_cache.findTable(f.number, f.file_size);
                var rtl = try table.rangeTombstones(gpa);
                defer rtl.deinit();
                for (rtl.tombstones.items) |t| {
                    if (t.seq <= snapshot) try agg.add(t.begin, t.end, t.seq);
                }
            }
        }
        return agg;
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
// M7.1 — MergeOperator: lazy read-modify-write operands.
// ===========================================================================

const merge_operator_mod = @import("../rocks/merge_operator.zig");
const Uint64AddOperator = merge_operator_mod.Uint64AddOperator;

/// Build an 8-byte LE u64 in a caller buffer (merge-operand helper for tests).
fn u64le(buf: *[8]u8, v: u64) []const u8 {
    std.mem.writeInt(u64, buf, v, .little);
    return buf[0..];
}

/// Decode an 8-byte LE u64 (test helper).
fn decU64(bytes: []const u8) !u64 {
    try testing.expectEqual(@as(usize, 8), bytes.len);
    return std.mem.readInt(u64, bytes[0..8], .little);
}

test "M7.1 merge: merge on empty key sums from 0" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    var add = Uint64AddOperator{};
    const db = try DB.open(gpa, me.env(), "mergebasic", .{ .merge_operator = add.operator() });
    defer db.close();

    var b: [8]u8 = undefined;
    try db.merge(.{}, "c", u64le(&b, 5));
    {
        const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqual(@as(u64, 5), try decU64(got));
    }

    try db.merge(.{}, "c", u64le(&b, 3));
    {
        const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqual(@as(u64, 8), try decU64(got));
    }
}

test "M7.1 merge: put base then merge operand combines" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    var add = Uint64AddOperator{};
    const db = try DB.open(gpa, me.env(), "mergebase", .{ .merge_operator = add.operator() });
    defer db.close();

    var b: [8]u8 = undefined;
    try db.put(.{}, "c", u64le(&b, 100));
    try db.merge(.{}, "c", u64le(&b, 1));

    const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqual(@as(u64, 101), try decU64(got));
}

test "M7.1 merge: delete then merge starts a fresh accumulation" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    var add = Uint64AddOperator{};
    const db = try DB.open(gpa, me.env(), "mergedel", .{ .merge_operator = add.operator() });
    defer db.close();

    var b: [8]u8 = undefined;
    try db.put(.{}, "c", u64le(&b, 100));
    try db.delete(.{}, "c");
    try db.merge(.{}, "c", u64le(&b, 7));

    const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    // Delete stops the merge — operand merges with no base → 7.
    try testing.expectEqual(@as(u64, 7), try decU64(got));
}

test "M7.1 merge: many operands accumulate" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    var add = Uint64AddOperator{};
    const db = try DB.open(gpa, me.env(), "mergemany", .{ .merge_operator = add.operator() });
    defer db.close();

    var b: [8]u8 = undefined;
    var expected: u64 = 0;
    var i: u64 = 1;
    while (i <= 50) : (i += 1) {
        try db.merge(.{}, "c", u64le(&b, i));
        expected += i;
    }
    const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqual(expected, try decU64(got));
}

test "M7.1 merge: null operator -> merge entry treated as not-found on get" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    // No merge_operator configured.
    const db = try DB.open(gpa, me.env(), "mergenull", .{});
    defer db.close();

    // DB.merge with no operator is a usage error.
    var b: [8]u8 = undefined;
    try testing.expectError(error.MergeOperatorNotConfigured, db.merge(.{}, "c", u64le(&b, 1)));
}

test "M7.1 merge: get reads merge across a flush boundary" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    var add = Uint64AddOperator{};
    // Tiny write buffer so operands spread across SSTs + the live memtable.
    const db = try DB.open(gpa, me.env(), "mergeflush", .{
        .merge_operator = add.operator(),
        .write_buffer_size = 1,
    });
    defer db.close();

    var b: [8]u8 = undefined;
    try db.put(.{}, "c", u64le(&b, 10)); // base -> flush
    try db.merge(.{}, "c", u64le(&b, 1)); // operand -> flush
    try db.merge(.{}, "c", u64le(&b, 2)); // operand -> flush
    try db.merge(.{}, "c", u64le(&b, 3)); // operand (live memtable)

    const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqual(@as(u64, 16), try decU64(got));
}

test "M7.1 merge: iterator scan surfaces merged values" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    var add = Uint64AddOperator{};
    const db = try DB.open(gpa, me.env(), "mergeiter", .{ .merge_operator = add.operator() });
    defer db.close();

    var b: [8]u8 = undefined;
    // a: base 10 + 1 + 2 = 13
    try db.put(.{}, "a", u64le(&b, 10));
    try db.merge(.{}, "a", u64le(&b, 1));
    try db.merge(.{}, "a", u64le(&b, 2));
    // b: pure merges 5 + 5 = 10
    try db.merge(.{}, "b", u64le(&b, 5));
    try db.merge(.{}, "b", u64le(&b, 5));
    // d: plain put = 42
    try db.put(.{}, "d", u64le(&b, 42));

    var it = try db.newIterator(gpa, .{});
    defer it.deinit();

    const exp_k = [_][]const u8{ "a", "b", "d" };
    const exp_v = [_]u64{ 13, 10, 42 };
    var i: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        try testing.expect(i < exp_k.len);
        try testing.expectEqualStrings(exp_k[i], it.key());
        try testing.expectEqual(exp_v[i], try decU64(it.value()));
        i += 1;
    }
    try testing.expectEqual(exp_k.len, i);
    try testing.expect(it.status() == null);
}

test "M7.1 merge: snapshot sees the merge state as of its sequence" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    var add = Uint64AddOperator{};
    const db = try DB.open(gpa, me.env(), "mergesnap", .{ .merge_operator = add.operator() });
    defer db.close();

    var b: [8]u8 = undefined;
    try db.put(.{}, "c", u64le(&b, 100)); // base = 100
    try db.merge(.{}, "c", u64le(&b, 5)); // -> 105
    const snap = try db.getSnapshot();
    defer db.releaseSnapshot(snap);
    try db.merge(.{}, "c", u64le(&b, 7)); // -> 112 (after the snapshot)

    // At the snapshot: 100 + 5 = 105 (the +7 is not visible).
    {
        const got = try db.get(.{ .snapshot = snap.sequence }, "c") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqual(@as(u64, 105), try decU64(got));
    }
    // Latest: 100 + 5 + 7 = 112.
    {
        const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqual(@as(u64, 112), try decU64(got));
    }
}

test "M7.1 merge: snapshot-pinned merge survives compaction" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    var add = Uint64AddOperator{};
    // Tiny buffer + low trigger so flushes + a compaction fire while a snapshot
    // pins the intermediate merge state (exercises the keep-above-snapshot path).
    const db = try DB.open(gpa, me.env(), "mergesnapc", .{
        .merge_operator = add.operator(),
        .write_buffer_size = 1,
        .level0_file_num_compaction_trigger = 2,
    });
    defer db.close();

    var b: [8]u8 = undefined;
    try db.put(.{}, "c", u64le(&b, 100));
    try db.merge(.{}, "c", u64le(&b, 5));
    const snap = try db.getSnapshot(); // pins 105
    defer db.releaseSnapshot(snap);

    // More merges + unrelated keys to force flushes + compaction.
    try db.merge(.{}, "c", u64le(&b, 7));
    try db.merge(.{}, "c", u64le(&b, 9));
    try db.put(.{}, "a", u64le(&b, 1));
    try db.put(.{}, "z", u64le(&b, 2));
    try db.merge(.{}, "c", u64le(&b, 11));

    {
        const got = try db.get(.{ .snapshot = snap.sequence }, "c") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqual(@as(u64, 105), try decU64(got));
    }
    {
        const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqual(@as(u64, 132), try decU64(got)); // 100+5+7+9+11
    }
}

test "M7.1 merge: iterator scan surfaces merged values across a flush" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    var add = Uint64AddOperator{};
    const db = try DB.open(gpa, me.env(), "mergeiterflush", .{
        .merge_operator = add.operator(),
        .write_buffer_size = 1,
    });
    defer db.close();

    var b: [8]u8 = undefined;
    try db.put(.{}, "a", u64le(&b, 10)); // flush
    try db.merge(.{}, "a", u64le(&b, 1)); // flush
    try db.merge(.{}, "a", u64le(&b, 2)); // flush
    try db.merge(.{}, "b", u64le(&b, 5)); // flush
    try db.merge(.{}, "b", u64le(&b, 5)); // live

    var it = try db.newIterator(gpa, .{});
    defer it.deinit();

    const exp_k = [_][]const u8{ "a", "b" };
    const exp_v = [_]u64{ 13, 10 };
    var i: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        try testing.expect(i < exp_k.len);
        try testing.expectEqualStrings(exp_k[i], it.key());
        try testing.expectEqual(exp_v[i], try decU64(it.value()));
        i += 1;
    }
    try testing.expectEqual(exp_k.len, i);
    try testing.expect(it.status() == null);
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

// ===========================================================================
// M7.5 — DeleteRange (range tombstones): the correctness gate.
// ===========================================================================

test "M7.5: basic deleteRange hides [begin,end), end exclusive" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "a", "av");
    try db.put(.{}, "b", "bv");
    try db.put(.{}, "c", "cv");
    try db.put(.{}, "d", "dv");

    try db.deleteRange(.{}, "b", "d"); // deletes b, c — NOT d (exclusive).

    {
        const a = try db.get(.{}, "a") orelse return error.TestExpectedFound;
        defer gpa.free(a);
        try testing.expectEqualStrings("av", a);
    }
    try testing.expect((try db.get(.{}, "b")) == null);
    try testing.expect((try db.get(.{}, "c")) == null);
    {
        const d = try db.get(.{}, "d") orelse return error.TestExpectedFound;
        defer gpa.free(d);
        try testing.expectEqualStrings("dv", d);
    }
}

test "M7.5: point put AFTER a covering range delete is visible (higher seq wins)" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    // A put BEFORE the covering range delete is hidden.
    try db.put(.{}, "before", "bv"); // covered by [a,z) below
    try db.deleteRange(.{}, "a", "z");
    // A put AFTER the range delete (higher seq) survives.
    try db.put(.{}, "m", "mv");

    try testing.expect((try db.get(.{}, "before")) == null);
    {
        const m = try db.get(.{}, "m") orelse return error.TestExpectedFound;
        defer gpa.free(m);
        try testing.expectEqualStrings("mv", m);
    }
}

test "M7.5: snapshot sees pre-delete value; latest sees it deleted" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "k", "kv"); // seq 1
    const snap = try db.getSnapshot(); // pins seq 1
    defer db.releaseSnapshot(snap);

    try db.deleteRange(.{}, "a", "z"); // seq 2, covers "k"

    // At the snapshot (before the range delete) "k" is still visible.
    {
        const got = try db.get(.{ .snapshot = snap.sequence }, "k") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("kv", got);
    }
    // Latest read sees "k" deleted.
    try testing.expect((try db.get(.{}, "k")) == null);
}

test "M7.5: iterator scan skips covered keys" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const db = try openTestDB(gpa, &me);
    defer db.close();

    try db.put(.{}, "a", "av");
    try db.put(.{}, "b", "bv");
    try db.put(.{}, "c", "cv");
    try db.put(.{}, "d", "dv");
    try db.put(.{}, "e", "ev");
    try db.deleteRange(.{}, "b", "d"); // hides b, c
    // A re-add of "c" AFTER the range delete is visible again.
    try db.put(.{}, "c", "c2");

    var it = try db.newIterator(gpa, .{});
    defer it.deinit();

    const exp_k = [_][]const u8{ "a", "c", "d", "e" };
    const exp_v = [_][]const u8{ "av", "c2", "dv", "ev" };
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

test "M7.5: range delete survives flush + compaction + reopen" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    {
        // Tiny buffer + low trigger so writes flush + compact.
        const db = try DB.open(gpa, e, "rddb", .{
            .write_buffer_size = 1,
            .level0_file_num_compaction_trigger = 2,
        });
        defer db.close();

        try db.put(.{}, "a", "av");
        try db.put(.{}, "b", "bv");
        try db.put(.{}, "c", "cv");
        try db.put(.{}, "d", "dv");
        try db.deleteRange(.{}, "b", "d"); // hides b, c
        // Force a bunch of flushes + compactions.
        try db.put(.{}, "e", "ev");
        try db.put(.{}, "f", "fv");
        try db.put(.{}, "g", "gv");

        try testing.expect((try db.get(.{}, "b")) == null);
        try testing.expect((try db.get(.{}, "c")) == null);
    }

    // Reopen: the tombstone (in an SST range-del block and/or the WAL) must
    // still hide the covered keys.
    {
        const db = try DB.open(gpa, e, "rddb", .{
            .write_buffer_size = 1,
            .level0_file_num_compaction_trigger = 2,
        });
        defer db.close();

        try testing.expect((try db.get(.{}, "b")) == null);
        try testing.expect((try db.get(.{}, "c")) == null);
        {
            const a = try db.get(.{}, "a") orelse return error.TestExpectedFound;
            defer gpa.free(a);
            try testing.expectEqualStrings("av", a);
        }
        {
            const d = try db.get(.{}, "d") orelse return error.TestExpectedFound;
            defer gpa.free(d);
            try testing.expectEqualStrings("dv", d);
        }
    }
}

// --- THE DELETE-RANGE RANDOMIZED GATE --------------------------------------

/// Reference model for deleteRange semantics: a latest-wins live map.  `put`
/// overwrites; `delete` removes; `deleteRange(b,e)` removes every CURRENTLY
/// PRESENT key in `[b,e)` (a later put re-adds it).  This mirrors the DB's
/// sequence semantics (a put with a higher seq than a covering range tombstone
/// is visible; one with a lower seq is hidden).
const RangeRefMap = struct {
    map: std.StringHashMapUnmanaged([]u8) = .empty,
    gpa: std.mem.Allocator,

    fn init(gpa: std.mem.Allocator) RangeRefMap {
        return .{ .gpa = gpa };
    }
    fn deinit(self: *RangeRefMap) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.gpa.free(entry.key_ptr.*);
            self.gpa.free(entry.value_ptr.*);
        }
        self.map.deinit(self.gpa);
    }
    fn put(self: *RangeRefMap, key: []const u8, value: []const u8) !void {
        const gop = try self.map.getOrPut(self.gpa, key);
        if (gop.found_existing) {
            self.gpa.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.gpa.dupe(u8, key);
        }
        gop.value_ptr.* = try self.gpa.dupe(u8, value);
    }
    fn delete(self: *RangeRefMap, key: []const u8) void {
        if (self.map.fetchRemove(key)) |kv| {
            self.gpa.free(kv.key);
            self.gpa.free(kv.value);
        }
    }
    /// Remove every present key in [begin, end) (bytewise, end exclusive).
    fn deleteRange(self: *RangeRefMap, begin: []const u8, end: []const u8) !void {
        var to_remove: std.ArrayListUnmanaged([]const u8) = .empty;
        defer to_remove.deinit(self.gpa);
        var it = self.map.keyIterator();
        while (it.next()) |kp| {
            const k = kp.*;
            if (std.mem.order(u8, k, begin) != .lt and std.mem.order(u8, k, end) == .lt) {
                try to_remove.append(self.gpa, k);
            }
        }
        for (to_remove.items) |k| self.delete(k);
    }
    fn get(self: *RangeRefMap, key: []const u8) ?[]const u8 {
        return self.map.get(key);
    }
};

fn verifyAgainstRangeRef(gpa: std.mem.Allocator, db: *DB, ref: *RangeRefMap, key_space: usize) !void {
    var i: usize = 0;
    while (i < key_space) : (i += 1) {
        var kbuf: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{i});
        const want = ref.get(k);
        const got = try db.get(.{}, k);
        if (want) |w| {
            const g = got orelse {
                std.debug.print("missing key {s}: ref={s} db=null\n", .{ k, w });
                return error.TestKeyMissing;
            };
            defer gpa.free(g);
            testing.expectEqualSlices(u8, w, g) catch {
                std.debug.print("mismatch key {s}: ref={s} db={s}\n", .{ k, w, g });
                return error.TestValueMismatch;
            };
        } else {
            if (got) |g| {
                defer gpa.free(g);
                std.debug.print("unexpected key {s}: db={s}\n", .{ k, g });
                return error.TestUnexpectedKey;
            }
        }
    }

    // Full forward scan == sorted live reference entries.
    var sorted_keys: std.ArrayListUnmanaged([]const u8) = .empty;
    defer sorted_keys.deinit(gpa);
    var it_ref = ref.map.iterator();
    while (it_ref.next()) |entry| try sorted_keys.append(gpa, entry.key_ptr.*);
    std.mem.sort([]const u8, sorted_keys.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lt);

    var it = try db.newIterator(gpa, .{});
    defer it.deinit();
    var idx: usize = 0;
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        if (idx >= sorted_keys.items.len) {
            std.debug.print("scan has extra key {s}\n", .{it.key()});
            return error.TestScanTooLong;
        }
        const want_k = sorted_keys.items[idx];
        const want_v = ref.get(want_k).?;
        testing.expectEqualSlices(u8, want_k, it.key()) catch {
            std.debug.print("scan key mismatch at {d}: ref={s} db={s}\n", .{ idx, want_k, it.key() });
            return error.TestScanKeyMismatch;
        };
        testing.expectEqualSlices(u8, want_v, it.value()) catch {
            std.debug.print("scan value mismatch at key {s}: ref={s} db={s}\n", .{ want_k, want_v, it.value() });
            return error.TestScanValueMismatch;
        };
        idx += 1;
    }
    if (idx != sorted_keys.items.len) {
        std.debug.print("scan too short: got {d} want {d}\n", .{ idx, sorted_keys.items.len });
        return error.TestScanTooShort;
    }
    try testing.expect(it.status() == null);
}

test "M7.5: randomized deleteRange gate vs reference map (get + scan + reopen)" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const key_space: usize = 60; // keys "key000".."key059"
    const opts = options_mod.Options{
        .write_buffer_size = 256, // many flushes
        .level0_file_num_compaction_trigger = 2, // many compactions
        .max_bytes_for_level_base = 4096,
        .target_file_size_base = 2048,
    };

    var ref = RangeRefMap.init(gpa);
    defer ref.deinit();

    var prng = std.Random.DefaultPrng.init(0xDE1E_7E_4A56);
    const rand = prng.random();

    {
        const db = try DB.open(gpa, e, "rangefuzz", opts);
        defer db.close();

        var op: usize = 0;
        while (op < 2000) : (op += 1) {
            const roll = rand.uintLessThan(u32, 100);
            if (roll < 55) {
                // 55% put random key -> random value.
                const key_idx = rand.uintLessThan(usize, key_space);
                var kbuf: [8]u8 = undefined;
                const k = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{key_idx});
                var vbuf: [24]u8 = undefined;
                const vlen = 1 + rand.uintLessThan(usize, vbuf.len);
                for (vbuf[0..vlen]) |*b| b.* = 'a' + rand.uintLessThan(u8, 26);
                const v = vbuf[0..vlen];
                try db.put(.{}, k, v);
                try ref.put(k, v);
            } else if (roll < 75) {
                // 20% point delete.
                const key_idx = rand.uintLessThan(usize, key_space);
                var kbuf: [8]u8 = undefined;
                const k = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{key_idx});
                try db.delete(.{}, k);
                ref.delete(k);
            } else {
                // 25% deleteRange over a random [b, e).
                var lo = rand.uintLessThan(usize, key_space);
                var hi = rand.uintLessThan(usize, key_space);
                if (lo > hi) {
                    const t = lo;
                    lo = hi;
                    hi = t;
                }
                hi += 1; // make end exclusive bound past `hi`
                var bbuf: [8]u8 = undefined;
                var ebuf: [8]u8 = undefined;
                const b = try std.fmt.bufPrint(&bbuf, "key{d:0>3}", .{lo});
                const en = try std.fmt.bufPrint(&ebuf, "key{d:0>3}", .{hi});
                try db.deleteRange(.{}, b, en);
                try ref.deleteRange(b, en);
            }

            if (op % 250 == 249) {
                try verifyAgainstRangeRef(gpa, db, &ref, key_space);
            }
        }
        try verifyAgainstRangeRef(gpa, db, &ref, key_space);
    }

    // Reopen and re-verify (tombstones must survive recovery + compaction).
    {
        const db = try DB.open(gpa, e, "rangefuzz", opts);
        defer db.close();
        try verifyAgainstRangeRef(gpa, db, &ref, key_space);
    }
}

// ===========================================================================
// gc3-wal — Single-CF WAL GC: delete the old .log after flush.
// ===========================================================================

test "gc3-wal: old log file is deleted after flush" {
    // After a flush installs a VersionEdit with the new log number, the old
    // .log file is no longer needed (its data is now in the L0 SST).  It must
    // be deleted so the directory does not accumulate stale log files.
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const db = try DB.open(gpa, e, "walGcDb", .{ .write_buffer_size = 1 });
    defer db.close();

    // Record the initial (pre-flush) log number.
    const old_log_number = db.versions.logNumber();
    const old_log_path = try filename.logFileName(gpa, "walGcDb", old_log_number);
    defer gpa.free(old_log_path);

    // The initial log file exists before any flush.
    try testing.expect(e.fileExists(old_log_path));

    // Write enough to trigger a flush (write_buffer_size = 1 forces immediate flush).
    try db.put(.{}, "k", "v");
    // A second write forces the flush of the first write (the first write fills
    // the buffer, the second write triggers the rotation+flush).
    try db.put(.{}, "k2", "v2");

    // A flush must have happened: the current log number must have changed.
    const new_log_number = db.versions.logNumber();
    try testing.expect(new_log_number != old_log_number);

    // The OLD log file must have been deleted by the WAL GC.
    try testing.expect(!e.fileExists(old_log_path));

    // The NEW (current) log file must still exist.
    const new_log_path = try filename.logFileName(gpa, "walGcDb", new_log_number);
    defer gpa.free(new_log_path);
    try testing.expect(e.fileExists(new_log_path));
}

test "gc3-wal: reopen recovers correctly after old log deleted" {
    // Recovery must still work after the old log has been GC'd: data is in the
    // SST (the flush produced it) + the current WAL (any unflushed writes).
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Phase 1: write + force flush + close.
    {
        const db = try DB.open(gpa, e, "walGcRecov", .{ .write_buffer_size = 1 });
        defer db.close();

        // These writes trigger flushes (and WAL GC); data ends up in SSTs.
        try db.put(.{}, "a", "alpha");
        try db.put(.{}, "b", "beta");
        try db.put(.{}, "c", "gamma");

        // At least one flush must have happened.
        try testing.expect(totalSSTFiles(db) >= 1);
    }

    // Phase 2: reopen and verify all data is still readable.
    {
        const db = try DB.open(gpa, e, "walGcRecov", .{ .write_buffer_size = 1 });
        defer db.close();

        {
            const got = try db.get(.{}, "a") orelse return error.TestExpectedFound;
            defer gpa.free(got);
            try testing.expectEqualStrings("alpha", got);
        }
        {
            const got = try db.get(.{}, "b") orelse return error.TestExpectedFound;
            defer gpa.free(got);
            try testing.expectEqualStrings("beta", got);
        }
        {
            const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
            defer gpa.free(got);
            try testing.expectEqualStrings("gamma", got);
        }
    }
}

// ===========================================================================
// D3a-M1 — range-tombstone guard (fast path when no tombstones exist).
// ===========================================================================

test "rangetomb-guard: no-tombstone fast path — get and newIterator skip aggregator build" {
    // This test verifies the D3a-M1 optimisation: when no range tombstones exist
    // in any source (memtable, imm, SST files), DB.hasAnyRangeTombstones returns
    // false, and neither get() nor newIterator() build a RangeTombstoneList.
    // Correctness is unchanged: all gets and scans return the right values.
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Use a tiny write buffer so flushes fire and SSTs are produced — the guard
    // must remain correct even when SST files are present (but carry no tombstones).
    const db = try DB.open(gpa, e, "rtGuardDb", .{ .write_buffer_size = 64 });
    defer db.close();

    // No tombstones yet: fast path must be taken (no-op, no aggregator).
    try testing.expect(!db.hasAnyRangeTombstones());

    // Write several keys and trigger multiple flushes.
    try db.put(.{}, "a", "av");
    try db.put(.{}, "b", "bv");
    try db.put(.{}, "c", "cv");
    try db.put(.{}, "d", "dv");
    try db.put(.{}, "e", "ev");
    // Force at least one flush by writing a large value.
    var big: [128]u8 = undefined;
    @memset(&big, 'x');
    try db.put(.{}, "big", &big);

    // Point lookups via the fast path (no aggregator build).
    try testing.expect(!db.hasAnyRangeTombstones());
    {
        const got = try db.get(.{}, "a") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("av", got);
    }
    {
        const got = try db.get(.{}, "c") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("cv", got);
    }
    try testing.expect((try db.get(.{}, "missing")) == null);

    // Scan via the fast path (range_aggregator stays null in the DBIterator).
    {
        var it = try db.newIterator(gpa, .{});
        defer it.deinit();
        // The iterator must have no aggregator (null pointer = fast path taken).
        try testing.expect(it.range_aggregator == null);
        var seen: usize = 0;
        it.seekToFirst();
        while (it.valid()) : (it.next()) seen += 1;
        // At least the 5 puts above must all be visible.
        try testing.expect(seen >= 5);
        try testing.expect(it.status() == null);
    }

    // Now introduce a range tombstone: the fast path must no longer be taken.
    try db.deleteRange(.{}, "b", "d"); // hides b, c
    try testing.expect(db.hasAnyRangeTombstones());

    // Correctness: get + scan must respect the tombstone even after the guard
    // transitions from fast to slow path.
    try testing.expect((try db.get(.{}, "b")) == null);
    try testing.expect((try db.get(.{}, "c")) == null);
    {
        const got = try db.get(.{}, "a") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("av", got);
    }
    {
        const got = try db.get(.{}, "d") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("dv", got);
    }
    {
        var it = try db.newIterator(gpa, .{});
        defer it.deinit();
        // Aggregator must now be set (slow path taken).
        try testing.expect(it.range_aggregator != null);
    }
}

test "rangetomb-guard: fast path with flushed SSTs — hasAnyRangeTombstones false" {
    // After flushing a tombstone-free memtable, the produced SST's
    // has_range_tombstones flag is false.  hasAnyRangeTombstones must return
    // false even when SST files exist.
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Force flushes by using a minimal write buffer.
    const db = try DB.open(gpa, e, "rtGuardSstDb", .{ .write_buffer_size = 1 });
    defer db.close();

    try db.put(.{}, "x", "xv");
    try db.put(.{}, "y", "yv"); // triggers flush of {x}
    try db.put(.{}, "z", "zv"); // triggers flush of {y}

    // At least one SST must exist.
    try testing.expect(totalSSTFiles(db) >= 1);
    // But no tombstones anywhere — fast path must hold.
    try testing.expect(!db.hasAnyRangeTombstones());

    // All values must still be readable.
    {
        const got = try db.get(.{}, "x") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("xv", got);
    }
    {
        const got = try db.get(.{}, "y") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("yv", got);
    }
    {
        const got = try db.get(.{}, "z") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("zv", got);
    }
}

// ===========================================================================
// compress-perlevel — per-level compression chosen by the output level.
// ===========================================================================

const footer_mod = @import("../format/footer.zig");
const block_mod = @import("../format/block.zig");

/// Read an entire file from `e` into a freshly allocated buffer (caller frees).
fn readWholeFile(e: env.Env, gpa: std.mem.Allocator, path: []const u8) ![]u8 {
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
    return buf;
}

/// Parse an on-disk SST and report whether ANY of its data blocks is stored
/// Snappy-compressed (trailer byte == kSnappyCompression).  Mirrors the
/// table_builder mini-reader: footer -> index block -> per-data-block trailer.
fn sstHasSnappyDataBlock(gpa: std.mem.Allocator, file: []const u8) !bool {
    const footer = try footer_mod.Footer.decodeFrom(file[file.len - footer_mod.kEncodedLength ..]);

    // Index block is always stored uncompressed (kNoCompression).
    const ih_start: usize = @intCast(footer.index_handle.offset);
    const ih_size: usize = @intCast(footer.index_handle.size);
    const index_contents = file[ih_start .. ih_start + ih_size];
    const index_block = try block_mod.Block.init(gpa, index_contents);

    // The index block's comparator only affects ordered seeks; a forward scan
    // works with any comparator, so use bytewise.
    var it = index_block.iterator(comparator.bytewise);
    defer it.deinit();
    it.seekToFirst();
    while (it.valid()) : (it.next()) {
        var hv: []const u8 = it.value();
        const h = try footer_mod.BlockHandle.decodeFrom(&hv);
        const start: usize = @intCast(h.offset);
        const size: usize = @intCast(h.size);
        // trailer[0] (the byte immediately after the block payload) is the
        // compression type.
        if (file[start + size] == table_builder.kSnappyCompression) return true;
    }
    return false;
}

test "compress-perlevel: deeper-level compaction output is compressed, data correct + reopen" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // L0 uncompressed, L1+ Snappy.  Tiny write_buffer forces an L0 file per write
    // batch; a low L0 trigger forces an L0->L1 compaction whose output lands at
    // L1 (where the per-level policy selects Snappy).
    const per_level = [_]options_mod.CompressionType{ .none, .snappy, .snappy };
    const opts = options_mod.Options{
        .create_if_missing = true,
        .compaction_style = .level,
        .compression_per_level = &per_level,
        .write_buffer_size = 1,
        .level0_file_num_compaction_trigger = 2,
        // Keep base byte budget small so the merged L1 file is not immediately
        // shoved further down before we can inspect it.
        .max_bytes_for_level_base = 64 * 1024 * 1024,
        .target_file_size_base = 64 * 1024 * 1024,
    };

    const N = 40;

    {
        const db = try DB.open(gpa, e, "perlevel", opts);
        defer db.close();

        // Highly compressible values (long runs) so a data block shrinks.
        var i: usize = 0;
        while (i < N) : (i += 1) {
            var kbuf: [16]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>5}", .{i});
            const v = "A" ** 200;
            try db.put(.{}, k, v);
        }

        // A compaction must have moved data into L1 (or deeper).
        const v = db.versions.currentVersion();
        var deeper_files: usize = 0;
        var lvl: usize = 1;
        while (lvl < version_set.kNumLevels) : (lvl += 1) deeper_files += v.files[lvl].items.len;
        try testing.expect(deeper_files >= 1);

        // Every L1 file's data must be Snappy-compressed on disk.
        var found_snappy = false;
        lvl = 1;
        while (lvl < version_set.kNumLevels) : (lvl += 1) {
            for (v.files[lvl].items) |f| {
                const path = try filename.tableFileName(gpa, "perlevel", f.number);
                defer gpa.free(path);
                const bytes = try readWholeFile(e, gpa, path);
                defer gpa.free(bytes);
                if (try sstHasSnappyDataBlock(gpa, bytes)) found_snappy = true;
            }
        }
        try testing.expect(found_snappy);

        // Data is intact through the compressed path.
        i = 0;
        while (i < N) : (i += 1) {
            var kbuf: [16]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>5}", .{i});
            const got = try db.get(.{}, k) orelse return error.TestExpectedFound;
            defer gpa.free(got);
            try testing.expectEqualStrings("A" ** 200, got);
        }
    }

    // Reopen: the compressed SSTs decompress correctly on read.
    {
        const db = try DB.open(gpa, e, "perlevel", opts);
        defer db.close();
        var i: usize = 0;
        while (i < N) : (i += 1) {
            var kbuf: [16]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>5}", .{i});
            const got = try db.get(.{}, k) orelse return error.TestExpectedFound;
            defer gpa.free(got);
            try testing.expectEqualStrings("A" ** 200, got);
        }
    }
}
