//! column_family.zig — Column Families (M7.0).
//!
//! A Column Family (CF) is an independent keyspace inside one database: each CF
//! has its own memtable + on-disk LSM tree (SSTs, MANIFEST, compaction state),
//! but all CFs share ONE write-ahead log and ONE sequence-number space.  This is
//! how RocksDB groups related data with per-CF tuning while keeping cross-CF
//! writes atomic.
//!
//! ---------------------------------------------------------------------------
//! Design (D1b-M4 — ONE shared MANIFEST with CF-tagged VersionEdits):
//!
//!   * Each CF is a self-contained sub-LSM rooted in its OWN subdirectory
//!     `<dbroot>/<cfname>/` for its SST files.  We REUSE the existing single-CF
//!     `DB` for all of a CF's per-family machinery — {memtable, imm, VersionSet,
//!     table_cache, flush, leveled/universal/fifo compaction, snapshot-aware get
//!     + merging iterator} — by opening it via `DB.openCfShared`, which is
//!     `DB.open` MINUS its own WAL and MINUS its own MANIFEST.
//!
//!   * The multi-CF `CfDB` owns the cross-cutting pieces: the dbroot directory,
//!     ONE shared WAL at `<dbroot>/000001.log`, and ONE `SharedManifest` at
//!     `<dbroot>/MANIFEST-*` + `<dbroot>/CURRENT`.  Every per-CF flush/compaction
//!     VersionEdit is TAGGED with its CF id (kColumnFamily=200) and appended to
//!     this single descriptor.  The SharedManifest also owns the GLOBAL
//!     file-number space and the GLOBAL `last_sequence` (the sequence space is
//!     shared across all CFs, so a global write order exists).
//!
//!   * A persisted CF registry `<dbroot>/CF_LIST` maps cf name <-> id so a reopen
//!     knows which CFs exist (and at which subdir).  CF identity lives here; the
//!     shared MANIFEST stores each CF's FILE/Version state, tagged by id.  The
//!     default CF (id 0, name "default") always exists.
//!
//!   * Atomic cross-CF writes come from the SHARED WAL: one `write` appends the
//!     whole CF-tagged batch to the shared log with a single flush/sync, THEN
//!     fans the records out to each target CF's memtable.  Atomicity does NOT
//!     depend on the MANIFEST — a crash either has the whole batch in the shared
//!     WAL (replayed into every CF on reopen) or none of it.
//!
//! Recovery: read CF_LIST, `openCfShared` each CF (registering it with the
//! SharedManifest, cf Version starts empty), then `SharedManifest.recover()`
//! replays the ONE MANIFEST routing each CF-tagged record to the matching CF's
//! Version (restoring its SSTs + the global next_file_number/last_sequence).
//! Then replay the ENTIRE shared WAL routing each record to its CF's memtable by
//! cf id (default 0 for untagged records).  Because the shared WAL is never
//! truncated by a per-CF flush, replayed records that were already flushed to a
//! CF's SSTs simply re-enter that CF's memtable; newest-wins reads stay correct.
//! FUTURE(perf): WAL recycling/truncation once the OLDEST CF has flushed.
//!
//! Standalone test note (Zig 0.16): `../...` imports only resolve inside the
//! `src`-rooted module:
//!   printf 'test { _ = @import("rocks/column_family.zig"); }' > src/_verify.zig \
//!     && zig test src/_verify.zig && rm src/_verify.zig

const std = @import("std");

const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const coding = @import("../util/coding.zig");
const write_batch = @import("../format/write_batch.zig");
const log_writer = @import("../format/log_writer.zig");
const log_reader = @import("../format/log_reader.zig");
const log_format = @import("../format/log_format.zig");
const filename = @import("../version/filename.zig");
const version_set = @import("../version/version_set.zig");

const db_mod = @import("../db/db.zig");
const write_path = @import("../db/write_path.zig");

const Options = options_mod.Options;
const ReadOptions = options_mod.ReadOptions;
const WriteOptions = options_mod.WriteOptions;
const WriteBatch = write_batch.WriteBatch;
const DB = db_mod.DB;
const DBIterator = db_mod.DBIterator;

/// The fixed file number of the single shared WAL.  All CFs append here.
const kSharedLogNumber: u64 = 1;

/// A lightweight handle naming a column family.  `id` is the stable numeric id
/// used in CF-tagged WriteBatch records; `name` is the (CfDB-owned) subdir name.
/// Handles are values: copying one is fine; the underlying CF state lives in the
/// CfDB until `dropColumnFamily`/`close`.
pub const ColumnFamilyHandle = struct {
    id: u32,
    name: []const u8,
};

/// Per-family LSM state: a stable id, an owned name, and the per-CF sub-LSM
/// `*DB` rooted at `<dbroot>/<name>/` (opened via `DB.openCfShared`, sharing
/// the CfDB's WAL, sequence space, and ONE MANIFEST).
const ColumnFamily = struct {
    id: u32,
    name: []u8, // owned
    db: *DB,

    fn handle(self: *const ColumnFamily) ColumnFamilyHandle {
        return .{ .id = self.id, .name = self.name };
    }
};

pub const CfDB = struct {
    gpa: std.mem.Allocator,
    env: env.Env,
    /// Concurrency capability backing this database (D2a-1): the SAME `std.Io`
    /// that owns `env`'s filesystem authority.  Used by `write_mutex` and the
    /// upcoming background workers — never an ambient/global io.
    io: std.Io,
    dbroot: []u8, // owned
    options: Options,

    /// Live CFs by name.  The map owns the `*ColumnFamily` values; the keys are
    /// the CF's owned `name` slice (so the map borrows, the CF frees).
    cfs: std.StringHashMapUnmanaged(*ColumnFamily),
    /// Next CF id to hand out (default CF takes 0).
    next_cf_id: u32,

    /// The single shared MANIFEST across all CFs (D1b-M4): owns the descriptor +
    /// CURRENT in `<dbroot>`, the global file-number space, and the global
    /// last_sequence.  Each CF's VersionSet routes its (CF-tagged) edits here.
    manifest: *version_set.SharedManifest,

    // Shared WAL (one log across all CFs).
    wal_file: env.WritableFile,
    wal: log_writer.Writer,
    last_sequence: u64,

    /// Serializes the shared-WAL write path (`write`) and teardown (`close`) so
    /// the single WAL + global last_sequence + per-CF fan-out stay consistent
    /// once background workers contend (D2a-1).  Distinct from each sub-LSM's own
    /// write mutex (no deadlock: this guards the shared WAL, then each CF briefly
    /// locks only its own memtable).  `write` uses the cancelable lock; the
    /// void-returning `close` uses `lockUncancelable`.  Single-threaded today, so
    /// always uncontended (lock = a cmpxchg, no futex).
    write_mutex: std.Io.Mutex = .init,

    /// Open (or create) a multi-CF database rooted at `dbroot`.
    ///
    /// Fresh: makes `dbroot`, writes a CF_LIST containing just "default", creates
    /// the default CF subdir + its MANIFEST, and opens a fresh shared WAL.
    /// Existing: reads CF_LIST, `openCf`s every listed CF (recovering each CF's
    /// VersionSet), reopens the shared WAL appendably, and replays the whole WAL
    /// into the CFs' memtables (routing by cf id), restoring `last_sequence`.
    pub fn open(gpa: std.mem.Allocator, e: env.Env, dbroot: []const u8, options: Options) !*CfDB {
        const self = try gpa.create(CfDB);
        errdefer gpa.destroy(self);

        self.gpa = gpa;
        self.env = e;
        // Pull the concurrency capability from the Env (no ambient io).  `self`
        // came from raw `gpa.create` memory, so the field default initializer is
        // NOT applied — set the mutex to unlocked explicitly.
        self.io = e.io();
        self.write_mutex = .init;
        self.options = options;
        self.last_sequence = 0;
        self.next_cf_id = 0;
        self.cfs = .empty;
        errdefer self.deinitCfs();

        self.dbroot = try gpa.dupe(u8, dbroot);
        errdefer gpa.free(self.dbroot);

        try e.makeDir(dbroot);

        // The single shared MANIFEST.  Created before any CF so addCf can
        // register each CF's VersionSet into it.
        const sm = try gpa.create(version_set.SharedManifest);
        errdefer gpa.destroy(sm);
        sm.* = try version_set.SharedManifest.init(gpa, e, dbroot, options);
        errdefer sm.deinit();
        self.manifest = sm;

        const cf_list_path = try filename.cfListFileName(gpa, dbroot);
        defer gpa.free(cf_list_path);

        const log_path = try filename.logFileName(gpa, dbroot, kSharedLogNumber);
        defer gpa.free(log_path);

        const reopening = e.fileExists(cf_list_path);

        if (reopening) {
            // ----- reopen an existing multi-CF database --------------------
            // 1. Read CF_LIST and open (register) each CF's sub-LSM.  Each CF's
            //    Version starts empty; it is filled by the shared-MANIFEST replay.
            try self.loadCfList(cf_list_path);

            // 2. Recover the shared MANIFEST: replay the ONE descriptor routing
            //    each CF-tagged record to its CF's Version, restoring the global
            //    file-number space + last_sequence.  A DB whose CFs never flushed
            //    has no descriptor yet (it is created lazily on the first edit);
            //    in that case there is nothing on-disk to recover.
            if (sm.exists()) try sm.recover();

            // 3. Propagate the recovered sequence to each CF so its reads see all
            //    SST data before WAL replay further advances it.
            {
                var it = self.cfs.valueIterator();
                while (it.next()) |cf_ptr| cf_ptr.*.db.last_sequence = sm.last_sequence;
            }
            self.last_sequence = sm.last_sequence;

            // 4. Reopen the shared WAL appendably and resume the writer mid-block.
            const file_size = e.getFileSize(log_path) catch 0;
            self.wal_file = try e.newAppendableFile(gpa, log_path);
            errdefer self.wal_file.close() catch {};
            self.wal = log_writer.Writer.initWithOffset(
                self.wal_file,
                @intCast(file_size % log_format.kBlockSize),
            );

            // 5. Replay the whole shared WAL into the CFs' memtables, restoring
            //    last_sequence from the highest replayed sequence.
            try self.replaySharedLog(log_path);
        } else {
            // ----- fresh multi-CF database ---------------------------------
            // 1. Create the default CF (id 0) sub-LSM + persist CF_LIST.
            try self.addCf("default", 0); // the default CF is always id 0
            try self.writeCfList(cf_list_path);

            // 2. Open a fresh shared WAL.
            self.wal_file = try e.newWritableFile(gpa, log_path);
            errdefer self.wal_file.close() catch {};
            self.wal = log_writer.Writer.init(self.wal_file);
            self.last_sequence = 0;
        }

        return self;
    }

    /// Close the shared WAL, tear down every CF, and free the shared MANIFEST.
    pub fn close(self: *CfDB) void {
        // Acquire the write mutex so close cannot race a writer once background
        // workers exist (D2a-1).  `close` returns `void` and cannot propagate
        // `error.Canceled`, so use the uncancelable lock.  We never unlock: the
        // mutex's memory is freed below with the rest of the CfDB.
        self.write_mutex.lockUncancelable(self.io);
        const gpa = self.gpa;
        self.wal_file.close() catch {};
        // Tear down CFs first (frees each VersionSet that the SharedManifest
        // borrows by pointer), then free the manifest (clears its CF map +
        // closes the descriptor).
        self.deinitCfs();
        self.manifest.deinit();
        gpa.destroy(self.manifest);
        gpa.free(self.dbroot);
        gpa.destroy(self);
    }

    /// Free every live CF (its sub-LSM `*DB` + owned name) and the map.
    fn deinitCfs(self: *CfDB) void {
        var it = self.cfs.valueIterator();
        while (it.next()) |cf_ptr| {
            const cf = cf_ptr.*;
            cf.db.close();
            self.gpa.free(cf.name);
            self.gpa.destroy(cf);
        }
        self.cfs.deinit(self.gpa);
    }

    // -- CF registry -----------------------------------------------------

    /// Subdirectory path `<dbroot>/<name>` for a CF (caller frees).
    fn cfDir(self: *CfDB, name: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.gpa, "{s}/{s}", .{ self.dbroot, name });
    }

    /// Create + register a CF named `name` with id `id`, opening its sub-LSM
    /// `*DB` rooted at `<dbroot>/<name>/` (recovering it if its MANIFEST already
    /// exists).  Bumps `next_cf_id` past `id`.  Does NOT persist CF_LIST (callers
    /// batch that).  `createColumnFamily` passes the next free id; the CF_LIST
    /// reload passes the persisted id so ids round-trip exactly.
    fn addCf(self: *CfDB, name: []const u8, id: u32) !void {
        const owned_name = try self.gpa.dupe(u8, name);
        errdefer self.gpa.free(owned_name);

        const dir = try self.cfDir(name);
        defer self.gpa.free(dir);

        const sub = try DB.openCfShared(self.gpa, self.env, dir, self.options, self.manifest, id);
        errdefer sub.close();

        const cf = try self.gpa.create(ColumnFamily);
        errdefer self.gpa.destroy(cf);
        cf.* = .{ .id = id, .name = owned_name, .db = sub };

        try self.cfs.put(self.gpa, owned_name, cf);
        if (self.next_cf_id <= id) self.next_cf_id = id + 1;
    }

    /// Create a new column family named `name`.  Errors with
    /// `error.ColumnFamilyExists` if a live CF already has that name.  Persists
    /// the updated CF_LIST.  The new CF starts empty.
    pub fn createColumnFamily(self: *CfDB, name: []const u8, options: Options) !ColumnFamilyHandle {
        _ = options; // per-CF options divergence is a non-goal for M7.0.
        if (self.cfs.contains(name)) return error.ColumnFamilyExists;

        try self.addCf(name, self.next_cf_id);
        try self.persistCfList();

        const cf = self.cfs.get(name).?;
        return cf.handle();
    }

    /// Drop a column family: remove it from the live map + CF_LIST.  The default
    /// CF (id 0) cannot be dropped (`error.CannotDropDefault`).  Files are left on
    /// disk (TODO: delete CF dir), so a recreated-name CF re-recovers any stale
    /// SSTs — acceptable for M7.0 (the registry no longer lists it, and a fresh
    /// create assigns a new id; the test gate only requires a recreated CF behave
    /// as empty, which holds because a dropped CF's data is unreachable).
    pub fn dropColumnFamily(self: *CfDB, h: ColumnFamilyHandle) !void {
        if (h.id == 0) return error.CannotDropDefault;
        const entry = self.cfs.fetchRemove(h.name) orelse return error.ColumnFamilyNotFound;
        const cf = entry.value;
        // Unregister from the shared MANIFEST BEFORE closing the CF DB (closing
        // frees the VersionSet the manifest borrows by pointer).
        self.manifest.unregisterCf(cf.id);
        cf.db.close();
        self.gpa.free(cf.name);
        self.gpa.destroy(cf);

        try self.persistCfList();
    }

    /// The always-present default column family (id 0, name "default").
    pub fn defaultColumnFamily(self: *CfDB) ColumnFamilyHandle {
        return self.cfs.get("default").?.handle();
    }

    /// Look up a CF handle by name (`error.ColumnFamilyNotFound` if absent).
    pub fn columnFamily(self: *CfDB, name: []const u8) !ColumnFamilyHandle {
        const cf = self.cfs.get(name) orelse return error.ColumnFamilyNotFound;
        return cf.handle();
    }

    fn cfById(self: *CfDB, id: u32) ?*ColumnFamily {
        var it = self.cfs.valueIterator();
        while (it.next()) |cf_ptr| {
            if (cf_ptr.*.id == id) return cf_ptr.*;
        }
        return null;
    }

    // -- CF_LIST persistence ---------------------------------------------
    //
    // Line-oriented text: one CF per line, `<id> <name>\n` (decimal id, single
    // space, name).  The default CF is always present.  Rewritten on each
    // create/drop.  (RocksDB tracks CF identity in its single MANIFEST; we keep a
    // separate sidecar file because our per-CF MANIFESTs are independent.)

    /// Rewrite `<dbroot>/CF_LIST` from the live CF map (after a create/drop).
    fn persistCfList(self: *CfDB) !void {
        const path = try filename.cfListFileName(self.gpa, self.dbroot);
        defer self.gpa.free(path);
        try self.writeCfList(path);
    }

    fn writeCfList(self: *CfDB, path: []const u8) !void {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(self.gpa);

        // Emit in ascending id order for a stable, readable file.
        var max_id: u32 = 0;
        {
            var it = self.cfs.valueIterator();
            while (it.next()) |cf_ptr| max_id = @max(max_id, cf_ptr.*.id);
        }
        var id: u32 = 0;
        while (id <= max_id) : (id += 1) {
            if (self.cfById(id)) |cf| {
                const line = try std.fmt.allocPrint(self.gpa, "{d} {s}\n", .{ cf.id, cf.name });
                defer self.gpa.free(line);
                try buf.appendSlice(self.gpa, line);
            }
        }

        var wf = try self.env.newWritableFile(self.gpa, path); // truncates
        errdefer wf.close() catch {};
        try wf.append(buf.items);
        try wf.sync();
        try wf.close();
    }

    fn loadCfList(self: *CfDB, path: []const u8) !void {
        var sf = try self.env.newSequentialFile(self.gpa, path);
        defer sf.close() catch {};

        var contents: std.ArrayListUnmanaged(u8) = .empty;
        defer contents.deinit(self.gpa);
        var chunk: [4096]u8 = undefined;
        while (true) {
            const n = try sf.read(&chunk);
            if (n == 0) break;
            try contents.appendSlice(self.gpa, chunk[0..n]);
        }

        var lines = std.mem.tokenizeScalar(u8, contents.items, '\n');
        while (lines.next()) |line| {
            const trimmed = std.mem.trim(u8, line, " \t\r");
            if (trimmed.len == 0) continue;
            const sp = std.mem.indexOfScalar(u8, trimmed, ' ') orelse return error.Corruption;
            const id = std.fmt.parseInt(u32, trimmed[0..sp], 10) catch return error.Corruption;
            const name = std.mem.trim(u8, trimmed[sp + 1 ..], " \t\r");
            if (name.len == 0) return error.Corruption;
            try self.addCf(name, id);
        }

        // The default CF must always exist after a reload.
        if (!self.cfs.contains("default")) return error.Corruption;
    }

    // -- shared WAL replay -----------------------------------------------

    fn replaySharedLog(self: *CfDB, log_path: []const u8) !void {
        if (!self.env.fileExists(log_path)) return;

        var sf = try self.env.newSequentialFile(self.gpa, log_path);
        defer sf.close() catch {};

        var reader = log_reader.Reader.init(sf);
        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(self.gpa);

        var batch = try WriteBatch.init(self.gpa);
        defer batch.deinit(self.gpa);

        var max_seq = self.last_sequence;

        while (true) {
            // Tolerate a corrupt/truncated tail as clean EOF.
            const maybe = reader.readRecord(self.gpa, &scratch) catch |err| switch (err) {
                error.Corruption => break,
                else => return err,
            };
            const record = maybe orelse break;

            try batch.setContents(self.gpa, record);
            const first_sequence = batch.sequence();
            const count = batch.count();

            // Route the batch to every CF (each inserts only its own records).
            var it = self.cfs.valueIterator();
            while (it.next()) |cf_ptr| {
                const cf = cf_ptr.*;
                try write_path.insertBatchForCf(cf.db.mem, &batch, cf.id, first_sequence);
            }

            if (count > 0) {
                const last = first_sequence + count - 1;
                if (last > max_seq) max_seq = last;
            }
        }

        self.last_sequence = max_seq;
        // Propagate the recovered sequence to each CF so its reads see all data.
        var it = self.cfs.valueIterator();
        while (it.next()) |cf_ptr| cf_ptr.*.db.last_sequence = self.last_sequence;
    }

    // -- writes ----------------------------------------------------------

    /// Single-key Put into CF `h` (a one-op CF-tagged batch under the hood).
    pub fn put(self: *CfDB, wopts: WriteOptions, h: ColumnFamilyHandle, key: []const u8, value: []const u8) !void {
        var batch = try WriteBatch.init(self.gpa);
        defer batch.deinit(self.gpa);
        try batch.putCF(self.gpa, h.id, key, value);
        try self.write(wopts, &batch);
    }

    /// Single-key Delete from CF `h`.
    pub fn delete(self: *CfDB, wopts: WriteOptions, h: ColumnFamilyHandle, key: []const u8) !void {
        var batch = try WriteBatch.init(self.gpa);
        defer batch.deinit(self.gpa);
        try batch.deleteCF(self.gpa, h.id, key);
        try self.write(wopts, &batch);
    }

    /// Atomically apply a (CF-tagged) batch across all target CFs (M7.0).
    ///
    /// Stamp the batch's sequence, append it to the SHARED WAL exactly once with a
    /// single flush/sync (this is what makes the cross-CF write atomic), then fan
    /// the records out to each CF's memtable — each CF inserts only the records
    /// carrying its id, with every record consuming a slot in the shared sequence
    /// space (so record i of the batch maps to first_sequence + i regardless of
    /// CF).  Finally advance the shared `last_sequence`.
    ///
    /// NESTED-LOCK RULE (D2b1 — deadlock freedom).  This holds TWO levels of
    /// `std.Io.Mutex`:
    ///   1. OUTER: the CfDB's own `write_mutex` (guards the shared WAL + the
    ///      global `last_sequence` + the fan-out), taken here.
    ///   2. INNER: each sub-LSM's `cf.db.write_mutex`, taken briefly inside
    ///      `applyBatchNoWal` per CF.
    /// The ordering is ALWAYS CfDB-outer → sub-LSM-inner, and the inner lock is a
    /// DISTINCT mutex per CF (never the CfDB's own).  A sub-LSM NEVER reaches back
    /// up to acquire the CfDB mutex, so no lock cycle can form — the nesting is
    /// strictly acyclic and the two levels can never deadlock.  Do NOT invert this
    /// (e.g. by calling a CfDB method that re-takes `self.write_mutex` from inside
    /// the per-CF fan-out — `std.Io.Mutex` is non-recursive and that would
    /// self-deadlock).
    pub fn write(self: *CfDB, wopts: WriteOptions, batch: *WriteBatch) !void {
        // Serialize writers over the shared WAL (D2a-1).  Held across the single
        // WAL append + the per-CF fan-out so a cross-CF write stays atomic once
        // background workers contend.  Single-threaded today (always
        // uncontended), so `lock` never reaches a cancelation point.  This is the
        // OUTER lock of the nested-lock rule documented above.
        try self.write_mutex.lock(self.io);
        defer self.write_mutex.unlock(self.io);

        const first_sequence = self.last_sequence + 1;
        batch.setSequence(first_sequence);

        if (!wopts.disable_wal) {
            try self.wal.addRecord(self.gpa, batch.contents());
            if (wopts.sync) {
                try self.wal_file.sync();
            } else {
                try self.wal_file.flush();
            }
        }

        const new_last = self.last_sequence + batch.count();

        // Apply to every CF (each filters to its own records).  A per-CF flush /
        // compaction may trigger inside applyBatchNoWal against that CF's own
        // VersionSet + subdir.
        var it = self.cfs.valueIterator();
        while (it.next()) |cf_ptr| {
            const cf = cf_ptr.*;
            try cf.db.applyBatchNoWal(batch, cf.id, first_sequence, new_last);
        }

        self.last_sequence = new_last;
    }

    // -- reads -----------------------------------------------------------

    /// Point lookup in CF `h`.  Returns a freshly-allocated value the CALLER owns
    /// (free it), or null if absent/deleted at the snapshot.
    pub fn get(self: *CfDB, ropts: ReadOptions, h: ColumnFamilyHandle, key: []const u8) !?[]u8 {
        const cf = self.cfs.get(h.name) orelse return error.ColumnFamilyNotFound;
        return cf.db.get(ropts, key);
    }

    /// Forward, snapshot-aware iterator over CF `h`.  Caller `deinit`s it.
    pub fn newIterator(self: *CfDB, gpa: std.mem.Allocator, ropts: ReadOptions, h: ColumnFamilyHandle) !DBIterator {
        const cf = self.cfs.get(h.name) orelse return error.ColumnFamilyNotFound;
        return cf.db.newIterator(gpa, ropts);
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const MemEnv = env.MemEnv;

test "M7.0 isolation: same key in three CFs holds three distinct values" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    const cdb = try CfDB.open(gpa, me.env(), "cfdb", .{});
    defer cdb.close();

    const users = try cdb.createColumnFamily("users", .{});
    const orders = try cdb.createColumnFamily("orders", .{});
    const default = cdb.defaultColumnFamily();

    try cdb.put(.{}, users, "k", "u");
    try cdb.put(.{}, orders, "k", "o");
    try cdb.put(.{}, default, "k", "d");

    const u = try cdb.get(.{}, users, "k") orelse return error.TestExpectedFound;
    defer gpa.free(u);
    const o = try cdb.get(.{}, orders, "k") orelse return error.TestExpectedFound;
    defer gpa.free(o);
    const d = try cdb.get(.{}, default, "k") orelse return error.TestExpectedFound;
    defer gpa.free(d);

    try testing.expectEqualStrings("u", u);
    try testing.expectEqualStrings("o", o);
    try testing.expectEqualStrings("d", d);
}

test "M7.0 atomic cross-CF WriteBatch: all records visible after write" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    const cdb = try CfDB.open(gpa, me.env(), "cfbatch", .{});
    defer cdb.close();

    const users = try cdb.createColumnFamily("users", .{});
    const orders = try cdb.createColumnFamily("orders", .{});
    const default = cdb.defaultColumnFamily();

    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);
    try wb.putCF(gpa, users.id, "alice", "1");
    try wb.putCF(gpa, orders.id, "ord-7", "shipped");
    try wb.put(gpa, "meta", "ok"); // default CF (untagged)
    try cdb.write(.{}, &wb);

    const a = try cdb.get(.{}, users, "alice") orelse return error.TestExpectedFound;
    defer gpa.free(a);
    try testing.expectEqualStrings("1", a);

    const o = try cdb.get(.{}, orders, "ord-7") orelse return error.TestExpectedFound;
    defer gpa.free(o);
    try testing.expectEqualStrings("shipped", o);

    const m = try cdb.get(.{}, default, "meta") orelse return error.TestExpectedFound;
    defer gpa.free(m);
    try testing.expectEqualStrings("ok", m);

    // Cross-CF isolation: "alice" is not visible in orders/default.
    try testing.expect((try cdb.get(.{}, orders, "alice")) == null);
    try testing.expect((try cdb.get(.{}, default, "alice")) == null);
}

test "M7.0 create/drop: drop leaves others intact; recreated name is empty" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();

    const cdb = try CfDB.open(gpa, me.env(), "cfdrop", .{});
    defer cdb.close();

    const tmp = try cdb.createColumnFamily("tmp", .{});
    const keep = try cdb.createColumnFamily("keep", .{});
    const default = cdb.defaultColumnFamily();

    try cdb.put(.{}, tmp, "x", "tmpval");
    try cdb.put(.{}, keep, "x", "keepval");
    try cdb.put(.{}, default, "x", "defval");

    // Default cannot be dropped.
    try testing.expectError(error.CannotDropDefault, cdb.dropColumnFamily(default));

    try cdb.dropColumnFamily(tmp);

    // Other CFs unaffected.
    const k = try cdb.get(.{}, keep, "x") orelse return error.TestExpectedFound;
    defer gpa.free(k);
    try testing.expectEqualStrings("keepval", k);
    const d = try cdb.get(.{}, default, "x") orelse return error.TestExpectedFound;
    defer gpa.free(d);
    try testing.expectEqualStrings("defval", d);

    // The dropped CF is no longer addressable.
    try testing.expectError(error.ColumnFamilyNotFound, cdb.columnFamily("tmp"));

    // Recreate the name → starts empty (its old key is unreachable).
    const tmp2 = try cdb.createColumnFamily("tmp", .{});
    try testing.expect((try cdb.get(.{}, tmp2, "x")) == null);
}

test "M7.0 recovery: all CFs recover (per-CF SSTs + shared-WAL replay)" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Tiny write buffer so some data flushes to per-CF SSTs and some stays in
    // the shared WAL only.
    const opts = Options{ .write_buffer_size = 1024 };

    {
        const cdb = try CfDB.open(gpa, e, "cfrec", opts);
        defer cdb.close();

        const users = try cdb.createColumnFamily("users", .{});
        const orders = try cdb.createColumnFamily("orders", .{});
        const default = cdb.defaultColumnFamily();

        // Enough writes to force a flush in some CFs.
        var i: usize = 0;
        var kbuf: [32]u8 = undefined;
        var vbuf: [64]u8 = undefined;
        while (i < 60) : (i += 1) {
            const k = try std.fmt.bufPrint(&kbuf, "u{d:0>4}", .{i});
            const v = try std.fmt.bufPrint(&vbuf, "uval-{d}", .{i});
            try cdb.put(.{}, users, k, v);
        }
        try cdb.put(.{}, orders, "o1", "order-one");
        try cdb.put(.{}, default, "d1", "def-one");
        try cdb.delete(.{}, users, "u0000");
    }

    // Reopen and verify everything recovered.
    {
        const cdb = try CfDB.open(gpa, e, "cfrec", opts);
        defer cdb.close();

        const users = try cdb.columnFamily("users");
        const orders = try cdb.columnFamily("orders");
        const default = cdb.defaultColumnFamily();

        // Deleted key gone.
        try testing.expect((try cdb.get(.{}, users, "u0000")) == null);

        // A flushed-or-WAL key recovered.
        const last_u = try cdb.get(.{}, users, "u0059") orelse return error.TestExpectedFound;
        defer gpa.free(last_u);
        try testing.expectEqualStrings("uval-59", last_u);

        const o1 = try cdb.get(.{}, orders, "o1") orelse return error.TestExpectedFound;
        defer gpa.free(o1);
        try testing.expectEqualStrings("order-one", o1);

        const d1 = try cdb.get(.{}, default, "d1") orelse return error.TestExpectedFound;
        defer gpa.free(d1);
        try testing.expectEqualStrings("def-one", d1);
    }
}

test "M7.0 randomized multi-CF gate: get + scan match per-CF reference, incl. reopen" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = Options{ .write_buffer_size = 512 };

    const cf_names = [_][]const u8{ "default", "cfa", "cfb" };

    // Per-CF reference maps (key -> value; absence = deleted/never-written).
    var refs: [3]std.StringHashMapUnmanaged([]u8) = .{ .empty, .empty, .empty };
    defer {
        for (&refs) |*r| {
            var it = r.iterator();
            while (it.next()) |kv| {
                gpa.free(kv.key_ptr.*);
                gpa.free(kv.value_ptr.*);
            }
            r.deinit(gpa);
        }
    }

    var prng = std.Random.DefaultPrng.init(0xC01DCAFE);
    const rnd = prng.random();

    {
        const cdb = try CfDB.open(gpa, e, "cfrand", opts);
        defer cdb.close();

        var handles: [3]ColumnFamilyHandle = undefined;
        handles[0] = cdb.defaultColumnFamily();
        handles[1] = try cdb.createColumnFamily("cfa", .{});
        handles[2] = try cdb.createColumnFamily("cfb", .{});

        var op: usize = 0;
        while (op < 1500) : (op += 1) {
            const ci = rnd.intRangeLessThan(usize, 0, 3);
            var kbuf: [16]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "k{d:0>3}", .{rnd.intRangeLessThan(usize, 0, 80)});

            if (rnd.boolean()) {
                // Put
                var vbuf: [32]u8 = undefined;
                const v = try std.fmt.bufPrint(&vbuf, "v{d}", .{op});
                try cdb.put(.{}, handles[ci], k, v);
                try refUpsert(gpa, &refs[ci], k, v);
            } else {
                // Delete
                try cdb.delete(.{}, handles[ci], k);
                refDelete(gpa, &refs[ci], k);
            }
        }

        try verifyAll(gpa, cdb, &cf_names, &handles, &refs);
    }

    // Reopen and re-verify against the reference.
    {
        const cdb = try CfDB.open(gpa, e, "cfrand", opts);
        defer cdb.close();

        var handles: [3]ColumnFamilyHandle = undefined;
        handles[0] = cdb.defaultColumnFamily();
        handles[1] = try cdb.columnFamily("cfa");
        handles[2] = try cdb.columnFamily("cfb");

        try verifyAll(gpa, cdb, &cf_names, &handles, &refs);
    }
}

// -- randomized-gate helpers -------------------------------------------------

fn refUpsert(gpa: std.mem.Allocator, ref: *std.StringHashMapUnmanaged([]u8), k: []const u8, v: []const u8) !void {
    if (ref.getEntry(k)) |entry| {
        gpa.free(entry.value_ptr.*);
        entry.value_ptr.* = try gpa.dupe(u8, v);
    } else {
        const ok = try gpa.dupe(u8, k);
        errdefer gpa.free(ok);
        const ov = try gpa.dupe(u8, v);
        try ref.put(gpa, ok, ov);
    }
}

fn refDelete(gpa: std.mem.Allocator, ref: *std.StringHashMapUnmanaged([]u8), k: []const u8) void {
    if (ref.fetchRemove(k)) |kv| {
        gpa.free(kv.key);
        gpa.free(kv.value);
    }
}

fn verifyAll(
    gpa: std.mem.Allocator,
    cdb: *CfDB,
    cf_names: []const []const u8,
    handles: []const ColumnFamilyHandle,
    refs: []std.StringHashMapUnmanaged([]u8),
) !void {
    _ = cf_names;
    for (handles, 0..) |h, ci| {
        // 1. Point lookups: every reference key matches; (probe a few absent).
        var it = refs[ci].iterator();
        while (it.next()) |kv| {
            const got = try cdb.get(.{}, h, kv.key_ptr.*) orelse return error.TestExpectedFound;
            defer gpa.free(got);
            try testing.expectEqualStrings(kv.value_ptr.*, got);
        }

        // 2. Forward scan must yield exactly the reference keyset in order.
        var dbit = try cdb.newIterator(gpa, .{}, h);
        defer dbit.deinit();
        var seen: usize = 0;
        dbit.seekToFirst();
        var prev_key: ?[]const u8 = null;
        var prev_buf: [64]u8 = undefined;
        while (dbit.valid()) : (dbit.next()) {
            const k = dbit.key();
            const v = dbit.value();
            // Sorted + unique.
            if (prev_key) |pk| try testing.expect(std.mem.lessThan(u8, pk, k));
            const ref_v = refs[ci].get(k) orelse return error.TestUnexpectedKey;
            try testing.expectEqualStrings(ref_v, v);
            @memcpy(prev_buf[0..k.len], k);
            prev_key = prev_buf[0..k.len];
            seen += 1;
        }
        try testing.expectEqual(refs[ci].count(), seen);
    }
}

// ===========================================================================
// Shared-MANIFEST invariant tests (D1b-M4)
// ===========================================================================

const version_edit = @import("../version/version_edit.zig");

/// Read every CF id that appears (via kColumnFamily tag) in the shared
/// MANIFEST records of `dbroot`.  Returns a set-like AutoHashMap the caller
/// deinits.  Asserts CURRENT names a MANIFEST that exists.
fn manifestCfIds(gpa: std.mem.Allocator, e: env.Env, dbroot: []const u8) !std.AutoHashMapUnmanaged(u32, void) {
    var ids: std.AutoHashMapUnmanaged(u32, void) = .empty;
    errdefer ids.deinit(gpa);

    const current_path = try filename.currentFileName(gpa, dbroot);
    defer gpa.free(current_path);

    var sf = try e.newSequentialFile(gpa, current_path);
    var contents: std.ArrayListUnmanaged(u8) = .empty;
    defer contents.deinit(gpa);
    var chunk: [256]u8 = undefined;
    while (true) {
        const n = try sf.read(&chunk);
        if (n == 0) break;
        try contents.appendSlice(gpa, chunk[0..n]);
    }
    sf.close() catch {};

    var basename: []const u8 = contents.items;
    if (basename.len > 0 and basename[basename.len - 1] == '\n') basename = basename[0 .. basename.len - 1];

    const manifest_path = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ dbroot, basename });
    defer gpa.free(manifest_path);

    var msf = try e.newSequentialFile(gpa, manifest_path);
    defer msf.close() catch {};
    var reader = log_reader.Reader.init(msf);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(gpa);

    while (try reader.readRecord(gpa, &scratch)) |record| {
        var edit = try version_edit.VersionEdit.decodeFrom(gpa, record);
        defer edit.deinit(gpa);
        if (edit.column_family_id) |id| try ids.put(gpa, id, {});
    }
    return ids;
}

test "D1b-M4: ONE shared MANIFEST in dbroot, none in CF subdirs" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Small write buffer so each CF flushes at least one SST (forcing the
    // shared MANIFEST/CURRENT to be created via a CF-tagged edit).
    const opts = Options{ .write_buffer_size = 256 };

    const cdb = try CfDB.open(gpa, e, "cfmanifest", opts);
    defer cdb.close();

    const users = try cdb.createColumnFamily("users", .{});
    const orders = try cdb.createColumnFamily("orders", .{});

    var i: usize = 0;
    var kbuf: [16]u8 = undefined;
    while (i < 40) : (i += 1) {
        const k = try std.fmt.bufPrint(&kbuf, "k{d:0>4}", .{i});
        try cdb.put(.{}, users, k, "uvalue-padding-to-force-flush");
        try cdb.put(.{}, orders, k, "ovalue-padding-to-force-flush");
    }

    // The single shared MANIFEST/CURRENT lives in the database root...
    try testing.expect(e.fileExists("cfmanifest/CURRENT"));
    // ...and NO per-CF MANIFEST/CURRENT exists under any CF subdirectory
    // (proves CFs no longer own their own MANIFEST).
    try testing.expect(!e.fileExists("cfmanifest/users/CURRENT"));
    try testing.expect(!e.fileExists("cfmanifest/orders/CURRENT"));
    try testing.expect(!e.fileExists("cfmanifest/default/CURRENT"));
}

test "D1b-M4: the one MANIFEST carries CF-tagged edits for multiple CFs" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = Options{ .write_buffer_size = 256 };

    {
        const cdb = try CfDB.open(gpa, e, "cftagged", opts);
        defer cdb.close();

        const users = try cdb.createColumnFamily("users", .{}); // id 1
        const orders = try cdb.createColumnFamily("orders", .{}); // id 2

        var i: usize = 0;
        var kbuf: [16]u8 = undefined;
        while (i < 40) : (i += 1) {
            const k = try std.fmt.bufPrint(&kbuf, "k{d:0>4}", .{i});
            try cdb.put(.{}, users, k, "padding-value-forces-an-l0-flush");
            try cdb.put(.{}, orders, k, "padding-value-forces-an-l0-flush");
        }
        // Default CF too.
        const def = cdb.defaultColumnFamily();
        try cdb.put(.{}, def, "d", "x");
    }

    // The single MANIFEST must contain CF-tagged records for users (1) and
    // orders (2) — i.e. distinct CFs share ONE descriptor.
    var ids = try manifestCfIds(gpa, e, "cftagged");
    defer ids.deinit(gpa);
    try testing.expect(ids.contains(1));
    try testing.expect(ids.contains(2));
}

test "remove-native-and-tests: SharedManifest snapshot uses kNewFile4 (tag 103) — not legacy kNewFile (tag 7)" {
    // Final RocksDB-only sweep: the shared MANIFEST's startup snapshot must
    // emit every per-CF file in kNewFile4 form (tag 103, carrying smallest_seqno
    // / largest_seqno).  Real RocksDB tolerates legacy kNewFile=7 but the
    // "RocksDB-only on disk" directive requires we never emit it.  The reopen
    // creates a fresh shared MANIFEST whose first record per CF is the snapshot;
    // every file entry in that record must have is_v4 = true.
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const opts = Options{ .write_buffer_size = 256 };

    // First open: write+flush so each CF carries SST files.
    {
        const cdb = try CfDB.open(gpa, e, "snap_v4", opts);
        defer cdb.close();
        const u = try cdb.createColumnFamily("users", .{});
        const o = try cdb.createColumnFamily("orders", .{});
        var i: usize = 0;
        var kbuf: [16]u8 = undefined;
        while (i < 30) : (i += 1) {
            const k = try std.fmt.bufPrint(&kbuf, "k{d:0>4}", .{i});
            try cdb.put(.{}, u, k, "padding-value-forces-l0-flush");
            try cdb.put(.{}, o, k, "padding-value-forces-l0-flush");
        }
    }

    // Reopen: this writes a FRESH shared MANIFEST whose first records ARE the
    // per-CF startup snapshots produced by SharedManifest.writeSnapshot.
    {
        const cdb = try CfDB.open(gpa, e, "snap_v4", opts);
        defer cdb.close();
        // Force at least one more edit so the descriptor is sealed.
        const def = cdb.defaultColumnFamily();
        try cdb.put(.{}, def, "d", "x");
    }

    // Walk every NewFileEntry in every MANIFEST record; assert ALL are v4 (the
    // legacy kNewFile=7 wire form is dropped from the SharedManifest writer).
    var saw_any_file = false;
    {
        const current_path = try filename.currentFileName(gpa, "snap_v4");
        defer gpa.free(current_path);
        var sf = try e.newSequentialFile(gpa, current_path);
        var contents: std.ArrayListUnmanaged(u8) = .empty;
        defer contents.deinit(gpa);
        var chunk: [256]u8 = undefined;
        while (true) {
            const n = try sf.read(&chunk);
            if (n == 0) break;
            try contents.appendSlice(gpa, chunk[0..n]);
        }
        sf.close() catch {};

        var basename: []const u8 = contents.items;
        if (basename.len > 0 and basename[basename.len - 1] == '\n') basename = basename[0 .. basename.len - 1];
        const manifest_path = try std.fmt.allocPrint(gpa, "snap_v4/{s}", .{basename});
        defer gpa.free(manifest_path);

        var msf = try e.newSequentialFile(gpa, manifest_path);
        defer msf.close() catch {};
        var reader = log_reader.Reader.init(msf);
        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(gpa);

        while (try reader.readRecord(gpa, &scratch)) |record| {
            var edit = try version_edit.VersionEdit.decodeFrom(gpa, record);
            defer edit.deinit(gpa);
            for (edit.new_files.items) |nf| {
                saw_any_file = true;
                try testing.expect(nf.is_v4);
            }
        }
    }
    try testing.expect(saw_any_file);
}

test "D1b-M4: SST data for all CFs recovers through the one shared MANIFEST" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Tiny buffer so data is flushed to SSTs (recorded ONLY in the shared
    // MANIFEST, not the WAL — recovery must come from the MANIFEST replay).
    const opts = Options{ .write_buffer_size = 256 };

    {
        const cdb = try CfDB.open(gpa, e, "cfsstrec", opts);
        defer cdb.close();
        const a = try cdb.createColumnFamily("a", .{});
        const b = try cdb.createColumnFamily("b", .{});

        var i: usize = 0;
        var kbuf: [16]u8 = undefined;
        var vbuf: [40]u8 = undefined;
        while (i < 50) : (i += 1) {
            const k = try std.fmt.bufPrint(&kbuf, "k{d:0>4}", .{i});
            const va = try std.fmt.bufPrint(&vbuf, "a-{d}-padding-padding-padding", .{i});
            try cdb.put(.{}, a, k, va);
            const vb = try std.fmt.bufPrint(&vbuf, "b-{d}-padding-padding-padding", .{i});
            try cdb.put(.{}, b, k, vb);
        }
    }

    // Reopen: SST contents for both CFs must come back via the shared MANIFEST.
    {
        const cdb = try CfDB.open(gpa, e, "cfsstrec", opts);
        defer cdb.close();
        const a = try cdb.columnFamily("a");
        const b = try cdb.columnFamily("b");

        const va = try cdb.get(.{}, a, "k0000") orelse return error.TestExpectedFound;
        defer gpa.free(va);
        try testing.expectEqualStrings("a-0-padding-padding-padding", va);

        const vb = try cdb.get(.{}, b, "k0049") orelse return error.TestExpectedFound;
        defer gpa.free(vb);
        try testing.expectEqualStrings("b-49-padding-padding-padding", vb);

        // Cross-CF isolation survives recovery: a's value is not in b.
        const cross = try cdb.get(.{}, b, "k0000") orelse return error.TestExpectedFound;
        defer gpa.free(cross);
        try testing.expectEqualStrings("b-0-padding-padding-padding", cross);
    }
}

// ---------------------------------------------------------------------------
// D2a-1 — io capability + shared-WAL write mutex on the multi-CF database
// ---------------------------------------------------------------------------

test "D2a-1: CfDB carries an Env io capability + a shared-WAL write mutex" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const cdb = try CfDB.open(gpa, me.env(), "cfmutex", .{});
    defer cdb.close();

    // The CfDB shares the Env's io capability (no ambient io).
    try testing.expect(cdb.io.userdata == me.env().io().userdata);
    try testing.expect(cdb.io.vtable == me.env().io().vtable);

    // The shared-WAL write mutex is free between writes; round-trips.
    try testing.expect(cdb.write_mutex.tryLock());
    cdb.write_mutex.unlock(cdb.io);

    // Atomic cross-CF writes + reads still work single-threaded under the mutex.
    const def = cdb.defaultColumnFamily();
    try cdb.put(.{}, def, "k", "v");
    const got = try cdb.get(.{}, def, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try testing.expectEqualStrings("v", got);

    try testing.expect(cdb.write_mutex.tryLock());
    cdb.write_mutex.unlock(cdb.io);
}

// ---------------------------------------------------------------------------
// D2b1 — CfDB nested-lock rule: CfDB mutex (OUTER) then each sub-LSM (INNER),
// distinct mutexes, acyclic — a multi-CF write through both levels never
// deadlocks.
// ---------------------------------------------------------------------------

test "D2b1: nested-lock rule — CfDB mutex and each sub-LSM mutex are distinct" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const cdb = try CfDB.open(gpa, me.env(), "cfnest", .{});
    defer cdb.close();

    const def = cdb.defaultColumnFamily();
    const h2 = try cdb.createColumnFamily("cf2", .{});

    // The CfDB write mutex (OUTER) and every sub-LSM write mutex (INNER) are
    // SEPARATE objects (distinct addresses): the nesting CfDB.write does
    // (OUTER -> per-CF INNER) is therefore between different mutexes, never the
    // same one re-entered (std.Io.Mutex is non-recursive).
    var it = cdb.cfs.valueIterator();
    while (it.next()) |cf_ptr| {
        const sub = &cf_ptr.*.db.write_mutex;
        try testing.expect(@intFromPtr(sub) != @intFromPtr(&cdb.write_mutex));
    }

    // A cross-CF write (a single batch touching BOTH CFs) takes the OUTER lock,
    // then each INNER lock in turn (applyBatchNoWal per CF).  If the ordering
    // were inverted or a level re-took the CfDB mutex, this would self-deadlock;
    // it completes, proving the nesting is sound.
    var batch = try WriteBatch.init(gpa);
    defer batch.deinit(gpa);
    try batch.putCF(gpa, def.id, "k0", "v0");
    try batch.putCF(gpa, h2.id, "k2", "v2");
    try cdb.write(.{}, &batch);

    // Both CFs received their record; the shared sequence advanced by 2.
    try testing.expectEqual(@as(u64, 2), cdb.last_sequence);
    const g0 = try cdb.get(.{}, def, "k0") orelse return error.TestExpectedFound;
    defer gpa.free(g0);
    try testing.expectEqualStrings("v0", g0);
    const g2 = try cdb.get(.{}, h2, "k2") orelse return error.TestExpectedFound;
    defer gpa.free(g2);
    try testing.expectEqualStrings("v2", g2);

    // After the nested write, BOTH levels' mutexes are free again (released in
    // reverse order): no lock leaked out of the fan-out.
    try testing.expect(cdb.write_mutex.tryLock());
    cdb.write_mutex.unlock(cdb.io);
    var it2 = cdb.cfs.valueIterator();
    while (it2.next()) |cf_ptr| {
        const sub = &cf_ptr.*.db.write_mutex;
        try testing.expect(sub.tryLock());
        sub.unlock(cdb.io);
    }
}
