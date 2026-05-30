//! checkpoint.zig — Point-in-time DB snapshot into a new directory (M7.7).
//!
//! A checkpoint is a byte-for-byte consistent copy of a DB's live SSTs,
//! MANIFEST, active WAL, and CURRENT pointer into a new directory.  The
//! resulting directory is a valid, independent DB that can be opened with
//! `DB.open` — it replays the copied WAL to recover any writes that had not
//! yet been flushed to SSTs at checkpoint time.
//!
//! Design constraints
//! ------------------
//! * Self-contained new file: imports DB and version modules by relative path.
//! * No edits to db.zig, env.zig, version_set.zig, build.zig, or root.zig.
//! * Immutable SSTs are HARD-LINKED into the checkpoint dir (shared inode —
//!   cheap, space-efficient), with a byte-copy fallback when a link cannot be
//!   made (e.g. cross-device dest).  Mutable/small files (MANIFEST, WAL,
//!   CURRENT) are byte-copied, matching RocksDB.
//! * Flush WAL before copying so MemEnv's buffered bytes are visible on read.
//!
//! Standalone test verify:
//!   printf 'test { _ = @import("rocks/checkpoint.zig"); }' > src/_verify.zig
//!   /home/ghpu/zig/zig test src/_verify.zig
//!   rm src/_verify.zig

const std = @import("std");

const env_mod = @import("../env/env.zig");
const db_mod = @import("../db/db.zig");
const filename = @import("../version/filename.zig");

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create a consistent checkpoint of `db` in the directory `dest_dir`.
///
/// Steps:
///   1. Flush the active WAL so all committed writes are visible in the MemEnv.
///   2. Create `dest_dir` via `db.env.makeDir`.
///   3. Hard-link every live SST referenced by the current Version (byte-copy
///      fallback if the link cannot be made).
///   4. Copy the current MANIFEST.
///   5. Copy the active WAL (if it exists).
///   6. Copy the CURRENT pointer file (its MANIFEST basename is unchanged in
///      the dest because we copied the same MANIFEST with the same number).
///
/// The checkpoint can be opened as an independent DB with `DB.open`; it will
/// replay the copied WAL to recover writes not yet flushed to SSTs.
pub fn createCheckpoint(
    gpa: std.mem.Allocator,
    db: *db_mod.DB,
    dest_dir: []const u8,
) !void {
    // 1. Flush the WAL so MemEnv's in-memory buffer is committed to its
    //    backing byte slice (the SequentialFile open below will see all bytes).
    try db.wal_file.flush();

    // 2. Create destination directory (no-op success if already exists on MemEnv).
    try db.env.makeDir(dest_dir);

    const e = db.env;
    const src = db.name;

    // 3. Hard-link every live SST across all levels.  SSTs are IMMUTABLE once
    //    written, so a hard link (shared inode — cheap, space-efficient) is a
    //    correct, zero-copy alias into the checkpoint dir, exactly as RocksDB
    //    does.  If the link cannot be made for ANY reason (cross-device dest →
    //    `error.NotSupported`, an unsupporting filesystem, etc.) fall back to a
    //    full byte copy so the checkpoint is always complete.
    const version = db.versions.currentVersion();
    for (&version.files) |level| {
        for (level.items) |file_meta| {
            const src_path = try filename.tableFileName(gpa, src, file_meta.number);
            defer gpa.free(src_path);
            const dst_path = try filename.tableFileName(gpa, dest_dir, file_meta.number);
            defer gpa.free(dst_path);
            e.linkFile(src_path, dst_path) catch {
                try copyFile(gpa, e, src_path, dst_path);
            };
        }
    }

    // 4. Copy the current MANIFEST.
    {
        const src_path = try filename.manifestFileName(gpa, src, db.versions.manifestFileNumber());
        defer gpa.free(src_path);
        const dst_path = try filename.manifestFileName(gpa, dest_dir, db.versions.manifestFileNumber());
        defer gpa.free(dst_path);
        try copyFile(gpa, e, src_path, dst_path);
    }

    // 5. Copy the active WAL (the file is always created by DB.open but may
    //    be empty on a brand-new DB with no writes yet; guard with fileExists
    //    to be safe, then copy unconditionally when it is there).
    {
        const src_path = try filename.logFileName(gpa, src, db.versions.logNumber());
        defer gpa.free(src_path);
        if (e.fileExists(src_path)) {
            const dst_path = try filename.logFileName(gpa, dest_dir, db.versions.logNumber());
            defer gpa.free(dst_path);
            try copyFile(gpa, e, src_path, dst_path);
        }
    }

    // 6. Copy CURRENT.  Its content ("MANIFEST-XXXXXX\n") names the same
    //    MANIFEST number we just copied into dest, so it is valid as-is.
    {
        const src_path = try filename.currentFileName(gpa, src);
        defer gpa.free(src_path);
        const dst_path = try filename.currentFileName(gpa, dest_dir);
        defer gpa.free(dst_path);
        try copyFile(gpa, e, src_path, dst_path);
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// I/O chunk size used by `copyFile`.  32 KiB is large enough to amortise
/// per-call overhead on MemEnv while staying small enough to live on the stack.
const kCopyBufSize = 32 * 1024;

/// Copy every byte of `src_path` into `dest_path` using the provided `Env`.
/// Opens `src_path` as a SequentialFile, reads in `kCopyBufSize` chunks, and
/// appends each chunk to a newly created WritableFile at `dest_path`.  The
/// destination is flushed and closed on success; the source is always closed.
fn copyFile(
    gpa: std.mem.Allocator,
    e: env_mod.Env,
    src_path: []const u8,
    dest_path: []const u8,
) !void {
    var src_file = try e.newSequentialFile(gpa, src_path);
    defer src_file.close() catch {};

    var dst_file = try e.newWritableFile(gpa, dest_path);
    errdefer dst_file.close() catch {};

    var buf: [kCopyBufSize]u8 = undefined;
    while (true) {
        const n = try src_file.read(&buf);
        if (n == 0) break; // EOF
        try dst_file.append(buf[0..n]);
    }

    try dst_file.flush();
    try dst_file.close();
}

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const MemEnv = env_mod.MemEnv;
const RealEnv = env_mod.RealEnv;
const DB = db_mod.DB;

test "checkpoint: round-trip — SST + WAL data visible in checkpoint DB" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Use a small write_buffer_size so some data flushes to L0 SSTs and
    // some stays only in the active WAL (unflushed memtable).
    const src_db = try DB.open(gpa, e, "srcdb", .{ .write_buffer_size = 512 });
    defer src_db.close();

    // Write enough keys to trigger at least one L0 flush.
    const n: usize = 32;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>4}", .{i});
        const v = try std.fmt.bufPrint(&vbuf, "val{d:0>4}", .{i});
        try src_db.put(.{}, k, v);
    }

    // Sanity: at least one SST should have been flushed.
    {
        const ver = src_db.versions.currentVersion();
        var total_sst: usize = 0;
        for (&ver.files) |level| total_sst += level.items.len;
        try testing.expect(total_sst >= 1);
    }

    // Create checkpoint.
    try createCheckpoint(gpa, src_db, "ckpt");

    // Open the checkpoint as a fully independent DB.
    const ckpt_db = try DB.open(gpa, e, "ckpt", .{});
    defer ckpt_db.close();

    // Every key must be readable in the checkpoint (both SST-flushed and
    // WAL-only data recovered via WAL replay on ckpt open).
    i = 0;
    while (i < n) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>4}", .{i});
        const want = try std.fmt.bufPrint(&vbuf, "val{d:0>4}", .{i});
        const got = try ckpt_db.get(.{}, k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(want, got);
    }
}

test "checkpoint: source-unaffected — new source writes absent from checkpoint" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const src_db = try DB.open(gpa, e, "srcdb2", .{ .write_buffer_size = 512 });
    defer src_db.close();

    try src_db.put(.{}, "before", "v1");

    // Create checkpoint — captures the state with only "before".
    try createCheckpoint(gpa, src_db, "ckpt2");

    // Write a NEW key into the source AFTER the checkpoint.
    try src_db.put(.{}, "after", "v2");

    // The source has both keys.
    {
        const got = try src_db.get(.{}, "before") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("v1", got);
    }
    {
        const got = try src_db.get(.{}, "after") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("v2", got);
    }

    // The checkpoint is frozen: "before" is present, "after" is absent.
    const ckpt_db = try DB.open(gpa, e, "ckpt2", .{});
    defer ckpt_db.close();

    {
        const got = try ckpt_db.get(.{}, "before") orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings("v1", got);
    }
    try testing.expect((try ckpt_db.get(.{}, "after")) == null);
}

test "checkpoint (RealEnv): SSTs are hard-linked, mutable files copied, data reads back" {
    const gpa = testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    defer re.deinit();
    const e = re.env();

    // Tiny write buffer forces at least one L0 flush so a real SST exists.
    const src_db = try DB.open(gpa, e, "srcdb_hl", .{ .write_buffer_size = 512 });
    defer src_db.close();

    const n: usize = 64;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>4}", .{i});
        const v = try std.fmt.bufPrint(&vbuf, "val{d:0>4}", .{i});
        try src_db.put(.{}, k, v);
    }

    // At least one SST must have flushed.
    const ver = src_db.versions.currentVersion();
    var total_sst: usize = 0;
    for (&ver.files) |level| total_sst += level.items.len;
    try testing.expect(total_sst >= 1);

    try createCheckpoint(gpa, src_db, "ckpt_hl");

    // Every live SST in the checkpoint must share an inode with its source
    // (nlink==2) — proving it was hard-linked, not byte-copied.
    for (&ver.files) |level| {
        for (level.items) |fm| {
            const src_path = try filename.tableFileName(gpa, "srcdb_hl", fm.number);
            defer gpa.free(src_path);
            const dst_path = try filename.tableFileName(gpa, "ckpt_hl", fm.number);
            defer gpa.free(dst_path);
            const st_src = try tmp.dir.statFile(io, src_path, .{});
            const st_dst = try tmp.dir.statFile(io, dst_path, .{});
            try testing.expectEqual(st_src.inode, st_dst.inode);
            try testing.expectEqual(@as(@TypeOf(st_src.nlink), 2), st_src.nlink);
        }
    }

    // MANIFEST and CURRENT are COPIED (mutable/small): they exist in the dest
    // and are NOT hard-linked (nlink stays 1).
    {
        const cur = try filename.currentFileName(gpa, "ckpt_hl");
        defer gpa.free(cur);
        try testing.expect(e.fileExists(cur));
        const st = try tmp.dir.statFile(io, cur, .{});
        try testing.expectEqual(@as(@TypeOf(st.nlink), 1), st.nlink);

        const man = try filename.manifestFileName(gpa, "ckpt_hl", src_db.versions.manifestFileNumber());
        defer gpa.free(man);
        try testing.expect(e.fileExists(man));
        const st_man = try tmp.dir.statFile(io, man, .{});
        try testing.expectEqual(@as(@TypeOf(st_man.nlink), 1), st_man.nlink);
    }

    // The checkpoint opens as an independent DB and every key reads back.
    const ckpt_db = try DB.open(gpa, e, "ckpt_hl", .{});
    defer ckpt_db.close();
    i = 0;
    while (i < n) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>4}", .{i});
        const want = try std.fmt.bufPrint(&vbuf, "val{d:0>4}", .{i});
        const got = try ckpt_db.get(.{}, k) orelse return error.TestExpectedFound;
        defer gpa.free(got);
        try testing.expectEqualStrings(want, got);
    }
}

test "checkpoint: expected files exist in dest dir" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const src_db = try DB.open(gpa, e, "srcdb3", .{ .write_buffer_size = 512 });
    defer src_db.close();

    // Write enough to trigger at least one flush (ensures SST + MANIFEST + WAL + CURRENT).
    var i: usize = 0;
    while (i < 32) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>4}", .{i});
        const v = try std.fmt.bufPrint(&vbuf, "val{d:0>4}", .{i});
        try src_db.put(.{}, k, v);
    }

    try createCheckpoint(gpa, src_db, "ckpt3");

    // CURRENT must exist in the checkpoint dir.
    {
        const p = try filename.currentFileName(gpa, "ckpt3");
        defer gpa.free(p);
        try testing.expect(e.fileExists(p));
    }

    // The MANIFEST must exist.
    {
        const p = try filename.manifestFileName(gpa, "ckpt3", src_db.versions.manifestFileNumber());
        defer gpa.free(p);
        try testing.expect(e.fileExists(p));
    }

    // The active WAL must exist.
    {
        const p = try filename.logFileName(gpa, "ckpt3", src_db.versions.logNumber());
        defer gpa.free(p);
        try testing.expect(e.fileExists(p));
    }

    // Every live SST must exist in the checkpoint dir.
    {
        const ver = src_db.versions.currentVersion();
        var found_sst = false;
        for (&ver.files) |level| {
            for (level.items) |fm| {
                const p = try filename.tableFileName(gpa, "ckpt3", fm.number);
                defer gpa.free(p);
                try testing.expect(e.fileExists(p));
                found_sst = true;
            }
        }
        try testing.expect(found_sst);
    }
}
