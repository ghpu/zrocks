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
//! * Plain byte-copy (no hard-links).
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
// Public API — RED stub (returns immediately without copying anything)
// ---------------------------------------------------------------------------

/// Create a consistent checkpoint of `db` in the directory `dest_dir`.
/// (Stubbed — not yet implemented.)
pub fn createCheckpoint(
    gpa: std.mem.Allocator,
    db: *db_mod.DB,
    dest_dir: []const u8,
) !void {
    _ = gpa;
    _ = db;
    _ = dest_dir;
    return error.NotImplemented;
}

// ---------------------------------------------------------------------------
// Private helpers (stubs)
// ---------------------------------------------------------------------------

fn copyFile(
    gpa: std.mem.Allocator,
    e: env_mod.Env,
    src_path: []const u8,
    dest_path: []const u8,
) !void {
    _ = gpa;
    _ = e;
    _ = src_path;
    _ = dest_path;
    return error.NotImplemented;
}

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;
const MemEnv = env_mod.MemEnv;
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

    // Create checkpoint — must fail with NotImplemented in RED.
    try createCheckpoint(gpa, src_db, "ckpt");

    // Open the checkpoint as a fully independent DB.
    const ckpt_db = try DB.open(gpa, e, "ckpt", .{});
    defer ckpt_db.close();

    // Every key must be readable in the checkpoint.
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

    // Create checkpoint — must fail with NotImplemented in RED.
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

test "checkpoint: expected files exist in dest dir" {
    const gpa = testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    const src_db = try DB.open(gpa, e, "srcdb3", .{ .write_buffer_size = 512 });
    defer src_db.close();

    // Write enough to trigger at least one flush.
    var i: usize = 0;
    while (i < 32) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        var vbuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key{d:0>4}", .{i});
        const v = try std.fmt.bufPrint(&vbuf, "val{d:0>4}", .{i});
        try src_db.put(.{}, k, v);
    }

    // Create checkpoint — must fail with NotImplemented in RED.
    try createCheckpoint(gpa, src_db, "ckpt3");

    // CURRENT must exist.
    {
        const p = try filename.currentFileName(gpa, "ckpt3");
        defer gpa.free(p);
        try testing.expect(e.fileExists(p));
    }

    // MANIFEST must exist.
    {
        const p = try filename.manifestFileName(gpa, "ckpt3", src_db.versions.manifestFileNumber());
        defer gpa.free(p);
        try testing.expect(e.fileExists(p));
    }

    // Active WAL must exist.
    {
        const p = try filename.logFileName(gpa, "ckpt3", src_db.versions.logNumber());
        defer gpa.free(p);
        try testing.expect(e.fileExists(p));
    }

    // At least one SST must exist (the L0 flush happened).
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
