//! integration_db_test.zig — public-API integration tests for zrocks (M6.3).
//!
//! Unlike the in-`src` tests (which use the in-memory `MemEnv` and reach into
//! private fields), these exercise the SAME code through the PUBLIC `zrocks`
//! module API on a REAL on-disk filesystem env (`RealEnv` over a temp dir).
//! This is the milestone's integration gate: it must run as part of
//! `zig build test`.
//!
//! Two properties are covered:
//!   1. Randomized durability — ~1500 random ops over a small key space with a
//!      tiny write buffer (so flushes + compactions fire), verified against a
//!      reference `std.StringHashMap`, including a close + reopen re-verify.
//!   2. Snapshot-respects-compaction — the key M6.3 correctness: a value pinned
//!      by a live snapshot survives a compaction that would otherwise drop it.

const std = @import("std");
const zrocks = @import("zrocks");

const DB = zrocks.db.DB;
const Options = zrocks.options.Options;
const RealEnv = zrocks.env.RealEnv;
const CfDB = zrocks.column_family.CfDB;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Reference model mirroring the DB's live state.  Owns its key + value bytes.
const RefMap = struct {
    map: std.StringHashMapUnmanaged([]u8) = .empty,
    gpa: std.mem.Allocator,

    fn init(gpa: std.mem.Allocator) RefMap {
        return .{ .gpa = gpa };
    }
    fn deinit(self: *RefMap) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.gpa.free(entry.key_ptr.*);
            self.gpa.free(entry.value_ptr.*);
        }
        self.map.deinit(self.gpa);
    }
    fn put(self: *RefMap, key: []const u8, value: []const u8) !void {
        const gop = try self.map.getOrPut(self.gpa, key);
        if (gop.found_existing) {
            self.gpa.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.gpa.dupe(u8, key);
        }
        gop.value_ptr.* = try self.gpa.dupe(u8, value);
    }
    fn delete(self: *RefMap, key: []const u8) void {
        if (self.map.fetchRemove(key)) |kv| {
            self.gpa.free(kv.key);
            self.gpa.free(kv.value);
        }
    }
    fn get(self: *RefMap, key: []const u8) ?[]const u8 {
        return self.map.get(key);
    }
};

/// Assert DB.get matches the reference for every key in the space, and that a
/// full forward scan equals the reference's sorted live entries.
fn verifyAgainstRef(gpa: std.mem.Allocator, db: *DB, ref: *RefMap, key_space: usize) !void {
    // 1. Point lookups for every possible key.
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
            std.testing.expectEqualSlices(u8, w, g) catch {
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

    // 2. Full forward scan == sorted live reference entries.
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
        std.testing.expectEqualSlices(u8, want_k, it.key()) catch {
            std.debug.print("scan key mismatch at {d}: ref={s} db={s}\n", .{ idx, want_k, it.key() });
            return error.TestScanKeyMismatch;
        };
        std.testing.expectEqualSlices(u8, want_v, it.value()) catch {
            std.debug.print("scan value mismatch at key {s}: ref={s} db={s}\n", .{ want_k, want_v, it.value() });
            return error.TestScanValueMismatch;
        };
        idx += 1;
    }
    if (idx != sorted_keys.items.len) {
        std.debug.print("scan too short: got {d} want {d}\n", .{ idx, sorted_keys.items.len });
        return error.TestScanTooShort;
    }
    try std.testing.expect(it.status() == null);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "M6.3 integration: snapshot-respects-compaction (pinned value survives)" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    const e = re.env();

    // Tiny write buffer + low L0 trigger so flushes + compactions fire readily.
    const opts = Options{
        .write_buffer_size = 1,
        .level0_file_num_compaction_trigger = 2,
        .max_bytes_for_level_base = 4096,
        .target_file_size_base = 2048,
    };

    const db = try DB.open(gpa, e, "snapdb", opts);
    defer db.close();

    // 1. Put k=v1, then take a snapshot pinning that version.
    try db.put(.{}, "k", "v1");
    const snap = try db.getSnapshot();
    defer db.releaseSnapshot(snap);

    // 2. Overwrite then delete k after the snapshot.
    try db.put(.{}, "k", "v2");
    try db.delete(.{}, "k");

    // 3. Write a flood of OTHER keys to force flushes + compaction.  Without
    //    snapshot-pinning, compaction would drop v1 (and the v2/tombstone) since
    //    k is dead at the latest sequence — but the live snapshot must keep v1.
    var i: usize = 0;
    while (i < 200) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        var vbuf: [16]u8 = undefined;
        const fk = try std.fmt.bufPrint(&kbuf, "fill{d:0>5}", .{i});
        const fv = try std.fmt.bufPrint(&vbuf, "fv{d:0>5}", .{i});
        try db.put(.{}, fk, fv);
    }

    // 4. A normal get sees the delete → null.
    try std.testing.expect((try db.get(.{}, "k")) == null);

    // 5. A get AT THE SNAPSHOT still returns v1 — the snapshot pinned it through
    //    all the flushes + compactions.  THIS is the M6.3 correctness property.
    {
        const at_snap = try db.get(.{ .snapshot = snap.sequence }, "k") orelse
            return error.TestSnapshotLostV1;
        defer gpa.free(at_snap);
        try std.testing.expectEqualStrings("v1", at_snap);
    }
}

test "M6.3 integration: randomized ~1500-op durability gate on a real fs (reopen)" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    const e = re.env();

    const key_space: usize = 150; // keys "key000".."key149"
    const opts = Options{
        .write_buffer_size = 256, // many flushes
        .level0_file_num_compaction_trigger = 2, // many compactions
        .max_bytes_for_level_base = 4096, // small levels -> deep compaction
        .target_file_size_base = 2048, // small output files (multiple splits)
    };

    var ref = RefMap.init(gpa);
    defer ref.deinit();

    var prng = std.Random.DefaultPrng.init(0xABCDEF_55AA);
    const rand = prng.random();

    {
        const db = try DB.open(gpa, e, "fuzzdb", opts);
        defer db.close();

        var op: usize = 0;
        while (op < 1500) : (op += 1) {
            const key_idx = rand.uintLessThan(usize, key_space);
            var kbuf: [8]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{key_idx});

            if (rand.uintLessThan(u32, 100) < 70) {
                var vbuf: [40]u8 = undefined;
                const vlen = 1 + rand.uintLessThan(usize, vbuf.len);
                for (vbuf[0..vlen]) |*b| b.* = 'a' + rand.uintLessThan(u8, 26);
                const v = vbuf[0..vlen];
                try db.put(.{}, k, v);
                try ref.put(k, v);
            } else {
                try db.delete(.{}, k);
                ref.delete(k);
            }

            if (op % 300 == 299) {
                try verifyAgainstRef(gpa, db, &ref, key_space);
            }
        }
        try verifyAgainstRef(gpa, db, &ref, key_space);
    }

    // Reopen the SAME on-disk DB and re-verify (recovery after compaction).
    {
        const db = try DB.open(gpa, e, "fuzzdb", opts);
        defer db.close();
        try verifyAgainstRef(gpa, db, &ref, key_space);
    }
}

// ===========================================================================
// C2 — DB-level advisory LOCK file (RealEnv-backed, real cross-handle locking).
// ===========================================================================

test "C2 lock: second writable open on the same dir FAILS while first is open" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    defer re.deinit();
    const e = re.env();

    // First writable open acquires the LOCK.
    const db1 = try DB.open(gpa, e, "lockdb", .{});

    // A SECOND writable open on the SAME directory must FAIL (lock held).
    try std.testing.expectError(error.IoError, DB.open(gpa, e, "lockdb", .{}));

    // Close the first DB — the lock is released.
    db1.close();

    // A fresh writable open now SUCCEEDS (lock was released on close).
    const db2 = try DB.open(gpa, e, "lockdb", .{});
    db2.close();
}

test "C2 lock: read-only open does NOT take the exclusive lock (opens on existing DB)" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    defer re.deinit();
    const e = re.env();

    // Create an on-disk DB with some data, then close it.
    {
        const db = try DB.open(gpa, e, "rodb", .{});
        try db.put(.{}, "k", "v");
        db.close();
    }

    // A read-only open succeeds (and must not take an exclusive lock).
    const ro = try DB.open(gpa, e, "rodb", .{ .read_only = true });
    defer ro.close();
    const got = try ro.get(.{}, "k") orelse return error.TestExpectedFound;
    defer gpa.free(got);
    try std.testing.expectEqualStrings("v", got);
}

test "C2 lock: CfDB single dbroot lock does NOT self-conflict across per-CF sub-LSMs" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    defer re.deinit();
    const e = re.env();

    {
        const cdb = try CfDB.open(gpa, e, "cflock", .{});
        defer cdb.close();

        const users = try cdb.createColumnFamily("users", .{});
        const orders = try cdb.createColumnFamily("orders", .{});
        const default = cdb.defaultColumnFamily();

        try cdb.put(.{}, users, "u1", "alice");
        try cdb.put(.{}, orders, "o1", "order-one");
        try cdb.put(.{}, default, "d1", "def-one");

        const uv = try cdb.get(.{}, users, "u1") orelse return error.TestExpectedFound;
        defer gpa.free(uv);
        try std.testing.expectEqualStrings("alice", uv);
    }

    // While the CfDB is open, a SECOND CfDB.open on the same dbroot must FAIL.
    {
        const cdb = try CfDB.open(gpa, e, "cflock2", .{});
        try std.testing.expectError(error.IoError, CfDB.open(gpa, e, "cflock2", .{}));
        cdb.close();
    }

    // After close, re-open the multi-CF DB SUCCEEDS (lock released).
    {
        const cdb = try CfDB.open(gpa, e, "cflock", .{});
        defer cdb.close();
        const users = try cdb.columnFamily("users");
        const uv = try cdb.get(.{}, users, "u1") orelse return error.TestExpectedFound;
        defer gpa.free(uv);
        try std.testing.expectEqualStrings("alice", uv);
    }
}
