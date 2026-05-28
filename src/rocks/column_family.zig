//! column_family.zig — Column Families (M7.0) — RED skeleton.
//!
//! This is the failing-test checkpoint of the TDD cycle: the public API surface
//! (CfDB / ColumnFamilyHandle + CF-tagged WriteBatch ops) exists so the tests
//! COMPILE, but every CfDB operation is unimplemented and returns
//! `error.Unimplemented`, so the M7.0 tests below fail at runtime (RED).  The
//! GREEN commit replaces these stubs with the real shared-WAL + per-CF sub-LSM
//! implementation.

const std = @import("std");

const env = @import("../env/env.zig");
const options_mod = @import("../options.zig");
const write_batch = @import("../format/write_batch.zig");

const db_mod = @import("../db/db.zig");

const Options = options_mod.Options;
const ReadOptions = options_mod.ReadOptions;
const WriteOptions = options_mod.WriteOptions;
const WriteBatch = write_batch.WriteBatch;
const DBIterator = db_mod.DBIterator;

pub const ColumnFamilyHandle = struct {
    id: u32,
    name: []const u8,
};

pub const CfDB = struct {
    gpa: std.mem.Allocator,

    pub fn open(gpa: std.mem.Allocator, e: env.Env, dbroot: []const u8, options: Options) !*CfDB {
        _ = e;
        _ = dbroot;
        _ = options;
        const self = try gpa.create(CfDB);
        self.* = .{ .gpa = gpa };
        return self;
    }

    pub fn close(self: *CfDB) void {
        self.gpa.destroy(self);
    }

    pub fn createColumnFamily(self: *CfDB, name: []const u8, options: Options) !ColumnFamilyHandle {
        _ = self;
        _ = name;
        _ = options;
        return error.Unimplemented;
    }

    pub fn dropColumnFamily(self: *CfDB, h: ColumnFamilyHandle) !void {
        _ = self;
        _ = h;
        return error.Unimplemented;
    }

    pub fn defaultColumnFamily(self: *CfDB) ColumnFamilyHandle {
        _ = self;
        return .{ .id = 0, .name = "default" };
    }

    pub fn columnFamily(self: *CfDB, name: []const u8) !ColumnFamilyHandle {
        _ = self;
        _ = name;
        return error.Unimplemented;
    }

    pub fn put(self: *CfDB, wopts: WriteOptions, h: ColumnFamilyHandle, key: []const u8, value: []const u8) !void {
        _ = self;
        _ = wopts;
        _ = h;
        _ = key;
        _ = value;
        return error.Unimplemented;
    }

    pub fn delete(self: *CfDB, wopts: WriteOptions, h: ColumnFamilyHandle, key: []const u8) !void {
        _ = self;
        _ = wopts;
        _ = h;
        _ = key;
        return error.Unimplemented;
    }

    pub fn write(self: *CfDB, wopts: WriteOptions, batch: *WriteBatch) !void {
        _ = self;
        _ = wopts;
        _ = batch;
        return error.Unimplemented;
    }

    pub fn get(self: *CfDB, ropts: ReadOptions, h: ColumnFamilyHandle, key: []const u8) !?[]u8 {
        _ = self;
        _ = ropts;
        _ = h;
        _ = key;
        return error.Unimplemented;
    }

    pub fn newIterator(self: *CfDB, gpa: std.mem.Allocator, ropts: ReadOptions, h: ColumnFamilyHandle) !DBIterator {
        _ = self;
        _ = gpa;
        _ = ropts;
        _ = h;
        return error.Unimplemented;
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
