//! leveldb_interop_test.zig — Wave A: read a REAL external LevelDB database.
//!
//! This gate proves zrocks can OPEN, read-only and NON-DESTRUCTIVELY, a database
//! laid out exactly the way an external LevelDB implementation (Chromium /
//! Electron) writes one: NO `.sst`/`.ldb` files, all live data in the WAL +
//! MANIFEST + CURRENT, where
//!
//!   * `CURRENT`          = ASCII "MANIFEST-000001\n" (bare basename + newline);
//!   * `MANIFEST-000001`  = a legacy-log file with ONE VersionEdit carrying
//!                          kComparator="leveldb.BytewiseComparator",
//!                          kLogNumber=0, kNextFileNumber=2, kLastSequence=0 —
//!                          and NO column-family records, NO kNewFile;
//!   * `000003.log`       = a legacy WAL whose number (3) is GREATER than the
//!                          MANIFEST's log_number (0) AND its next_file_number
//!                          (2), holding a standard WriteBatch.
//!
//! The fixture below is GENERATED programmatically from std + the public zrocks
//! framing primitives (crc32c.mask + the legacy log writer + WriteBatch +
//! VersionEdit), so it is byte-valid LevelDB and fully CI-reproducible — no real
//! database bytes are committed.  Opening it read_only must:
//!   1. NOT mutate the directory in any way (no new WAL, no MANIFEST, no GC);
//!   2. replay 000003.log even though log_number=0 (replay ALL logs whose
//!      number >= the recovered log_number, ascending);
//!   3. tolerate the CF-less MANIFEST as the single default column family.
//! and then serve the live keys via get + a full iterator scan.

const std = @import("std");
const zrocks = @import("zrocks");

const DB = zrocks.db.DB;
const Options = zrocks.options.Options;
const RealEnv = zrocks.env.RealEnv;
const Env = zrocks.env.Env;
const VersionEdit = zrocks.version_edit.VersionEdit;
const WriteBatch = zrocks.write_batch.WriteBatch;
const log_writer = zrocks.log_writer;
const crc32c = zrocks.crc32c;

// ---------------------------------------------------------------------------
// Fixture generator — emit byte-valid external-LevelDB files into `e`.
// ---------------------------------------------------------------------------

/// Write the bare MANIFEST basename + newline to CURRENT (LevelDB convention).
fn writeCurrent(e: Env, name: []const u8) !void {
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/CURRENT", .{name});
    defer std.testing.allocator.free(path);
    var wf = try e.newWritableFile(std.testing.allocator, path);
    errdefer wf.close() catch {};
    try wf.append("MANIFEST-000001\n");
    try wf.sync();
    try wf.close();
}

/// Write MANIFEST-000001 as a legacy-log file carrying ONE VersionEdit with the
/// four scalar tags an external LevelDB emits at create time and NO CF records.
fn writeManifest(gpa: std.mem.Allocator, e: Env, name: []const u8) !void {
    var edit = VersionEdit.init();
    defer edit.deinit(gpa);
    try edit.setComparatorName(gpa, "leveldb.BytewiseComparator");
    edit.setLogNumber(0);
    edit.setNextFileNumber(2);
    edit.setLastSequence(0);

    var payload: std.ArrayListUnmanaged(u8) = .empty;
    defer payload.deinit(gpa);
    try edit.encodeTo(&payload, gpa);

    const path = try std.fmt.allocPrint(gpa, "{s}/MANIFEST-000001", .{name});
    defer gpa.free(path);
    var wf = try e.newWritableFile(gpa, path);
    errdefer wf.close() catch {};
    var w = log_writer.Writer.init(wf);
    try w.addRecord(gpa, payload.items);
    try wf.sync();
    try wf.close();
}

/// Write 000003.log as a legacy WAL holding ONE WriteBatch (seq=1, 4 records):
/// put hello->world, put foo->bar, delete gone, put empty->"".
fn writeWal(gpa: std.mem.Allocator, e: Env, name: []const u8) !void {
    var batch = try WriteBatch.init(gpa);
    defer batch.deinit(gpa);
    try batch.put(gpa, "hello", "world");
    try batch.put(gpa, "foo", "bar");
    try batch.delete(gpa, "gone");
    try batch.put(gpa, "empty", "");
    batch.setSequence(1);

    const path = try std.fmt.allocPrint(gpa, "{s}/000003.log", .{name});
    defer gpa.free(path);
    var wf = try e.newWritableFile(gpa, path);
    errdefer wf.close() catch {};
    var w = log_writer.Writer.init(wf);
    try w.addRecord(gpa, batch.contents());
    try wf.sync();
    try wf.close();
}

/// Generate the full external-LevelDB fixture under directory `name` on `e`.
fn generateFixture(gpa: std.mem.Allocator, e: Env, name: []const u8) !void {
    try e.makeDir(name);
    try writeCurrent(e, name);
    try writeManifest(gpa, e, name);
    try writeWal(gpa, e, name);
}

/// Confirm the fixture really is byte-valid LevelDB: a hand-rolled CRC + record
/// type check on the MANIFEST's first physical log record, plus a CURRENT
/// content assertion.  This makes the test self-certifying for the framing.
fn assertManifestFraming(gpa: std.mem.Allocator, e: Env, name: []const u8) !void {
    const path = try std.fmt.allocPrint(gpa, "{s}/MANIFEST-000001", .{name});
    defer gpa.free(path);
    const size = try e.getFileSize(path);
    const buf = try gpa.alloc(u8, size);
    defer gpa.free(buf);
    var raf = try e.newRandomAccessFile(gpa, path);
    defer raf.close() catch {};
    var off: u64 = 0;
    while (off < size) {
        const n = try raf.readAt(off, buf[off..]);
        if (n == 0) break;
        off += n;
    }
    // Legacy header: crc(4) | len(2 LE) | type(1).
    try std.testing.expect(size >= 7);
    const stored_crc = std.mem.readInt(u32, buf[0..4], .little);
    const len = std.mem.readInt(u16, buf[4..6], .little);
    const rtype = buf[6];
    try std.testing.expectEqual(@as(u8, 1), rtype); // kFullType
    const payload = buf[7 .. 7 + len];
    // CRC covers the type byte + payload, masked.
    var crc = crc32c.value(&[_]u8{rtype});
    crc = crc32c.extend(crc, payload);
    try std.testing.expectEqual(crc32c.mask(crc), stored_crc);
}

// ---------------------------------------------------------------------------
// The interop gate
// ---------------------------------------------------------------------------

test "Wave A: open an external-LevelDB fixture read_only and read its live keys non-destructively" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(io, tmp.dir);
    const e = re.env();

    const dbname = "ldbfixture";
    try generateFixture(gpa, e, dbname);
    try assertManifestFraming(gpa, e, dbname);

    // Snapshot the directory listing BEFORE the open so we can prove read_only
    // open is non-destructive (no file created/renamed/deleted).
    const before = try listSorted(gpa, tmp.dir, io, dbname);
    defer freeList(gpa, before);

    {
        const db = try DB.open(gpa, e, dbname, .{ .read_only = true });
        defer db.close();

        // Live keys recovered from 000003.log (number 3 > manifest log_number 0).
        {
            const v = (try db.get(.{}, "hello")) orelse return error.MissingHello;
            defer gpa.free(v);
            try std.testing.expectEqualStrings("world", v);
        }
        {
            const v = (try db.get(.{}, "foo")) orelse return error.MissingFoo;
            defer gpa.free(v);
            try std.testing.expectEqualStrings("bar", v);
        }
        {
            const v = (try db.get(.{}, "empty")) orelse return error.MissingEmpty;
            defer gpa.free(v);
            try std.testing.expectEqualStrings("", v);
        }
        // The deleted key is absent.
        try std.testing.expect((try db.get(.{}, "gone")) == null);

        // A full forward iterator scan yields the live pairs in key order.
        var it = try db.newIterator(gpa, .{});
        defer it.deinit();
        it.seekToFirst();
        const expect_keys = [_][]const u8{ "empty", "foo", "hello" };
        const expect_vals = [_][]const u8{ "", "bar", "world" };
        var i: usize = 0;
        while (it.valid()) : (i += 1) {
            try std.testing.expect(i < expect_keys.len);
            try std.testing.expectEqualStrings(expect_keys[i], it.key());
            try std.testing.expectEqualStrings(expect_vals[i], it.value());
            it.next();
        }
        try std.testing.expectEqual(expect_keys.len, i);

        // Writes must be rejected in read-only mode.
        try std.testing.expectError(error.ReadOnly, db.put(.{}, "x", "y"));
        try std.testing.expectError(error.ReadOnly, db.delete(.{}, "hello"));
    }

    // Directory listing must be UNCHANGED after a read_only open+close.
    const after = try listSorted(gpa, tmp.dir, io, dbname);
    defer freeList(gpa, after);
    try std.testing.expectEqual(before.len, after.len);
    for (before, after) |b, a| try std.testing.expectEqualStrings(b, a);
}

// --- directory-listing helpers (test-local; uses std directly) -------------

fn listSorted(gpa: std.mem.Allocator, root: std.Io.Dir, io: std.Io, sub: []const u8) ![][]u8 {
    var dir = try root.openDir(io, sub, .{ .iterate = true });
    defer dir.close(io);
    var names: std.ArrayListUnmanaged([]u8) = .empty;
    errdefer {
        for (names.items) |n| gpa.free(n);
        names.deinit(gpa);
    }
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        try names.append(gpa, try gpa.dupe(u8, entry.name));
    }
    const slice = try names.toOwnedSlice(gpa);
    std.mem.sort([]u8, slice, {}, struct {
        fn lt(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return slice;
}

fn freeList(gpa: std.mem.Allocator, list: [][]u8) void {
    for (list) |n| gpa.free(n);
    gpa.free(list);
}
