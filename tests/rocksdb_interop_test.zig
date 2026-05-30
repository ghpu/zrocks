//! rocksdb_interop_test.zig — Wave B core: read a DB written by REAL RocksDB.
//!
//! This is the byte-exact interop gate.  `tests/fixtures/rocksdb/basic/` is a
//! genuine RocksDB v11.4.0 database (format_version 5, kNoCompression,
//! block_size=256 → a multi-data-block SST with a multi-entry index), holding
//! keys `key000`..`key099` → `value-000`..`value-099` with `key050` DELETED
//! (99 live keys).  zrocks must open it read_only, NON-DESTRUCTIVELY, and serve
//! every live key — proving it decodes a real RocksDB MANIFEST (kNewFile4=103,
//! kMinLogNumberToKeep=10, CF setup, and the safe-ignore tags kDbId /
//! kPersistUserDefinedTimestamps / kLastCompactedManifestFileSize) and a real
//! RocksDB block-based SST (metaindex with rocksdb.properties + filter blocks,
//! a binary-search index, prefix-compressed data blocks).
//!
//! To keep the committed fixture pristine and prove non-destructiveness, the
//! test COPIES the fixture into a fresh tmp dir, opens the copy read_only, then
//! asserts the copy is byte-for-byte unchanged after open+close.

const std = @import("std");
const zrocks = @import("zrocks");
const build_options = @import("build_options");

const DB = zrocks.db.DB;
const RealEnv = zrocks.env.RealEnv;
const Env = zrocks.env.Env;

/// Absolute path to the committed real-RocksDB fixture directory, injected by
/// build.zig so the test is independent of the process working directory.
const fixture_path = build_options.rocksdb_fixture_path;

// ---------------------------------------------------------------------------
// Fixture copy — duplicate the committed fixture into a writable tmp dir.
// ---------------------------------------------------------------------------

/// Copy every regular file from `src_abs` into `<e>/sub` via the zrocks Env
/// writable-file API, reading the source via std.Io.Dir.  `dst` is the same
/// directory `e` is rooted at, used here to create the subdir.
fn copyFixture(gpa: std.mem.Allocator, io: std.Io, src_abs: []const u8, e: Env, sub: []const u8) !void {
    try e.makeDir(sub);

    var src = try std.Io.Dir.cwd().openDir(io, src_abs, .{ .iterate = true });
    defer src.close(io);

    var it = src.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .file) continue;
        const data = try src.readFileAlloc(io, entry.name, gpa, .limited(1 << 24));
        defer gpa.free(data);
        const path = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ sub, entry.name });
        defer gpa.free(path);
        var wf = try e.newWritableFile(gpa, path);
        errdefer wf.close() catch {};
        try wf.append(data);
        try wf.sync();
        try wf.close();
    }
}

/// Read every file in `dir/sub` into a name->bytes map snapshot (sorted names
/// + their contents), so we can prove the open was byte-for-byte non-destructive.
const Snapshot = struct {
    names: [][]u8,
    contents: [][]u8,

    fn deinit(self: *Snapshot, gpa: std.mem.Allocator) void {
        for (self.names) |n| gpa.free(n);
        for (self.contents) |c| gpa.free(c);
        gpa.free(self.names);
        gpa.free(self.contents);
    }
};

fn snapshot(gpa: std.mem.Allocator, io: std.Io, root: std.Io.Dir, sub: []const u8) !Snapshot {
    var dir = try root.openDir(io, sub, .{ .iterate = true });
    defer dir.close(io);

    var names: std.ArrayListUnmanaged([]u8) = .empty;
    var contents: std.ArrayListUnmanaged([]u8) = .empty;
    errdefer {
        for (names.items) |n| gpa.free(n);
        for (contents.items) |c| gpa.free(c);
        names.deinit(gpa);
        contents.deinit(gpa);
    }

    // First gather names, sorted, so the snapshot is order-stable.
    var raw: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (raw.items) |n| gpa.free(n);
        raw.deinit(gpa);
    }
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .file) continue;
        try raw.append(gpa, try gpa.dupe(u8, entry.name));
    }
    std.mem.sort([]u8, raw.items, {}, struct {
        fn lt(_: void, a: []u8, b: []u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);

    for (raw.items) |name| {
        const data = try dir.readFileAlloc(io, name, gpa, .limited(1 << 24));
        try names.append(gpa, try gpa.dupe(u8, name));
        try contents.append(gpa, data);
    }

    return .{
        .names = try names.toOwnedSlice(gpa),
        .contents = try contents.toOwnedSlice(gpa),
    };
}

// ---------------------------------------------------------------------------
// The interop gate
// ---------------------------------------------------------------------------

test "Wave B: open a REAL RocksDB v11.4.0 database read_only and read every live key non-destructively" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    const e = re.env();

    const dbname = "rocksdb_basic";
    try copyFixture(gpa, io, fixture_path, e, dbname);

    // Snapshot the fixture BEFORE the open to prove read_only is non-destructive.
    var before = try snapshot(gpa, io, tmp.dir, dbname);
    defer before.deinit(gpa);

    {
        const db = try DB.open(gpa, e, dbname, .{ .read_only = true });
        defer db.close();

        // Point lookups across the data blocks.
        {
            const v = (try db.get(.{}, "key000")) orelse return error.MissingKey000;
            defer gpa.free(v);
            try std.testing.expectEqualStrings("value-000", v);
        }
        {
            const v = (try db.get(.{}, "key042")) orelse return error.MissingKey042;
            defer gpa.free(v);
            try std.testing.expectEqualStrings("value-042", v);
        }
        {
            const v = (try db.get(.{}, "key099")) orelse return error.MissingKey099;
            defer gpa.free(v);
            try std.testing.expectEqualStrings("value-099", v);
        }
        // key050 was deleted in the source DB — its tombstone must be honored.
        try std.testing.expect((try db.get(.{}, "key050")) == null);

        // A full forward scan must yield exactly the 99 live keys in order.
        var it = try db.newIterator(gpa, .{});
        defer it.deinit();
        it.seekToFirst();
        var i: usize = 0;
        while (it.valid()) : (i += 1) {
            // Expected key index: 0..99 skipping 50.
            const expect_idx: usize = if (i < 50) i else i + 1;
            var kbuf: [16]u8 = undefined;
            var vbuf: [16]u8 = undefined;
            const ek = try std.fmt.bufPrint(&kbuf, "key{d:0>3}", .{expect_idx});
            const ev = try std.fmt.bufPrint(&vbuf, "value-{d:0>3}", .{expect_idx});
            try std.testing.expectEqualStrings(ek, it.key());
            try std.testing.expectEqualStrings(ev, it.value());
            it.next();
        }
        try std.testing.expectEqual(@as(usize, 99), i);
    }

    // The fixture directory must be byte-for-byte UNCHANGED after open+close.
    var after = try snapshot(gpa, io, tmp.dir, dbname);
    defer after.deinit(gpa);
    try std.testing.expectEqual(before.names.len, after.names.len);
    for (before.names, after.names) |b, a| try std.testing.expectEqualStrings(b, a);
    for (before.contents, after.contents, before.names) |b, a, name| {
        std.testing.expectEqualSlices(u8, b, a) catch |err| {
            std.debug.print("fixture file '{s}' changed after read_only open\n", .{name});
            return err;
        };
    }
}
