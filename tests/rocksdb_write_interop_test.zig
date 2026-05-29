//! rocksdb_write_interop_test.zig — TWO-TRACK rocksdb-write gate.
//!
//! ## Track 1 — CI self-consistency (runs in every `zig build test`)
//!
//! Proves that a zrocks DB (the format is now unconditionally RocksDB-compatible),
//! FULLY FLUSHED to SSTs, is internally consistent and re-readable end to end
//! WITHOUT a real RocksDB binary:
//!
//!   1. Write a DB via the public API in `.rocksdb` mode (keys spanning >= 2
//!      data blocks plus a delete), force a flush so all data lives in SSTs and
//!      the new WAL is empty, then close.
//!   2. Re-open the SAME directory with a fresh zrocks DB (the SST/MANIFEST
//!      readers transcode the on-disk RocksDB form) and assert every live key
//!      reads back with its exact value, the
//!      deleted key is absent, and a full scan yields exactly the live set.
//!   3. Structural asserts on the on-disk bytes: the SST footer is fv5 + crc32c
//!      with the RocksDB magic, its metaindex carries `rocksdb.properties` (the
//!      read-side discriminator), and the MANIFEST emits NO default-CF add and
//!      uses kNewFile4 (tag 103) records.
//!
//! ## Track 2 — Dev-loop real-RocksDB authenticity gate (NOT wired into CI)
//!
//! After the CI test passes, developers run the real RocksDB oracle against the
//! same flushed DB directory.  The oracle binary (`/home/ghpu/rocksdb-interop/verify_open`)
//! is built from `tools/rocksdb-interop/verify_open.cc` and performs:
//!   a. OpenForReadOnly — confirms RocksDB can open the DB at all.
//!   b. Full forward scan (SeekToFirst → end) — exercises data blocks + bloom.
//!   c. Point Get on sampled live keys — exercises the Get path and bloom probe.
//!   d. Point Get on a synthetic absent key — must return NotFound.
//!   e. Seek to a mid-key + forward range scan — exercises Seek + partial scan.
//!
//! Expected oracle output for THIS test's DB (11 live keys, "charlie" deleted):
//!
//!   OPEN_OK count=11
//!   alpha=v-alpha
//!   bravo=v-bravo
//!   ... (all live keys in sorted order)
//!
//! Dev-loop invocation (after writing the DB to /tmp/rdbwrite or similar):
//!   /home/ghpu/rocksdb-interop/verify_open <db_dir>
//!
//! This gate is NOT in CI because it requires the external librocksdb binary
//! (not present in CI).  The oracle-widen milestone (oracle-widen) established
//! the widened checks above and verified OPEN_OK count=11 against this DB.

const std = @import("std");
const zrocks = @import("zrocks");

const DB = zrocks.db.DB;
const Options = zrocks.options.Options;
const RealEnv = zrocks.env.RealEnv;
const Footer = zrocks.footer.Footer;
const Block = zrocks.block.Block;
const BlockHandle = zrocks.footer.BlockHandle;
const bytewise = zrocks.comparator.bytewise;

const KV = struct { k: []const u8, v: []const u8 };

/// 12 sorted keys + one deleted ("charlie").  Small enough keys that a tiny
/// block_size yields several data blocks.
const live = [_]KV{
    .{ .k = "alpha", .v = "v-alpha" },
    .{ .k = "bravo", .v = "v-bravo" },
    .{ .k = "delta", .v = "v-delta" },
    .{ .k = "echo", .v = "v-echo" },
    .{ .k = "foxtrot", .v = "v-foxtrot" },
    .{ .k = "golf", .v = "v-golf" },
    .{ .k = "hotel", .v = "v-hotel" },
    .{ .k = "india", .v = "v-india" },
    .{ .k = "juliet", .v = "v-juliet" },
    .{ .k = "kilo", .v = "v-kilo" },
    .{ .k = "lima", .v = "v-lima" },
};

fn readWhole(gpa: std.mem.Allocator, e: zrocks.env.Env, path: []const u8) ![]u8 {
    const size = try e.getFileSize(path);
    const buf = try gpa.alloc(u8, size);
    errdefer gpa.free(buf);
    var raf = try e.newRandomAccessFile(gpa, path);
    defer raf.close() catch {};
    var off: u64 = 0;
    while (off < size) {
        const n = try raf.readAt(off, buf[off..]);
        if (n == 0) break;
        off += n;
    }
    if (off != size) return error.ShortRead;
    return buf;
}

/// Find the single `<dbname>/NNNNNN.sst` and `<dbname>/MANIFEST-NNNNNN` names in
/// `dir/dbname` via the Env list capability (caller frees both).
const DbFiles = struct {
    sst: []u8,
    manifest: []u8,
    fn deinit(self: *DbFiles, gpa: std.mem.Allocator) void {
        gpa.free(self.sst);
        gpa.free(self.manifest);
    }
};

fn findDbFiles(gpa: std.mem.Allocator, e: zrocks.env.Env, dbname: []const u8) !DbFiles {
    const names = try e.listDir(gpa, dbname);
    defer {
        for (names) |n| gpa.free(n);
        gpa.free(names);
    }
    var sst: ?[]u8 = null;
    var manifest: ?[]u8 = null;
    for (names) |n| {
        if (std.mem.endsWith(u8, n, ".sst") and sst == null) {
            sst = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ dbname, n });
        } else if (std.mem.startsWith(u8, n, "MANIFEST-") and manifest == null) {
            manifest = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ dbname, n });
        }
    }
    return .{ .sst = sst orelse return error.NoSst, .manifest = manifest orelse return error.NoManifest };
}

test "rocksdb-write: fully-flushed .rocksdb DB round-trips through a fresh zrocks open + carries RocksDB-form on disk" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(io, tmp.dir);
    const e = re.env();

    const dbname = "rdbwrite";

    // ---- 1. Write a fully-flushed DB in rocksdb-compatible mode ----
    {
        const opts = Options{
            .create_if_missing = true,
            .block_size = 48, // tiny -> several data blocks
            .block_restart_interval = 4,
        };
        const db = try DB.open(gpa, e, dbname, opts);
        for (live) |p| try db.put(.{}, p.k, p.v);
        try db.delete(.{}, "charlie"); // a tombstone for an absent key
        try db.flushAll(); // force all data into SSTs; new WAL is empty
        db.close();
    }

    // ---- 2. Re-open with a fresh zrocks DB and read everything back ----
    {
        // The readers transcode the on-disk RocksDB index form.
        const db = try DB.open(gpa, e, dbname, .{});
        defer db.close();

        for (live) |p| {
            const got = (try db.get(.{}, p.k)) orelse {
                std.debug.print("missing key {s}\n", .{p.k});
                return error.MissingKey;
            };
            defer gpa.free(got);
            try std.testing.expectEqualStrings(p.v, got);
        }
        // Deleted key absent.
        try std.testing.expect((try db.get(.{}, "charlie")) == null);

        // Full forward scan == exactly the live set, in sorted order.
        var it = try db.newIterator(gpa, .{});
        defer it.deinit();
        it.seekToFirst();
        var i: usize = 0;
        while (it.valid()) : (i += 1) {
            try std.testing.expect(i < live.len);
            try std.testing.expectEqualStrings(live[i].k, it.key());
            try std.testing.expectEqualStrings(live[i].v, it.value());
            it.next();
        }
        try std.testing.expectEqual(live.len, i);
    }

    // ---- 3. Structural asserts on the on-disk bytes ----
    var files = try findDbFiles(gpa, e, dbname);
    defer files.deinit(gpa);

    // -- SST: fv5 + crc32c footer, RocksDB magic, metaindex has properties --
    {
        const sst = try readWhole(gpa, e, files.sst);
        defer gpa.free(sst);
        const footer = try Footer.decodeFrom(sst[sst.len - 53 ..]);
        try std.testing.expectEqual(@as(u32, 5), footer.format_version);
        try std.testing.expectEqual(zrocks.footer.ChecksumType.crc32c, footer.checksum_type);

        // Metaindex block (uncompressed; verify trailer is kNoCompression).
        const mstart: usize = @intCast(footer.metaindex_handle.offset);
        const msize: usize = @intCast(footer.metaindex_handle.size);
        try std.testing.expectEqual(@as(u8, 0), sst[mstart + msize]); // kNoCompression
        const meta = try Block.init(gpa, sst[mstart .. mstart + msize]);
        var mit = meta.iterator(bytewise);
        defer mit.deinit();
        mit.seek("rocksdb.properties");
        try std.testing.expect(mit.valid());
        try std.testing.expectEqualStrings("rocksdb.properties", mit.key());
    }

    // -- MANIFEST: NO default-CF add (tag 201) + uses kNewFile4 (tag 103) --
    {
        const manifest = try readWhole(gpa, e, files.manifest);
        defer gpa.free(manifest);
        // The MANIFEST is a sequence of log records: 7-byte header (crc32, len,
        // type) then payload.  Concatenate every record payload and scan the
        // decoded VersionEdit tags.  A kColumnFamilyAdd (201 -> 0xC9,0x01) must
        // NOT appear; at least one kNewFile4 (103 -> 0x67) must.
        var saw_newfile4 = false;
        var saw_cf_add = false;
        var off: usize = 0;
        while (off + 7 <= manifest.len) {
            const len = @as(usize, manifest[off + 4]) | (@as(usize, manifest[off + 5]) << 8);
            const payload_start = off + 7;
            if (payload_start + len > manifest.len) break;
            const payload = manifest[payload_start .. payload_start + len];
            var edit = try zrocks.version_edit.VersionEdit.decodeFrom(gpa, payload);
            defer edit.deinit(gpa);
            if (edit.column_family_name != null) saw_cf_add = true;
            for (edit.new_files.items) |nf| {
                if (nf.is_v4) saw_newfile4 = true;
            }
            off = payload_start + len;
        }
        try std.testing.expect(!saw_cf_add); // RocksDB rejects a second "default" CF
        try std.testing.expect(saw_newfile4); // kNewFile4 (tag 103) records
    }
}
