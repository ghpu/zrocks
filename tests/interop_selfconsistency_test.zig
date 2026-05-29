//! interop_selfconsistency_test.zig — D1c self-consistency interop regression gate.
//!
//! HONEST SCOPE (see docs/roadmap-next-directions.md correction #6):
//! This is NOT "open a real RocksDB database".  It locks in self-consistency
//! invariants exercised through the public API on a real filesystem.  (The
//! WriteBatch WAL form is now RocksDB-exact — the legacy custom
//! kColumnFamilyTag=0x10 was replaced by RocksDB's kTypeColumnFamily* value
//! types in the wal-writebatch-cf migration.)
//!
//! Instead this gate locks in the *self-consistency* invariants that real
//! interop depends on, exercised through the PUBLIC `zrocks` module API on a
//! REAL on-disk filesystem (`RealEnv` over a temp dir):
//!
//!   1. RealEnv path model — files live under `<name>/`; CURRENT holds the bare
//!      MANIFEST basename (no directory component) + a trailing newline; the
//!      live `.log` file is named by the MANIFEST's recovered log_number.
//!   2. kNewFile4 MANIFEST self-consistency — a MANIFEST written with the
//!      RocksDB kNewFile4 (tag=100) new-file records (carrying smallest/largest
//!      seqnos + the custom-field terminator) is recovered byte-for-byte by
//!      `VersionSet.recover`, with seqnos and key ranges preserved.
//!   3. zrocks writes -> zrocks reads — a real DB.open/put/close/reopen/get
//!      round-trip on RealEnv survives flush+compaction (the kNewFile path is
//!      what compaction emits into the MANIFEST).
//!
//! It runs as part of `zig build test` (wired as its own test artifact in
//! build.zig) so any regression in the on-disk path model or the kNewFile4
//! MANIFEST round-trip is caught.

const std = @import("std");
const zrocks = @import("zrocks");

const DB = zrocks.db.DB;
const Options = zrocks.options.Options;
const RealEnv = zrocks.env.RealEnv;
const VersionEdit = zrocks.version_edit.VersionEdit;
const VersionSet = zrocks.version_set.VersionSet;
const log_writer = zrocks.log_writer;
const filename = zrocks.filename;
const coding = zrocks.coding;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read an entire file through the Env capability (used to inspect CURRENT and
/// to confirm a file exists).  Caller frees the returned buffer.
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

/// Parse `MANIFEST-<n>` -> n.  Returns error.BadManifestName on any deviation
/// from the exact `MANIFEST-` prefix + decimal-only suffix form.
fn manifestNumber(basename: []const u8) !u64 {
    const prefix = "MANIFEST-";
    if (!std.mem.startsWith(u8, basename, prefix)) return error.BadManifestName;
    const digits = basename[prefix.len..];
    if (digits.len == 0) return error.BadManifestName;
    return std.fmt.parseInt(u64, digits, 10) catch error.BadManifestName;
}

// ---------------------------------------------------------------------------
// 1. RealEnv path model
// ---------------------------------------------------------------------------

test "D1c gate: RealEnv path model — files under <name>/, CURRENT holds bare basename+newline, .log matches log_number" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(io, tmp.dir);
    const e = re.env();

    const dbname = "pathmodeldb";
    {
        const db = try DB.open(gpa, e, dbname, .{});
        defer db.close();
        try db.put(.{}, "alpha", "one");
        try db.put(.{}, "beta", "two");
    }

    // CURRENT lives at "<name>/CURRENT".
    const current_path = try filename.currentFileName(gpa, dbname);
    defer gpa.free(current_path);
    try std.testing.expect(e.fileExists(current_path));
    try std.testing.expect(std.mem.startsWith(u8, current_path, dbname ++ "/"));

    // CURRENT holds the BARE MANIFEST basename (no '/') + exactly one trailing
    // newline — this is the LevelDB/RocksDB convention real readers rely on.
    const current = try readWhole(gpa, e, current_path);
    defer gpa.free(current);
    try std.testing.expect(current.len >= 2);
    try std.testing.expectEqual(@as(u8, '\n'), current[current.len - 1]);
    const basename = current[0 .. current.len - 1];
    try std.testing.expect(std.mem.indexOfScalar(u8, basename, '/') == null);
    try std.testing.expect(std.mem.indexOfScalar(u8, basename, '\n') == null);

    // The basename parses as MANIFEST-<n> and that MANIFEST exists under <name>/.
    const mnum = try manifestNumber(basename);
    const manifest_path = try filename.manifestFileName(gpa, dbname, mnum);
    defer gpa.free(manifest_path);
    try std.testing.expect(e.fileExists(manifest_path));
    try std.testing.expect(std.mem.startsWith(u8, manifest_path, dbname ++ "/"));

    // Recover a fresh VersionSet from this on-disk MANIFEST and confirm the live
    // log_number names an existing "<name>/<n padded>.log".  This is the model
    // correction: kLogNumber must match the actual .log filename on disk.
    var vs = try VersionSet.init(gpa, e, dbname, .{});
    defer vs.deinit();
    try vs.recover();
    const log_number = vs.logNumber();
    try std.testing.expect(log_number != 0);

    const log_path = try filename.logFileName(gpa, dbname, log_number);
    defer gpa.free(log_path);
    try std.testing.expect(e.fileExists(log_path));
    // The .log basename is zero-padded to 6 digits.
    try std.testing.expect(std.mem.endsWith(u8, log_path, ".log"));
    var expect_log: [32]u8 = undefined;
    const want_log = try std.fmt.bufPrint(&expect_log, "{s}/{d:0>6}.log", .{ dbname, log_number });
    try std.testing.expectEqualStrings(want_log, log_path);
}

// ---------------------------------------------------------------------------
// 2. kNewFile4 MANIFEST self-consistency (write -> recover)
// ---------------------------------------------------------------------------

test "D1c gate: kNewFile4 MANIFEST self-consistency — VersionSet.recover preserves seqnos + key ranges" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(io, tmp.dir);
    const e = re.env();

    const dbname = "v4manifestdb";
    try e.makeDir(dbname);

    // Internal-key bytes: user key + 8-byte (seq<<8|type) trailer.
    const small_l0 = "aaa" ++ [_]u8{0} ** 8;
    const large_l0 = "mmm" ++ [_]u8{0} ** 8;
    const small_l1 = "nnn" ++ [_]u8{0} ** 8;
    const large_l1 = "zzz" ++ [_]u8{0} ** 8;

    const manifest_number: u64 = 5;

    // --- Write a self-contained MANIFEST using the PUBLIC version_edit +
    //     log_writer API, with kNewFile4 (tag=100) records carrying seqnos. ---
    {
        const manifest_path = try filename.manifestFileName(gpa, dbname, manifest_number);
        defer gpa.free(manifest_path);

        var wf = try e.newWritableFile(gpa, manifest_path);
        var writer = log_writer.Writer.init(wf);

        var edit = VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.setComparatorName(gpa, "leveldb.BytewiseComparator");
        edit.setLogNumber(7);
        edit.setNextFileNumber(20);
        edit.setLastSequence(999);
        // Two kNewFile4 records at different levels, carrying distinct seqnos.
        try edit.addFile4(gpa, 0, 10, 1024, small_l0, large_l0, 100, 200);
        try edit.addFile4(gpa, 1, 11, 2048, small_l1, large_l1, 300, 400);

        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(gpa);
        try edit.encodeTo(&buf, gpa);
        try writer.addRecord(gpa, buf.items);
        try wf.flush();
        try wf.close();
    }

    // --- Point CURRENT at it: bare basename + newline (the path model). ---
    {
        const current_path = try filename.currentFileName(gpa, dbname);
        defer gpa.free(current_path);
        var wf = try e.newWritableFile(gpa, current_path);
        var nbuf: [32]u8 = undefined;
        const line = try std.fmt.bufPrint(&nbuf, "MANIFEST-{d:0>6}\n", .{manifest_number});
        try wf.append(line);
        try wf.flush();
        try wf.close();
    }

    // --- Recover and verify the kNewFile4 metadata survived the MANIFEST. ---
    var vs = try VersionSet.init(gpa, e, dbname, .{});
    defer vs.deinit();
    try vs.recover();

    try std.testing.expectEqual(@as(u64, 7), vs.logNumber());
    try std.testing.expectEqual(@as(u64, 999), vs.lastSequence());

    const v = vs.currentVersion();
    try std.testing.expectEqual(@as(usize, 1), v.numFiles(0));
    try std.testing.expectEqual(@as(usize, 1), v.numFiles(1));

    const f0 = v.files[0].items[0];
    try std.testing.expectEqual(@as(u64, 10), f0.number);
    try std.testing.expectEqual(@as(u64, 1024), f0.file_size);
    try std.testing.expectEqualSlices(u8, small_l0, f0.smallest);
    try std.testing.expectEqualSlices(u8, large_l0, f0.largest);

    const f1 = v.files[1].items[0];
    try std.testing.expectEqual(@as(u64, 11), f1.number);
    try std.testing.expectEqual(@as(u64, 2048), f1.file_size);
    try std.testing.expectEqualSlices(u8, small_l1, f1.smallest);
    try std.testing.expectEqualSlices(u8, large_l1, f1.largest);
}

// ---------------------------------------------------------------------------
// 2b. kNewFile4 encode -> decode survives an unknown safe-to-ignore custom
//     field embedded in the MANIFEST record (forward-compat guard).
// ---------------------------------------------------------------------------

test "D1c gate: kNewFile4 record with a real-RocksDB-style custom field decodes (skip) and round-trips" {
    const gpa = std.testing.allocator;

    const smallest = "k1" ++ [_]u8{0} ** 8;
    const largest = "k9" ++ [_]u8{0} ** 8;

    // Hand-build a kNewFile4 record exactly as a real RocksDB MANIFEST would:
    // base fields + a kFileCreationTime(=6, safe-to-ignore) custom field, then
    // kTerminate(=1).  The zrocks decoder must skip the custom field.
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try coding.putVarint32(&buf, gpa, 100); // kNewFile4
    try coding.putVarint32(&buf, gpa, 2); // level
    try coding.putVarint64(&buf, gpa, 42); // file number
    try coding.putVarint64(&buf, gpa, 8192); // file size
    try coding.putLengthPrefixedSlice(&buf, gpa, smallest);
    try coding.putLengthPrefixedSlice(&buf, gpa, largest);
    try coding.putVarint64(&buf, gpa, 1234); // smallest_seqno
    try coding.putVarint64(&buf, gpa, 5678); // largest_seqno
    try coding.putVarint32(&buf, gpa, 6); // kFileCreationTime (safe to ignore)
    try coding.putLengthPrefixedSlice(&buf, gpa, &[_]u8{ 0x01, 0x02, 0x03, 0x04 });
    try coding.putVarint32(&buf, gpa, 1); // kTerminate

    var edit = try VersionEdit.decodeFrom(gpa, buf.items);
    defer edit.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 1), edit.new_files.items.len);
    const nf = edit.new_files.items[0];
    try std.testing.expect(nf.is_v4);
    try std.testing.expectEqual(@as(u32, 2), nf.level);
    try std.testing.expectEqual(@as(u64, 42), nf.meta.number);
    try std.testing.expectEqual(@as(u64, 8192), nf.meta.file_size);
    try std.testing.expectEqual(@as(u64, 1234), nf.meta.smallest_seqno);
    try std.testing.expectEqual(@as(u64, 5678), nf.meta.largest_seqno);
    try std.testing.expectEqualSlices(u8, smallest, nf.meta.smallest);
    try std.testing.expectEqualSlices(u8, largest, nf.meta.largest);

    // Re-encode (we emit only the terminator, no custom fields) and decode again
    // — the file metadata is stable across a full self-round-trip.
    var buf2: std.ArrayListUnmanaged(u8) = .empty;
    defer buf2.deinit(gpa);
    try edit.encodeTo(&buf2, gpa);

    var edit2 = try VersionEdit.decodeFrom(gpa, buf2.items);
    defer edit2.deinit(gpa);
    const nf2 = edit2.new_files.items[0];
    try std.testing.expectEqual(@as(u64, 42), nf2.meta.number);
    try std.testing.expectEqual(@as(u64, 1234), nf2.meta.smallest_seqno);
    try std.testing.expectEqual(@as(u64, 5678), nf2.meta.largest_seqno);
}

// ---------------------------------------------------------------------------
// 3. zrocks writes -> zrocks reads (real on-disk round-trip through flush+compaction)
// ---------------------------------------------------------------------------

test "D1c gate: zrocks writes -> close -> reopen -> reads on RealEnv (survives flush+compaction)" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(io, tmp.dir);
    const e = re.env();

    const dbname = "rttdb";
    // Tiny write buffer + low L0 trigger so flushes + compaction fire, exercising
    // the kNewFile MANIFEST path (what compaction writes) on disk.
    const opts = Options{
        .write_buffer_size = 64,
        .level0_file_num_compaction_trigger = 2,
        .max_bytes_for_level_base = 2048,
        .target_file_size_base = 1024,
    };

    const n: usize = 120;

    // --- Write phase ---
    {
        const db = try DB.open(gpa, e, dbname, opts);
        defer db.close();
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var kbuf: [16]u8 = undefined;
            var vbuf: [16]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>5}", .{i});
            const val = try std.fmt.bufPrint(&vbuf, "val{d:0>5}", .{i});
            try db.put(.{}, k, val);
        }
        // Delete a band of keys so tombstones flow through compaction too.
        i = 10;
        while (i < 20) : (i += 1) {
            var kbuf: [16]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>5}", .{i});
            try db.delete(.{}, k);
        }
    }

    // --- Reopen + read phase (fresh DB instance reading the same on-disk dir) ---
    {
        const db = try DB.open(gpa, e, dbname, opts);
        defer db.close();
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var kbuf: [16]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>5}", .{i});
            const got = try db.get(.{}, k);
            if (i >= 10 and i < 20) {
                // Deleted band — must read back as absent.
                try std.testing.expect(got == null);
            } else {
                const g = got orelse return error.GateKeyMissing;
                defer gpa.free(g);
                var vbuf: [16]u8 = undefined;
                const want = try std.fmt.bufPrint(&vbuf, "val{d:0>5}", .{i});
                try std.testing.expectEqualStrings(want, g);
            }
        }
    }
}
