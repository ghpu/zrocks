//! recovery.zig — WAL replay for DB.open (M5.2 durability).
//!
//! After the VersionSet is recovered from the MANIFEST/CURRENT, the live
//! MemTable is rebuilt by replaying the records of the active WAL.  Each WAL
//! record is the `contents()` of a `WriteBatch` (header = sequence + count,
//! followed by put/delete records); we wrap it, assign sequences from the
//! batch's own header (so recovered sequences exactly match what was written),
//! and apply it into the MemTable.
//!
//! Truncated-tail policy: the log_reader already treats a partial/corrupt tail
//! at the physical end of the file as a clean EOF (returns null).  A
//! `error.Corruption` it does surface for a tail fragment is swallowed here so
//! that the committed prefix is always recovered without propagating a
//! tail-corruption error — matching LevelDB's default non-paranoid behavior.

const std = @import("std");

const env = @import("../env/env.zig");
const log_reader = @import("../format/log_reader.zig");
const write_batch = @import("../format/write_batch.zig");
const memtable_mod = @import("../memtable/memtable.zig");
const write_path = @import("write_path.zig");
const filename = @import("../version/filename.zig");

const WriteBatch = write_batch.WriteBatch;
const MemTable = memtable_mod.MemTable;

/// Parse a WAL basename of the exact form `NNNNNN.log` (one-or-more decimal
/// digits + the `.log` suffix) into its file number.  Returns null for any
/// other name (CURRENT, MANIFEST-*, LOCK, LOG, *.sst, subdirectories, ...), so
/// the caller can skip non-WAL entries.  Leading zeros are accepted (LevelDB
/// writes 6-digit zero-padded numbers, e.g. `000003.log`).
pub fn parseLogNumber(basename: []const u8) ?u64 {
    const suffix = ".log";
    if (!std.mem.endsWith(u8, basename, suffix)) return null;
    const digits = basename[0 .. basename.len - suffix.len];
    if (digits.len == 0) return null;
    for (digits) |c| if (c < '0' or c > '9') return null;
    return std.fmt.parseInt(u64, digits, 10) catch null;
}

/// Replay EVERY WAL in directory `dbname` whose file number is >= `min_number`,
/// in ASCENDING number order, into `memtable` (leveldb-interop, Wave A).
///
/// LevelDB/RocksDB recovery semantics: the MANIFEST records a `log_number` (the
/// oldest log still needed), but the live data may sit in a HIGHER-numbered log
/// that the MANIFEST never re-pointed at — e.g. an externally-written LevelDB DB
/// whose MANIFEST has log_number=0 while the actual WriteBatch lives in
/// `000003.log`.  Recovering only `logFileName(log_number)` would miss it.  So
/// we list the directory, collect every `NNNNNN.log` with number >= `min_number`
/// (the caller passes `@min(logNumber, prevLogNumber-or-logNumber)`), sort
/// ascending, and replay them in order so sequences are applied oldest-first.
///
/// Returns the highest sequence observed across all replayed logs (floored at
/// `start_sequence`).
pub fn replayAllLogs(
    gpa: std.mem.Allocator,
    e: env.Env,
    dbname: []const u8,
    min_number: u64,
    memtable: *MemTable,
    start_sequence: u64,
) !u64 {
    const entries = e.listDir(gpa, dbname) catch |err| switch (err) {
        // A missing directory has no logs to replay.
        error.NotFound => return start_sequence,
        else => return err,
    };
    defer env.Env.freeListing(gpa, entries);

    // Collect the qualifying log numbers, then sort ascending.
    var numbers: std.ArrayListUnmanaged(u64) = .empty;
    defer numbers.deinit(gpa);
    for (entries) |name| {
        const n = parseLogNumber(name) orelse continue;
        if (n < min_number) continue;
        try numbers.append(gpa, n);
    }
    std.mem.sort(u64, numbers.items, {}, std.sort.asc(u64));

    var max_seq = start_sequence;
    for (numbers.items) |n| {
        const log_path = try filename.logFileName(gpa, dbname, n);
        defer gpa.free(log_path);
        const seq = try replayLog(gpa, e, log_path, memtable, max_seq + 1);
        if (seq > max_seq) max_seq = seq;
    }
    return max_seq;
}

/// Replay `log_path` into `memtable`, applying every committed WriteBatch.
///
/// Returns the highest sequence number observed across all replayed records
/// (`batch.sequence() + batch.count() - 1` for the last non-empty batch), or
/// `start_sequence` if the log is missing or contains no records.
///
/// `start_sequence` is the caller's notion of "the next sequence to assign";
/// it is the floor of the returned value so an empty/missing log is a no-op.
pub fn replayLog(
    gpa: std.mem.Allocator,
    e: env.Env,
    log_path: []const u8,
    memtable: *MemTable,
    start_sequence: u64,
) !u64 {
    // A missing log (never created, or no writes since the MANIFEST) is a
    // no-op: nothing to replay, sequence unchanged.
    if (!e.fileExists(log_path)) return start_sequence;

    var sf = try e.newSequentialFile(gpa, log_path);
    defer sf.close() catch {};

    var reader = log_reader.Reader.init(sf);

    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(gpa);

    // A reusable batch wrapping each record's contents.  We rewrap per record
    // via setContents so the rep buffer is reused across the whole replay.
    var batch = try WriteBatch.init(gpa);
    defer batch.deinit(gpa);

    var max_seq = start_sequence;

    while (true) {
        // Tolerate a corrupt/truncated tail as clean EOF: recover the committed
        // prefix and stop without propagating a tail-corruption error.
        const maybe_record = reader.readRecord(gpa, &scratch) catch |err| switch (err) {
            error.Corruption => break,
            else => return err,
        };
        const record = maybe_record orelse break;

        try batch.setContents(gpa, record);

        const first_sequence = batch.sequence();
        const count = batch.count();
        try write_path.insertBatch(memtable, &batch, first_sequence);

        if (count > 0) {
            const last = first_sequence + count - 1;
            if (last > max_seq) max_seq = last;
        }
    }

    return max_seq;
}

// ===========================================================================
// Tests
// ===========================================================================

test "parseLogNumber accepts NNNNNN.log and rejects everything else" {
    try std.testing.expectEqual(@as(?u64, 3), parseLogNumber("000003.log"));
    try std.testing.expectEqual(@as(?u64, 0), parseLogNumber("000000.log"));
    try std.testing.expectEqual(@as(?u64, 1234567), parseLogNumber("1234567.log"));
    try std.testing.expectEqual(@as(?u64, 7), parseLogNumber("7.log"));
    // Non-WAL names are rejected.
    try std.testing.expectEqual(@as(?u64, null), parseLogNumber("CURRENT"));
    try std.testing.expectEqual(@as(?u64, null), parseLogNumber("MANIFEST-000001"));
    try std.testing.expectEqual(@as(?u64, null), parseLogNumber("000003.sst"));
    try std.testing.expectEqual(@as(?u64, null), parseLogNumber("LOG"));
    try std.testing.expectEqual(@as(?u64, null), parseLogNumber(".log"));
    try std.testing.expectEqual(@as(?u64, null), parseLogNumber("00x3.log"));
    try std.testing.expectEqual(@as(?u64, null), parseLogNumber("000003.log.old"));
}

test "replayAllLogs replays every log >= floor in ascending order" {
    const env_mod = @import("../env/env.zig");
    const log_writer = @import("../format/log_writer.zig");
    const filename_mod = @import("../version/filename.zig");
    const gpa = std.testing.allocator;

    var me = env_mod.MemEnv.init(gpa);
    defer me.deinit();
    const e = me.env();

    // Write three logs: 000001 (below floor), 000003, 000005 — each one batch.
    const Spec = struct { num: u64, seq: u64, key: []const u8, val: []const u8 };
    const specs = [_]Spec{
        .{ .num = 1, .seq = 1, .key = "old", .val = "skipme" },
        .{ .num = 5, .seq = 7, .key = "newer", .val = "five" },
        .{ .num = 3, .seq = 3, .key = "mid", .val = "three" },
    };
    for (specs) |s| {
        var batch = try WriteBatch.init(gpa);
        defer batch.deinit(gpa);
        try batch.put(gpa, s.key, s.val);
        batch.setSequence(s.seq);
        const path = try filename_mod.logFileName(gpa, "rad", s.num);
        defer gpa.free(path);
        var wf = try e.newWritableFile(gpa, path);
        var w = log_writer.Writer.init(wf);
        try w.addRecord(gpa, batch.contents());
        try wf.close();
    }

    const bytewise = @import("../util/comparator.zig").bytewise;
    var mem = try MemTable.init(gpa, bytewise);
    defer mem.deinit();

    // Floor = 3: 000001.log is skipped, 000003 + 000005 replayed.
    const max_seq = try replayAllLogs(gpa, e, "rad", 3, mem, 1);
    try std.testing.expectEqual(@as(u64, 7), max_seq);

    // "old" (from the skipped log) is absent; "mid" and "newer" are present.
    {
        var lk = try memtable_mod.LookupKey.init(gpa, "old", 1000);
        defer lk.deinit(gpa);
        try std.testing.expect(mem.get(lk) == null);
    }
    {
        var lk = try memtable_mod.LookupKey.init(gpa, "mid", 1000);
        defer lk.deinit(gpa);
        const r = mem.get(lk) orelse return error.TestExpectedFound;
        try std.testing.expectEqualStrings("three", r.found);
    }
    {
        var lk = try memtable_mod.LookupKey.init(gpa, "newer", 1000);
        defer lk.deinit(gpa);
        const r = mem.get(lk) orelse return error.TestExpectedFound;
        try std.testing.expectEqualStrings("five", r.found);
    }
}
