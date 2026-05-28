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

const WriteBatch = write_batch.WriteBatch;
const MemTable = memtable_mod.MemTable;

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
