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
    _ = gpa;
    _ = e;
    _ = log_path;
    _ = memtable;
    _ = start_sequence;
    @panic("TODO(m5.2): replayLog");
}
