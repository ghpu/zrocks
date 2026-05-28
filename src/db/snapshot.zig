//! snapshot.zig — point-in-time read markers.
//!
//! A `Snapshot` is simply a sequence number: reads tagged with a snapshot see
//! only the database state at or before that sequence.  In LevelDB/RocksDB a
//! snapshot also pins the sequence so compaction does not drop versions still
//! visible to it; that pinning (a SnapshotList) is deferred — for the in-memory
//! M4.1 DB (no compaction) a bare sequence value is sufficient.

const std = @import("std");

/// A point-in-time read marker = a sequence number.
pub const Snapshot = struct {
    sequence: u64,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Snapshot holds a sequence" {
    const s = Snapshot{ .sequence = 42 };
    try std.testing.expectEqual(@as(u64, 42), s.sequence);
}
