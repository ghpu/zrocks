//! snapshot.zig — point-in-time read markers + the live SnapshotList (M6.3).
//!
//! A `Snapshot` is a sequence number: reads tagged with a snapshot see only the
//! database state at or before that sequence.  In LevelDB/RocksDB a snapshot
//! also *pins* the sequence so compaction does not drop versions still visible
//! to it; that pinning lives in the `SnapshotList`, an ordered set of the live
//! snapshots whose `oldest()` sequence bounds what compaction may discard.
//!
//! The list is kept in ascending sequence order (snapshots are taken at the
//! current `last_sequence`, which only ever grows, so each `newSnapshot` simply
//! appends to the tail).  `oldest()` is therefore the head's sequence.
//! `release` removes an arbitrary node (a client may release out of order) and
//! frees it; the DB owns the list and frees any leftovers on close.

const std = @import("std");

/// A point-in-time read marker = a sequence number.  Heap-allocated and owned
/// by the `SnapshotList` it was created in; handed back to the client as a
/// stable `*Snapshot` so `release` can identify the exact node.
pub const Snapshot = struct {
    sequence: u64,
    /// Intrusive doubly-linked list links (managed by SnapshotList).
    prev: ?*Snapshot = null,
    next: ?*Snapshot = null,
};

/// An ordered set of the live snapshots, oldest (smallest sequence) first.
///
/// Implemented as an intrusive doubly-linked list with a sentinel-free
/// head/tail; since snapshots are created at a monotonically increasing
/// sequence, `newSnapshot` always appends to the tail and the list stays
/// sorted.  `oldest()` returns the head sequence in O(1).
pub const SnapshotList = struct {
    gpa: std.mem.Allocator,
    head: ?*Snapshot = null,
    tail: ?*Snapshot = null,
    len: usize = 0,

    pub fn init(gpa: std.mem.Allocator) SnapshotList {
        return .{ .gpa = gpa };
    }

    /// Free every remaining snapshot node (defensive cleanup for leftovers the
    /// client never released).
    pub fn deinit(self: *SnapshotList) void {
        var node = self.head;
        while (node) |n| {
            const nxt = n.next;
            self.gpa.destroy(n);
            node = nxt;
        }
        self.* = undefined;
    }

    /// Create + append a new snapshot pinned at `sequence`.  Because sequences
    /// only grow, the new node belongs at the tail and the list stays ordered.
    pub fn newSnapshot(self: *SnapshotList, sequence: u64) !*Snapshot {
        const snap = try self.gpa.create(Snapshot);
        snap.* = .{ .sequence = sequence, .prev = self.tail, .next = null };
        if (self.tail) |t| {
            t.next = snap;
        } else {
            self.head = snap;
        }
        self.tail = snap;
        self.len += 1;
        return snap;
    }

    /// Unlink `snap` from the list and free it.  Safe to call out of order.
    pub fn release(self: *SnapshotList, snap: *Snapshot) void {
        if (snap.prev) |p| {
            p.next = snap.next;
        } else {
            self.head = snap.next;
        }
        if (snap.next) |n| {
            n.prev = snap.prev;
        } else {
            self.tail = snap.prev;
        }
        self.len -= 1;
        self.gpa.destroy(snap);
    }

    /// The smallest live sequence (the head), or null if no snapshots are live.
    pub fn oldest(self: *const SnapshotList) ?u64 {
        return if (self.head) |h| h.sequence else null;
    }

    pub fn isEmpty(self: *const SnapshotList) bool {
        return self.head == null;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "Snapshot holds a sequence" {
    const s = Snapshot{ .sequence = 42 };
    try testing.expectEqual(@as(u64, 42), s.sequence);
}

test "SnapshotList: empty list has no oldest and is empty" {
    var list = SnapshotList.init(testing.allocator);
    defer list.deinit();
    try testing.expect(list.isEmpty());
    try testing.expectEqual(@as(?u64, null), list.oldest());
}

test "SnapshotList: newSnapshot appends and oldest tracks the head" {
    var list = SnapshotList.init(testing.allocator);
    defer list.deinit();

    const a = try list.newSnapshot(10);
    try testing.expect(!list.isEmpty());
    try testing.expectEqual(@as(?u64, 10), list.oldest());
    try testing.expectEqual(@as(u64, 10), a.sequence);

    const b = try list.newSnapshot(20);
    try testing.expectEqual(@as(?u64, 10), list.oldest());
    try testing.expectEqual(@as(u64, 20), b.sequence);

    _ = try list.newSnapshot(30);
    try testing.expectEqual(@as(?u64, 10), list.oldest());
    try testing.expectEqual(@as(usize, 3), list.len);
}

test "SnapshotList: releasing the oldest advances oldest to the next" {
    var list = SnapshotList.init(testing.allocator);
    defer list.deinit();

    const a = try list.newSnapshot(10);
    _ = try list.newSnapshot(20);
    const c = try list.newSnapshot(30);

    list.release(a);
    try testing.expectEqual(@as(?u64, 20), list.oldest());

    list.release(c); // releasing the tail leaves the middle as the only/oldest
    try testing.expectEqual(@as(?u64, 20), list.oldest());
    try testing.expectEqual(@as(usize, 1), list.len);
}

test "SnapshotList: out-of-order release keeps the list consistent" {
    var list = SnapshotList.init(testing.allocator);
    defer list.deinit();

    const a = try list.newSnapshot(5);
    const b = try list.newSnapshot(15);
    const c = try list.newSnapshot(25);

    // Release the middle node first.
    list.release(b);
    try testing.expectEqual(@as(?u64, 5), list.oldest());
    try testing.expectEqual(@as(usize, 2), list.len);

    list.release(a);
    try testing.expectEqual(@as(?u64, 25), list.oldest());

    list.release(c);
    try testing.expect(list.isEmpty());
    try testing.expectEqual(@as(?u64, null), list.oldest());
}

test "SnapshotList: deinit frees leftover snapshots (no leak)" {
    var list = SnapshotList.init(testing.allocator);
    _ = try list.newSnapshot(1);
    _ = try list.newSnapshot(2);
    _ = try list.newSnapshot(3);
    // No explicit release — deinit must free all three (leak detector enforces).
    list.deinit();
}
