//! write_path.zig — apply a WriteBatch into a MemTable, assigning sequences.
//!
//! `MemTableInserter` is a WriteBatch *handler* (it implements `put` / `delete`
//! as required by `WriteBatch.iterate`).  It carries a `*MemTable` and a running
//! `sequence`; each record is inserted at the current sequence which is then
//! bumped, exactly mirroring RocksDB where record i of a batch gets
//! `batch.sequence + i`.

const std = @import("std");

const memtable = @import("../memtable/memtable.zig");
const write_batch = @import("../format/write_batch.zig");
const internal_key = @import("../format/internal_key.zig");

const MemTable = memtable.MemTable;
const WriteBatch = write_batch.WriteBatch;

/// WriteBatch handler that inserts each record into a MemTable, assigning a
/// monotonically increasing sequence number per record.
pub const MemTableInserter = struct {
    mem: *MemTable,
    /// Sequence to assign to the NEXT record (advances after each insert).
    sequence: u64,

    pub fn init(mem: *MemTable, first_sequence: u64) MemTableInserter {
        return .{ .mem = mem, .sequence = first_sequence };
    }

    /// Handle a Put record: insert as a `.value` entry, then bump the sequence.
    pub fn put(self: *MemTableInserter, key: []const u8, value: []const u8) !void {
        try self.mem.add(self.sequence, .value, key, value);
        self.sequence += 1;
    }

    /// Handle a Delete record: insert as a `.deletion` tombstone (empty value),
    /// then bump the sequence.
    pub fn delete(self: *MemTableInserter, key: []const u8) !void {
        try self.mem.add(self.sequence, .deletion, key, "");
        self.sequence += 1;
    }

    /// Handle a Merge record (M7.1): insert as a `.merge` operand entry, then
    /// bump the sequence.  The operand is combined lazily on read/compaction.
    pub fn merge(self: *MemTableInserter, key: []const u8, value: []const u8) !void {
        try self.mem.add(self.sequence, .merge, key, value);
        self.sequence += 1;
    }
};

/// Insert every record of `batch` into `mem`, assigning sequence numbers
/// starting at `first_sequence` (record i → first_sequence + i).
pub fn insertBatch(mem: *MemTable, batch: *const WriteBatch, first_sequence: u64) !void {
    var inserter = MemTableInserter.init(mem, first_sequence);
    try batch.iterate(&inserter);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const comparator = @import("../util/comparator.zig");
const LookupKey = memtable.LookupKey;

test "insertBatch assigns per-record sequences and applies puts/deletes" {
    const gpa = std.testing.allocator;
    const mem = try MemTable.init(gpa, comparator.bytewise);
    defer mem.deinit();

    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);
    try wb.put(gpa, "a", "1"); // seq 10
    try wb.put(gpa, "b", "2"); // seq 11
    try wb.delete(gpa, "a"); // seq 12

    try insertBatch(mem, &wb, 10);

    // "a" was put@10 then deleted@12 → newest visible is a tombstone.
    {
        var lk = try LookupKey.init(gpa, "a", 100);
        defer lk.deinit(gpa);
        const r = mem.get(lk) orelse return error.TestExpectedDeleted;
        try std.testing.expect(r == .deleted);
    }
    // "b" → "2".
    {
        var lk = try LookupKey.init(gpa, "b", 100);
        defer lk.deinit(gpa);
        const r = mem.get(lk) orelse return error.TestExpectedFound;
        try std.testing.expectEqualStrings("2", r.found);
    }
    // At snapshot 11, the delete of "a" (seq 12) is not yet visible → "1".
    {
        var lk = try LookupKey.init(gpa, "a", 11);
        defer lk.deinit(gpa);
        const r = mem.get(lk) orelse return error.TestExpectedFound;
        try std.testing.expectEqualStrings("1", r.found);
    }
}
