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

    /// Handle a DeleteRange record (M7.5): record a range tombstone over
    /// `[begin, end)` in the memtable's tombstone list at the current sequence,
    /// then bump the sequence.  Encoded as `.range_deletion` (key=begin,
    /// value=end), which `MemTable.add` routes into `range_tombstones`.
    pub fn deleteRange(self: *MemTableInserter, begin: []const u8, end: []const u8) !void {
        try self.mem.add(self.sequence, .range_deletion, begin, end);
        self.sequence += 1;
    }
};

/// Insert every record of `batch` into `mem`, assigning sequence numbers
/// starting at `first_sequence` (record i → first_sequence + i).
pub fn insertBatch(mem: *MemTable, batch: *const WriteBatch, first_sequence: u64) !void {
    var inserter = MemTableInserter.init(mem, first_sequence);
    try batch.iterate(&inserter);
}

/// CF-aware WriteBatch handler (M7.0): walks a (possibly CF-tagged) batch,
/// advancing the sequence for EVERY record so cross-CF ordering matches RocksDB
/// (record i of the batch → first_sequence + i, regardless of which CF it
/// targets), but only INSERTS the records whose cf id equals `target_cf` into
/// `mem`.  Records for other CFs still consume a sequence slot (so the shared
/// sequence space stays consistent across families) but are skipped here.
pub const CfMemTableInserter = struct {
    mem: *MemTable,
    target_cf: u32,
    /// Sequence to assign to the NEXT record in the batch (advances for ALL
    /// records, matched or not).
    sequence: u64,

    pub fn init(mem: *MemTable, target_cf: u32, first_sequence: u64) CfMemTableInserter {
        return .{ .mem = mem, .target_cf = target_cf, .sequence = first_sequence };
    }

    pub fn putCF(self: *CfMemTableInserter, cf_id: u32, key: []const u8, value: []const u8) !void {
        if (cf_id == self.target_cf) try self.mem.add(self.sequence, .value, key, value);
        self.sequence += 1;
    }

    pub fn deleteCF(self: *CfMemTableInserter, cf_id: u32, key: []const u8) !void {
        if (cf_id == self.target_cf) try self.mem.add(self.sequence, .deletion, key, "");
        self.sequence += 1;
    }

    pub fn mergeCF(self: *CfMemTableInserter, cf_id: u32, key: []const u8, value: []const u8) !void {
        if (cf_id == self.target_cf) try self.mem.add(self.sequence, .merge, key, value);
        self.sequence += 1;
    }

    pub fn deleteRangeCF(self: *CfMemTableInserter, cf_id: u32, begin: []const u8, end: []const u8) !void {
        if (cf_id == self.target_cf) try self.mem.add(self.sequence, .range_deletion, begin, end);
        self.sequence += 1;
    }
};

/// Insert into `mem` ONLY the records of `batch` that target column family
/// `target_cf`, assigning per-record sequences from the shared space starting at
/// `first_sequence` (record i of the WHOLE batch → first_sequence + i).
pub fn insertBatchForCf(mem: *MemTable, batch: *const WriteBatch, target_cf: u32, first_sequence: u64) !void {
    var inserter = CfMemTableInserter.init(mem, target_cf, first_sequence);
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

test "M7.5 insertBatch records a range tombstone in the memtable's list" {
    const gpa = std.testing.allocator;
    const mem = try MemTable.init(gpa, comparator.bytewise);
    defer mem.deinit();

    var wb = try WriteBatch.init(gpa);
    defer wb.deinit(gpa);
    try wb.put(gpa, "a", "1"); // seq 10
    try wb.deleteRange(gpa, "b", "d"); // seq 11
    try wb.put(gpa, "e", "5"); // seq 12

    try insertBatch(mem, &wb, 10);

    // The tombstone landed in the memtable's range-tombstone list at seq 11.
    try std.testing.expectEqual(@as(usize, 1), mem.range_tombstones.count());
    const t = mem.range_tombstones.tombstones.items[0];
    try std.testing.expectEqualStrings("b", t.begin);
    try std.testing.expectEqualStrings("d", t.end);
    try std.testing.expectEqual(@as(u64, 11), t.seq);

    // Point puts still applied with their own sequences.
    {
        var lk = try LookupKey.init(gpa, "a", 100);
        defer lk.deinit(gpa);
        const r = mem.get(lk) orelse return error.TestExpectedFound;
        try std.testing.expectEqualStrings("1", r.found);
    }
    {
        var lk = try LookupKey.init(gpa, "e", 100);
        defer lk.deinit(gpa);
        const r = mem.get(lk) orelse return error.TestExpectedFound;
        try std.testing.expectEqualStrings("5", r.found);
    }
}
