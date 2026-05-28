//! memtable.zig — RocksDB/LevelDB-compatible in-memory write buffer.
//!
//! A `MemTable` is an arena-backed, skiplist-ordered set of *encoded entries*.
//! Each entry is one contiguous arena buffer laid out exactly as LevelDB:
//!
//!     entry := varint32(internal_key_size) internal_key
//!              varint32(value_size)        value
//!     internal_key := user_key ++ fixed64_LE((sequence << 8) | value_type)
//!
//! where `internal_key_size == user_key.len + 8`.
//!
//! The skiplist orders entries with an "entry comparator": a `Comparator`
//! vtable whose `ctx` points at this MemTable's `InternalKeyComparator`.  Its
//! `compare` reads the length-prefixed internal key out of each entry buffer
//! and delegates to `InternalKeyComparator.compare` (user-key ascending, then
//! trailer DESCENDING — so the newest version of a key sorts first).
//!
//! Standalone test note (Zig 0.16): this file uses `../...` imports that only
//! resolve when compiled as part of the `src`-rooted module.  To run the suite:
//!   printf 'test { _ = @import("memtable/memtable.zig"); }' > src/_verify.zig \
//!     && zig test src/_verify.zig && rm src/_verify.zig

const std = @import("std");

const skiplist = @import("skiplist.zig");
const internal_key = @import("../format/internal_key.zig");
const arena = @import("../util/arena.zig");
const comparator = @import("../util/comparator.zig");
const coding = @import("../util/coding.zig");
const delete_range = @import("../rocks/delete_range.zig");

// ---------------------------------------------------------------------------
// LookupKey
// ---------------------------------------------------------------------------

/// A key for memtable lookups, holding a heap-allocated buffer laid out as:
///
///     memtable_key := varint32(user_key.len + 8) ++ user_key
///                     ++ fixed64_LE((sequence << 8) | kValueTypeForSeek)
///
/// `memtableKey()` is what you seek the skiplist with; `internalKey()` is the
/// `user_key ++ trailer` slice (the length-prefixed payload); `userKey()` is
/// the raw user key.  The kValueTypeForSeek trailer (highest type byte) makes
/// the seek land at/before any same-sequence entry for the user key.
pub const LookupKey = struct {
    /// Owned buffer holding the full memtable_key.
    buf: []u8,
    /// Offset within `buf` where the internal key (user_key ++ trailer) begins
    /// (i.e. just past the varint32 length prefix).
    ikey_start: usize,

    pub fn init(gpa: std.mem.Allocator, user_key: []const u8, sequence: u64) !LookupKey {
        const internal_key_size = user_key.len + 8;
        const prefix_len = coding.varintLength(@intCast(internal_key_size));
        const total = prefix_len + internal_key_size;

        const buf = try gpa.alloc(u8, total);
        errdefer gpa.free(buf);

        var list: std.ArrayListUnmanaged(u8) = .{ .items = buf[0..0], .capacity = buf.len };
        coding.putVarint32(&list, gpa, @intCast(internal_key_size)) catch unreachable;
        const ikey_start = list.items.len;
        list.appendSliceAssumeCapacity(user_key);
        const trailer = internal_key.packSequenceAndType(sequence, internal_key.kValueTypeForSeek);
        var tbuf: [8]u8 = undefined;
        coding.encodeFixed64(&tbuf, trailer);
        list.appendSliceAssumeCapacity(&tbuf);

        std.debug.assert(list.items.len == total);
        return .{ .buf = buf, .ikey_start = ikey_start };
    }

    pub fn deinit(self: LookupKey, gpa: std.mem.Allocator) void {
        gpa.free(self.buf);
    }

    /// The full length-prefixed memtable key (varint ++ internal_key).
    pub fn memtableKey(self: LookupKey) []const u8 {
        return self.buf;
    }

    /// The internal key: user_key ++ fixed64_LE(trailer).
    pub fn internalKey(self: LookupKey) []const u8 {
        return self.buf[self.ikey_start..];
    }

    /// The raw user key.
    pub fn userKey(self: LookupKey) []const u8 {
        return self.buf[self.ikey_start .. self.buf.len - 8];
    }
};

// ---------------------------------------------------------------------------
// MemTable
// ---------------------------------------------------------------------------

pub const GetResult = union(enum) {
    found: []const u8,
    deleted,
};

pub const MemTable = struct {
    arena: arena.Arena,
    ikcmp: internal_key.InternalKeyComparator,
    table: skiplist.SkipList,
    /// Range tombstones recorded via `add(.range_deletion, begin, end)` (M7.5).
    /// These are kept OUT of the skiplist (which orders point entries) and
    /// queried by the read path's aggregator + carried to SSTs on flush.  Owns
    /// its key bytes in the gpa (not the arena) so they survive independently;
    /// freed in `deinit`.
    range_tombstones: delete_range.RangeTombstoneList,

    /// Construct a heap-allocated MemTable.  Heap allocation keeps `arena` and
    /// `ikcmp` at stable addresses for the lifetime of the table, which matters
    /// because the skiplist holds `*Arena` and the entry comparator's `ctx`
    /// points at `&self.ikcmp`.
    pub fn init(gpa: std.mem.Allocator, user_cmp: comparator.Comparator) !*MemTable {
        const self = try gpa.create(MemTable);
        errdefer gpa.destroy(self);

        self.arena = arena.Arena.init(gpa);
        self.ikcmp = .{ .user = user_cmp };
        self.range_tombstones = delete_range.RangeTombstoneList.init(gpa);

        // Entry comparator: ctx = &self.ikcmp (stable). Each "key" handed to it
        // is an encoded entry buffer; it extracts the length-prefixed internal
        // key and compares via the InternalKeyComparator.
        const entry_cmp = comparator.Comparator{
            .ctx = &self.ikcmp,
            .vtable = &entry_cmp_vtable,
        };
        self.table = skiplist.SkipList.init(&self.arena, entry_cmp, 0xC0FFEE);
        return self;
    }

    pub fn deinit(self: *MemTable) void {
        const gpa = self.arena.backing;
        self.range_tombstones.deinit();
        self.arena.deinit();
        gpa.destroy(self);
    }

    /// Encode an entry and insert it into the skiplist.
    /// For a deletion, `value` is typically empty (but any value is accepted).
    ///
    /// The entry is built into a `gpa`-backed scratch buffer and handed to
    /// `SkipList.insert`, which copies it once into the arena (the skiplist
    /// owns the arena copy).  Building straight into the arena would orphan a
    /// duplicate block, so we use a freed scratch buffer instead.
    pub fn add(
        self: *MemTable,
        sequence: u64,
        t: internal_key.ValueType,
        key: []const u8,
        value: []const u8,
    ) !void {
        // A range tombstone (M7.5) is NOT a skiplist point entry: `key` is the
        // begin user key and `value` is the end user key.  Record it in the
        // tombstone list at `sequence` and return.
        if (t == .range_deletion) {
            try self.range_tombstones.add(key, value, sequence);
            return;
        }

        const gpa = self.arena.backing;
        const internal_key_size = key.len + 8;
        const encoded_len = coding.varintLength(@intCast(internal_key_size)) + internal_key_size +
            coding.varintLength(@intCast(value.len)) + value.len;

        const scratch = try gpa.alloc(u8, encoded_len);
        defer gpa.free(scratch);
        var list: std.ArrayListUnmanaged(u8) = .{ .items = scratch[0..0], .capacity = scratch.len };

        // varint32(internal_key_size) ++ user_key ++ fixed64(trailer)
        coding.putVarint32(&list, gpa, @intCast(internal_key_size)) catch unreachable;
        list.appendSliceAssumeCapacity(key);
        const trailer = internal_key.packSequenceAndType(sequence, t);
        var tbuf: [8]u8 = undefined;
        coding.encodeFixed64(&tbuf, trailer);
        list.appendSliceAssumeCapacity(&tbuf);

        // varint32(value_size) ++ value
        coding.putVarint32(&list, gpa, @intCast(value.len)) catch unreachable;
        list.appendSliceAssumeCapacity(value);

        std.debug.assert(list.items.len == encoded_len);

        // SkipList.insert copies the slice into the arena (the lasting copy).
        try self.table.insert(list.items);
    }

    /// Look up the newest version of `lookup.userKey()` visible at the lookup's
    /// snapshot sequence.  Returns:
    ///   - `.{ .found = value }` if the newest visible entry is a value,
    ///   - `.deleted` if it is a tombstone,
    ///   - `null` if there is no entry for the user key at/under the snapshot.
    pub fn get(self: *MemTable, lookup: LookupKey) ?GetResult {
        return self.getWithSeq(lookup, null);
    }

    /// Like `get`, but also writes the sequence of the surfaced entry into
    /// `seq_out` (when non-null) on a `.found`/`.deleted` result.  M7.5 uses the
    /// sequence to decide whether a covering range tombstone (at some seq T)
    /// shadows the value (value_seq < T) or the value outranks it (value_seq >= T).
    pub fn getWithSeq(self: *MemTable, lookup: LookupKey, seq_out: ?*u64) ?GetResult {
        var it = skiplist.SkipList.Iterator.init(&self.table);
        // Seek using the encoded memtable key (length-prefixed internal key).
        // Because entries sort by user key then trailer DESCENDING, the first
        // entry >= the lookup key is the newest version with sequence <= the
        // snapshot sequence (the seek trailer has the max type byte).
        it.seek(lookup.memtableKey());
        if (!it.valid()) return null;

        const entry = it.key();
        var rest: []const u8 = entry;
        const stored_ikey = coding.getLengthPrefixedSlice(&rest) catch return null;

        // Compare the user-key portions with the *user* comparator.
        const stored_uk = internal_key.extractUserKey(stored_ikey);
        if (self.ikcmp.user.compare(stored_uk, lookup.userKey()) != .eq) return null;

        const parsed = internal_key.parseInternalKey(stored_ikey) catch return null;
        if (seq_out) |p| p.* = parsed.sequence;
        switch (parsed.type) {
            .value => {
                const value = coding.getLengthPrefixedSlice(&rest) catch return null;
                return .{ .found = value };
            },
            .deletion, .single_deletion, .range_deletion => return .deleted,
            // Merge operands are NOT resolved by this point-lookup (it returns
            // the single newest entry).  Returning null routes callers with a
            // merge operator to DB.mergeGet, which scans the operand run and
            // applies fullMerge (M7.1); callers without one treat it as absent.
            .merge => return null,
        }
    }

    /// Approximate bytes of memory held by this memtable's arena.
    pub fn approximateMemoryUsage(self: *const MemTable) usize {
        return self.arena.memoryUsage();
    }

    // -----------------------------------------------------------------------
    // Iterator
    // -----------------------------------------------------------------------

    /// Forward cursor over memtable entries in internal-key order (user key
    /// ascending, then sequence descending).  Wraps the skiplist iterator and
    /// decodes the length-prefixed fields of each entry on demand.
    pub const Iterator = struct {
        inner: skiplist.SkipList.Iterator,

        pub fn init(table: *const MemTable) Iterator {
            return .{ .inner = skiplist.SkipList.Iterator.init(&table.table) };
        }

        pub fn valid(self: *const Iterator) bool {
            return self.inner.valid();
        }

        pub fn seekToFirst(self: *Iterator) void {
            self.inner.seekToFirst();
        }

        /// Seek to the first entry whose internal key is >= `target`, where
        /// `target` is an encoded *memtable key* (varint32 ++ internal_key),
        /// e.g. `LookupKey.memtableKey()`.
        pub fn seek(self: *Iterator, target_memtable_key: []const u8) void {
            self.inner.seek(target_memtable_key);
        }

        pub fn next(self: *Iterator) void {
            self.inner.next();
        }

        /// The internal key (user_key ++ trailer) of the current entry.
        pub fn internalKey(self: *const Iterator) []const u8 {
            var rest: []const u8 = self.inner.key();
            return coding.getLengthPrefixedSlice(&rest) catch unreachable;
        }

        /// The value bytes of the current entry.
        pub fn value(self: *const Iterator) []const u8 {
            var rest: []const u8 = self.inner.key();
            _ = coding.getLengthPrefixedSlice(&rest) catch unreachable; // skip internal key
            return coding.getLengthPrefixedSlice(&rest) catch unreachable;
        }
    };
};

// ---------------------------------------------------------------------------
// Entry comparator vtable
// ---------------------------------------------------------------------------

/// Compare two encoded entry buffers by their embedded internal keys.
/// `ctx` is a `*const InternalKeyComparator`.
fn entryCompare(ctx: *const anyopaque, a: []const u8, b: []const u8) std.math.Order {
    const ikc: *const internal_key.InternalKeyComparator = @ptrCast(@alignCast(ctx));
    var ra: []const u8 = a;
    var rb: []const u8 = b;
    const ika = coding.getLengthPrefixedSlice(&ra) catch unreachable;
    const ikb = coding.getLengthPrefixedSlice(&rb) catch unreachable;
    return ikc.comparatorInterface().compare(ika, ikb);
}

fn entryName(_: *const anyopaque) []const u8 {
    return "zrocks.MemTableEntryComparator";
}

fn entryFindShortestSeparator(_: *const anyopaque, _: *std.ArrayList(u8), _: []const u8) void {}
fn entryFindShortSuccessor(_: *const anyopaque, _: *std.ArrayList(u8)) void {}

const entry_cmp_vtable = comparator.Comparator.VTable{
    .compare = entryCompare,
    .name = entryName,
    .findShortestSeparator = entryFindShortestSeparator,
    .findShortSuccessor = entryFindShortSuccessor,
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;

/// Build a fixed64-LE trailer for assertions.
fn trailerBytes(seq: u64, t: internal_key.ValueType) [8]u8 {
    var b: [8]u8 = undefined;
    coding.encodeFixed64(&b, internal_key.packSequenceAndType(seq, t));
    return b;
}

test "get: basic put then read at later snapshot" {
    const gpa = testing.allocator;
    const mt = try MemTable.init(gpa, comparator.bytewise);
    defer mt.deinit();

    try mt.add(1, .value, "k", "v1");

    var lk = try LookupKey.init(gpa, "k", 100);
    defer lk.deinit(gpa);

    const r = mt.get(lk) orelse return error.TestExpectedFound;
    try testing.expectEqualStrings("v1", r.found);
}

test "get: overwrite — newer sequence wins" {
    const gpa = testing.allocator;
    const mt = try MemTable.init(gpa, comparator.bytewise);
    defer mt.deinit();

    try mt.add(1, .value, "k", "v1");
    try mt.add(2, .value, "k", "v2");

    var lk = try LookupKey.init(gpa, "k", 100);
    defer lk.deinit(gpa);

    const r = mt.get(lk) orelse return error.TestExpectedFound;
    try testing.expectEqualStrings("v2", r.found);
}

test "get: snapshot visibility — older snapshot sees older value" {
    const gpa = testing.allocator;
    const mt = try MemTable.init(gpa, comparator.bytewise);
    defer mt.deinit();

    try mt.add(1, .value, "k", "v1");
    try mt.add(2, .value, "k", "v2");

    // snapshot seq=1 sees only v1 (not v2).
    {
        var lk = try LookupKey.init(gpa, "k", 1);
        defer lk.deinit(gpa);
        const r = mt.get(lk) orelse return error.TestExpectedFound;
        try testing.expectEqualStrings("v1", r.found);
    }
    // snapshot seq=2 sees v2.
    {
        var lk = try LookupKey.init(gpa, "k", 2);
        defer lk.deinit(gpa);
        const r = mt.get(lk) orelse return error.TestExpectedFound;
        try testing.expectEqualStrings("v2", r.found);
    }
}

test "get: delete tombstone with snapshot visibility" {
    const gpa = testing.allocator;
    const mt = try MemTable.init(gpa, comparator.bytewise);
    defer mt.deinit();

    try mt.add(1, .value, "k", "v1");
    try mt.add(2, .value, "k", "v2");
    try mt.add(3, .deletion, "k", "");

    // At snapshot 100 the tombstone is visible.
    {
        var lk = try LookupKey.init(gpa, "k", 100);
        defer lk.deinit(gpa);
        const r = mt.get(lk) orelse return error.TestExpectedDeleted;
        try testing.expect(r == .deleted);
    }
    // At snapshot 2 the delete is NOT visible → still v2.
    {
        var lk = try LookupKey.init(gpa, "k", 2);
        defer lk.deinit(gpa);
        const r = mt.get(lk) orelse return error.TestExpectedFound;
        try testing.expectEqualStrings("v2", r.found);
    }
}

test "get: absent key returns null" {
    const gpa = testing.allocator;
    const mt = try MemTable.init(gpa, comparator.bytewise);
    defer mt.deinit();

    try mt.add(1, .value, "k", "v1");

    var lk = try LookupKey.init(gpa, "missing", 100);
    defer lk.deinit(gpa);

    try testing.expect(mt.get(lk) == null);
}

test "iterator: multiple distinct keys in user-key order with decoded values" {
    const gpa = testing.allocator;
    const mt = try MemTable.init(gpa, comparator.bytewise);
    defer mt.deinit();

    try mt.add(3, .value, "b", "vb");
    try mt.add(1, .value, "a", "va");
    try mt.add(2, .value, "c", "vc");

    var it = MemTable.Iterator.init(mt);
    it.seekToFirst();

    const exp_keys = [_][]const u8{ "a", "b", "c" };
    const exp_vals = [_][]const u8{ "va", "vb", "vc" };
    const exp_seqs = [_]u64{ 1, 3, 2 };

    var i: usize = 0;
    while (it.valid()) : (it.next()) {
        try testing.expect(i < exp_keys.len);
        const ik = it.internalKey();
        const parsed = try internal_key.parseInternalKey(ik);
        try testing.expectEqualStrings(exp_keys[i], parsed.user_key);
        try testing.expectEqual(exp_seqs[i], parsed.sequence);
        try testing.expectEqualStrings(exp_vals[i], it.value());
        i += 1;
    }
    try testing.expectEqual(exp_keys.len, i);
}

test "approximateMemoryUsage grows after adds" {
    const gpa = testing.allocator;
    const mt = try MemTable.init(gpa, comparator.bytewise);
    defer mt.deinit();

    try mt.add(1, .value, "key", "value");
    try testing.expect(mt.approximateMemoryUsage() > 0);
}

test "golden: entry encoding for (seq=1, value, foo, bar)" {
    const gpa = testing.allocator;
    const mt = try MemTable.init(gpa, comparator.bytewise);
    defer mt.deinit();

    try mt.add(1, .value, "foo", "bar");

    // Seek the iterator to the entry and assert the decoded internal key and
    // value byte-for-byte.
    var lk = try LookupKey.init(gpa, "foo", internal_key.kMaxSequenceNumber);
    defer lk.deinit(gpa);

    var it = MemTable.Iterator.init(mt);
    it.seek(lk.memtableKey());
    try testing.expect(it.valid());

    // internal_key == "foo" ++ fixed64_LE((1<<8)|1)
    const expected_ik = "foo" ++ trailerBytes(1, .value);
    try testing.expectEqualSlices(u8, expected_ik, it.internalKey());
    // value == "bar"
    try testing.expectEqualStrings("bar", it.value());
}

test "M7.5: add with range_deletion records a tombstone (key=begin, value=end)" {
    const gpa = testing.allocator;
    const mt = try MemTable.init(gpa, comparator.bytewise);
    defer mt.deinit();

    try mt.add(7, .range_deletion, "b", "d");

    try testing.expectEqual(@as(usize, 1), mt.range_tombstones.count());
    const t = mt.range_tombstones.tombstones.items[0];
    try testing.expectEqualStrings("b", t.begin);
    try testing.expectEqualStrings("d", t.end);
    try testing.expectEqual(@as(u64, 7), t.seq);

    // A range tombstone does NOT create a point entry in the skiplist; an
    // unrelated point put is the only skiplist entry.
    try mt.add(8, .value, "c", "cv");
    {
        var lk = try LookupKey.init(gpa, "c", 100);
        defer lk.deinit(gpa);
        const r = mt.get(lk) orelse return error.TestExpectedFound;
        try testing.expectEqualStrings("cv", r.found);
    }
}

test "LookupKey: layout — memtableKey, internalKey, userKey" {
    const gpa = testing.allocator;
    var lk = try LookupKey.init(gpa, "foo", 5);
    defer lk.deinit(gpa);

    // memtable_key = varint32(3+8=11) ++ "foo" ++ fixed64((5<<8)|kValueTypeForSeek)
    const expected_ik = "foo" ++ trailerBytes(5, internal_key.kValueTypeForSeek);
    const expected_mt = [_]u8{11} ++ expected_ik;

    try testing.expectEqualSlices(u8, expected_mt, lk.memtableKey());
    try testing.expectEqualSlices(u8, expected_ik, lk.internalKey());
    try testing.expectEqualStrings("foo", lk.userKey());
}
