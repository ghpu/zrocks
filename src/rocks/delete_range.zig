//! delete_range.zig — range tombstones for `deleteRange(begin, end)` (M7.5).
//!
//! A range tombstone deletes every key `k` with `begin <= k < end` (half-open,
//! `end` exclusive) whose value sequence is STRICTLY LESS than the tombstone's
//! sequence.  It is visible to a reader at snapshot `S` iff `tomb.seq <= S`.
//!
//! `RangeTombstone` is the in-memory record (user-key `begin`/`end` + a seq).
//! `RangeTombstoneList` is the aggregator: it owns a duped set of tombstones and
//! answers the core read query `covered(user_key, value_seq, snapshot, cmp)` —
//! true iff some visible tombstone covers the key and shadows the value's seq.
//!
//! Serialization (our own clean format; NOT RocksDB byte-compatible — see the
//! TODO in table_builder/table_reader): a count varint followed by, per
//! tombstone, `lenpfx(begin) ++ lenpfx(end) ++ varint(seq)`.  This is what the
//! "rocksdb.range_del" SST meta block carries.
//!
//! Standalone test note (Zig 0.16): `../...` imports only resolve inside the
//! `src`-rooted module:
//!   printf 'test { _ = @import("rocks/delete_range.zig"); }' > src/_verify.zig \
//!     && zig test src/_verify.zig && rm src/_verify.zig
//!
//! TODO(perf): fragmented tombstone iterator (RocksDB's
//! FragmentedRangeTombstoneList).  We keep whole, unfragmented tombstones and
//! re-scan them linearly per query (correctness-first).

const std = @import("std");

const comparator = @import("../util/comparator.zig");
const coding = @import("../util/coding.zig");

/// One range tombstone over USER keys: deletes `[begin, end)` as of `seq`.
pub const RangeTombstone = struct {
    /// Inclusive lower bound (user key).
    begin: []const u8,
    /// Exclusive upper bound (user key).
    end: []const u8,
    /// The sequence at which the deletion takes effect.  A key/value with
    /// sequence `< seq` covered by `[begin, end)` is deleted; a value with
    /// sequence `>= seq` is NOT (a later write outranks the tombstone).
    seq: u64,
};

/// An owning, append-only set of range tombstones (the read-side aggregator and
/// the flush/compaction carrier).  All `begin`/`end` bytes are duped into the
/// allocator and freed in `deinit`.
pub const RangeTombstoneList = struct {
    gpa: std.mem.Allocator,
    tombstones: std.ArrayListUnmanaged(RangeTombstone) = .empty,

    pub fn init(gpa: std.mem.Allocator) RangeTombstoneList {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *RangeTombstoneList) void {
        for (self.tombstones.items) |t| {
            self.gpa.free(t.begin);
            self.gpa.free(t.end);
        }
        self.tombstones.deinit(self.gpa);
    }

    /// Number of tombstones held.
    pub fn count(self: *const RangeTombstoneList) usize {
        return self.tombstones.items.len;
    }

    pub fn isEmpty(self: *const RangeTombstoneList) bool {
        return self.tombstones.items.len == 0;
    }

    /// Add a tombstone, duping `begin`/`end` into this list's allocator.  A
    /// degenerate range (`begin >= end` under bytewise order) is dropped — it
    /// covers nothing — but we accept any bytes and let `covered` decide via the
    /// supplied comparator, so we only skip the trivially-empty case here.
    pub fn add(self: *RangeTombstoneList, begin: []const u8, end: []const u8, seq: u64) !void {
        const b = try self.gpa.dupe(u8, begin);
        errdefer self.gpa.free(b);
        const e = try self.gpa.dupe(u8, end);
        errdefer self.gpa.free(e);
        try self.tombstones.append(self.gpa, .{ .begin = b, .end = e, .seq = seq });
    }

    /// THE aggregator query.  Returns true iff some held tombstone deletes the
    /// value `(user_key, value_seq)` for a reader at `snapshot`:
    ///   * coverage:   `begin <= user_key < end` (by `user_cmp`), and
    ///   * shadowing:  `value_seq < tomb.seq` (the tombstone outranks the value),
    ///   * visibility: `tomb.seq <= snapshot` (the reader can see the tombstone).
    pub fn covered(
        self: *const RangeTombstoneList,
        user_key: []const u8,
        value_seq: u64,
        snapshot: u64,
        user_cmp: comparator.Comparator,
    ) bool {
        for (self.tombstones.items) |t| {
            if (t.seq > snapshot) continue; // not visible to this reader
            if (value_seq >= t.seq) continue; // value outranks the tombstone
            // coverage: begin <= key < end
            if (user_cmp.compare(user_key, t.begin) == .lt) continue;
            if (user_cmp.compare(user_key, t.end) != .lt) continue; // key >= end
            return true;
        }
        return false;
    }

    /// The largest tombstone sequence (visible at `snapshot`) that covers
    /// `user_key`, or 0 if none.  A surfaced value with sequence `< ` this is
    /// shadowed; one with sequence `>=` it outranks the tombstone.  Used by the
    /// point-get read path (M7.5) to fold all covering tombstones into one
    /// effective deletion sequence.
    pub fn maxCoveringSeq(
        self: *const RangeTombstoneList,
        user_key: []const u8,
        snapshot: u64,
        user_cmp: comparator.Comparator,
    ) u64 {
        var best: u64 = 0;
        for (self.tombstones.items) |t| {
            if (t.seq > snapshot) continue;
            if (t.seq <= best) continue;
            if (user_cmp.compare(user_key, t.begin) == .lt) continue;
            if (user_cmp.compare(user_key, t.end) != .lt) continue; // key >= end
            best = t.seq;
        }
        return best;
    }

    /// Serialize into `out` (our clean range-del meta-block format):
    ///   varint(count) ++ [ lenpfx(begin) lenpfx(end) varint(seq) ]*
    pub fn encode(self: *const RangeTombstoneList, out: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator) !void {
        try coding.putVarint64(out, gpa, @intCast(self.tombstones.items.len));
        for (self.tombstones.items) |t| {
            try coding.putLengthPrefixedSlice(out, gpa, t.begin);
            try coding.putLengthPrefixedSlice(out, gpa, t.end);
            try coding.putVarint64(out, gpa, t.seq);
        }
    }

    /// Parse a range-del meta block (the inverse of `encode`) into a freshly
    /// initialized list (caller `deinit`s).  An empty input yields an empty list.
    pub fn decode(gpa: std.mem.Allocator, bytes: []const u8) !RangeTombstoneList {
        var list = RangeTombstoneList.init(gpa);
        errdefer list.deinit();
        if (bytes.len == 0) return list;

        var input: []const u8 = bytes;
        const n = try coding.getVarint64(&input);
        var i: u64 = 0;
        while (i < n) : (i += 1) {
            const begin = try coding.getLengthPrefixedSlice(&input);
            const end = try coding.getLengthPrefixedSlice(&input);
            const seq = try coding.getVarint64(&input);
            try list.add(begin, end, seq);
        }
        return list;
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;

test "covered: basic half-open coverage (end exclusive)" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();

    // deleteRange("b","d") @ seq=10.
    try list.add("b", "d", 10);

    // A value at seq 5 < 10, snapshot 100.
    try testing.expect(!list.covered("a", 5, 100, comparator.bytewise)); // before begin
    try testing.expect(list.covered("b", 5, 100, comparator.bytewise)); // == begin (inclusive)
    try testing.expect(list.covered("c", 5, 100, comparator.bytewise)); // inside
    try testing.expect(!list.covered("d", 5, 100, comparator.bytewise)); // == end (exclusive)
    try testing.expect(!list.covered("e", 5, 100, comparator.bytewise)); // after end
}

test "covered: value sequence vs tombstone sequence (precedence)" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();

    try list.add("a", "z", 10);

    // value_seq < 10 → covered (hidden).
    try testing.expect(list.covered("m", 9, 100, comparator.bytewise));
    // value_seq == 10 → NOT covered (a write at the tombstone seq is not deleted).
    try testing.expect(!list.covered("m", 10, 100, comparator.bytewise));
    // value_seq > 10 → NOT covered (a later put outranks the tombstone).
    try testing.expect(!list.covered("m", 11, 100, comparator.bytewise));
}

test "covered: snapshot visibility" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();

    try list.add("a", "z", 10);

    // A value at seq 5; at snapshot 9 the tombstone (seq 10) is not yet visible.
    try testing.expect(!list.covered("m", 5, 9, comparator.bytewise));
    // At snapshot 10 it is visible → covered.
    try testing.expect(list.covered("m", 5, 10, comparator.bytewise));
}

test "encode/decode round-trip" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();

    try list.add("b", "d", 10);
    try list.add("foo", "quux", 42);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    defer buf.deinit(gpa);
    try list.encode(&buf, gpa);

    var back = try RangeTombstoneList.decode(gpa, buf.items);
    defer back.deinit();

    try testing.expectEqual(@as(usize, 2), back.count());
    try testing.expectEqualStrings("b", back.tombstones.items[0].begin);
    try testing.expectEqualStrings("d", back.tombstones.items[0].end);
    try testing.expectEqual(@as(u64, 10), back.tombstones.items[0].seq);
    try testing.expectEqualStrings("foo", back.tombstones.items[1].begin);
    try testing.expectEqualStrings("quux", back.tombstones.items[1].end);
    try testing.expectEqual(@as(u64, 42), back.tombstones.items[1].seq);
}

test "decode empty yields empty list" {
    const gpa = testing.allocator;
    var back = try RangeTombstoneList.decode(gpa, "");
    defer back.deinit();
    try testing.expect(back.isEmpty());
}

test "covered: multiple tombstones, newest applicable wins coverage" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();

    try list.add("a", "c", 5);
    try list.add("c", "e", 20);

    // "b" covered by [a,c)@5 — value seq 4 < 5.
    try testing.expect(list.covered("b", 4, 100, comparator.bytewise));
    // "d" covered by [c,e)@20 — value seq 4 < 20.
    try testing.expect(list.covered("d", 4, 100, comparator.bytewise));
    // "d" with value seq 19 still covered (19 < 20).
    try testing.expect(list.covered("d", 19, 100, comparator.bytewise));
    // "d" with value seq 25 NOT covered.
    try testing.expect(!list.covered("d", 25, 100, comparator.bytewise));
}
