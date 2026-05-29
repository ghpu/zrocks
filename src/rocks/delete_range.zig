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
//! `FragmentedRangeTombstoneList` (this file) is the perf read-side aggregator
//! (frag-tombstone milestone): it fragments a possibly-overlapping set of
//! tombstones into a sorted vector of NON-overlapping intervals, each carrying
//! the descending list of seqs of the tombstones covering it.  A read then
//! binary-searches for the fragment containing `user_key` (O(log n)) instead of
//! re-scanning every tombstone (O(n)).  `covered`/`maxCoveringSeq` carry the
//! same half-open / shadowing / visibility semantics as the linear list.

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

/// A single non-overlapping fragment of the tombstone landscape: the half-open
/// user-key interval `[start, end)` plus the seqs (DESCENDING) of every original
/// tombstone that covers this whole interval.
pub const Fragment = struct {
    /// Inclusive lower bound (user key).  Owned (duped) by the
    /// FragmentedRangeTombstoneList so it is self-contained — the source
    /// `RangeTombstoneList` may be freed after construction.
    start: []const u8,
    /// Exclusive upper bound (user key).  Owned like `start`.
    end: []const u8,
    /// Descending seqs of the tombstones covering `[start, end)`.  Index 0 is the
    /// largest (newest) seq.  Owned by the FragmentedRangeTombstoneList.
    seqs: []u64,
};

/// The fragmented, binary-searchable read-side aggregator (frag-tombstone).
///
/// Construction (`fromList`) sweeps all distinct boundary points of the input
/// tombstones in user-key order; between two adjacent boundaries the covering
/// set is constant, yielding one fragment per non-empty gap.  Fragments are
/// stored sorted ascending by `start`, so `covered`/`maxCoveringSeq` binary
/// search for the (unique) fragment whose interval contains the query key.
///
/// Self-contained: the per-fragment `start`/`end` key bytes and `seqs` arrays
/// are all OWNED (allocated here, freed in `deinit`), so the source
/// `RangeTombstoneList` may be freed immediately after `fromList` returns.
pub const FragmentedRangeTombstoneList = struct {
    gpa: std.mem.Allocator,
    fragments: []Fragment,

    /// Build the fragmented view from a (possibly overlapping) tombstone set.
    /// Only tombstones with a non-degenerate `begin < end` (under `user_cmp`)
    /// contribute.  The resulting fragments borrow `src`'s key bytes.
    pub fn fromList(
        gpa: std.mem.Allocator,
        src: *const RangeTombstoneList,
        user_cmp: comparator.Comparator,
    ) !FragmentedRangeTombstoneList {
        // 1. Collect every distinct boundary point (begin/end) of a valid range.
        var points: std.ArrayListUnmanaged([]const u8) = .empty;
        defer points.deinit(gpa);
        for (src.tombstones.items) |t| {
            if (user_cmp.compare(t.begin, t.end) != .lt) continue; // degenerate
            try points.append(gpa, t.begin);
            try points.append(gpa, t.end);
        }
        if (points.items.len == 0) {
            return .{ .gpa = gpa, .fragments = &[_]Fragment{} };
        }

        const Less = struct {
            cmp: comparator.Comparator,
            fn lessThan(ctx: @This(), a: []const u8, b: []const u8) bool {
                return ctx.cmp.compare(a, b) == .lt;
            }
        };
        std.mem.sort([]const u8, points.items, Less{ .cmp = user_cmp }, Less.lessThan);

        // 2. For each adjacent distinct boundary pair [p_i, p_{i+1}) gather the
        //    covering tombstones' seqs (descending).  Skip gaps no tombstone
        //    covers (the covering set is empty between disjoint ranges).
        var frags: std.ArrayListUnmanaged(Fragment) = .empty;
        errdefer {
            for (frags.items) |f| {
                gpa.free(f.start);
                gpa.free(f.end);
                gpa.free(f.seqs);
            }
            frags.deinit(gpa);
        }

        var i: usize = 0;
        while (i + 1 < points.items.len) : (i += 1) {
            const lo = points.items[i];
            const hi = points.items[i + 1];
            if (user_cmp.compare(lo, hi) == .eq) continue; // duplicate boundary

            var seqs: std.ArrayListUnmanaged(u64) = .empty;
            errdefer seqs.deinit(gpa);
            for (src.tombstones.items) |t| {
                if (user_cmp.compare(t.begin, t.end) != .lt) continue;
                // t covers [lo,hi) iff t.begin <= lo and hi <= t.end.
                if (user_cmp.compare(t.begin, lo) == .gt) continue;
                if (user_cmp.compare(hi, t.end) == .gt) continue;
                try seqs.append(gpa, t.seq);
            }
            if (seqs.items.len == 0) {
                seqs.deinit(gpa);
                continue; // a hole — no tombstone here.
            }
            std.mem.sort(u64, seqs.items, {}, std.sort.desc(u64));
            const owned_seqs = try seqs.toOwnedSlice(gpa);
            errdefer gpa.free(owned_seqs);
            // Own the boundary bytes so the source list may be freed afterwards.
            const start = try gpa.dupe(u8, lo);
            errdefer gpa.free(start);
            const end = try gpa.dupe(u8, hi);
            errdefer gpa.free(end);
            try frags.append(gpa, .{ .start = start, .end = end, .seqs = owned_seqs });
        }

        return .{ .gpa = gpa, .fragments = try frags.toOwnedSlice(gpa) };
    }

    pub fn deinit(self: *FragmentedRangeTombstoneList) void {
        for (self.fragments) |f| {
            self.gpa.free(f.start);
            self.gpa.free(f.end);
            self.gpa.free(f.seqs);
        }
        self.gpa.free(self.fragments);
    }

    pub fn isEmpty(self: *const FragmentedRangeTombstoneList) bool {
        return self.fragments.len == 0;
    }

    /// Number of non-overlapping fragments.
    pub fn count(self: *const FragmentedRangeTombstoneList) usize {
        return self.fragments.len;
    }

    /// Binary-search the fragment whose `[start, end)` contains `user_key`, or
    /// null when no fragment covers it.
    fn find(self: *const FragmentedRangeTombstoneList, user_key: []const u8, user_cmp: comparator.Comparator) ?*const Fragment {
        var lo: usize = 0;
        var hi: usize = self.fragments.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const f = &self.fragments[mid];
            if (user_cmp.compare(user_key, f.start) == .lt) {
                hi = mid; // key is left of this fragment.
            } else if (user_cmp.compare(user_key, f.end) != .lt) {
                lo = mid + 1; // key >= end → right of this fragment.
            } else {
                return f; // start <= key < end.
            }
        }
        return null;
    }

    /// THE aggregator query (fragmented equivalent of
    /// `RangeTombstoneList.covered`).  True iff the fragment containing
    /// `user_key` holds a tombstone seq `s` with `value_seq < s <= snapshot`.
    pub fn covered(
        self: *const FragmentedRangeTombstoneList,
        user_key: []const u8,
        value_seq: u64,
        snapshot: u64,
        user_cmp: comparator.Comparator,
    ) bool {
        return self.maxCoveringSeq(user_key, snapshot, user_cmp) > value_seq;
    }

    /// The largest tombstone seq (visible at `snapshot`) covering `user_key`, or
    /// 0 when none.  Fragmented equivalent of `RangeTombstoneList.maxCoveringSeq`.
    pub fn maxCoveringSeq(
        self: *const FragmentedRangeTombstoneList,
        user_key: []const u8,
        snapshot: u64,
        user_cmp: comparator.Comparator,
    ) u64 {
        const f = self.find(user_key, user_cmp) orelse return 0;
        // seqs is descending; the first <= snapshot is the largest visible one.
        for (f.seqs) |s| {
            if (s <= snapshot) return s;
        }
        return 0;
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

// ---------------------------------------------------------------------------
// FragmentedRangeTombstoneList
// ---------------------------------------------------------------------------

test "fragmented: empty list is empty, covers nothing" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();

    var frag = try FragmentedRangeTombstoneList.fromList(gpa, &list, comparator.bytewise);
    defer frag.deinit();
    try testing.expect(frag.isEmpty());
    try testing.expect(!frag.covered("a", 0, 100, comparator.bytewise));
    try testing.expectEqual(@as(u64, 0), frag.maxCoveringSeq("a", 100, comparator.bytewise));
}

test "fragmented: single tombstone, half-open coverage matches linear" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();
    try list.add("b", "d", 10);

    var frag = try FragmentedRangeTombstoneList.fromList(gpa, &list, comparator.bytewise);
    defer frag.deinit();

    try testing.expectEqual(@as(usize, 1), frag.count());
    try testing.expect(!frag.covered("a", 5, 100, comparator.bytewise)); // before begin
    try testing.expect(frag.covered("b", 5, 100, comparator.bytewise)); // == begin
    try testing.expect(frag.covered("c", 5, 100, comparator.bytewise)); // inside
    try testing.expect(!frag.covered("d", 5, 100, comparator.bytewise)); // == end (exclusive)
    try testing.expect(!frag.covered("e", 5, 100, comparator.bytewise)); // after end
}

test "fragmented: value-seq precedence and snapshot visibility" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();
    try list.add("a", "z", 10);

    var frag = try FragmentedRangeTombstoneList.fromList(gpa, &list, comparator.bytewise);
    defer frag.deinit();

    try testing.expect(frag.covered("m", 9, 100, comparator.bytewise)); // 9 < 10
    try testing.expect(!frag.covered("m", 10, 100, comparator.bytewise)); // ==10
    try testing.expect(!frag.covered("m", 11, 100, comparator.bytewise)); // >10
    try testing.expect(!frag.covered("m", 5, 9, comparator.bytewise)); // tomb not visible
    try testing.expect(frag.covered("m", 5, 10, comparator.bytewise)); // visible at 10
}

test "fragmented: overlapping tombstones split into non-overlapping fragments" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();
    // [a,e)@5 and [c,g)@20 overlap on [c,e).  Boundaries: a c e g →
    // fragments [a,c)@{5}, [c,e)@{20,5}, [e,g)@{20}.
    try list.add("a", "e", 5);
    try list.add("c", "g", 20);

    var frag = try FragmentedRangeTombstoneList.fromList(gpa, &list, comparator.bytewise);
    defer frag.deinit();

    try testing.expectEqual(@as(usize, 3), frag.count());

    // In the overlap, the newest covering seq (20) wins.
    try testing.expectEqual(@as(u64, 20), frag.maxCoveringSeq("c", 100, comparator.bytewise));
    try testing.expectEqual(@as(u64, 20), frag.maxCoveringSeq("d", 100, comparator.bytewise));
    // Left-only fragment: only seq 5.
    try testing.expectEqual(@as(u64, 5), frag.maxCoveringSeq("b", 100, comparator.bytewise));
    // Right-only fragment: only seq 20.
    try testing.expectEqual(@as(u64, 20), frag.maxCoveringSeq("f", 100, comparator.bytewise));
    // Outside everything.
    try testing.expectEqual(@as(u64, 0), frag.maxCoveringSeq("z", 100, comparator.bytewise));

    // Snapshot below the newer tombstone in the overlap falls back to the older.
    try testing.expectEqual(@as(u64, 5), frag.maxCoveringSeq("d", 10, comparator.bytewise));
    try testing.expectEqual(@as(u64, 0), frag.maxCoveringSeq("f", 10, comparator.bytewise)); // 20 not visible
}

test "fragmented: disjoint ranges leave a hole (no spurious coverage)" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();
    try list.add("a", "c", 5);
    try list.add("e", "g", 7);

    var frag = try FragmentedRangeTombstoneList.fromList(gpa, &list, comparator.bytewise);
    defer frag.deinit();

    // Only [a,c) and [e,g) become fragments; [c,e) is a hole.
    try testing.expectEqual(@as(usize, 2), frag.count());
    try testing.expect(frag.covered("b", 0, 100, comparator.bytewise));
    try testing.expect(!frag.covered("d", 0, 100, comparator.bytewise)); // in the hole
    try testing.expect(frag.covered("f", 0, 100, comparator.bytewise));
}

test "fragmented: degenerate range (begin >= end) contributes nothing" {
    const gpa = testing.allocator;
    var list = RangeTombstoneList.init(gpa);
    defer list.deinit();
    try list.add("d", "b", 9); // degenerate
    try list.add("b", "d", 10); // valid

    var frag = try FragmentedRangeTombstoneList.fromList(gpa, &list, comparator.bytewise);
    defer frag.deinit();
    try testing.expectEqual(@as(usize, 1), frag.count());
    try testing.expectEqual(@as(u64, 10), frag.maxCoveringSeq("c", 100, comparator.bytewise));
}

test "fragmented: random agreement with linear RangeTombstoneList" {
    const gpa = testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xF7A6_70_B511);
    const rand = prng.random();

    var trial: usize = 0;
    while (trial < 200) : (trial += 1) {
        var list = RangeTombstoneList.init(gpa);
        defer list.deinit();

        const n = rand.uintLessThan(usize, 8);
        var t: usize = 0;
        while (t < n) : (t += 1) {
            var a = rand.uintLessThan(u8, 16);
            var b = rand.uintLessThan(u8, 16);
            if (a > b) {
                const tmp = a;
                a = b;
                b = tmp;
            }
            // Sometimes leave a == b degenerate to exercise the skip path.
            const bb: [1]u8 = .{'a' + a};
            const eb: [1]u8 = .{'a' + b};
            const seq = 1 + rand.uintLessThan(u64, 30);
            try list.add(&bb, &eb, seq);
        }

        var frag = try FragmentedRangeTombstoneList.fromList(gpa, &list, comparator.bytewise);
        defer frag.deinit();

        // Probe every user key and a spread of (value_seq, snapshot) pairs.
        var k: u8 = 0;
        while (k < 18) : (k += 1) {
            const key: [1]u8 = .{'a' + k};
            const snap = rand.uintLessThan(u64, 32);
            const vseq = rand.uintLessThan(u64, 32);
            try testing.expectEqual(
                list.maxCoveringSeq(&key, snap, comparator.bytewise),
                frag.maxCoveringSeq(&key, snap, comparator.bytewise),
            );
            try testing.expectEqual(
                list.covered(&key, vseq, snap, comparator.bytewise),
                frag.covered(&key, vseq, snap, comparator.bytewise),
            );
        }
    }
}
