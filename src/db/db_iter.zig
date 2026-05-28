//! db_iter.zig — user-facing iterator with snapshot + tombstone semantics.
//!
//! Wraps an INTERNAL iterator (a generic `iterator.Iterator` whose *keys are
//! internal keys* — user_key ++ trailer — and whose *values are user values*)
//! and presents USER keys/values, hiding sequence numbers and tombstones.  This
//! is LevelDB's `DBIter` FORWARD logic.
//!
//! The internal iterator orders entries by user key ascending, then sequence
//! DESCENDING (newest first).  So when scanning forward, the FIRST entry for a
//! given user key is the newest version; if it is a value (and visible at the
//! snapshot) we surface it, then skip all older versions of that same user key;
//! if it is a deletion we hide the key entirely and skip its older versions.
//!
//! Only forward iteration (seekToFirst / seek / next) is implemented for M4.1.
//! Reverse (prev / seekToLast) is intentionally unsupported here (LevelDB's
//! reverse DBIter is significantly more complex and is deferred).

const std = @import("std");

const iterator = @import("../iterator/iterator.zig");
const comparator = @import("../util/comparator.zig");
const internal_key = @import("../format/internal_key.zig");
const coding = @import("../util/coding.zig");

/// User-facing iterator over a single internal iterator at a fixed snapshot.
pub const DBIterator = struct {
    gpa: std.mem.Allocator,
    /// The wrapped internal iterator (keys = internal keys, values = user vals).
    inner: iterator.Iterator,
    /// User comparator (compares user keys).
    user_cmp: comparator.Comparator,
    /// Highest sequence visible to this iterator.
    snapshot: u64,

    /// Whether we are currently positioned at a surfaced user entry.
    is_valid: bool = false,
    /// First error encountered (parse corruption), surfaced via `status`.
    saved_status: ?anyerror = null,

    /// Stable buffer holding the current surfaced USER key (copied out of the
    /// inner iterator so it survives `inner` advancing while we skip).
    saved_key: std.ArrayListUnmanaged(u8) = .empty,
    /// Stable buffer holding the current surfaced VALUE.
    saved_value: std.ArrayListUnmanaged(u8) = .empty,

    /// Optional ownership hook for the heap-allocated context behind `inner`.
    /// When set, `deinit` calls `owned_inner_destroy(gpa, owned_inner)` so a
    /// caller (e.g. `DB.newIterator`) can hand off a heap-allocated adapter and
    /// have the DBIterator free it.  When null, the caller owns `inner`.
    owned_inner: ?*anyopaque = null,
    owned_inner_destroy: ?*const fn (gpa: std.mem.Allocator, ctx: *anyopaque) void = null,

    pub fn init(
        gpa: std.mem.Allocator,
        inner: iterator.Iterator,
        user_cmp: comparator.Comparator,
        snapshot: u64,
    ) DBIterator {
        return .{
            .gpa = gpa,
            .inner = inner,
            .user_cmp = user_cmp,
            .snapshot = snapshot,
        };
    }

    pub fn deinit(self: *DBIterator) void {
        self.saved_key.deinit(self.gpa);
        self.saved_value.deinit(self.gpa);
        if (self.owned_inner) |ctx| {
            if (self.owned_inner_destroy) |destroy| destroy(self.gpa, ctx);
            self.owned_inner = null;
        }
    }

    pub fn valid(self: *const DBIterator) bool {
        return self.is_valid;
    }

    pub fn status(self: *const DBIterator) ?anyerror {
        return self.saved_status;
    }

    /// The current surfaced user key (stable until the next mutating call).
    pub fn key(self: *const DBIterator) []const u8 {
        std.debug.assert(self.is_valid);
        return self.saved_key.items;
    }

    /// The current surfaced value (stable until the next mutating call).
    pub fn value(self: *const DBIterator) []const u8 {
        std.debug.assert(self.is_valid);
        return self.saved_value.items;
    }

    pub fn seekToFirst(self: *DBIterator) void {
        self.inner.seekToFirst();
        self.findNextUserEntry(false) catch |e| self.fail(e);
    }

    /// Seek to the first user key >= `user_target` (visible at the snapshot).
    pub fn seek(self: *DBIterator, user_target: []const u8) void {
        // Build an internal lookup key: user_target ++ trailer(snapshot, seek).
        // Because internal keys sort by trailer DESCENDING, seeking to this
        // lands at/after the newest version of user_target with seq <= snapshot.
        var lookup: std.ArrayListUnmanaged(u8) = .empty;
        defer lookup.deinit(self.gpa);
        lookup.appendSlice(self.gpa, user_target) catch |e| return self.fail(e);
        const trailer = internal_key.packSequenceAndType(self.snapshot, internal_key.kValueTypeForSeek);
        var tbuf: [8]u8 = undefined;
        coding.encodeFixed64(&tbuf, trailer);
        lookup.appendSlice(self.gpa, &tbuf) catch |e| return self.fail(e);

        self.inner.seek(lookup.items);
        self.findNextUserEntry(false) catch |e| self.fail(e);
    }

    /// Advance to the next distinct visible user key.
    pub fn next(self: *DBIterator) void {
        std.debug.assert(self.is_valid);
        // The saved_key already holds the just-returned user key; everything
        // with that user key must be skipped.  Advance once past the current
        // inner entry, then resume the skip-loop with skipping = true.
        self.inner.next();
        self.findNextUserEntry(true) catch |e| self.fail(e);
    }

    // -----------------------------------------------------------------------
    // Core LevelDB DBIter forward loop.
    // -----------------------------------------------------------------------
    //
    // Scan inner entries.  `skipping` true means: hide every entry whose user
    // key is <= `saved_key` (the key we just returned / the key of a tombstone
    // we just saw).  Stop at the first surfaced value.
    fn findNextUserEntry(self: *DBIterator, skipping_init: bool) !void {
        var skipping = skipping_init;
        while (self.inner.valid()) {
            const ikey = try internal_key.parseInternalKey(self.inner.key());
            if (ikey.sequence <= self.snapshot) {
                switch (ikey.type) {
                    .deletion, .single_deletion, .range_deletion => {
                        // Record this user key as the skip target and hide it.
                        try self.saveKey(ikey.user_key);
                        skipping = true;
                    },
                    .value => {
                        if (skipping and
                            self.user_cmp.compare(ikey.user_key, self.saved_key.items) != .gt)
                        {
                            // An older version of an already-handled user key.
                        } else {
                            // Surface this entry.
                            try self.saveKey(ikey.user_key);
                            try self.saveValue(self.inner.value());
                            self.is_valid = true;
                            return;
                        }
                    },
                    .merge => {
                        // Merge operands are not handled at this layer.
                    },
                }
            }
            self.inner.next();
        }
        // Exhausted the inner source.
        self.saved_key.clearRetainingCapacity();
        self.saved_value.clearRetainingCapacity();
        self.is_valid = false;
    }

    fn saveKey(self: *DBIterator, user_key: []const u8) !void {
        self.saved_key.clearRetainingCapacity();
        try self.saved_key.appendSlice(self.gpa, user_key);
    }

    fn saveValue(self: *DBIterator, v: []const u8) !void {
        self.saved_value.clearRetainingCapacity();
        try self.saved_value.appendSlice(self.gpa, v);
    }

    fn fail(self: *DBIterator, e: anyerror) void {
        if (self.saved_status == null) self.saved_status = e;
        self.is_valid = false;
    }

    // -----------------------------------------------------------------------
    // Generic Iterator view.
    // -----------------------------------------------------------------------
    // Reverse methods (prev / seekToLast) are unsupported in M4.1: they record
    // error.NotSupported in status and leave the iterator invalid.

    pub fn iter(self: *DBIterator) iterator.Iterator {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable = iterator.Iterator.VTable{
        .seekToFirst = vSeekToFirst,
        .seekToLast = vSeekToLast,
        .seek = vSeek,
        .next = vNext,
        .prev = vPrev,
        .valid = vValid,
        .key = vKey,
        .value = vValue,
        .status = vStatus,
    };

    fn cast(ctx: *anyopaque) *DBIterator {
        return @ptrCast(@alignCast(ctx));
    }

    fn vSeekToFirst(ctx: *anyopaque) void {
        cast(ctx).seekToFirst();
    }
    fn vSeekToLast(ctx: *anyopaque) void {
        cast(ctx).fail(error.NotSupported);
    }
    fn vSeek(ctx: *anyopaque, target: []const u8) void {
        cast(ctx).seek(target);
    }
    fn vNext(ctx: *anyopaque) void {
        cast(ctx).next();
    }
    fn vPrev(ctx: *anyopaque) void {
        cast(ctx).fail(error.NotSupported);
    }
    fn vValid(ctx: *anyopaque) bool {
        return cast(ctx).valid();
    }
    fn vKey(ctx: *anyopaque) []const u8 {
        return cast(ctx).key();
    }
    fn vValue(ctx: *anyopaque) []const u8 {
        return cast(ctx).value();
    }
    fn vStatus(ctx: *anyopaque) ?anyerror {
        return cast(ctx).status();
    }
};

// ---------------------------------------------------------------------------
// Tests — DBIterator over a VectorIterator of internal entries.
// ---------------------------------------------------------------------------

const VectorIterator = iterator.VectorIterator;

/// Encode an internal key (user_key ++ fixed64 trailer) into a caller buffer.
fn ik(buf: *std.ArrayListUnmanaged(u8), gpa: std.mem.Allocator, user_key: []const u8, seq: u64, t: internal_key.ValueType) ![]const u8 {
    const start = buf.items.len;
    try buf.appendSlice(gpa, user_key);
    var tbuf: [8]u8 = undefined;
    coding.encodeFixed64(&tbuf, internal_key.packSequenceAndType(seq, t));
    try buf.appendSlice(gpa, &tbuf);
    return buf.items[start..];
}

test "DBIterator: latest-only, sorted, tombstones hidden (forward scan)" {
    const gpa = std.testing.allocator;

    // Backing storage for internal keys (kept alive for the iterator's life).
    var store: std.ArrayListUnmanaged(u8) = .empty;
    defer store.deinit(gpa);

    // Build internal entries in internal-key order: user asc, seq DESC.
    //   a: value@2="a2", value@1="a1"
    //   b: value@4="b4"
    //   c: deletion@6, value@5="c5"
    //   d: value@7="d7"
    const k_a2 = try ik(&store, gpa, "a", 2, .value);
    const k_a1 = try ik(&store, gpa, "a", 1, .value);
    const k_b4 = try ik(&store, gpa, "b", 4, .value);
    const k_c6 = try ik(&store, gpa, "c", 6, .deletion);
    const k_c5 = try ik(&store, gpa, "c", 5, .value);
    const k_d7 = try ik(&store, gpa, "d", 7, .value);

    const entries = [_]VectorIterator.Entry{
        .{ .key = k_a2, .value = "a2" },
        .{ .key = k_a1, .value = "a1" },
        .{ .key = k_b4, .value = "b4" },
        .{ .key = k_c6, .value = "" },
        .{ .key = k_c5, .value = "c5" },
        .{ .key = k_d7, .value = "d7" },
    };

    const ikc = internal_key.InternalKeyComparator{ .user = comparator.bytewise };
    var vi = VectorIterator.init(&entries);
    const inner = vi.iterator(ikc.comparatorInterface());

    var dbit = DBIterator.init(gpa, inner, comparator.bytewise, 100);
    defer dbit.deinit();

    // Forward scan should yield: a=a2, b=b4, d=d7 (c is deleted).
    const exp_k = [_][]const u8{ "a", "b", "d" };
    const exp_v = [_][]const u8{ "a2", "b4", "d7" };
    var i: usize = 0;
    dbit.seekToFirst();
    while (dbit.valid()) : (dbit.next()) {
        try std.testing.expect(i < exp_k.len);
        try std.testing.expectEqualStrings(exp_k[i], dbit.key());
        try std.testing.expectEqualStrings(exp_v[i], dbit.value());
        i += 1;
    }
    try std.testing.expectEqual(exp_k.len, i);
}

test "DBIterator: snapshot hides newer versions" {
    const gpa = std.testing.allocator;
    var store: std.ArrayListUnmanaged(u8) = .empty;
    defer store.deinit(gpa);

    const k_k2 = try ik(&store, gpa, "k", 2, .value);
    const k_k1 = try ik(&store, gpa, "k", 1, .value);
    const entries = [_]VectorIterator.Entry{
        .{ .key = k_k2, .value = "v2" },
        .{ .key = k_k1, .value = "v1" },
    };

    const ikc = internal_key.InternalKeyComparator{ .user = comparator.bytewise };
    var vi = VectorIterator.init(&entries);
    const inner = vi.iterator(ikc.comparatorInterface());

    // Snapshot 1 sees only v1.
    var dbit = DBIterator.init(gpa, inner, comparator.bytewise, 1);
    defer dbit.deinit();
    dbit.seekToFirst();
    try std.testing.expect(dbit.valid());
    try std.testing.expectEqualStrings("k", dbit.key());
    try std.testing.expectEqualStrings("v1", dbit.value());
    dbit.next();
    try std.testing.expect(!dbit.valid());
}

test "DBIterator: seek lands at first user key >= target" {
    const gpa = std.testing.allocator;
    var store: std.ArrayListUnmanaged(u8) = .empty;
    defer store.deinit(gpa);

    const k_a = try ik(&store, gpa, "a", 1, .value);
    const k_c = try ik(&store, gpa, "c", 1, .value);
    const k_e = try ik(&store, gpa, "e", 1, .value);
    const entries = [_]VectorIterator.Entry{
        .{ .key = k_a, .value = "va" },
        .{ .key = k_c, .value = "vc" },
        .{ .key = k_e, .value = "ve" },
    };

    const ikc = internal_key.InternalKeyComparator{ .user = comparator.bytewise };
    var vi = VectorIterator.init(&entries);
    const inner = vi.iterator(ikc.comparatorInterface());

    var dbit = DBIterator.init(gpa, inner, comparator.bytewise, 100);
    defer dbit.deinit();

    // seek("c") → lands on c.
    dbit.seek("c");
    try std.testing.expect(dbit.valid());
    try std.testing.expectEqualStrings("c", dbit.key());
    try std.testing.expectEqualStrings("vc", dbit.value());

    // seek("b") → lands on c (first >= b).
    dbit.seek("b");
    try std.testing.expect(dbit.valid());
    try std.testing.expectEqualStrings("c", dbit.key());

    // seek("z") → past end.
    dbit.seek("z");
    try std.testing.expect(!dbit.valid());
}
