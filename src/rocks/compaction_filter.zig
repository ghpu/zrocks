//! compaction_filter.zig — CompactionFilter capability (runtime vtable, like
//! Comparator / MergeOperator / PrefixExtractor) for M7.4.
//!
//! A `CompactionFilter` is a hook invoked on each SURVIVING key during a
//! compaction.  For every snapshot-eligible, newest `.value` entry of a user key
//! the filter returns a `Decision`:
//!   - `.keep`            — emit the entry unchanged.
//!   - `.remove`          — DROP the key during this compaction.  Because the
//!     compaction physically removes it, no tombstone is needed (this matches
//!     RocksDB's CompactionFilter remove semantics for entries at/below the
//!     oldest snapshot).
//!   - `.change: []const u8` — emit the entry with the replacement value.
//!
//! Mirrors RocksDB's `CompactionFilter::FilterV2` (the value variant):
//!   - `filter(level, key, value, gpa)`: `key` is the USER key, `value` the
//!     current value; returns a `Decision`.
//!   - `name()`: a stable identifier.
//!
//! Change-buffer OWNERSHIP CONTRACT
//! --------------------------------
//! When a filter returns `.change`, the carried slice MUST be allocated with the
//! `gpa` passed to `filter`.  The COMPACTION takes ownership of that slice: it
//! writes the replacement value into the output SST and then frees the slice
//! with the same `gpa`.  The filter must NOT retain or free the slice itself.
//! For `.keep` / `.remove` no allocation is made and nothing is freed.
//!
//! Because a filter may carry runtime configuration (the `ctx` pointer points at
//! stable storage), built-ins are exposed as small structs with a `.filter()`
//! accessor — exactly like `InternalKeyComparator` / `MergeOperator` /
//! `PrefixExtractor`.  The caller MUST keep the struct alive for the lifetime of
//! any `CompactionFilter` it hands out (e.g. store it next to its `Options`).
//!
//! IMPORTANT (import-cycle): this module is imported by `options.zig`, so it MUST
//! NOT import `options.zig`.  It depends only on `std`.

const std = @import("std");

// ---------------------------------------------------------------------------
// Decision — the per-key verdict returned by a CompactionFilter
// ---------------------------------------------------------------------------

/// What the compaction should do with a surviving `.value` entry.
///   - `keep`           : emit it unchanged.
///   - `remove`         : drop it (the key disappears after this compaction).
///   - `change`         : emit it with the carried replacement value.  The slice
///     must be allocated with the `gpa` handed to `filter`; the COMPACTION frees
///     it after writing.  See the change-buffer ownership contract above.
pub const Decision = union(enum) {
    keep,
    remove,
    change: []const u8,
};

// ---------------------------------------------------------------------------
// CompactionFilter — runtime vtable interface (capability pattern)
// ---------------------------------------------------------------------------

pub const CompactionFilter = struct {
    ctx: *const anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Decide the fate of a surviving `.value` entry.  `level` is the
        /// compaction's input level (the level being compacted).  `key` is the
        /// USER key, `value` the current value (both transient — copy if you need
        /// them beyond the call).  For `.change`, the returned slice must be
        /// `gpa`-allocated and is OWNED BY THE COMPACTION (which frees it after
        /// writing).  See the change-buffer ownership contract at the top.
        filter: *const fn (
            ctx: *const anyopaque,
            level: usize,
            key: []const u8,
            value: []const u8,
            gpa: std.mem.Allocator,
        ) anyerror!Decision,

        /// Stable identifier of this filter.
        name: *const fn (ctx: *const anyopaque) []const u8,
    };

    // Thin method wrappers --------------------------------------------------

    pub fn filter(
        self: CompactionFilter,
        level: usize,
        key: []const u8,
        value: []const u8,
        gpa: std.mem.Allocator,
    ) anyerror!Decision {
        return self.vtable.filter(self.ctx, level, key, value, gpa);
    }

    pub fn name(self: CompactionFilter) []const u8 {
        return self.vtable.name(self.ctx);
    }
};

// ---------------------------------------------------------------------------
// RemoveByValuePrefixFilter — example: drop keys whose VALUE starts with a
// configured prefix.  Demonstrates a `.remove` decision with runtime config.
// ---------------------------------------------------------------------------

/// Removes (during compaction) any entry whose VALUE begins with `prefix`;
/// everything else is kept.  The `prefix` slice must stay alive for the lifetime
/// of any `CompactionFilter` this hands out (it is captured by reference).
pub const RemoveByValuePrefixFilter = struct {
    prefix: []const u8,

    pub fn filter(self: *const RemoveByValuePrefixFilter) CompactionFilter {
        return .{ .ctx = self, .vtable = &remove_by_value_prefix_vtable };
    }
};

fn removeByValuePrefixFilter(
    ctx: *const anyopaque,
    _: usize,
    _: []const u8,
    value: []const u8,
    _: std.mem.Allocator,
) anyerror!Decision {
    const self: *const RemoveByValuePrefixFilter = @ptrCast(@alignCast(ctx));
    if (std.mem.startsWith(u8, value, self.prefix)) return .remove;
    return .keep;
}

fn removeByValuePrefixName(_: *const anyopaque) []const u8 {
    return "RemoveByValuePrefixFilter";
}

const remove_by_value_prefix_vtable = CompactionFilter.VTable{
    .filter = removeByValuePrefixFilter,
    .name = removeByValuePrefixName,
};

// ---------------------------------------------------------------------------
// AppendSuffixFilter — example: rewrite the VALUE by appending a configured
// suffix.  Demonstrates a `.change` decision (gpa-allocated; compaction frees).
// ---------------------------------------------------------------------------

/// Rewrites every value to `value ++ suffix` (a `.change` decision).  The
/// replacement is allocated with the `gpa` handed to `filter`; the compaction
/// frees it after writing (see the ownership contract).  The `suffix` slice must
/// stay alive for the lifetime of any `CompactionFilter` this hands out.
pub const AppendSuffixFilter = struct {
    suffix: []const u8,

    pub fn filter(self: *const AppendSuffixFilter) CompactionFilter {
        return .{ .ctx = self, .vtable = &append_suffix_vtable };
    }
};

fn appendSuffixFilter(
    ctx: *const anyopaque,
    _: usize,
    _: []const u8,
    value: []const u8,
    gpa: std.mem.Allocator,
) anyerror!Decision {
    const self: *const AppendSuffixFilter = @ptrCast(@alignCast(ctx));
    const out = try gpa.alloc(u8, value.len + self.suffix.len);
    @memcpy(out[0..value.len], value);
    @memcpy(out[value.len..], self.suffix);
    return .{ .change = out };
}

fn appendSuffixName(_: *const anyopaque) []const u8 {
    return "AppendSuffixFilter";
}

const append_suffix_vtable = CompactionFilter.VTable{
    .filter = appendSuffixFilter,
    .name = appendSuffixName,
};

// ---------------------------------------------------------------------------
// Tests — the example filters' decisions (unit level)
// ---------------------------------------------------------------------------

const testing = std.testing;

test "RemoveByValuePrefixFilter: removes matching-value entries, keeps others" {
    const gpa = testing.allocator;
    var f = RemoveByValuePrefixFilter{ .prefix = "DEL" };
    const cf = f.filter();

    // A value starting with the prefix → remove.
    const d_remove = try cf.filter(0, "k1", "DELvalue", gpa);
    try testing.expect(d_remove == .remove);

    // A value NOT starting with the prefix → keep.
    const d_keep = try cf.filter(0, "k2", "keepme", gpa);
    try testing.expect(d_keep == .keep);

    // An empty prefix would match everything — but our example uses "DEL".
    // A value shorter than the prefix cannot start with it → keep.
    const d_short = try cf.filter(0, "k3", "DE", gpa);
    try testing.expect(d_short == .keep);
}

test "RemoveByValuePrefixFilter: name is stable" {
    var f = RemoveByValuePrefixFilter{ .prefix = "DEL" };
    try testing.expectEqualStrings("RemoveByValuePrefixFilter", f.filter().name());
}

test "AppendSuffixFilter: change carries gpa-allocated rewritten value" {
    const gpa = testing.allocator;
    var f = AppendSuffixFilter{ .suffix = "!" };
    const cf = f.filter();

    const d = try cf.filter(0, "k", "hello", gpa);
    // The decision is a change; the carried slice is gpa-allocated and owned by
    // the caller (here we free it, mirroring the compaction's contract).
    switch (d) {
        .change => |v| {
            defer gpa.free(v);
            try testing.expectEqualStrings("hello!", v);
        },
        else => return error.TestExpectedChange,
    }
}

test "AppendSuffixFilter: name is stable" {
    var f = AppendSuffixFilter{ .suffix = "!" };
    try testing.expectEqualStrings("AppendSuffixFilter", f.filter().name());
}
