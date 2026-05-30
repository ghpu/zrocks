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
//!   - `filterMergeOperand(level, key, operand)`: OPTIONAL hook (mirrors
//!     RocksDB `CompactionFilter::FilterMergeOperand`).  Returns `true` to KEEP
//!     the operand, `false` to REMOVE it from the merge run.  When the vtable
//!     hook is null, every operand is kept (so filters written before this hook
//!     existed keep working unchanged).
//!   - `name()`: a stable identifier.
//!   - `destroy(ctx, gpa)`: OPTIONAL teardown hook.  A plain (statically-stored)
//!     filter leaves this null.  A filter produced by a `CompactionFilterFactory`
//!     (whose `ctx` points at gpa-allocated per-compaction state) sets it so the
//!     compaction can release that state when the compaction finishes.
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

        /// OPTIONAL: decide the fate of a single MERGE operand during compaction.
        /// `key` is the USER key, `operand` the operand bytes (transient).
        /// Returns `true` to KEEP the operand, `false` to REMOVE it.  When null,
        /// the compaction keeps every operand (back-compat for filters that only
        /// set `filter`).  Mirrors RocksDB `CompactionFilter::FilterMergeOperand`.
        filterMergeOperand: ?*const fn (
            ctx: *const anyopaque,
            level: usize,
            key: []const u8,
            operand: []const u8,
        ) anyerror!bool = null,

        /// Stable identifier of this filter.
        name: *const fn (ctx: *const anyopaque) []const u8,

        /// OPTIONAL: release any gpa-allocated state behind `ctx`.  Set by a
        /// `CompactionFilterFactory` for the per-compaction filter it produces;
        /// the compaction calls it (with the same `gpa`) when it finishes.  A
        /// statically-stored filter leaves this null.
        destroy: ?*const fn (ctx: *const anyopaque, gpa: std.mem.Allocator) void = null,
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

    /// Decide whether a merge `operand` is kept (`true`) or removed (`false`).
    /// Returns `true` when the vtable has no `filterMergeOperand` hook, so a
    /// filter that does not implement it keeps every operand.
    pub fn filterMergeOperand(
        self: CompactionFilter,
        level: usize,
        key: []const u8,
        operand: []const u8,
    ) anyerror!bool {
        const hook = self.vtable.filterMergeOperand orelse return true;
        return hook(self.ctx, level, key, operand);
    }

    pub fn name(self: CompactionFilter) []const u8 {
        return self.vtable.name(self.ctx);
    }

    /// Release per-compaction state behind `ctx` (no-op when `destroy` is null).
    pub fn deinit(self: CompactionFilter, gpa: std.mem.Allocator) void {
        if (self.vtable.destroy) |d| d(self.ctx, gpa);
    }
};

// ---------------------------------------------------------------------------
// CompactionFilterFactory — produces a fresh CompactionFilter per compaction
// (capability pattern).  Mirrors RocksDB's `CompactionFilterFactory`.
// ---------------------------------------------------------------------------

pub const CompactionFilterFactory = struct {
    ctx: *const anyopaque,
    vtable: *const VTable,

    /// Per-compaction context handed to the factory, mirroring RocksDB's
    /// `CompactionFilter::Context`.
    pub const Context = struct {
        /// The level being compacted (the compaction's input level).
        level: usize,
        /// True when the compaction includes every file of the key range so a
        /// removed key cannot be resurrected from an unread level.
        is_full_compaction: bool,
        /// True when the compaction was triggered manually (RocksDB
        /// `CompactRange`).  False for background/auto compactions.
        is_manual_compaction: bool,
    };

    pub const VTable = struct {
        /// Produce a fresh `CompactionFilter` for one compaction, or null to run
        /// that compaction WITHOUT filtering.  Any per-compaction state the
        /// returned filter needs must be `gpa`-allocated, and the returned filter
        /// must set its vtable `destroy` so the compaction can free it.  The
        /// produced filter is OWNED BY THE COMPACTION for the compaction's
        /// duration: the compaction calls `filter.deinit(gpa)` (which dispatches
        /// to `destroy`) when it finishes.
        createCompactionFilter: *const fn (
            ctx: *const anyopaque,
            context: Context,
            gpa: std.mem.Allocator,
        ) anyerror!?CompactionFilter,

        /// Stable identifier of this factory.
        name: *const fn (ctx: *const anyopaque) []const u8,
    };

    pub fn createCompactionFilter(
        self: CompactionFilterFactory,
        context: Context,
        gpa: std.mem.Allocator,
    ) anyerror!?CompactionFilter {
        return self.vtable.createCompactionFilter(self.ctx, context, gpa);
    }

    pub fn name(self: CompactionFilterFactory) []const u8 {
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
// RemoveOperandByValueFilter — example: a filter that keeps all plain values but
// REMOVES any merge operand equal to a configured `drop` operand.  Demonstrates
// the optional `filterMergeOperand` hook.
// ---------------------------------------------------------------------------

/// Keeps every plain `.value` (its `filter` always returns `.keep`), but drops
/// any merge operand whose bytes equal `drop`.  The `drop` slice must stay alive
/// for the lifetime of any `CompactionFilter` this hands out.
pub const RemoveOperandByValueFilter = struct {
    drop: []const u8,

    pub fn filter(self: *const RemoveOperandByValueFilter) CompactionFilter {
        return .{ .ctx = self, .vtable = &remove_operand_by_value_vtable };
    }
};

fn removeOperandByValueFilter(
    _: *const anyopaque,
    _: usize,
    _: []const u8,
    _: []const u8,
    _: std.mem.Allocator,
) anyerror!Decision {
    return .keep;
}

fn removeOperandByValueMergeOperand(
    ctx: *const anyopaque,
    _: usize,
    _: []const u8,
    operand: []const u8,
) anyerror!bool {
    const self: *const RemoveOperandByValueFilter = @ptrCast(@alignCast(ctx));
    // KEEP when the operand differs from `drop`; REMOVE when it matches.
    return !std.mem.eql(u8, operand, self.drop);
}

fn removeOperandByValueName(_: *const anyopaque) []const u8 {
    return "RemoveOperandByValueFilter";
}

const remove_operand_by_value_vtable = CompactionFilter.VTable{
    .filter = removeOperandByValueFilter,
    .filterMergeOperand = removeOperandByValueMergeOperand,
    .name = removeOperandByValueName,
};

// ---------------------------------------------------------------------------
// PrefixDropFilterFactory — example factory: produces a per-compaction filter
// that removes any key whose USER key starts with a configured prefix.  The
// produced filter carries gpa-allocated state (so a leak is observable if the
// compaction forgets to release it), torn down via the vtable `destroy` hook.
// ---------------------------------------------------------------------------

/// gpa-allocated per-compaction state behind a filter produced by
/// `PrefixDropFilterFactory`.  Owns a private copy of the prefix.
const PrefixDropFilterState = struct {
    prefix: []u8,
    level: usize,
};

/// A factory that, for each compaction, allocates a fresh `PrefixDropFilterState`
/// (copying the configured `prefix`) and returns a `CompactionFilter` whose `ctx`
/// is that state and whose `destroy` frees it.  The `prefix` slice must stay
/// alive for the lifetime of any factory this hands out.
pub const PrefixDropFilterFactory = struct {
    prefix: []const u8,

    pub fn factory(self: *const PrefixDropFilterFactory) CompactionFilterFactory {
        return .{ .ctx = self, .vtable = &prefix_drop_factory_vtable };
    }
};

fn prefixDropCreate(
    ctx: *const anyopaque,
    context: CompactionFilterFactory.Context,
    gpa: std.mem.Allocator,
) anyerror!?CompactionFilter {
    const self: *const PrefixDropFilterFactory = @ptrCast(@alignCast(ctx));
    const state = try gpa.create(PrefixDropFilterState);
    errdefer gpa.destroy(state);
    state.* = .{ .prefix = try gpa.dupe(u8, self.prefix), .level = context.level };
    return CompactionFilter{ .ctx = state, .vtable = &prefix_drop_filter_vtable };
}

fn prefixDropFactoryName(_: *const anyopaque) []const u8 {
    return "PrefixDropFilterFactory";
}

const prefix_drop_factory_vtable = CompactionFilterFactory.VTable{
    .createCompactionFilter = prefixDropCreate,
    .name = prefixDropFactoryName,
};

fn prefixDropFilter(
    ctx: *const anyopaque,
    _: usize,
    key: []const u8,
    _: []const u8,
    _: std.mem.Allocator,
) anyerror!Decision {
    const state: *const PrefixDropFilterState = @ptrCast(@alignCast(ctx));
    if (std.mem.startsWith(u8, key, state.prefix)) return .remove;
    return .keep;
}

fn prefixDropFilterName(_: *const anyopaque) []const u8 {
    return "PrefixDropFilter";
}

fn prefixDropDestroy(ctx: *const anyopaque, gpa: std.mem.Allocator) void {
    const state: *PrefixDropFilterState = @constCast(@ptrCast(@alignCast(ctx)));
    gpa.free(state.prefix);
    gpa.destroy(state);
}

const prefix_drop_filter_vtable = CompactionFilter.VTable{
    .filter = prefixDropFilter,
    .name = prefixDropFilterName,
    .destroy = prefixDropDestroy,
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

test "filterMergeOperand wrapper defaults to KEEP when the hook is null" {
    // RemoveByValuePrefixFilter sets no operand hook → every operand is kept.
    var f = RemoveByValuePrefixFilter{ .prefix = "DEL" };
    const cf = f.filter();
    try testing.expect(try cf.filterMergeOperand(0, "k", "anything") == true);
}

test "RemoveOperandByValueFilter: drops the matching operand, keeps others/values" {
    const gpa = testing.allocator;
    var f = RemoveOperandByValueFilter{ .drop = "skip" };
    const cf = f.filter();

    // Plain values are always kept (the value filter returns .keep).
    try testing.expect((try cf.filter(0, "k", "skip", gpa)) == .keep);

    // The matching operand is removed; a different operand is kept.
    try testing.expect((try cf.filterMergeOperand(0, "k", "skip")) == false);
    try testing.expect((try cf.filterMergeOperand(0, "k", "take")) == true);
    try testing.expectEqualStrings("RemoveOperandByValueFilter", cf.name());
}

test "PrefixDropFilterFactory: produces a per-compaction filter that is freed" {
    const gpa = testing.allocator;
    var fac = PrefixDropFilterFactory{ .prefix = "tmp_" };
    const factory = fac.factory();
    try testing.expectEqualStrings("PrefixDropFilterFactory", factory.name());

    const cf = (try factory.createCompactionFilter(
        .{ .level = 3, .is_full_compaction = true, .is_manual_compaction = false },
        gpa,
    )) orelse return error.TestExpectedFilter;
    // The produced filter must release its state (testing.allocator catches a
    // leak if `deinit`/`destroy` is wrong).
    defer cf.deinit(gpa);

    try testing.expect((try cf.filter(3, "tmp_x", "v", gpa)) == .remove);
    try testing.expect((try cf.filter(3, "keep", "v", gpa)) == .keep);
    try testing.expectEqualStrings("PrefixDropFilter", cf.name());
}
