//! merge_operator.zig — MergeOperator capability (runtime vtable, like
//! Comparator / PrefixExtractor) for read-modify-write "merge" operands.
//!
//! A MergeOperator lets writers record an *operand* (via `DB.merge`) that is
//! combined lazily — on read or during compaction — with the existing value and
//! any other pending operands.  This mirrors RocksDB's `MergeOperator`:
//!
//!   - `fullMerge(key, existing, operands, gpa)`: combine an optional base value
//!     (`existing`, e.g. from a Put) with the ordered `operands` (OLDEST-first)
//!     into one final value.  The caller frees the returned slice.  Returning
//!     null signals failure — callers treat it as "no value" (the existing value
//!     or not-found).
//!   - `partialMerge(key, operands, gpa)`: optionally combine a run of operands
//!     WITHOUT a base into one operand, so a compaction can shrink a long run
//!     even when the base is not in the same input.  Returning null means "not
//!     supported" — the operands are kept verbatim.
//!   - `name()`: a stable identifier.
//!
//! Because an operator may carry runtime configuration (the `ctx` pointer points
//! at stable storage), built-ins are exposed as small structs with an
//! `.operator()` accessor — exactly like `InternalKeyComparator` /
//! `PrefixExtractor`.  The caller MUST keep the struct alive for the lifetime of
//! any `MergeOperator` it hands out.
//!
//! IMPORTANT (import-cycle): this module is imported by `options.zig`, so it MUST
//! NOT import `options.zig`.  It depends only on `std`.

const std = @import("std");

// ---------------------------------------------------------------------------
// MergeOperator — runtime vtable interface (capability pattern)
// ---------------------------------------------------------------------------

pub const MergeOperator = struct {
    ctx: *const anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Combine an optional base value (`existing`) with `operands`
        /// (OLDEST-first) into a final value.  The returned slice is allocated
        /// with `gpa` and OWNED BY THE CALLER (who frees it).  Returning null
        /// signals failure — callers treat it as the existing value / no value.
        fullMerge: *const fn (
            ctx: *const anyopaque,
            key: []const u8,
            existing: ?[]const u8,
            operands: []const []const u8,
            gpa: std.mem.Allocator,
        ) anyerror!?[]u8,

        /// Optionally combine `operands` (OLDEST-first) WITHOUT a base into one
        /// operand.  Returns null if partial merge is not supported (operands
        /// are then kept verbatim).  The returned slice is `gpa`-allocated and
        /// owned by the caller.
        partialMerge: *const fn (
            ctx: *const anyopaque,
            key: []const u8,
            operands: []const []const u8,
            gpa: std.mem.Allocator,
        ) anyerror!?[]u8,

        /// Stable identifier of this operator.
        name: *const fn (ctx: *const anyopaque) []const u8,
    };

    // Thin method wrappers --------------------------------------------------

    pub fn fullMerge(
        self: MergeOperator,
        key: []const u8,
        existing: ?[]const u8,
        operands: []const []const u8,
        gpa: std.mem.Allocator,
    ) anyerror!?[]u8 {
        return self.vtable.fullMerge(self.ctx, key, existing, operands, gpa);
    }

    pub fn partialMerge(
        self: MergeOperator,
        key: []const u8,
        operands: []const []const u8,
        gpa: std.mem.Allocator,
    ) anyerror!?[]u8 {
        return self.vtable.partialMerge(self.ctx, key, operands, gpa);
    }

    pub fn name(self: MergeOperator) []const u8 {
        return self.vtable.name(self.ctx);
    }
};

// ---------------------------------------------------------------------------
// Uint64AddOperator — example operator: values are 8-byte LE u64 counters.
// ---------------------------------------------------------------------------

/// An associative counter operator: every value/operand is an 8-byte
/// little-endian u64.  `fullMerge` sums the base (default 0 when absent) and all
/// operands; `partialMerge` sums the operands.  This is the canonical RocksDB
/// "UInt64AddOperator" used to demonstrate merge correctness.
///
/// Stateless, but exposed via the same stable-struct + `.operator()` pattern as
/// the other capabilities so callers store it next to their `Options`.
pub const Uint64AddOperator = struct {
    pub fn operator(self: *const Uint64AddOperator) MergeOperator {
        return .{ .ctx = self, .vtable = &uint64_add_vtable };
    }
};

/// Decode an 8-byte LE u64; a malformed (non-8-byte) operand counts as 0 so a
/// stray value never corrupts the sum (RocksDB logs + treats as 0 likewise).
fn decodeU64(bytes: []const u8) u64 {
    if (bytes.len != 8) return 0;
    return std.mem.readInt(u64, bytes[0..8], .little);
}

fn allocU64(gpa: std.mem.Allocator, v: u64) ![]u8 {
    const out = try gpa.alloc(u8, 8);
    std.mem.writeInt(u64, out[0..8], v, .little);
    return out;
}

fn uint64AddFullMerge(
    _: *const anyopaque,
    _: []const u8,
    existing: ?[]const u8,
    operands: []const []const u8,
    gpa: std.mem.Allocator,
) anyerror!?[]u8 {
    var sum: u64 = if (existing) |e| decodeU64(e) else 0;
    for (operands) |op| sum +%= decodeU64(op);
    return try allocU64(gpa, sum);
}

fn uint64AddPartialMerge(
    _: *const anyopaque,
    _: []const u8,
    operands: []const []const u8,
    gpa: std.mem.Allocator,
) anyerror!?[]u8 {
    var sum: u64 = 0;
    for (operands) |op| sum +%= decodeU64(op);
    return try allocU64(gpa, sum);
}

fn uint64AddName(_: *const anyopaque) []const u8 {
    return "UInt64AddOperator";
}

const uint64_add_vtable = MergeOperator.VTable{
    .fullMerge = uint64AddFullMerge,
    .partialMerge = uint64AddPartialMerge,
    .name = uint64AddName,
};

/// Encode a u64 as 8 LE bytes into a caller-provided buffer (test helper, also
/// handy for callers building merge operands).
pub fn encodeU64LE(buf: *[8]u8, v: u64) void {
    std.mem.writeInt(u64, buf, v, .little);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

fn u64le(buf: *[8]u8, v: u64) []const u8 {
    std.mem.writeInt(u64, buf, v, .little);
    return buf[0..];
}

test "Uint64AddOperator: fullMerge with base + operands sums all" {
    const gpa = testing.allocator;
    var op = Uint64AddOperator{};
    const mo = op.operator();

    var b0: [8]u8 = undefined;
    var b1: [8]u8 = undefined;
    var b2: [8]u8 = undefined;
    const existing = u64le(&b0, 100);
    const operands = [_][]const u8{ u64le(&b1, 5), u64le(&b2, 3) };

    const out = (try mo.fullMerge("c", existing, &operands, gpa)) orelse return error.TestExpectedValue;
    defer gpa.free(out);
    try testing.expectEqual(@as(u64, 108), decodeU64(out));
}

test "Uint64AddOperator: fullMerge with no base defaults to 0" {
    const gpa = testing.allocator;
    var op = Uint64AddOperator{};
    const mo = op.operator();

    var b1: [8]u8 = undefined;
    var b2: [8]u8 = undefined;
    const operands = [_][]const u8{ u64le(&b1, 5), u64le(&b2, 3) };

    const out = (try mo.fullMerge("c", null, &operands, gpa)) orelse return error.TestExpectedValue;
    defer gpa.free(out);
    try testing.expectEqual(@as(u64, 8), decodeU64(out));
}

test "Uint64AddOperator: partialMerge sums operands without base" {
    const gpa = testing.allocator;
    var op = Uint64AddOperator{};
    const mo = op.operator();

    var b1: [8]u8 = undefined;
    var b2: [8]u8 = undefined;
    var b3: [8]u8 = undefined;
    const operands = [_][]const u8{ u64le(&b1, 1), u64le(&b2, 2), u64le(&b3, 4) };

    const out = (try mo.partialMerge("c", &operands, gpa)) orelse return error.TestExpectedValue;
    defer gpa.free(out);
    try testing.expectEqual(@as(u64, 7), decodeU64(out));
}

test "Uint64AddOperator: fullMerge with empty operands returns the base" {
    const gpa = testing.allocator;
    var op = Uint64AddOperator{};
    const mo = op.operator();

    var b0: [8]u8 = undefined;
    const existing = u64le(&b0, 42);
    const operands = [_][]const u8{};

    const out = (try mo.fullMerge("c", existing, &operands, gpa)) orelse return error.TestExpectedValue;
    defer gpa.free(out);
    try testing.expectEqual(@as(u64, 42), decodeU64(out));
}

test "Uint64AddOperator: name is stable" {
    var op = Uint64AddOperator{};
    try testing.expectEqualStrings("UInt64AddOperator", op.operator().name());
}
