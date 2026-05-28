//! prefix.zig — PrefixExtractor capability (runtime vtable, like Comparator).
//!
//! A `PrefixExtractor` maps a USER key to a "prefix" used for prefix-keyed
//! bloom filters and prefix-bounded iteration.  It mirrors RocksDB's
//! `SliceTransform` interface:
//!   - `transform(user_key)` returns the prefix (a sub-slice of `user_key`).
//!   - `inDomain(user_key)` reports whether the extractor applies to this key.
//!   - `name()` is a stable identifier (e.g. "rocksdb.FixedPrefix.3").
//!
//! Two built-ins are provided, exactly as in RocksDB:
//!   - Fixed prefix:  first `n` bytes; inDomain ⇔ key.len >= n.
//!   - Capped prefix: first min(key.len, n) bytes; inDomain is always true.
//!
//! IMPORTANT (import-cycle): this module is imported by `options.zig`, so it
//! MUST NOT import `options.zig`.  It depends only on `std`.
//!
//! RED phase: declarations with @panic stubs + full tests.

const std = @import("std");

// ---------------------------------------------------------------------------
// PrefixExtractor — runtime vtable interface (capability pattern)
// ---------------------------------------------------------------------------

pub const PrefixExtractor = struct {
    ctx: *const anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Map a user key to its prefix (a sub-slice, typically of `user_key`).
        transform: *const fn (ctx: *const anyopaque, user_key: []const u8) []const u8,
        /// Whether the extractor applies to `user_key`.  When false, the key is
        /// NOT added to a prefix filter and prefix-bounded scans treat it as
        /// out-of-domain.
        inDomain: *const fn (ctx: *const anyopaque, user_key: []const u8) bool,
        /// Stable identifier of this extractor.
        name: *const fn (ctx: *const anyopaque) []const u8,
    };

    // Thin method wrappers --------------------------------------------------

    pub fn transform(self: PrefixExtractor, user_key: []const u8) []const u8 {
        return self.vtable.transform(self.ctx, user_key);
    }

    pub fn inDomain(self: PrefixExtractor, user_key: []const u8) bool {
        return self.vtable.inDomain(self.ctx, user_key);
    }

    pub fn name(self: PrefixExtractor) []const u8 {
        return self.vtable.name(self.ctx);
    }
};

// ---------------------------------------------------------------------------
// FixedPrefixExtractor — first `n` bytes (inDomain ⇔ key.len >= n)
// ---------------------------------------------------------------------------

/// A fixed-length prefix extractor.  Holds `n` plus a precomputed name buffer
/// ("rocksdb.FixedPrefix.N"); the caller MUST keep this struct alive for the
/// lifetime of any `PrefixExtractor` it hands out (the ctx points into it),
/// exactly like `InternalKeyComparator`.
pub const FixedPrefixExtractor = struct {
    n: usize,
    /// Precomputed "rocksdb.FixedPrefix.N" (avoids needing an allocator in
    /// `name()`).  32 bytes comfortably fits the literal plus a u64 in decimal.
    name_buf: [32]u8 = undefined,
    name_len: usize = 0,

    pub fn init(n: usize) FixedPrefixExtractor {
        var self = FixedPrefixExtractor{ .n = n };
        const s = std.fmt.bufPrint(&self.name_buf, "rocksdb.FixedPrefix.{d}", .{n}) catch unreachable;
        self.name_len = s.len;
        return self;
    }

    pub fn extractor(self: *const FixedPrefixExtractor) PrefixExtractor {
        return .{ .ctx = self, .vtable = &fixed_vtable };
    }
};

fn fixedTransform(ctx: *const anyopaque, user_key: []const u8) []const u8 {
    const self: *const FixedPrefixExtractor = @ptrCast(@alignCast(ctx));
    // inDomain guarantees key.len >= n at call sites that prune; be defensive
    // here so a stray call returns the whole key rather than slicing OOB.
    if (user_key.len < self.n) return user_key;
    return user_key[0..self.n];
}

fn fixedInDomain(ctx: *const anyopaque, user_key: []const u8) bool {
    const self: *const FixedPrefixExtractor = @ptrCast(@alignCast(ctx));
    return user_key.len >= self.n;
}

fn fixedName(ctx: *const anyopaque) []const u8 {
    const self: *const FixedPrefixExtractor = @ptrCast(@alignCast(ctx));
    return self.name_buf[0..self.name_len];
}

const fixed_vtable = PrefixExtractor.VTable{
    .transform = fixedTransform,
    .inDomain = fixedInDomain,
    .name = fixedName,
};

// ---------------------------------------------------------------------------
// CappedPrefixExtractor — first min(key.len, n) bytes (inDomain always true)
// ---------------------------------------------------------------------------

/// A capped-length prefix extractor.  Same lifetime contract as the fixed one.
pub const CappedPrefixExtractor = struct {
    n: usize,
    name_buf: [32]u8 = undefined,
    name_len: usize = 0,

    pub fn init(n: usize) CappedPrefixExtractor {
        var self = CappedPrefixExtractor{ .n = n };
        const s = std.fmt.bufPrint(&self.name_buf, "rocksdb.CappedPrefix.{d}", .{n}) catch unreachable;
        self.name_len = s.len;
        return self;
    }

    pub fn extractor(self: *const CappedPrefixExtractor) PrefixExtractor {
        return .{ .ctx = self, .vtable = &capped_vtable };
    }
};

fn cappedTransform(ctx: *const anyopaque, user_key: []const u8) []const u8 {
    const self: *const CappedPrefixExtractor = @ptrCast(@alignCast(ctx));
    return user_key[0..@min(user_key.len, self.n)];
}

fn cappedInDomain(ctx: *const anyopaque, user_key: []const u8) bool {
    _ = ctx;
    _ = user_key;
    return true;
}

fn cappedName(ctx: *const anyopaque) []const u8 {
    const self: *const CappedPrefixExtractor = @ptrCast(@alignCast(ctx));
    return self.name_buf[0..self.name_len];
}

const capped_vtable = PrefixExtractor.VTable{
    .transform = cappedTransform,
    .inDomain = cappedInDomain,
    .name = cappedName,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "fixedPrefix: transform takes first n bytes" {
    const fpe = FixedPrefixExtractor.init(3);
    const ex = fpe.extractor();
    try std.testing.expectEqualStrings("abc", ex.transform("abc1"));
    try std.testing.expectEqualStrings("abc", ex.transform("abcdef"));
    try std.testing.expectEqualStrings("xyz", ex.transform("xyz9"));
}

test "fixedPrefix: inDomain requires key.len >= n" {
    const fpe = FixedPrefixExtractor.init(3);
    const ex = fpe.extractor();
    try std.testing.expect(ex.inDomain("abc"));
    try std.testing.expect(ex.inDomain("abcd"));
    try std.testing.expect(!ex.inDomain("ab"));
    try std.testing.expect(!ex.inDomain(""));
}

test "fixedPrefix: name is rocksdb.FixedPrefix.N" {
    const fpe = FixedPrefixExtractor.init(3);
    const ex = fpe.extractor();
    try std.testing.expectEqualStrings("rocksdb.FixedPrefix.3", ex.name());

    const fpe7 = FixedPrefixExtractor.init(7);
    try std.testing.expectEqualStrings("rocksdb.FixedPrefix.7", fpe7.extractor().name());
}

test "cappedPrefix: transform takes min(key.len, n) bytes" {
    const cpe = CappedPrefixExtractor.init(3);
    const ex = cpe.extractor();
    try std.testing.expectEqualStrings("abc", ex.transform("abcdef"));
    try std.testing.expectEqualStrings("ab", ex.transform("ab")); // shorter than n
    try std.testing.expectEqualStrings("", ex.transform("")); // empty
}

test "cappedPrefix: inDomain is always true" {
    const cpe = CappedPrefixExtractor.init(4);
    const ex = cpe.extractor();
    try std.testing.expect(ex.inDomain("abc"));
    try std.testing.expect(ex.inDomain(""));
    try std.testing.expect(ex.inDomain("abcdefgh"));
}

test "cappedPrefix: name is rocksdb.CappedPrefix.N" {
    const cpe = CappedPrefixExtractor.init(4);
    try std.testing.expectEqualStrings("rocksdb.CappedPrefix.4", cpe.extractor().name());
}

test "two distinct keys with same fixed prefix transform equal" {
    const fpe = FixedPrefixExtractor.init(3);
    const ex = fpe.extractor();
    try std.testing.expectEqualStrings(ex.transform("abc1"), ex.transform("abc2"));
}
