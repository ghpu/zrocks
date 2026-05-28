//! memtable.zig — RocksDB/LevelDB-compatible in-memory write buffer.
//!
//! RED phase: declarations with @panic stubs + the full spec test suite.
//! GREEN phase fills in the entry encoding, entry comparator, add/get/iterator.
//!
//! Entry encoding (LevelDB-exact):
//!     entry := varint32(internal_key_size) internal_key
//!              varint32(value_size)        value
//!     internal_key := user_key ++ fixed64_LE((sequence << 8) | value_type)
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

// ---------------------------------------------------------------------------
// LookupKey
// ---------------------------------------------------------------------------

pub const LookupKey = struct {
    buf: []u8,
    ikey_start: usize,

    pub fn init(gpa: std.mem.Allocator, user_key: []const u8, sequence: u64) !LookupKey {
        _ = gpa;
        _ = user_key;
        _ = sequence;
        @panic("LookupKey.init: not implemented (RED)");
    }

    pub fn deinit(self: LookupKey, gpa: std.mem.Allocator) void {
        _ = self;
        _ = gpa;
        @panic("LookupKey.deinit: not implemented (RED)");
    }

    pub fn memtableKey(self: LookupKey) []const u8 {
        _ = self;
        @panic("LookupKey.memtableKey: not implemented (RED)");
    }

    pub fn internalKey(self: LookupKey) []const u8 {
        _ = self;
        @panic("LookupKey.internalKey: not implemented (RED)");
    }

    pub fn userKey(self: LookupKey) []const u8 {
        _ = self;
        @panic("LookupKey.userKey: not implemented (RED)");
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

    pub fn init(gpa: std.mem.Allocator, user_cmp: comparator.Comparator) !*MemTable {
        _ = gpa;
        _ = user_cmp;
        @panic("MemTable.init: not implemented (RED)");
    }

    pub fn deinit(self: *MemTable) void {
        _ = self;
        @panic("MemTable.deinit: not implemented (RED)");
    }

    pub fn add(
        self: *MemTable,
        sequence: u64,
        t: internal_key.ValueType,
        key: []const u8,
        value: []const u8,
    ) !void {
        _ = self;
        _ = sequence;
        _ = t;
        _ = key;
        _ = value;
        @panic("MemTable.add: not implemented (RED)");
    }

    pub fn get(self: *MemTable, lookup: LookupKey) ?GetResult {
        _ = self;
        _ = lookup;
        @panic("MemTable.get: not implemented (RED)");
    }

    pub fn approximateMemoryUsage(self: *const MemTable) usize {
        _ = self;
        @panic("MemTable.approximateMemoryUsage: not implemented (RED)");
    }

    pub const Iterator = struct {
        inner: skiplist.SkipList.Iterator,

        pub fn init(table: *const MemTable) Iterator {
            _ = table;
            @panic("MemTable.Iterator.init: not implemented (RED)");
        }

        pub fn valid(self: *const Iterator) bool {
            _ = self;
            @panic("MemTable.Iterator.valid: not implemented (RED)");
        }

        pub fn seekToFirst(self: *Iterator) void {
            _ = self;
            @panic("MemTable.Iterator.seekToFirst: not implemented (RED)");
        }

        pub fn seek(self: *Iterator, target_memtable_key: []const u8) void {
            _ = self;
            _ = target_memtable_key;
            @panic("MemTable.Iterator.seek: not implemented (RED)");
        }

        pub fn next(self: *Iterator) void {
            _ = self;
            @panic("MemTable.Iterator.next: not implemented (RED)");
        }

        pub fn internalKey(self: *const Iterator) []const u8 {
            _ = self;
            @panic("MemTable.Iterator.internalKey: not implemented (RED)");
        }

        pub fn value(self: *const Iterator) []const u8 {
            _ = self;
            @panic("MemTable.Iterator.value: not implemented (RED)");
        }
    };
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

    {
        var lk = try LookupKey.init(gpa, "k", 1);
        defer lk.deinit(gpa);
        const r = mt.get(lk) orelse return error.TestExpectedFound;
        try testing.expectEqualStrings("v1", r.found);
    }
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

    {
        var lk = try LookupKey.init(gpa, "k", 100);
        defer lk.deinit(gpa);
        const r = mt.get(lk) orelse return error.TestExpectedDeleted;
        try testing.expect(r == .deleted);
    }
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

    var lk = try LookupKey.init(gpa, "foo", internal_key.kMaxSequenceNumber);
    defer lk.deinit(gpa);

    var it = MemTable.Iterator.init(mt);
    it.seek(lk.memtableKey());
    try testing.expect(it.valid());

    const expected_ik = "foo" ++ trailerBytes(1, .value);
    try testing.expectEqualSlices(u8, expected_ik, it.internalKey());
    try testing.expectEqualStrings("bar", it.value());
}

test "LookupKey: layout — memtableKey, internalKey, userKey" {
    const gpa = testing.allocator;
    var lk = try LookupKey.init(gpa, "foo", 5);
    defer lk.deinit(gpa);

    const expected_ik = "foo" ++ trailerBytes(5, internal_key.kValueTypeForSeek);
    const expected_mt = [_]u8{11} ++ expected_ik;

    try testing.expectEqualSlices(u8, expected_mt, lk.memtableKey());
    try testing.expectEqualSlices(u8, expected_ik, lk.internalKey());
    try testing.expectEqualStrings("foo", lk.userKey());
}
