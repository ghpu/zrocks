/// Arena allocator — LevelDB/RocksDB semantics.
///
/// # Memory accounting (`memoryUsage`)
/// We count the **sum of the raw byte lengths of every block** handed out by
/// the backing allocator.  That is:
///   - Normal blocks: each is exactly `kBlockSize` (4096) bytes.
///   - Large blocks:  each is exactly the requested size (rounded up to the
///     backing allocator's alignment; for `u8` slices that is 1).
///
/// We do NOT count any ArrayList bookkeeping overhead; we count only the bytes
/// that are actually available for bump-allocation.
///
/// This mirrors what RocksDB's `arena.cc` tracks in `blocks_memory_`.

const std = @import("std");

pub const kBlockSize: usize = 4096;

// Stub — will be replaced in GREEN phase.
pub const Arena = struct {
    _placeholder: u8 = 0,

    pub fn init(_: std.mem.Allocator) Arena {
        unreachable;
    }
    pub fn deinit(_: *Arena) void {}
    pub fn alloc(_: *Arena, _: usize) ![]u8 {
        return error.NotImplemented;
    }
    pub fn allocAligned(_: *Arena, _: usize, _: usize) ![]u8 {
        return error.NotImplemented;
    }
    pub fn memoryUsage(_: *const Arena) usize {
        return 0;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "many small allocs stay within one block" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    const s1 = try arena.alloc(100);
    const s2 = try arena.alloc(100);

    try std.testing.expectEqual(@as(usize, 100), s1.len);
    try std.testing.expectEqual(@as(usize, 100), s2.len);
    try std.testing.expectEqual(@as(usize, kBlockSize), arena.memoryUsage());
}

test "memory usage grows in block-sized steps" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        _ = try arena.alloc(400);
    }
    try std.testing.expectEqual(@as(usize, kBlockSize), arena.memoryUsage());

    _ = try arena.alloc(400);
    try std.testing.expectEqual(@as(usize, 2 * kBlockSize), arena.memoryUsage());
}

test "large alloc gets its own block" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    const big = 5000;
    const s = try arena.alloc(big);
    try std.testing.expectEqual(@as(usize, big), s.len);
    try std.testing.expectEqual(@as(usize, big), arena.memoryUsage());
}

test "large alloc does not disturb current normal block" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    _ = try arena.alloc(100);
    try std.testing.expectEqual(@as(usize, kBlockSize), arena.memoryUsage());

    _ = try arena.alloc(5000);
    try std.testing.expectEqual(@as(usize, kBlockSize + 5000), arena.memoryUsage());

    _ = try arena.alloc(100);
    try std.testing.expectEqual(@as(usize, kBlockSize + 5000), arena.memoryUsage());
}

test "allocAligned returns correctly aligned pointers" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    _ = try arena.alloc(1);
    _ = try arena.alloc(1);
    _ = try arena.alloc(1);

    const a8 = try arena.allocAligned(32, 8);
    try std.testing.expectEqual(@as(usize, 32), a8.len);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(a8.ptr) % 8);

    const a16 = try arena.allocAligned(64, 16);
    try std.testing.expectEqual(@as(usize, 64), a16.len);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(a16.ptr) % 16);
}

test "allocAligned large with alignment" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    const s = try arena.allocAligned(5000, 16);
    try std.testing.expectEqual(@as(usize, 5000), s.len);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(s.ptr) % 16);
}

test "write pattern then read back — no corruption between slices" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    const s1 = try arena.alloc(64);
    const s2 = try arena.alloc(64);
    const s3 = try arena.alloc(64);

    @memset(s1, 0xAA);
    @memset(s2, 0xBB);
    @memset(s3, 0xCC);

    for (s1) |b| try std.testing.expectEqual(@as(u8, 0xAA), b);
    for (s2) |b| try std.testing.expectEqual(@as(u8, 0xBB), b);
    for (s3) |b| try std.testing.expectEqual(@as(u8, 0xCC), b);
}

test "write pattern large block — no corruption" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    const small = try arena.alloc(100);
    const large = try arena.alloc(5000);
    const small2 = try arena.alloc(100);

    @memset(small, 0x11);
    @memset(large, 0x22);
    @memset(small2, 0x33);

    for (small) |b| try std.testing.expectEqual(@as(u8, 0x11), b);
    for (large) |b| try std.testing.expectEqual(@as(u8, 0x22), b);
    for (small2) |b| try std.testing.expectEqual(@as(u8, 0x33), b);
}

test "deinit with testing.allocator — zero leaks" {
    var arena = Arena.init(std.testing.allocator);
    _ = try arena.alloc(100);
    _ = try arena.alloc(5000);
    _ = try arena.allocAligned(256, 8);
    arena.deinit();
}
