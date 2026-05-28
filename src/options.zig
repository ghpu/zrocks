const std = @import("std");
const comparator = @import("util/comparator.zig");

// ---------------------------------------------------------------------------
// Tests — RED phase: types not yet defined; compilation will fail.
// ---------------------------------------------------------------------------

test "CompressionType byte values are RocksDB-compatible" {
    try std.testing.expectEqual(@as(u8, 0x0), @intFromEnum(CompressionType.none));
    try std.testing.expectEqual(@as(u8, 0x7), @intFromEnum(CompressionType.zstd));
}

test "Options default compression is none" {
    const opts = Options{};
    try std.testing.expectEqual(CompressionType.none, opts.compression);
}

test "Options default comparator is bytewise" {
    const opts = Options{};
    try std.testing.expectEqualStrings("leveldb.BytewiseComparator", opts.comparator.name());
}

test "Options default boolean flags are false" {
    const opts = Options{};
    try std.testing.expectEqual(false, opts.create_if_missing);
    try std.testing.expectEqual(false, opts.error_if_exists);
    try std.testing.expectEqual(false, opts.paranoid_checks);
}

test "Options default numeric fields" {
    const opts = Options{};
    try std.testing.expectEqual(@as(usize, 64 * 1024 * 1024), opts.write_buffer_size);
    try std.testing.expectEqual(@as(i32, 1000), opts.max_open_files);
    try std.testing.expectEqual(@as(usize, 4096), opts.block_size);
    try std.testing.expectEqual(@as(u32, 16), opts.block_restart_interval);
}

test "Options field override works" {
    const opts = Options{ .create_if_missing = true };
    try std.testing.expectEqual(true, opts.create_if_missing);
    try std.testing.expectEqual(false, opts.error_if_exists);
    try std.testing.expectEqual(CompressionType.none, opts.compression);
}

test "ReadOptions defaults" {
    const ro = ReadOptions{};
    try std.testing.expectEqual(false, ro.verify_checksums);
    try std.testing.expectEqual(true, ro.fill_cache);
    try std.testing.expectEqual(@as(?u64, null), ro.snapshot);
}

test "WriteOptions defaults" {
    const wo = WriteOptions{};
    try std.testing.expectEqual(false, wo.sync);
    try std.testing.expectEqual(false, wo.disable_wal);
}

test "ReadOptions field override works" {
    const ro = ReadOptions{ .verify_checksums = true, .snapshot = 42 };
    try std.testing.expectEqual(true, ro.verify_checksums);
    try std.testing.expectEqual(@as(?u64, 42), ro.snapshot);
    try std.testing.expectEqual(true, ro.fill_cache);
}

test "WriteOptions field override works" {
    const wo = WriteOptions{ .sync = true };
    try std.testing.expectEqual(true, wo.sync);
    try std.testing.expectEqual(false, wo.disable_wal);
}
