const std = @import("std");
const comparator = @import("util/comparator.zig");
const prefix = @import("rocks/prefix.zig");
const merge_operator = @import("rocks/merge_operator.zig");

// Re-export so callers can write `options_mod.PrefixExtractor`.
pub const PrefixExtractor = prefix.PrefixExtractor;
// Re-export so callers can write `options_mod.MergeOperator` (M7.1).
pub const MergeOperator = merge_operator.MergeOperator;

// ---------------------------------------------------------------------------
// CompressionType — RocksDB-compatible byte values
// Only `.none` is actually supported for now.
// ---------------------------------------------------------------------------

pub const CompressionType = enum(u8) {
    none = 0x0,
    snappy = 0x1,
    zlib = 0x2,
    bzip2 = 0x3,
    lz4 = 0x4,
    lz4hc = 0x5,
    xpress = 0x6,
    zstd = 0x7,
};

// ---------------------------------------------------------------------------
// Options — DB-open configuration (capability-based plain value struct)
// ---------------------------------------------------------------------------

pub const Options = struct {
    comparator: comparator.Comparator = comparator.bytewise,
    create_if_missing: bool = false,
    error_if_exists: bool = false,
    paranoid_checks: bool = false,
    write_buffer_size: usize = 64 * 1024 * 1024,
    max_open_files: i32 = 1000,
    block_size: usize = 4096,
    block_restart_interval: u32 = 16,
    compression: CompressionType = .none,
    max_file_size: usize = 2 * 1024 * 1024,
    level0_file_num_compaction_trigger: u32 = 4,
    level0_slowdown_writes_trigger: u32 = 20,
    level0_stop_writes_trigger: u32 = 36,
    /// Byte budget for level 1 (the first leveled level); each deeper level's
    /// budget is this times 10^(n-1).  Drives the size-based compaction score.
    max_bytes_for_level_base: u64 = 10 * 1024 * 1024,
    /// Target size of a single compaction-output SSTable; the compaction rolls
    /// over to a fresh output file once the current one reaches this size.
    target_file_size_base: u64 = 2 * 1024 * 1024,

    /// Optional prefix extractor (M7.2).  When set, SST filter blocks are built
    /// over key PREFIXES (instead of whole user keys) so point lookups can prune
    /// files/blocks whose prefix isn't present, and prefix-bounded iteration
    /// becomes available (see `ReadOptions.prefix_same_as_start`).  Default null
    /// keeps the original whole-key bloom behaviour.
    prefix_extractor: ?prefix.PrefixExtractor = null,

    /// Optional merge operator (M7.1).  When set, `DB.merge(key, operand)`
    /// records a read-modify-write operand that is combined lazily — on read,
    /// via the iterator, and during compaction — with the existing value and
    /// any other pending operands for the key (e.g. an additive counter).  When
    /// null, `DB.merge` is a usage error and any merge entry encountered on read
    /// is treated as not-found.
    merge_operator: ?merge_operator.MergeOperator = null,
};

// ---------------------------------------------------------------------------
// ReadOptions
// ---------------------------------------------------------------------------

pub const ReadOptions = struct {
    verify_checksums: bool = false,
    fill_cache: bool = true,
    /// Sequence-number placeholder for snapshot reads.
    // TODO: typed Snapshot in M6
    snapshot: ?u64 = null,
    /// Prefix-bounded scan (M7.2).  When true AND a `prefix_extractor` is
    /// configured, a `DBIterator` positioned with `seek(target)` iterates only
    /// entries whose user-key prefix equals the seek target's prefix; `valid()`
    /// becomes false once the prefix changes.  Has no effect without a prefix
    /// extractor or when scanning via `seekToFirst`.
    prefix_same_as_start: bool = false,
};

// ---------------------------------------------------------------------------
// WriteOptions
// ---------------------------------------------------------------------------

pub const WriteOptions = struct {
    sync: bool = false,
    disable_wal: bool = false,
};

// ---------------------------------------------------------------------------
// Tests
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
