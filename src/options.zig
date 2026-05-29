const std = @import("std");
const comparator = @import("util/comparator.zig");
const prefix = @import("rocks/prefix.zig");
const merge_operator = @import("rocks/merge_operator.zig");
const compaction_filter = @import("rocks/compaction_filter.zig");
const cache_mod = @import("util/cache.zig");

// Re-export so callers can write `options_mod.PrefixExtractor`.
pub const PrefixExtractor = prefix.PrefixExtractor;
// Re-export so callers can write `options_mod.MergeOperator` (M7.1).
pub const MergeOperator = merge_operator.MergeOperator;
// Re-export so callers can write `options_mod.CompactionFilter` / `.Decision` (M7.4).
pub const CompactionFilter = compaction_filter.CompactionFilter;
pub const Decision = compaction_filter.Decision;
// `CompactionStyle` is declared below; nothing to re-export from another module.
/// Re-export Cache so callers can write `options_mod.Cache`.
pub const Cache = cache_mod.Cache;

// ---------------------------------------------------------------------------
// CompactionStyle — which compaction algorithm the DB drives (M7.3)
// ---------------------------------------------------------------------------

/// Selects the compaction algorithm:
///   * `.level`     — classic LevelDB-style leveled compaction (default).
///   * `.universal` — tiered/size-tiered: each L0 file is a sorted "run";
///                    similarly-sized runs are merged together (kept in L0 in
///                    this implementation — see compaction.zig).
///   * `.fifo`      — cache-like: never merges, just DROPS the oldest L0 files
///                    once the total L0 byte budget is exceeded.
pub const CompactionStyle = enum { level, universal, fifo };

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

    /// Optional compaction filter (M7.4).  When set, each SURVIVING, newest
    /// `.value` entry whose sequence is at-or-below the oldest live snapshot is
    /// passed to the filter during compaction, which may keep it, drop it
    /// (`.remove`), or rewrite its value (`.change`).  Entries protected by a
    /// snapshot, merge operands, deletions, and older (hidden) versions are NOT
    /// filtered.  Default null leaves compaction behaviour unchanged.
    compaction_filter: ?compaction_filter.CompactionFilter = null,

    /// Which compaction algorithm the DB runs (M7.3).  Default `.level` keeps the
    /// classic leveled behaviour; `.universal` and `.fifo` switch to alternative
    /// styles whose extra knobs are below.
    compaction_style: CompactionStyle = .level,

    // -- FIFO compaction (M7.3) ---------------------------------------------
    /// Total byte budget for the L0 table files under `.fifo` style.  Once
    /// `totalFileSize(0)` exceeds this, the OLDEST L0 files (lowest file numbers)
    /// are DROPPED whole — cache-like eviction — until back under budget.  Has no
    /// effect under other styles.  Default 1 GiB.
    // TODO: ttl — RocksDB FIFO also supports a time-to-live eviction; only the
    // size-based policy is implemented here.
    fifo_max_table_files_size: u64 = 1 << 30,

    // -- Universal compaction (M7.3) ----------------------------------------
    /// Size-ratio trigger (PERCENT) for `.universal` style.  When extending the
    /// candidate run set from newest to older, the next older run is admitted
    /// while its size is within `(1 + universal_size_ratio/100)` of the running
    /// total of the already-selected runs.  Default 1 (%).
    universal_size_ratio: u32 = 1,
    /// Minimum number of runs that a size-ratio-selected candidate set must reach
    /// before it is compacted under `.universal` style.  Default 2.
    universal_min_merge_width: usize = 2,
    /// Space-amplification trigger (PERCENT) for `.universal` style.  When
    /// `(sum of all runs except the oldest) / (oldest run) * 100` exceeds this,
    /// ALL L0 runs are merged together regardless of size ratios.  Default 200.
    universal_max_size_amplification_percent: u32 = 200,

    // -- Block cache (bench + production) ------------------------------------
    /// Shared LRU block cache for decoded SST data blocks.  When non-null, the
    /// `TableCache` passes it to every `TableReader` it opens, so hot blocks are
    /// served from RAM instead of re-reading + re-decoding from disk on each
    /// lookup.  The caller owns the `Cache` and must ensure its lifetime exceeds
    /// the DB's.  Default null (no block-cache; each lookup re-reads the block).
    block_cache: ?*Cache = null,
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

test "M7.3: compaction style defaults to leveled with documented param defaults" {
    const opts = Options{};
    try std.testing.expectEqual(CompactionStyle.level, opts.compaction_style);
    try std.testing.expectEqual(@as(u64, 1 << 30), opts.fifo_max_table_files_size);
    try std.testing.expectEqual(@as(u32, 1), opts.universal_size_ratio);
    try std.testing.expectEqual(@as(usize, 2), opts.universal_min_merge_width);
    try std.testing.expectEqual(@as(u32, 200), opts.universal_max_size_amplification_percent);
}

test "M7.3: compaction style override works" {
    const opts = Options{ .compaction_style = .fifo, .fifo_max_table_files_size = 4096 };
    try std.testing.expectEqual(CompactionStyle.fifo, opts.compaction_style);
    try std.testing.expectEqual(@as(u64, 4096), opts.fifo_max_table_files_size);
}

test "bench: Options.block_cache defaults to null" {
    const opts = Options{};
    try std.testing.expectEqual(@as(?*Cache, null), opts.block_cache);
}

test "bench: Options.block_cache can be set" {
    var c = Cache.init(std.testing.allocator, 1024 * 1024);
    defer c.deinit();
    const opts = Options{ .block_cache = &c };
    try std.testing.expect(opts.block_cache != null);
    try std.testing.expectEqual(&c, opts.block_cache.?);
}
