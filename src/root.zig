//! zrocks — pure-Zig reimplementation of the RocksDB storage engine.

const std = @import("std");

/// Library version string.
pub const version: []const u8 = "0.0.0";

// Foundation utilities (Phase 0).
pub const slice = @import("util/slice.zig");
pub const status = @import("util/status.zig");
pub const coding = @import("util/coding.zig");
pub const comparator = @import("util/comparator.zig");
pub const arena = @import("util/arena.zig");
pub const crc32c = @import("util/crc32c.zig");
pub const cache = @import("util/cache.zig");
pub const options = @import("options.zig");

// Environment / filesystem capability (Phase 1).
pub const env = @import("env/env.zig");

// Write durability core (Phase 2).
pub const internal_key = @import("format/internal_key.zig");
pub const log_format = @import("format/log_format.zig");
pub const log_writer = @import("format/log_writer.zig");
pub const log_reader = @import("format/log_reader.zig");
pub const write_batch = @import("format/write_batch.zig");
pub const skiplist = @import("memtable/skiplist.zig");
pub const memtable = @import("memtable/memtable.zig");

// Block-based SST table format (Phase 3).
pub const block = @import("format/block.zig");
pub const bloom = @import("format/bloom.zig");
pub const filter_block = @import("format/filter_block.zig");
pub const footer = @import("format/footer.zig");
pub const table_builder = @import("format/table_builder.zig");
pub const table_reader = @import("format/table_reader.zig");

// Iterators (Phase 4).
pub const iterator = @import("iterator/iterator.zig");
pub const merging_iterator = @import("iterator/merging_iterator.zig");
pub const two_level_iterator = @import("iterator/two_level_iterator.zig");

// DB layer (Phase 4).
pub const db = @import("db/db.zig");
pub const db_iter = @import("db/db_iter.zig");
pub const write_path = @import("db/write_path.zig");
pub const recovery = @import("db/recovery.zig");
pub const flush = @import("db/flush.zig");
pub const compaction = @import("db/compaction.zig");
pub const snapshot = @import("db/snapshot.zig");

// RocksDB extensions (Phase 7).
pub const checkpoint = @import("rocks/checkpoint.zig");
pub const prefix = @import("rocks/prefix.zig");
pub const merge_operator = @import("rocks/merge_operator.zig");
pub const compaction_filter = @import("rocks/compaction_filter.zig");
pub const delete_range = @import("rocks/delete_range.zig");
pub const transaction = @import("rocks/txn/transaction.zig");
pub const optimistic_txn = @import("rocks/txn/optimistic.zig");
pub const pessimistic_txn = @import("rocks/txn/pessimistic.zig");
pub const lock_manager = @import("rocks/txn/lock_manager.zig");

// Version / MANIFEST (Phase 5).
pub const version_edit = @import("version/version_edit.zig");
pub const version_set = @import("version/version_set.zig");
pub const filename = @import("version/filename.zig");
pub const table_cache = @import("version/table_cache.zig");

test "version constant" {
    try std.testing.expectEqualStrings("0.0.0", version);
}

// Discover and run tests from all declarations in this module.
// Future source files added to the module and re-exported here will be
// picked up automatically without any changes to this test block.
test {
    std.testing.refAllDecls(@This());
}
