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
pub const options = @import("options.zig");

// Environment / filesystem capability (Phase 1).
pub const env = @import("env/env.zig");

test "version constant" {
    try std.testing.expectEqualStrings("0.0.0", version);
}

// Discover and run tests from all declarations in this module.
// Future source files added to the module and re-exported here will be
// picked up automatically without any changes to this test block.
test {
    std.testing.refAllDecls(@This());
}
