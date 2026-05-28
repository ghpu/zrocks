//! zrocks — pure-Zig reimplementation of the RocksDB storage engine.

const std = @import("std");

/// Library version string.
pub const version: []const u8 = "0.0.0";

test "version constant" {
    try std.testing.expectEqualStrings("0.0.0", version);
}

// Discover and run tests from all declarations in this module.
// Future source files added to the module and re-exported here will be
// picked up automatically without any changes to this test block.
test {
    std.testing.refAllDecls(@This());
}
