//! log_writer.zig — WAL/MANIFEST log record writer (legacy LevelDB format).
//!
//! See `log_format.zig` for the byte layout. The writer fragments an opaque
//! payload across 32 KiB blocks and emits one physical record per fragment.

const std = @import("std");
const env = @import("../env/env.zig");
const format = @import("log_format.zig");

pub const Writer = struct {
    file: env.WritableFile,
    /// Current write offset within the active block (0..kBlockSize).
    block_offset: usize = 0,

    pub fn init(file: env.WritableFile) Writer {
        return .{ .file = file, .block_offset = 0 };
    }

    /// Append one logical record. The payload is opaque bytes (may be empty).
    pub fn addRecord(self: *Writer, gpa: std.mem.Allocator, data: []const u8) !void {
        _ = self;
        _ = gpa;
        _ = data;
        return error.Unimplemented;
    }
};
