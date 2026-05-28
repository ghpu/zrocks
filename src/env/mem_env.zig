//! MemEnv — in-memory `Env` implementation (RED stub).

const std = @import("std");
const env_mod = @import("env.zig");

pub const MemEnv = struct {
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator) MemEnv {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *MemEnv) void {
        _ = self;
    }

    pub fn env(self: *MemEnv) env_mod.Env {
        _ = self;
        @panic("MemEnv not implemented yet");
    }
};
