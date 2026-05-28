//! RealEnv — OS-backed `Env` implementation over `std.Io` (RED stub).

const std = @import("std");
const env_mod = @import("env.zig");

pub const RealEnv = struct {
    io: std.Io,
    root: std.Io.Dir,

    pub fn init(io: std.Io, root: std.Io.Dir) RealEnv {
        return .{ .io = io, .root = root };
    }

    pub fn env(self: *RealEnv) env_mod.Env {
        _ = self;
        @panic("RealEnv not implemented yet");
    }
};
