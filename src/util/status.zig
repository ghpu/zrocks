const std = @import("std");

// ---------------------------------------------------------------------------
// Status / error vocabulary for zrocks, mirroring RocksDB's Status::Code.
// Stubs — implementations will follow in the green commit.
// ---------------------------------------------------------------------------

pub const Code = enum {
    ok,
    not_found,
    corruption,
    not_supported,
    invalid_argument,
    io_error,
    merge_in_progress,
    incomplete,
    shutdown_in_progress,
    timed_out,
    aborted,
    busy,
    expired,
    try_again,
};

pub const Error = error{
    NotFound,
    Corruption,
    NotSupported,
    InvalidArgument,
    IOError,
    MergeInProgress,
    Incomplete,
    ShutdownInProgress,
    TimedOut,
    Aborted,
    Busy,
    Expired,
    TryAgain,
};

pub fn codeName(c: Code) []const u8 {
    _ = c;
    @panic("TODO");
}

pub fn toError(c: Code) Error!void {
    _ = c;
    @panic("TODO");
}

pub const Status = struct {
    code: Code,
    msg: ?[]const u8 = null,

    pub fn ok() Status {
        @panic("TODO");
    }

    pub fn err(code: Code, msg: ?[]const u8) Status {
        _ = code;
        _ = msg;
        @panic("TODO");
    }

    pub fn isOk(self: Status) bool {
        _ = self;
        @panic("TODO");
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "codeName: ok" {
    try std.testing.expectEqualStrings("OK", codeName(.ok));
}

test "codeName: not_found" {
    try std.testing.expectEqualStrings("NotFound", codeName(.not_found));
}

test "codeName: corruption" {
    try std.testing.expectEqualStrings("Corruption", codeName(.corruption));
}

test "codeName: not_supported" {
    try std.testing.expectEqualStrings("NotSupported", codeName(.not_supported));
}

test "codeName: invalid_argument" {
    try std.testing.expectEqualStrings("InvalidArgument", codeName(.invalid_argument));
}

test "codeName: io_error" {
    try std.testing.expectEqualStrings("IOError", codeName(.io_error));
}

test "codeName: merge_in_progress" {
    try std.testing.expectEqualStrings("MergeInProgress", codeName(.merge_in_progress));
}

test "codeName: incomplete" {
    try std.testing.expectEqualStrings("Incomplete", codeName(.incomplete));
}

test "codeName: shutdown_in_progress" {
    try std.testing.expectEqualStrings("ShutdownInProgress", codeName(.shutdown_in_progress));
}

test "codeName: timed_out" {
    try std.testing.expectEqualStrings("TimedOut", codeName(.timed_out));
}

test "codeName: aborted" {
    try std.testing.expectEqualStrings("Aborted", codeName(.aborted));
}

test "codeName: busy" {
    try std.testing.expectEqualStrings("Busy", codeName(.busy));
}

test "codeName: expired" {
    try std.testing.expectEqualStrings("Expired", codeName(.expired));
}

test "codeName: try_again" {
    try std.testing.expectEqualStrings("TryAgain", codeName(.try_again));
}

test "toError: ok returns void (no error)" {
    try toError(.ok);
}

test "toError: not_found returns NotFound" {
    try std.testing.expectError(error.NotFound, toError(.not_found));
}

test "toError: corruption returns Corruption" {
    try std.testing.expectError(error.Corruption, toError(.corruption));
}

test "toError: not_supported returns NotSupported" {
    try std.testing.expectError(error.NotSupported, toError(.not_supported));
}

test "toError: invalid_argument returns InvalidArgument" {
    try std.testing.expectError(error.InvalidArgument, toError(.invalid_argument));
}

test "toError: io_error returns IOError" {
    try std.testing.expectError(error.IOError, toError(.io_error));
}

test "toError: merge_in_progress returns MergeInProgress" {
    try std.testing.expectError(error.MergeInProgress, toError(.merge_in_progress));
}

test "toError: incomplete returns Incomplete" {
    try std.testing.expectError(error.Incomplete, toError(.incomplete));
}

test "toError: shutdown_in_progress returns ShutdownInProgress" {
    try std.testing.expectError(error.ShutdownInProgress, toError(.shutdown_in_progress));
}

test "toError: timed_out returns TimedOut" {
    try std.testing.expectError(error.TimedOut, toError(.timed_out));
}

test "toError: aborted returns Aborted" {
    try std.testing.expectError(error.Aborted, toError(.aborted));
}

test "toError: busy returns Busy" {
    try std.testing.expectError(error.Busy, toError(.busy));
}

test "toError: expired returns Expired" {
    try std.testing.expectError(error.Expired, toError(.expired));
}

test "toError: try_again returns TryAgain" {
    try std.testing.expectError(error.TryAgain, toError(.try_again));
}

test "Status: ok helper" {
    const s = Status.ok();
    try std.testing.expect(s.isOk());
    try std.testing.expectEqual(Code.ok, s.code);
    try std.testing.expectEqual(@as(?[]const u8, null), s.msg);
}

test "Status: err helper with message" {
    const s = Status.err(.not_found, "key missing");
    try std.testing.expect(!s.isOk());
    try std.testing.expectEqual(Code.not_found, s.code);
    try std.testing.expectEqualStrings("key missing", s.msg.?);
}

test "Status: err helper without message" {
    const s = Status.err(.io_error, null);
    try std.testing.expect(!s.isOk());
    try std.testing.expectEqual(Code.io_error, s.code);
    try std.testing.expectEqual(@as(?[]const u8, null), s.msg);
}
