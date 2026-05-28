//! version_set.zig — Version / VersionSet / MANIFEST (LevelDB-style metadata).
//!
//! RED stub: signatures only.  Implemented in the GREEN phase.
const std = @import("std");
const version_edit = @import("version_edit.zig");
const log_writer = @import("../format/log_writer.zig");
const log_reader = @import("../format/log_reader.zig");
const env = @import("../env/env.zig");
const comparator = @import("../util/comparator.zig");
const coding = @import("../util/coding.zig");
const options = @import("../options.zig");
const filename = @import("filename.zig");

pub const kNumLevels = 7;

pub const Version = struct {
    files: [kNumLevels]std.ArrayListUnmanaged(version_edit.FileMetaData),

    pub fn deinit(self: *Version, gpa: std.mem.Allocator) void {
        _ = self;
        _ = gpa;
    }
};

pub const VersionSet = struct {
    pub fn init(
        gpa: std.mem.Allocator,
        e: env.Env,
        dbname: []const u8,
        opts: options.Options,
    ) !VersionSet {
        _ = gpa;
        _ = e;
        _ = dbname;
        _ = opts;
        return error.Unimplemented;
    }

    pub fn deinit(self: *VersionSet) void {
        _ = self;
    }

    pub fn newFileNumber(self: *VersionSet) u64 {
        _ = self;
        return 0;
    }

    pub fn logAndApply(self: *VersionSet, edit: *version_edit.VersionEdit) !void {
        _ = self;
        _ = edit;
        return error.Unimplemented;
    }

    pub fn recover(self: *VersionSet) !void {
        _ = self;
        return error.Unimplemented;
    }

    pub fn currentVersion(self: *const VersionSet) *const Version {
        _ = self;
        unreachable;
    }

    pub fn lastSequence(self: *const VersionSet) u64 {
        _ = self;
        return 0;
    }

    pub fn logNumber(self: *const VersionSet) u64 {
        _ = self;
        return 0;
    }

    pub fn manifestFileNumber(self: *const VersionSet) u64 {
        _ = self;
        return 0;
    }

    pub fn setLastSequence(self: *VersionSet, v: u64) void {
        _ = self;
        _ = v;
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;

/// Build an internal key from a user key + a zero trailer (8 bytes), matching
/// the convention used in version_edit.zig tests.
fn ikey(comptime user: []const u8) []const u8 {
    return user ++ [_]u8{0} ** 8;
}

/// Count files in a level of the current version.
fn levelFileCount(vs: *const VersionSet, level: usize) usize {
    return vs.currentVersion().files[level].items.len;
}

test "init: empty version, next_file_number starts at 2" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    var lvl: usize = 0;
    while (lvl < kNumLevels) : (lvl += 1) {
        try testing.expectEqual(@as(usize, 0), levelFileCount(&vs, lvl));
    }
}

test "newFileNumber is monotonic starting at 2" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    try testing.expectEqual(@as(u64, 2), vs.newFileNumber());
    try testing.expectEqual(@as(u64, 3), vs.newFileNumber());
    try testing.expectEqual(@as(u64, 4), vs.newFileNumber());
}

test "logAndApply: files land in their levels" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    var edit = version_edit.VersionEdit.init();
    defer edit.deinit(gpa);
    try edit.addFile(gpa, 0, 10, 100, ikey("a"), ikey("b"));
    try edit.addFile(gpa, 0, 11, 100, ikey("c"), ikey("d"));
    try edit.addFile(gpa, 1, 20, 200, ikey("m"), ikey("n"));
    try vs.logAndApply(&edit);

    try testing.expectEqual(@as(usize, 2), levelFileCount(&vs, 0));
    try testing.expectEqual(@as(usize, 1), levelFileCount(&vs, 1));
}

test "logAndApply: delete then add updates layout" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    {
        var edit = version_edit.VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.addFile(gpa, 1, 20, 200, ikey("m"), ikey("n"));
        try edit.addFile(gpa, 1, 21, 200, ikey("p"), ikey("q"));
        try vs.logAndApply(&edit);
    }
    try testing.expectEqual(@as(usize, 2), levelFileCount(&vs, 1));

    {
        var edit = version_edit.VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.removeFile(gpa, 1, 20);
        try edit.addFile(gpa, 1, 22, 200, ikey("a"), ikey("c"));
        try vs.logAndApply(&edit);
    }

    // 22 added, 20 removed -> still 2 files but {21, 22}.
    try testing.expectEqual(@as(usize, 2), levelFileCount(&vs, 1));
    const files = vs.currentVersion().files[1].items;
    var saw_21 = false;
    var saw_22 = false;
    var saw_20 = false;
    for (files) |f| {
        if (f.number == 20) saw_20 = true;
        if (f.number == 21) saw_21 = true;
        if (f.number == 22) saw_22 = true;
    }
    try testing.expect(!saw_20);
    try testing.expect(saw_21);
    try testing.expect(saw_22);
}

test "logAndApply: levels >= 1 stay sorted by smallest key" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    var vs = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs.deinit();

    var edit = version_edit.VersionEdit.init();
    defer edit.deinit(gpa);
    // Add out of order; expect sorted by smallest internal key on level 1.
    try edit.addFile(gpa, 1, 30, 100, ikey("m"), ikey("n"));
    try edit.addFile(gpa, 1, 31, 100, ikey("a"), ikey("c"));
    try edit.addFile(gpa, 1, 32, 100, ikey("e"), ikey("g"));
    try vs.logAndApply(&edit);

    const files = vs.currentVersion().files[1].items;
    try testing.expectEqual(@as(usize, 3), files.len);
    try testing.expectEqual(@as(u64, 31), files[0].number); // a..c
    try testing.expectEqual(@as(u64, 32), files[1].number); // e..g
    try testing.expectEqual(@as(u64, 30), files[2].number); // m..n
}

test "MANIFEST round-trip: recover reproduces identical state" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    // --- Build & persist with the first VersionSet ----------------------
    {
        var vs = try VersionSet.init(gpa, me.env(), "db", .{});
        defer vs.deinit();

        var edit = version_edit.VersionEdit.init();
        defer edit.deinit(gpa);
        try edit.setComparatorName(gpa, "leveldb.BytewiseComparator");
        edit.setLogNumber(5);
        edit.setNextFileNumber(50);
        edit.setLastSequence(999);
        try edit.addFile(gpa, 0, 10, 100, ikey("a"), ikey("b"));
        try edit.addFile(gpa, 0, 11, 150, ikey("c"), ikey("d"));
        try edit.addFile(gpa, 1, 20, 200, ikey("e"), ikey("g"));
        try edit.addFile(gpa, 2, 30, 300, ikey("x"), ikey("z"));
        try vs.logAndApply(&edit);
    }

    // CURRENT must exist and point at a MANIFEST.
    {
        const cur = try filename.currentFileName(gpa, "db");
        defer gpa.free(cur);
        try testing.expect(me.env().fileExists(cur));
    }

    // --- Recover into a fresh VersionSet --------------------------------
    var vs2 = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs2.deinit();
    try vs2.recover();

    try testing.expectEqual(@as(u64, 5), vs2.logNumber());
    try testing.expectEqual(@as(u64, 999), vs2.lastSequence());
    // next_file_number was set to 50 in the edit; recover must honour it.
    try testing.expect(vs2.newFileNumber() >= 50);

    const v = vs2.currentVersion();
    try testing.expectEqual(@as(usize, 2), v.files[0].items.len);
    try testing.expectEqual(@as(usize, 1), v.files[1].items.len);
    try testing.expectEqual(@as(usize, 1), v.files[2].items.len);

    // Level 0 keeps insertion order (10, 11).
    try testing.expectEqual(@as(u64, 10), v.files[0].items[0].number);
    try testing.expectEqual(@as(u64, 11), v.files[0].items[1].number);
    try testing.expectEqual(@as(u64, 100), v.files[0].items[0].file_size);
    try testing.expectEqualSlices(u8, ikey("a"), v.files[0].items[0].smallest);
    try testing.expectEqualSlices(u8, ikey("b"), v.files[0].items[0].largest);

    try testing.expectEqual(@as(u64, 20), v.files[1].items[0].number);
    try testing.expectEqual(@as(u64, 200), v.files[1].items[0].file_size);
    try testing.expectEqual(@as(u64, 30), v.files[2].items[0].number);
}

test "recover across multiple edits accumulates layout" {
    const gpa = testing.allocator;
    var me = env.MemEnv.init(gpa);
    defer me.deinit();

    {
        var vs = try VersionSet.init(gpa, me.env(), "db", .{});
        defer vs.deinit();

        {
            var edit = version_edit.VersionEdit.init();
            defer edit.deinit(gpa);
            edit.setLastSequence(10);
            try edit.addFile(gpa, 1, 20, 200, ikey("e"), ikey("g"));
            try edit.addFile(gpa, 1, 21, 200, ikey("m"), ikey("p"));
            try vs.logAndApply(&edit);
        }
        {
            var edit = version_edit.VersionEdit.init();
            defer edit.deinit(gpa);
            edit.setLastSequence(20);
            try edit.removeFile(gpa, 1, 20);
            try edit.addFile(gpa, 1, 22, 200, ikey("a"), ikey("c"));
            try vs.logAndApply(&edit);
        }
    }

    var vs2 = try VersionSet.init(gpa, me.env(), "db", .{});
    defer vs2.deinit();
    try vs2.recover();

    try testing.expectEqual(@as(u64, 20), vs2.lastSequence());
    const v = vs2.currentVersion();
    try testing.expectEqual(@as(usize, 2), v.files[1].items.len);
    // Sorted by smallest: 22 (a..c) then 21 (m..p); 20 was removed.
    try testing.expectEqual(@as(u64, 22), v.files[1].items[0].number);
    try testing.expectEqual(@as(u64, 21), v.files[1].items[1].number);
}
