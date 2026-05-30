//! filename.zig — DB file naming (LevelDB/RocksDB-style).
//!
//! All numbers are formatted as 6-digit zero-padded decimal, matching the
//! LevelDB/RocksDB on-disk convention (e.g. `MANIFEST-000007`, `000007.sst`).
//! Every helper allocates the returned path; the caller frees it.
const std = @import("std");

/// `<dbname>/CURRENT` — the pointer file naming the live MANIFEST.
pub fn currentFileName(gpa: std.mem.Allocator, dbname: []const u8) ![]u8 {
    return std.fmt.allocPrint(gpa, "{s}/CURRENT", .{dbname});
}

/// `<dbname>/MANIFEST-000007` — a descriptor (MANIFEST) log file.
pub fn manifestFileName(gpa: std.mem.Allocator, dbname: []const u8, number: u64) ![]u8 {
    return std.fmt.allocPrint(gpa, "{s}/MANIFEST-{d:0>6}", .{ dbname, number });
}

/// `<dbname>/000007.log` — a write-ahead log file.
pub fn logFileName(gpa: std.mem.Allocator, dbname: []const u8, number: u64) ![]u8 {
    return std.fmt.allocPrint(gpa, "{s}/{d:0>6}.log", .{ dbname, number });
}

/// `<dbname>/000007.sst` — an SSTable (table) file.
pub fn tableFileName(gpa: std.mem.Allocator, dbname: []const u8, number: u64) ![]u8 {
    return std.fmt.allocPrint(gpa, "{s}/{d:0>6}.sst", .{ dbname, number });
}

/// `<dbname>/000007.dbtmp` — a temp file used for the atomic CURRENT swap.
pub fn tempFileName(gpa: std.mem.Allocator, dbname: []const u8, number: u64) ![]u8 {
    return std.fmt.allocPrint(gpa, "{s}/{d:0>6}.dbtmp", .{ dbname, number });
}

/// `<dbname>/LOCK` — the DB-level advisory lock file (C2).  An exclusive flock
/// on this file's descriptor guards a writable DB directory against a second
/// concurrent writer (LevelDB/RocksDB convention).
pub fn lockFileName(gpa: std.mem.Allocator, dbname: []const u8) ![]u8 {
    return std.fmt.allocPrint(gpa, "{s}/LOCK", .{dbname});
}

/// `<dbroot>/CF_LIST` — the column-family registry (M7.0): a line-oriented
/// `<id> <name>` mapping rewritten on each create/drop so a reopen knows which
/// column families exist and at which subdirectory.
pub fn cfListFileName(gpa: std.mem.Allocator, dbroot: []const u8) ![]u8 {
    return std.fmt.allocPrint(gpa, "{s}/CF_LIST", .{dbroot});
}

// ===========================================================================
// Tests
// ===========================================================================

test "currentFileName" {
    const gpa = std.testing.allocator;
    const s = try currentFileName(gpa, "db");
    defer gpa.free(s);
    try std.testing.expectEqualStrings("db/CURRENT", s);
}

test "lockFileName" {
    const gpa = std.testing.allocator;
    const s = try lockFileName(gpa, "db");
    defer gpa.free(s);
    try std.testing.expectEqualStrings("db/LOCK", s);
}

test "manifestFileName is 6-digit zero-padded" {
    const gpa = std.testing.allocator;
    const s = try manifestFileName(gpa, "db", 7);
    defer gpa.free(s);
    try std.testing.expectEqualStrings("db/MANIFEST-000007", s);
}

test "manifestFileName with large number" {
    const gpa = std.testing.allocator;
    const s = try manifestFileName(gpa, "/tmp/mydb", 1234567);
    defer gpa.free(s);
    try std.testing.expectEqualStrings("/tmp/mydb/MANIFEST-1234567", s);
}

test "logFileName is 6-digit zero-padded" {
    const gpa = std.testing.allocator;
    const s = try logFileName(gpa, "db", 7);
    defer gpa.free(s);
    try std.testing.expectEqualStrings("db/000007.log", s);
}

test "tableFileName is 6-digit zero-padded" {
    const gpa = std.testing.allocator;
    const s = try tableFileName(gpa, "db", 7);
    defer gpa.free(s);
    try std.testing.expectEqualStrings("db/000007.sst", s);
}

test "tempFileName is 6-digit zero-padded" {
    const gpa = std.testing.allocator;
    const s = try tempFileName(gpa, "db", 7);
    defer gpa.free(s);
    try std.testing.expectEqualStrings("db/000007.dbtmp", s);
}
