//! Env — capability-based filesystem abstraction over `std.Io` (Zig 0.16).
//!
//! This module isolates ALL of Zig 0.16's `std.Io` filesystem surface behind a
//! small runtime-vtable interface, following the same capability pattern used
//! by `util/comparator.zig`.  Nothing in zrocks should touch `std.fs.*` or
//! `std.Io.Dir.cwd()` directly; everything that performs I/O receives an `Env`
//! value explicitly and goes through it.
//!
//! Two implementations satisfy the SAME interface:
//!   * `RealEnv`  — OS-backed, wraps an `std.Io` + a root `std.Io.Dir`.
//!   * `MemEnv`   — in-memory test double (path -> owned bytes).
//!
//! The DB, WAL, SST, and MANIFEST layers will accept an `Env` and can therefore
//! run against either implementation.

const std = @import("std");

// ---------------------------------------------------------------------------
// Error vocabulary
// ---------------------------------------------------------------------------
// Small, purposeful set.  Underlying `std.Io` errors are mapped into these so
// callers never have to reason about the (large, platform-specific) raw std
// error unions.
pub const Error = error{
    NotFound,
    AlreadyExists,
    IoError,
    PermissionDenied,
    NotSupported,
} || std.mem.Allocator.Error;

// ---------------------------------------------------------------------------
// File handles — each its own runtime-vtable fat pointer.
// ---------------------------------------------------------------------------

/// A file opened for writing/appending.  Implementations may buffer; `flush`
/// pushes buffered bytes to the backing store, `sync` additionally forces them
/// durable (fsync on the real impl).  `close` releases the heap-allocated impl
/// state.
pub const WritableFile = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        append: *const fn (ptr: *anyopaque, data: []const u8) Error!void,
        flush: *const fn (ptr: *anyopaque) Error!void,
        sync: *const fn (ptr: *anyopaque) Error!void,
        close: *const fn (ptr: *anyopaque) Error!void,
    };

    pub fn append(self: WritableFile, data: []const u8) Error!void {
        return self.vtable.append(self.ptr, data);
    }
    pub fn flush(self: WritableFile) Error!void {
        return self.vtable.flush(self.ptr);
    }
    pub fn sync(self: WritableFile) Error!void {
        return self.vtable.sync(self.ptr);
    }
    pub fn close(self: WritableFile) Error!void {
        return self.vtable.close(self.ptr);
    }
};

/// A file opened for sequential reading (WAL / MANIFEST replay).
pub const SequentialFile = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Reads up to `buf.len` bytes, advancing an internal cursor.  Returns
        /// the number of bytes read; 0 means end-of-file.
        read: *const fn (ptr: *anyopaque, buf: []u8) Error!usize,
        /// Advances the internal cursor by `n` bytes (clamped at EOF).
        skip: *const fn (ptr: *anyopaque, n: u64) Error!void,
        close: *const fn (ptr: *anyopaque) Error!void,
    };

    pub fn read(self: SequentialFile, buf: []u8) Error!usize {
        return self.vtable.read(self.ptr, buf);
    }
    pub fn skip(self: SequentialFile, n: u64) Error!void {
        return self.vtable.skip(self.ptr, n);
    }
    pub fn close(self: SequentialFile) Error!void {
        return self.vtable.close(self.ptr);
    }
};

/// A file opened for positional (random-access) reading (SST).
pub const RandomAccessFile = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Positional read at `offset`.  Returns the number of bytes read into
        /// `buf` (may be short at EOF).
        readAt: *const fn (ptr: *anyopaque, offset: u64, buf: []u8) Error!usize,
        close: *const fn (ptr: *anyopaque) Error!void,
    };

    pub fn readAt(self: RandomAccessFile, offset: u64, buf: []u8) Error!usize {
        return self.vtable.readAt(self.ptr, offset, buf);
    }
    pub fn close(self: RandomAccessFile) Error!void {
        return self.vtable.close(self.ptr);
    }
};

// ---------------------------------------------------------------------------
// Env — the capability object.
// ---------------------------------------------------------------------------

pub const Env = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        newWritableFile: *const fn (ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!WritableFile,
        newAppendableFile: *const fn (ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!WritableFile,
        newSequentialFile: *const fn (ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!SequentialFile,
        newRandomAccessFile: *const fn (ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!RandomAccessFile,
        deleteFile: *const fn (ptr: *anyopaque, path: []const u8) Error!void,
        renameFile: *const fn (ptr: *anyopaque, from: []const u8, to: []const u8) Error!void,
        fileExists: *const fn (ptr: *anyopaque, path: []const u8) bool,
        getFileSize: *const fn (ptr: *anyopaque, path: []const u8) Error!u64,
        makeDir: *const fn (ptr: *anyopaque, path: []const u8) Error!void,
        // List the basenames of the entries directly under directory `path`
        // (leveldb-interop, Wave A).  Allocates the returned slice AND each
        // basename via `gpa`; the caller frees them with `freeListing`.  Used by
        // recovery to discover ALL `NNNNNN.log` WAL files present (LevelDB
        // recovery replays every log whose number is >= the recovered
        // log_number, not just the one named in the MANIFEST).
        listDir: *const fn (ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error![][]u8,
        // DB-level advisory file locking (C2 — implemented).  `lockFile(path)`
        // takes a NON-BLOCKING exclusive lock on `path` (the `<dbdir>/LOCK`
        // file); a conflicting lock already held returns `error.IoError`
        // immediately so a second writable open fails fast instead of hanging.
        // The implementation RETAINS the open lock descriptor (POSIX advisory
        // locks live with the open file description) until `unlockFile(path)`
        // releases + closes it.  RealEnv tracks held locks per-path (the Env may
        // be shared across DBs); MemEnv is single-process and always succeeds.
        // Acquired by writable `DB.open` / `CfDB.open` (once per DB directory);
        // read-only opens and per-CF sub-LSMs take no lock.
        lockFile: *const fn (ptr: *anyopaque, path: []const u8) Error!void,
        unlockFile: *const fn (ptr: *anyopaque, path: []const u8) Error!void,
        // The async/concurrency capability backing this Env (D2a-1).  Threaded
        // through to the DB so its write mutex (`std.Io.Mutex`) and the upcoming
        // background flush/compaction workers use the SAME `std.Io` instance that
        // owns this Env's filesystem authority — no global/ambient `io`.
        io: *const fn (ptr: *anyopaque) std.Io,
    };

    /// Create/truncate `path` for writing/appending.
    pub fn newWritableFile(self: Env, gpa: std.mem.Allocator, path: []const u8) Error!WritableFile {
        return self.vtable.newWritableFile(self.ptr, gpa, path);
    }
    /// Open `path` for APPEND: if it exists, subsequent `append` calls extend
    /// the existing content; if it doesn't exist, behaves like
    /// `newWritableFile` (create empty).  Used to reuse an existing WAL across
    /// reopen so committed writes are never orphaned (M5.2 recovery).
    pub fn newAppendableFile(self: Env, gpa: std.mem.Allocator, path: []const u8) Error!WritableFile {
        return self.vtable.newAppendableFile(self.ptr, gpa, path);
    }
    /// Open `path` for sequential reading.
    pub fn newSequentialFile(self: Env, gpa: std.mem.Allocator, path: []const u8) Error!SequentialFile {
        return self.vtable.newSequentialFile(self.ptr, gpa, path);
    }
    /// Open `path` for positional reading.
    pub fn newRandomAccessFile(self: Env, gpa: std.mem.Allocator, path: []const u8) Error!RandomAccessFile {
        return self.vtable.newRandomAccessFile(self.ptr, gpa, path);
    }
    pub fn deleteFile(self: Env, path: []const u8) Error!void {
        return self.vtable.deleteFile(self.ptr, path);
    }
    /// Rename `from` to `to`.  Atomic where the platform allows (used for the
    /// CURRENT file pointer swap).
    pub fn renameFile(self: Env, from: []const u8, to: []const u8) Error!void {
        return self.vtable.renameFile(self.ptr, from, to);
    }
    pub fn fileExists(self: Env, path: []const u8) bool {
        return self.vtable.fileExists(self.ptr, path);
    }
    pub fn getFileSize(self: Env, path: []const u8) Error!u64 {
        return self.vtable.getFileSize(self.ptr, path);
    }
    /// Create a directory; success if it already exists.
    pub fn makeDir(self: Env, path: []const u8) Error!void {
        return self.vtable.makeDir(self.ptr, path);
    }
    /// List the basenames of the entries directly under directory `path`
    /// (leveldb-interop, Wave A).  Each returned name AND the outer slice are
    /// allocated via `gpa`; free with `freeListing`.  Order is unspecified.
    pub fn listDir(self: Env, gpa: std.mem.Allocator, path: []const u8) Error![][]u8 {
        return self.vtable.listDir(self.ptr, gpa, path);
    }
    /// Free a listing previously returned by `listDir`.
    pub fn freeListing(gpa: std.mem.Allocator, list: [][]u8) void {
        for (list) |name| gpa.free(name);
        gpa.free(list);
    }
    pub fn lockFile(self: Env, path: []const u8) Error!void {
        return self.vtable.lockFile(self.ptr, path);
    }
    pub fn unlockFile(self: Env, path: []const u8) Error!void {
        return self.vtable.unlockFile(self.ptr, path);
    }
    /// The `std.Io` capability backing this Env (D2a-1).  The DB pulls its write
    /// mutex's `io` (and, later, its background workers' `io`) from here so all
    /// concurrency uses the SAME instance that owns the filesystem authority.
    pub fn io(self: Env) std.Io {
        return self.vtable.io(self.ptr);
    }
};

// ---------------------------------------------------------------------------
// Implementations
// ---------------------------------------------------------------------------

pub const RealEnv = @import("real_env.zig").RealEnv;
pub const MemEnv = @import("mem_env.zig").MemEnv;

// ---------------------------------------------------------------------------
// Tests — the SAME contract runs against both implementations.
// ---------------------------------------------------------------------------

/// Generic conformance test exercised by both `MemEnv` and `RealEnv`.
fn runEnvContract(env: Env, gpa: std.mem.Allocator) !void {
    const expect = std.testing.expect;

    // ---- write ----------------------------------------------------------
    {
        var wf = try env.newWritableFile(gpa, "foo.txt");
        errdefer wf.close() catch {};
        try wf.append("hello ");
        try wf.append("world");
        try wf.sync();
        try wf.close();
    }

    // ---- metadata -------------------------------------------------------
    try expect(env.fileExists("foo.txt"));
    try std.testing.expectEqual(@as(u64, 11), try env.getFileSize("foo.txt"));

    // ---- sequential read ------------------------------------------------
    {
        var sf = try env.newSequentialFile(gpa, "foo.txt");
        errdefer sf.close() catch {};
        var assembled: [32]u8 = undefined;
        var total: usize = 0;
        while (true) {
            const n = try sf.read(assembled[total..]);
            if (n == 0) break; // EOF
            total += n;
        }
        try std.testing.expectEqualStrings("hello world", assembled[0..total]);
        // Reading again at EOF returns 0.
        var tmp: [4]u8 = undefined;
        try std.testing.expectEqual(@as(usize, 0), try sf.read(&tmp));
        try sf.close();
    }

    // ---- random-access read --------------------------------------------
    {
        var raf = try env.newRandomAccessFile(gpa, "foo.txt");
        errdefer raf.close() catch {};
        var buf: [5]u8 = undefined;
        try std.testing.expectEqual(@as(usize, 5), try raf.readAt(6, &buf));
        try std.testing.expectEqualStrings("world", &buf);
        try std.testing.expectEqual(@as(usize, 5), try raf.readAt(0, &buf));
        try std.testing.expectEqualStrings("hello", &buf);
        try raf.close();
    }

    // ---- rename ---------------------------------------------------------
    try env.renameFile("foo.txt", "bar.txt");
    try expect(!env.fileExists("foo.txt"));
    try expect(env.fileExists("bar.txt"));

    // ---- delete ---------------------------------------------------------
    try env.deleteFile("bar.txt");
    try expect(!env.fileExists("bar.txt"));

    // ---- missing-file error paths --------------------------------------
    try std.testing.expectError(error.NotFound, env.getFileSize("bar.txt"));
    try std.testing.expectError(error.NotFound, env.newSequentialFile(gpa, "bar.txt"));
}

/// `newAppendableFile` round-trip: write "abc", reopen for append + write
/// "def", read back "abcdef".  Also verifies that appending to a missing file
/// creates it (behaves like `newWritableFile`).
fn runAppendableContract(env: Env, gpa: std.mem.Allocator) !void {
    // Seed an existing file with "abc".
    {
        var wf = try env.newWritableFile(gpa, "app.txt");
        errdefer wf.close() catch {};
        try wf.append("abc");
        try wf.close();
    }

    // Reopen for append and extend with "def".
    {
        var wf = try env.newAppendableFile(gpa, "app.txt");
        errdefer wf.close() catch {};
        try wf.append("def");
        try wf.close();
    }

    try std.testing.expectEqual(@as(u64, 6), try env.getFileSize("app.txt"));
    {
        var sf = try env.newSequentialFile(gpa, "app.txt");
        errdefer sf.close() catch {};
        var buf: [16]u8 = undefined;
        var total: usize = 0;
        while (true) {
            const n = try sf.read(buf[total..]);
            if (n == 0) break;
            total += n;
        }
        try std.testing.expectEqualStrings("abcdef", buf[0..total]);
        try sf.close();
    }

    // newAppendableFile on a missing path creates it empty (like newWritableFile).
    {
        var wf = try env.newAppendableFile(gpa, "fresh.txt");
        errdefer wf.close() catch {};
        try wf.append("xyz");
        try wf.close();
    }
    try std.testing.expectEqual(@as(u64, 3), try env.getFileSize("fresh.txt"));

    try env.deleteFile("app.txt");
    try env.deleteFile("fresh.txt");
}

/// `listDir` contract (leveldb-interop, Wave A): create a directory with three
/// files, list it, and confirm exactly those basenames come back (order-free).
/// Listing a missing directory surfaces `error.NotFound`.
fn runListDirContract(env: Env, gpa: std.mem.Allocator) !void {
    try env.makeDir("listdir_d");
    for ([_][]const u8{ "listdir_d/CURRENT", "listdir_d/000003.log", "listdir_d/MANIFEST-000001" }) |p| {
        var wf = try env.newWritableFile(gpa, p);
        errdefer wf.close() catch {};
        try wf.append("x");
        try wf.close();
    }

    const names = try env.listDir(gpa, "listdir_d");
    defer Env.freeListing(gpa, names);
    try std.testing.expectEqual(@as(usize, 3), names.len);

    var saw_current = false;
    var saw_log = false;
    var saw_manifest = false;
    for (names) |n| {
        if (std.mem.eql(u8, n, "CURRENT")) saw_current = true;
        if (std.mem.eql(u8, n, "000003.log")) saw_log = true;
        if (std.mem.eql(u8, n, "MANIFEST-000001")) saw_manifest = true;
    }
    try std.testing.expect(saw_current and saw_log and saw_manifest);

    for ([_][]const u8{ "listdir_d/CURRENT", "listdir_d/000003.log", "listdir_d/MANIFEST-000001" }) |p| {
        try env.deleteFile(p);
    }

    // A non-existent directory must either surface NotFound (RealEnv) or list as
    // empty (MemEnv's flat map cannot tell "missing" from "empty") — recovery's
    // replayAllLogs treats both as "no logs to replay".
    if (env.listDir(gpa, "no_such_dir")) |empty| {
        defer Env.freeListing(gpa, empty);
        try std.testing.expectEqual(@as(usize, 0), empty.len);
    } else |err| {
        try std.testing.expectEqual(error.NotFound, err);
    }
}

test "MemEnv contract" {
    const gpa = std.testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    try runEnvContract(me.env(), gpa);
}

test "MemEnv appendable round-trip" {
    const gpa = std.testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    try runAppendableContract(me.env(), gpa);
}

test "RealEnv appendable round-trip" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    try runAppendableContract(re.env(), gpa);
}

test "RealEnv contract" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    try runEnvContract(re.env(), gpa);
}

test "MemEnv listDir contract" {
    const gpa = std.testing.allocator;
    var me = MemEnv.init(gpa);
    defer me.deinit();
    try runListDirContract(me.env(), gpa);
}

test "RealEnv listDir contract" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var re = RealEnv.init(gpa, io, tmp.dir);
    try runListDirContract(re.env(), gpa);
}
