//! RealEnv — OS-backed `Env` implementation over `std.Io` (Zig 0.16).
//!
//! Holds an `std.Io` and a root `std.Io.Dir` as capabilities passed in by the
//! caller (who also owns the `std.Io.Threaded` / `Io` lifetime and the root
//! dir).  Every filesystem call routes through `io` against `root`; there is no
//! ambient `std.fs` / `cwd()` authority.
//!
//! The large, platform-specific `std.Io` error unions are funnelled through
//! `mapOpen`/`mapIo`/... into zrocks' small `Error` set.

const std = @import("std");
const env_mod = @import("env.zig");

const Io = std.Io;
const Dir = std.Io.Dir;
const File = std.Io.File;

const Env = env_mod.Env;
const Error = env_mod.Error;
const WritableFile = env_mod.WritableFile;
const SequentialFile = env_mod.SequentialFile;
const RandomAccessFile = env_mod.RandomAccessFile;

// ---------------------------------------------------------------------------
// Error mapping — collapse std's platform error unions into zrocks Error.
// ---------------------------------------------------------------------------

fn mapOpenErr(e: File.OpenError) Error {
    return switch (e) {
        error.FileNotFound => error.NotFound,
        error.PathAlreadyExists => error.AlreadyExists,
        error.AccessDenied, error.PermissionDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

fn mapDeleteErr(e: Dir.DeleteFileError) Error {
    return switch (e) {
        error.FileNotFound => error.NotFound,
        error.AccessDenied, error.PermissionDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

fn mapRenameErr(e: Dir.RenameError) Error {
    return switch (e) {
        error.FileNotFound => error.NotFound,
        error.AccessDenied, error.PermissionDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

fn mapStatFileErr(e: Dir.StatFileError) Error {
    return switch (e) {
        error.FileNotFound => error.NotFound,
        error.AccessDenied, error.PermissionDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

fn mapCreateDirErr(e: Dir.CreateDirError) Error {
    return switch (e) {
        error.PathAlreadyExists => error.AlreadyExists,
        error.FileNotFound => error.NotFound,
        error.AccessDenied, error.PermissionDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

fn mapOpenDirErr(e: Dir.OpenError) Error {
    return switch (e) {
        error.FileNotFound => error.NotFound,
        error.AccessDenied, error.PermissionDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

fn mapReadErr(e: File.ReadPositionalError) Error {
    return switch (e) {
        error.AccessDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

fn mapWriteErr(e: File.WritePositionalError) Error {
    return switch (e) {
        error.AccessDenied, error.PermissionDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

fn mapSyncErr(e: File.SyncError) Error {
    return switch (e) {
        error.AccessDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

fn mapStatErr(e: File.StatError) Error {
    return switch (e) {
        error.AccessDenied, error.PermissionDenied => error.PermissionDenied,
        else => error.IoError,
    };
}

// ---------------------------------------------------------------------------
// RealEnv
// ---------------------------------------------------------------------------

pub const RealEnv = struct {
    io: Io,
    root: Dir,

    /// `io` and `root` are capabilities owned by the caller.  RealEnv does not
    /// close `root` or tear down the `Io`.
    pub fn init(io: Io, root: Dir) RealEnv {
        return .{ .io = io, .root = root };
    }

    pub fn env(self: *RealEnv) Env {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = Env.VTable{
        .newWritableFile = newWritableFile,
        .newAppendableFile = newAppendableFile,
        .newSequentialFile = newSequentialFile,
        .newRandomAccessFile = newRandomAccessFile,
        .deleteFile = deleteFile,
        .renameFile = renameFile,
        .fileExists = fileExists,
        .getFileSize = getFileSize,
        .makeDir = makeDir,
        .listDir = listDir,
        .lockFile = lockFile,
        .unlockFile = unlockFile,
        .io = ioCapability,
    };

    /// Expose the owned `std.Io` so the DB's write mutex / background workers
    /// share this Env's concurrency capability (D2a-1).
    fn ioCapability(ptr: *anyopaque) Io {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        return self.io;
    }

    fn newWritableFile(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!WritableFile {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        const file = self.root.createFile(self.io, path, .{ .truncate = true }) catch |e| return mapOpenErr(e);
        const h = gpa.create(RealWritable) catch |e| {
            file.close(self.io);
            return e;
        };
        h.* = .{ .io = self.io, .gpa = gpa, .file = file, .offset = 0 };
        return .{ .ptr = h, .vtable = &RealWritable.vtable };
    }

    fn newAppendableFile(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!WritableFile {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        // Open-or-create WITHOUT truncating, then position the positional-write
        // cursor at the current end of file so appends extend existing content.
        const file = self.root.createFile(self.io, path, .{ .truncate = false }) catch |e| return mapOpenErr(e);
        const start: u64 = blk: {
            const st = file.stat(self.io) catch |e| {
                file.close(self.io);
                return mapStatErr(e);
            };
            break :blk st.size;
        };
        const h = gpa.create(RealWritable) catch |e| {
            file.close(self.io);
            return e;
        };
        h.* = .{ .io = self.io, .gpa = gpa, .file = file, .offset = start };
        return .{ .ptr = h, .vtable = &RealWritable.vtable };
    }

    fn newSequentialFile(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!SequentialFile {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        const file = self.root.openFile(self.io, path, .{ .mode = .read_only }) catch |e| return mapOpenErr(e);
        const h = gpa.create(RealSequential) catch |e| {
            file.close(self.io);
            return e;
        };
        h.* = .{ .io = self.io, .gpa = gpa, .file = file, .pos = 0 };
        return .{ .ptr = h, .vtable = &RealSequential.vtable };
    }

    fn newRandomAccessFile(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!RandomAccessFile {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        const file = self.root.openFile(self.io, path, .{ .mode = .read_only }) catch |e| return mapOpenErr(e);
        const h = gpa.create(RealRandom) catch |e| {
            file.close(self.io);
            return e;
        };
        h.* = .{ .io = self.io, .gpa = gpa, .file = file };
        return .{ .ptr = h, .vtable = &RealRandom.vtable };
    }

    fn deleteFile(ptr: *anyopaque, path: []const u8) Error!void {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        self.root.deleteFile(self.io, path) catch |e| return mapDeleteErr(e);
    }

    fn renameFile(ptr: *anyopaque, from: []const u8, to: []const u8) Error!void {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        // Same-directory rename within `root`; atomic on POSIX/Windows.
        // NOTE: `io` is the LAST parameter of Dir.rename in std 0.16.
        Dir.rename(self.root, from, self.root, to, self.io) catch |e| return mapRenameErr(e);
    }

    fn fileExists(ptr: *anyopaque, path: []const u8) bool {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        _ = self.root.statFile(self.io, path, .{}) catch return false;
        return true;
    }

    fn getFileSize(ptr: *anyopaque, path: []const u8) Error!u64 {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        const st = self.root.statFile(self.io, path, .{}) catch |e| return mapStatFileErr(e);
        return st.size;
    }

    fn makeDir(ptr: *anyopaque, path: []const u8) Error!void {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        self.root.createDir(self.io, path, .default_dir) catch |e| switch (e) {
            // "ok if exists" semantics.
            error.PathAlreadyExists => return,
            else => return mapCreateDirErr(e),
        };
    }

    /// List the basenames directly under directory `path` (leveldb-interop,
    /// Wave A).  Opens the directory with `.iterate`, dupes each entry name via
    /// `gpa`, and returns the owned slice (caller frees with `Env.freeListing`).
    fn listDir(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error![][]u8 {
        const self: *RealEnv = @ptrCast(@alignCast(ptr));
        var dir = self.root.openDir(self.io, path, .{ .iterate = true }) catch |e| return mapOpenDirErr(e);
        defer dir.close(self.io);

        var names: std.ArrayListUnmanaged([]u8) = .empty;
        errdefer {
            for (names.items) |n| gpa.free(n);
            names.deinit(gpa);
        }

        var it = dir.iterate();
        while (it.next(self.io) catch return Error.IoError) |entry| {
            const dup = try gpa.dupe(u8, entry.name);
            errdefer gpa.free(dup);
            try names.append(gpa, dup);
        }
        return names.toOwnedSlice(gpa);
    }

    // Advisory file locking.  std 0.16 exposes File.lock / Dir open `lock`
    // option, but the DB-level single-process lock (LOCK file) is a M5/M6
    // concern with its own lifecycle.  Stub as no-op success for now.
    // TODO M5/M6: real advisory lock via Dir.openFile(.{ .lock = .exclusive }).
    fn lockFile(ptr: *anyopaque, path: []const u8) Error!void {
        _ = ptr;
        _ = path;
    }

    fn unlockFile(ptr: *anyopaque, path: []const u8) Error!void {
        _ = ptr;
        _ = path;
    }
};

// ---------------------------------------------------------------------------
// RealEnv file handles
// ---------------------------------------------------------------------------

const RealWritable = struct {
    io: Io,
    gpa: std.mem.Allocator,
    file: File,
    /// Next positional write offset (append cursor).
    offset: u64,

    const vtable = WritableFile.VTable{
        .append = append,
        .flush = flush,
        .sync = sync,
        .close = close,
    };

    fn append(ptr: *anyopaque, data: []const u8) Error!void {
        const h: *RealWritable = @ptrCast(@alignCast(ptr));
        if (data.len == 0) return;
        h.file.writePositionalAll(h.io, data, h.offset) catch |e| return mapWriteErr(e);
        h.offset += data.len;
    }

    fn flush(ptr: *anyopaque) Error!void {
        // Positional writes go straight to the OS; no userspace buffer to push.
        _ = ptr;
    }

    fn sync(ptr: *anyopaque) Error!void {
        const h: *RealWritable = @ptrCast(@alignCast(ptr));
        h.file.sync(h.io) catch |e| return mapSyncErr(e);
    }

    fn close(ptr: *anyopaque) Error!void {
        const h: *RealWritable = @ptrCast(@alignCast(ptr));
        h.file.close(h.io);
        h.gpa.destroy(h);
    }
};

const RealSequential = struct {
    io: Io,
    gpa: std.mem.Allocator,
    file: File,
    /// Sequential read cursor.
    pos: u64,

    const vtable = SequentialFile.VTable{
        .read = read,
        .skip = skip,
        .close = close,
    };

    fn read(ptr: *anyopaque, buf: []u8) Error!usize {
        const h: *RealSequential = @ptrCast(@alignCast(ptr));
        if (buf.len == 0) return 0;
        const n = h.file.readPositionalAll(h.io, buf, h.pos) catch |e| return mapReadErr(e);
        h.pos += n;
        return n;
    }

    fn skip(ptr: *anyopaque, n: u64) Error!void {
        const h: *RealSequential = @ptrCast(@alignCast(ptr));
        h.pos += n;
    }

    fn close(ptr: *anyopaque) Error!void {
        const h: *RealSequential = @ptrCast(@alignCast(ptr));
        h.file.close(h.io);
        h.gpa.destroy(h);
    }
};

const RealRandom = struct {
    io: Io,
    gpa: std.mem.Allocator,
    file: File,

    const vtable = RandomAccessFile.VTable{
        .readAt = readAt,
        .close = close,
    };

    fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) Error!usize {
        const h: *RealRandom = @ptrCast(@alignCast(ptr));
        if (buf.len == 0) return 0;
        return h.file.readPositionalAll(h.io, buf, offset) catch |e| return mapReadErr(e);
    }

    fn close(ptr: *anyopaque) Error!void {
        const h: *RealRandom = @ptrCast(@alignCast(ptr));
        h.file.close(h.io);
        h.gpa.destroy(h);
    }
};
