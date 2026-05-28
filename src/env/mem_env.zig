//! MemEnv — in-memory `Env` implementation (test double).
//!
//! A `StringHashMapUnmanaged([]u8)` maps an owned path string to owned file
//! bytes.  Writable files accumulate appended data in a growing buffer that is
//! committed to the map on `flush`/`sync`/`close`; sequential and random reads
//! operate on the committed bytes.  Ownership is tracked carefully so the whole
//! thing is leak-free under `std.testing.allocator`.
//!
//! No filesystem authority: the map IS the filesystem.  No locking is needed
//! because zrocks tests drive a MemEnv single-threaded.

const std = @import("std");
const env_mod = @import("env.zig");

const Env = env_mod.Env;
const Error = env_mod.Error;
const WritableFile = env_mod.WritableFile;
const SequentialFile = env_mod.SequentialFile;
const RandomAccessFile = env_mod.RandomAccessFile;

pub const MemEnv = struct {
    gpa: std.mem.Allocator,
    /// path (owned) -> file contents (owned).
    files: std.StringHashMapUnmanaged([]u8) = .empty,

    pub fn init(gpa: std.mem.Allocator) MemEnv {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *MemEnv) void {
        var it = self.files.iterator();
        while (it.next()) |entry| {
            self.gpa.free(entry.key_ptr.*);
            self.gpa.free(entry.value_ptr.*);
        }
        self.files.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn env(self: *MemEnv) Env {
        return .{ .ptr = self, .vtable = &vtable };
    }

    // -- internal helpers ------------------------------------------------

    /// Replace (or create) the contents of `path` with `bytes` (the map takes
    /// ownership of a freshly duped copy of both `path` and `bytes`).
    fn store(self: *MemEnv, path: []const u8, bytes: []const u8) Error!void {
        const gop = try self.files.getOrPut(self.gpa, path);
        if (gop.found_existing) {
            // Reuse the existing key; swap in fresh contents.
            const new_bytes = try self.gpa.dupe(u8, bytes);
            self.gpa.free(gop.value_ptr.*);
            gop.value_ptr.* = new_bytes;
        } else {
            // New entry: dup the key first, then contents.  On a failure after
            // the key was inserted, remove it to avoid a dangling key.
            const owned_key = self.gpa.dupe(u8, path) catch |e| {
                _ = self.files.remove(path);
                return e;
            };
            const new_bytes = self.gpa.dupe(u8, bytes) catch |e| {
                _ = self.files.remove(path);
                self.gpa.free(owned_key);
                return e;
            };
            gop.key_ptr.* = owned_key;
            gop.value_ptr.* = new_bytes;
        }
    }

    fn get(self: *MemEnv, path: []const u8) ?[]u8 {
        return self.files.get(path);
    }

    // -- Env vtable ------------------------------------------------------

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
        .lockFile = lockFile,
        .unlockFile = unlockFile,
    };

    fn newWritableFile(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!WritableFile {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        // create/truncate semantics: ensure the path exists with empty contents.
        try self.store(path, "");
        const h = try gpa.create(MemWritable);
        errdefer gpa.destroy(h);
        const owned_path = try gpa.dupe(u8, path);
        h.* = .{ .me = self, .gpa = gpa, .path = owned_path, .buf = .empty };
        return .{ .ptr = h, .vtable = &MemWritable.vtable };
    }

    fn newAppendableFile(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!WritableFile {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        // Append semantics: the writable's buffer is seeded with the existing
        // committed bytes (if any) so flush/close (which replace the file with
        // `buf.items`) extend rather than truncate.  If the path doesn't exist,
        // create it empty (like newWritableFile).
        const existing = self.get(path);
        if (existing == null) try self.store(path, "");

        const h = try gpa.create(MemWritable);
        errdefer gpa.destroy(h);
        const owned_path = try gpa.dupe(u8, path);
        errdefer gpa.free(owned_path);

        var buf: std.ArrayListUnmanaged(u8) = .empty;
        if (existing) |bytes| {
            buf.appendSlice(gpa, bytes) catch |err| {
                buf.deinit(gpa);
                return err;
            };
        }
        h.* = .{ .me = self, .gpa = gpa, .path = owned_path, .buf = buf };
        return .{ .ptr = h, .vtable = &MemWritable.vtable };
    }

    fn newSequentialFile(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!SequentialFile {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        const contents = self.get(path) orelse return error.NotFound;
        const h = try gpa.create(MemSequential);
        errdefer gpa.destroy(h);
        // Snapshot the contents so the handle is stable even if the map mutates.
        const snapshot = try gpa.dupe(u8, contents);
        h.* = .{ .gpa = gpa, .data = snapshot, .pos = 0 };
        return .{ .ptr = h, .vtable = &MemSequential.vtable };
    }

    fn newRandomAccessFile(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!RandomAccessFile {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        const contents = self.get(path) orelse return error.NotFound;
        const h = try gpa.create(MemRandom);
        errdefer gpa.destroy(h);
        const snapshot = try gpa.dupe(u8, contents);
        h.* = .{ .gpa = gpa, .data = snapshot };
        return .{ .ptr = h, .vtable = &MemRandom.vtable };
    }

    fn deleteFile(ptr: *anyopaque, path: []const u8) Error!void {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        if (self.files.fetchRemove(path)) |kv| {
            self.gpa.free(kv.key);
            self.gpa.free(kv.value);
        } else {
            return error.NotFound;
        }
    }

    fn renameFile(ptr: *anyopaque, from: []const u8, to: []const u8) Error!void {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        const kv = self.files.fetchRemove(from) orelse return error.NotFound;
        // `kv.value` ownership transfers to the destination.  Free any existing
        // destination contents and re-key.
        if (self.files.fetchRemove(to)) |old| {
            self.gpa.free(old.key);
            self.gpa.free(old.value);
        }
        const new_key = self.gpa.dupe(u8, to) catch |e| {
            // Restore `from` on failure to keep the filesystem consistent.
            self.files.put(self.gpa, kv.key, kv.value) catch {};
            return e;
        };
        self.gpa.free(kv.key);
        self.files.put(self.gpa, new_key, kv.value) catch |e| {
            self.gpa.free(new_key);
            self.gpa.free(kv.value);
            return e;
        };
    }

    fn fileExists(ptr: *anyopaque, path: []const u8) bool {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        return self.files.contains(path);
    }

    fn getFileSize(ptr: *anyopaque, path: []const u8) Error!u64 {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        const contents = self.get(path) orelse return error.NotFound;
        return contents.len;
    }

    fn makeDir(ptr: *anyopaque, path: []const u8) Error!void {
        // The map is flat; directories are implicit.  Treat as a no-op success
        // so callers that "ensure the DB dir exists" work uniformly.
        _ = ptr;
        _ = path;
    }

    fn lockFile(ptr: *anyopaque, path: []const u8) Error!void {
        // Stub (see RealEnv): no cross-process semantics in memory.
        _ = ptr;
        _ = path;
    }

    fn unlockFile(ptr: *anyopaque, path: []const u8) Error!void {
        _ = ptr;
        _ = path;
    }
};

// ---------------------------------------------------------------------------
// MemEnv file handles
// ---------------------------------------------------------------------------

const MemWritable = struct {
    me: *MemEnv,
    gpa: std.mem.Allocator,
    path: []u8,
    buf: std.ArrayListUnmanaged(u8),

    const vtable = WritableFile.VTable{
        .append = append,
        .flush = flush,
        .sync = sync,
        .close = close,
    };

    fn append(ptr: *anyopaque, data: []const u8) Error!void {
        const h: *MemWritable = @ptrCast(@alignCast(ptr));
        try h.buf.appendSlice(h.gpa, data);
    }

    fn flush(ptr: *anyopaque) Error!void {
        const h: *MemWritable = @ptrCast(@alignCast(ptr));
        try h.me.store(h.path, h.buf.items);
    }

    fn sync(ptr: *anyopaque) Error!void {
        // In memory, durability == visibility; just commit.
        return flush(ptr);
    }

    fn close(ptr: *anyopaque) Error!void {
        const h: *MemWritable = @ptrCast(@alignCast(ptr));
        const gpa = h.gpa;
        // Commit on close, but always release the handle's resources first so a
        // failed commit can never leak.  The commit error (if any) is surfaced
        // after cleanup.
        const commit_err: ?Error = if (h.me.store(h.path, h.buf.items)) |_| null else |e| e;
        h.buf.deinit(gpa);
        gpa.free(h.path);
        gpa.destroy(h);
        if (commit_err) |e| return e;
    }
};

const MemSequential = struct {
    gpa: std.mem.Allocator,
    data: []u8,
    pos: usize,

    const vtable = SequentialFile.VTable{
        .read = read,
        .skip = skip,
        .close = close,
    };

    fn read(ptr: *anyopaque, buf: []u8) Error!usize {
        const h: *MemSequential = @ptrCast(@alignCast(ptr));
        if (h.pos >= h.data.len) return 0;
        const n = @min(buf.len, h.data.len - h.pos);
        @memcpy(buf[0..n], h.data[h.pos .. h.pos + n]);
        h.pos += n;
        return n;
    }

    fn skip(ptr: *anyopaque, n: u64) Error!void {
        const h: *MemSequential = @ptrCast(@alignCast(ptr));
        const remaining = h.data.len - h.pos;
        h.pos += @intCast(@min(n, remaining));
    }

    fn close(ptr: *anyopaque) Error!void {
        const h: *MemSequential = @ptrCast(@alignCast(ptr));
        const gpa = h.gpa;
        gpa.free(h.data);
        gpa.destroy(h);
    }
};

const MemRandom = struct {
    gpa: std.mem.Allocator,
    data: []u8,

    const vtable = RandomAccessFile.VTable{
        .readAt = readAt,
        .close = close,
    };

    fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) Error!usize {
        const h: *MemRandom = @ptrCast(@alignCast(ptr));
        if (offset >= h.data.len) return 0;
        const start: usize = @intCast(offset);
        const n = @min(buf.len, h.data.len - start);
        @memcpy(buf[0..n], h.data[start .. start + n]);
        return n;
    }

    fn close(ptr: *anyopaque) Error!void {
        const h: *MemRandom = @ptrCast(@alignCast(ptr));
        const gpa = h.gpa;
        gpa.free(h.data);
        gpa.destroy(h);
    }
};
