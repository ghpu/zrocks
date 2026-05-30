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
const builtin = @import("builtin");
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
    /// Guards `files` (D2a-2).  zrocks once drove a MemEnv strictly
    /// single-threaded, but the background flush worker now runs the SST build
    /// on a concurrent fiber while the foreground continues writing the new WAL
    /// — both touch this map.  A `std.Io.Mutex` keyed off the test runner's
    /// global `io` serializes the (small, in-memory) map mutations so the
    /// hashmap's backing store is never corrupted by a data race.  A RealEnv
    /// needs no equivalent: distinct OS files are independent.
    mutex: std.Io.Mutex = .init,

    pub fn init(gpa: std.mem.Allocator) MemEnv {
        return .{ .gpa = gpa };
    }

    /// The `io` backing `mutex` (test-runner global in test builds).
    fn lockIo() std.Io {
        if (builtin.is_test) return std.testing.io;
        unreachable; // MemEnv is a test-only double.
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
        self.mutex.lockUncancelable(lockIo());
        defer self.mutex.unlock(lockIo());
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

    /// A freshly duped copy of `path`'s contents (caller owns + frees), or null
    /// if absent.  Locks across the lookup + dupe so a concurrent `store` cannot
    /// free the underlying bytes mid-read (D2a-2).
    fn dupContents(self: *MemEnv, gpa: std.mem.Allocator, path: []const u8) Error!?[]u8 {
        self.mutex.lockUncancelable(lockIo());
        defer self.mutex.unlock(lockIo());
        const bytes = self.files.get(path) orelse return null;
        return try gpa.dupe(u8, bytes);
    }

    /// The size of `path`'s contents, or null if absent (locked).
    fn sizeOf(self: *MemEnv, path: []const u8) ?u64 {
        self.mutex.lockUncancelable(lockIo());
        defer self.mutex.unlock(lockIo());
        const bytes = self.files.get(path) orelse return null;
        return bytes.len;
    }

    /// Whether `path` exists (locked).
    fn exists(self: *MemEnv, path: []const u8) bool {
        self.mutex.lockUncancelable(lockIo());
        defer self.mutex.unlock(lockIo());
        return self.files.contains(path);
    }

    // -- Env vtable ------------------------------------------------------

    const vtable = Env.VTable{
        .newWritableFile = newWritableFile,
        .newAppendableFile = newAppendableFile,
        .newSequentialFile = newSequentialFile,
        .newRandomAccessFile = newRandomAccessFile,
        .deleteFile = deleteFile,
        .deleteTree = deleteTree,
        .linkFile = linkFile,
        .renameFile = renameFile,
        .fileExists = fileExists,
        .getFileSize = getFileSize,
        .makeDir = makeDir,
        .listDir = listDir,
        .lockFile = lockFile,
        .unlockFile = unlockFile,
        .io = ioCapability,
    };

    /// The `std.Io` capability for this test double (D2a-1).  A MemEnv is driven
    /// single-threaded, so the DB's write mutex is never contended and this `io`
    /// is never actually dereferenced (an uncontended `std.Io.Mutex` lock/unlock
    /// is a pure cmpxchg with no futex call).  We still hand back a real `io` —
    /// the test runner's global `std.testing.io` in test builds — so nothing ever
    /// touches an undefined value; in a (non-test) build where MemEnv is unused
    /// there is no global io, so fall back to an unreachable stub.
    fn ioCapability(ptr: *anyopaque) std.Io {
        _ = ptr;
        if (builtin.is_test) return std.testing.io;
        // MemEnv is a test-only double; a non-test build never reaches here.
        unreachable;
    }

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
        const existing = try self.dupContents(gpa, path);
        defer if (existing) |bytes| gpa.free(bytes);
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
        // Snapshot the contents (locked) so the handle is stable even if the map
        // mutates; the snapshot is the handle's owned backing store.
        const snapshot = (try self.dupContents(gpa, path)) orelse return error.NotFound;
        errdefer gpa.free(snapshot);
        const h = try gpa.create(MemSequential);
        errdefer gpa.destroy(h);
        h.* = .{ .gpa = gpa, .data = snapshot, .pos = 0 };
        return .{ .ptr = h, .vtable = &MemSequential.vtable };
    }

    fn newRandomAccessFile(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error!RandomAccessFile {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        const snapshot = (try self.dupContents(gpa, path)) orelse return error.NotFound;
        errdefer gpa.free(snapshot);
        const h = try gpa.create(MemRandom);
        errdefer gpa.destroy(h);
        h.* = .{ .gpa = gpa, .data = snapshot };
        return .{ .ptr = h, .vtable = &MemRandom.vtable };
    }

    fn deleteFile(ptr: *anyopaque, path: []const u8) Error!void {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(lockIo());
        defer self.mutex.unlock(lockIo());
        if (self.files.fetchRemove(path)) |kv| {
            self.gpa.free(kv.key);
            self.gpa.free(kv.value);
        } else {
            return error.NotFound;
        }
    }

    /// Recursively delete the tree rooted at `path` (C3): remove every map entry
    /// whose key is `path` itself OR lives under the `path ++ "/"` prefix, and
    /// free each owned key + value (exactly like `deleteFile`).  The map is flat,
    /// so this is a single prefix sweep.  Deleting an absent tree is a no-op (no
    /// error), matching RealEnv's tolerant `Dir.deleteTree`.  We cannot mutate the
    /// map while iterating it, so we first collect the matching keys (borrowing
    /// the live key pointers under the lock) and then remove them.
    fn deleteTree(ptr: *anyopaque, path: []const u8) Error!void {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(lockIo());
        defer self.mutex.unlock(lockIo());

        const prefix = std.fmt.allocPrint(self.gpa, "{s}/", .{path}) catch return Error.OutOfMemory;
        defer self.gpa.free(prefix);

        // Collect matching keys first (borrowed pointers into the map's owned
        // keys; valid until we start removing).  Using the gpa for this scratch
        // list keeps the sweep leak-free even on an allocation failure mid-collect.
        var victims: std.ArrayListUnmanaged([]const u8) = .empty;
        defer victims.deinit(self.gpa);
        {
            var it = self.files.iterator();
            while (it.next()) |entry| {
                const key = entry.key_ptr.*;
                if (std.mem.eql(u8, key, path) or std.mem.startsWith(u8, key, prefix)) {
                    try victims.append(self.gpa, key);
                }
            }
        }

        for (victims.items) |key| {
            if (self.files.fetchRemove(key)) |kv| {
                self.gpa.free(kv.key);
                self.gpa.free(kv.value);
            }
        }
        // Absent tree → nothing collected → no-op success.
    }

    /// Emulate a hard link as a CONTENT COPY (C-d): MemEnv files have no inodes,
    /// and checkpointed SSTs are immutable, so duplicating `old_path`'s current
    /// bytes into `new_path` is semantically identical to a shared-inode link.
    /// `old_path` absent → NotFound; `new_path` already present → AlreadyExists
    /// (matching std `Dir.hardLink` and RealEnv).  Holds the map lock across the
    /// read + insert so the copy is atomic w.r.t. concurrent mutations; frees the
    /// owned key/value exactly like the new-entry path in `store`.
    fn linkFile(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) Error!void {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(lockIo());
        defer self.mutex.unlock(lockIo());

        if (self.files.contains(new_path)) return error.AlreadyExists;
        const src_bytes = self.files.get(old_path) orelse return error.NotFound;

        const gop = try self.files.getOrPut(self.gpa, new_path);
        // `new_path` cannot pre-exist (checked above), so this is always a fresh
        // entry; dup the key first, then the contents, unwinding on failure.
        const owned_key = self.gpa.dupe(u8, new_path) catch |e| {
            _ = self.files.remove(new_path);
            return e;
        };
        // NOTE: `src_bytes` stays valid — we hold the lock and have not mutated
        // the map's existing entries (getOrPut on a new key may have rehashed,
        // but the VALUE slices it stores are unchanged, so `src_bytes` still
        // points at live bytes).
        const new_bytes = self.gpa.dupe(u8, src_bytes) catch |e| {
            _ = self.files.remove(new_path);
            self.gpa.free(owned_key);
            return e;
        };
        gop.key_ptr.* = owned_key;
        gop.value_ptr.* = new_bytes;
    }

    fn renameFile(ptr: *anyopaque, from: []const u8, to: []const u8) Error!void {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(lockIo());
        defer self.mutex.unlock(lockIo());
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
        return self.exists(path);
    }

    fn getFileSize(ptr: *anyopaque, path: []const u8) Error!u64 {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        return self.sizeOf(path) orelse error.NotFound;
    }

    fn makeDir(ptr: *anyopaque, path: []const u8) Error!void {
        // The map is flat; directories are implicit.  Treat as a no-op success
        // so callers that "ensure the DB dir exists" work uniformly.
        _ = ptr;
        _ = path;
    }

    /// List the basenames of the DIRECT children of directory `path`
    /// (leveldb-interop, Wave A).  The map is flat (keys are full `dir/base`
    /// paths), so we select every key with the `path ++ "/"` prefix whose
    /// remainder has no further `/`, and dupe that remainder.  Caller frees with
    /// `Env.freeListing`.
    fn listDir(ptr: *anyopaque, gpa: std.mem.Allocator, path: []const u8) Error![][]u8 {
        const self: *MemEnv = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(lockIo());
        defer self.mutex.unlock(lockIo());

        const prefix = std.fmt.allocPrint(gpa, "{s}/", .{path}) catch return Error.IoError;
        defer gpa.free(prefix);

        var names: std.ArrayListUnmanaged([]u8) = .empty;
        errdefer {
            for (names.items) |n| gpa.free(n);
            names.deinit(gpa);
        }

        var it = self.files.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            if (!std.mem.startsWith(u8, key, prefix)) continue;
            const base = key[prefix.len..];
            if (base.len == 0) continue;
            if (std.mem.indexOfScalar(u8, base, '/') != null) continue; // not a direct child
            const dup = try gpa.dupe(u8, base);
            errdefer gpa.free(dup);
            try names.append(gpa, dup);
        }
        return names.toOwnedSlice(gpa);
    }

    fn lockFile(ptr: *anyopaque, path: []const u8) Error!void {
        // MemEnv is single-process and in-memory: there is no cross-process LOCK
        // file to guard (the real exclusive advisory lock lives in RealEnv, C2).
        // Always succeed so MemEnv-backed DB tests open without contention.
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
