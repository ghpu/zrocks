//! zrocks CLI — a small command-line tool over a real on-disk DB.
//!
//! Usage:
//!   zrocks <dbpath> put <key> <value>   open DB at dbpath, put, close
//!   zrocks <dbpath> get <key>           print value (or "(not found)" + exit 1)
//!   zrocks <dbpath> scan                print "key\tvalue" per line
//!   zrocks <dbpath> bench [n]           put+get n keys (default 10000), timed
//!
//! The DB lives in a directory `dbpath` created under the current working
//! directory; a `RealEnv` over `std.Io.Dir.cwd()` provides the filesystem
//! capability.  Data goes to stdout via `std.Io`; usage/diagnostics use
//! `std.debug.print` (stderr).

const std = @import("std");
const zrocks = @import("zrocks");

const DB = zrocks.db.DB;
const Options = zrocks.options.Options;
const RealEnv = zrocks.env.RealEnv;

const usage =
    \\usage:
    \\  zrocks <dbpath> put <key> <value>
    \\  zrocks <dbpath> get <key>
    \\  zrocks <dbpath> scan
    \\  zrocks <dbpath> bench [n]
    \\
;

/// Write `bytes` to stdout, swallowing errors (a broken pipe should not crash).
fn stdoutWrite(io: std.Io, bytes: []const u8) void {
    std.Io.File.stdout().writeStreamingAll(io, bytes) catch {};
}

pub fn main(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    const io = init.io;

    // Collect args into a slice we can index.  `init.minimal.args` is the raw
    // process args; iterate with the gpa-backed iterator (cross-platform).
    var arg_it = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer arg_it.deinit();

    var args: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (args.items) |a| gpa.free(a);
        args.deinit(gpa);
    }
    while (arg_it.next()) |a| try args.append(gpa, try gpa.dupe(u8, a));

    // args[0] = program name; need at least program + dbpath + command.
    if (args.items.len < 3) {
        std.debug.print("{s}", .{usage});
        return 2;
    }

    const dbpath = args.items[1];
    const command = args.items[2];

    // RealEnv rooted at the current working directory; the DB makes `dbpath` a
    // subdirectory of it.
    var re = RealEnv.init(io, std.Io.Dir.cwd());
    const e = re.env();

    if (std.mem.eql(u8, command, "put")) {
        if (args.items.len != 5) {
            std.debug.print("put requires <key> <value>\n{s}", .{usage});
            return 2;
        }
        const db = try DB.open(gpa, e, dbpath, .{});
        defer db.close();
        try db.put(.{ .sync = true }, args.items[3], args.items[4]);
        return 0;
    } else if (std.mem.eql(u8, command, "get")) {
        if (args.items.len != 4) {
            std.debug.print("get requires <key>\n{s}", .{usage});
            return 2;
        }
        const db = try DB.open(gpa, e, dbpath, .{});
        defer db.close();
        const got = try db.get(.{}, args.items[3]);
        if (got) |v| {
            defer gpa.free(v);
            stdoutWrite(io, v);
            stdoutWrite(io, "\n");
            return 0;
        } else {
            std.debug.print("(not found)\n", .{});
            return 1;
        }
    } else if (std.mem.eql(u8, command, "scan")) {
        if (args.items.len != 3) {
            std.debug.print("scan takes no extra args\n{s}", .{usage});
            return 2;
        }
        const db = try DB.open(gpa, e, dbpath, .{});
        defer db.close();
        var it = try db.newIterator(gpa, .{});
        defer it.deinit();
        var line: std.ArrayListUnmanaged(u8) = .empty;
        defer line.deinit(gpa);
        it.seekToFirst();
        while (it.valid()) : (it.next()) {
            line.clearRetainingCapacity();
            try line.appendSlice(gpa, it.key());
            try line.append(gpa, '\t');
            try line.appendSlice(gpa, it.value());
            try line.append(gpa, '\n');
            stdoutWrite(io, line.items);
        }
        if (it.status()) |err| return err;
        return 0;
    } else if (std.mem.eql(u8, command, "bench")) {
        const n: usize = if (args.items.len >= 4)
            std.fmt.parseInt(usize, args.items[3], 10) catch {
                std.debug.print("bench n must be an integer\n{s}", .{usage});
                return 2;
            }
        else
            10000;
        return bench(gpa, e, dbpath, n, io);
    }

    std.debug.print("unknown command: {s}\n{s}", .{ command, usage });
    return 2;
}

/// Put `n` keys, time it, then read them back; print ops/sec for each phase.
fn bench(gpa: std.mem.Allocator, e: zrocks.env.Env, dbpath: []const u8, n: usize, io: std.Io) !u8 {
    const db = try DB.open(gpa, e, dbpath, .{});
    defer db.close();

    // --- write phase -------------------------------------------------------
    const t_write_start = std.Io.Timestamp.now(io, .awake);
    {
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var kbuf: [32]u8 = undefined;
            var vbuf: [64]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>12}", .{i});
            const v = try std.fmt.bufPrint(&vbuf, "value-{d:0>12}-payload", .{i});
            try db.put(.{}, k, v);
        }
    }
    const write_ns = elapsedNs(t_write_start, std.Io.Timestamp.now(io, .awake));

    // --- read phase --------------------------------------------------------
    const t_read_start = std.Io.Timestamp.now(io, .awake);
    var found: usize = 0;
    {
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var kbuf: [32]u8 = undefined;
            const k = try std.fmt.bufPrint(&kbuf, "key{d:0>12}", .{i});
            if (try db.get(.{}, k)) |v| {
                found += 1;
                gpa.free(v);
            }
        }
    }
    const read_ns = elapsedNs(t_read_start, std.Io.Timestamp.now(io, .awake));

    const write_ops = opsPerSec(n, write_ns);
    const read_ops = opsPerSec(n, read_ns);

    var buf: [256]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf,
        \\bench: {d} ops
        \\  write: {d} ms  ({d} ops/sec)
        \\  read:  {d} ms  ({d} ops/sec, {d} found)
        \\
    , .{
        n,
        write_ns / std.time.ns_per_ms,
        write_ops,
        read_ns / std.time.ns_per_ms,
        read_ops,
        found,
    }) catch return 0;
    stdoutWrite(io, msg);
    return 0;
}

fn opsPerSec(n: usize, ns: u64) u64 {
    if (ns == 0) return 0;
    return @intCast((@as(u128, n) * std.time.ns_per_s) / ns);
}

/// Nonnegative nanoseconds elapsed from `start` to `end` on the same clock.
fn elapsedNs(start: std.Io.Timestamp, end: std.Io.Timestamp) u64 {
    const d = start.durationTo(end).nanoseconds;
    return if (d > 0) @intCast(d) else 0;
}
