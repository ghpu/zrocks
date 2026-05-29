//! bench_main.zig — ReleaseFast bench harness for zrocks.
//!
//! Measures raw write and read throughput with parameterised workloads.  Runs
//! two phases by default:
//!   1. WRITE — put `writes` random key/value pairs (key = u64 decimal, value =
//!              `value_size` random bytes from a Xoshiro256 PRNG seeded by
//!              `--seed`).
//!   2. READ  — get `reads` random keys chosen uniformly from [0, writes).
//!
//! The block cache is optionally wired via `Options.block_cache` so block cache
//! hit-rate uplift is measurable across different cache sizes.
//!
//! Usage (via `zig build bench -- [ARGS]`):
//!   --keys      N   Total key-space size (default 100_000)
//!   --writes    N   Keys to write in the write phase (default = keys)
//!   --reads     N   Lookups in the read  phase (default = keys)
//!   --value-size B  Value size in bytes  (default 128)
//!   --block-cache-mb M  LRU block cache capacity in MiB (default 0 = off)
//!   --seed      S   PRNG seed (default 0xdeadbeef)
//!   --db-path   P   DB directory path (default "./bench.db")
//!
//! When used as a library (imported by tests), only `BenchConfig` and `runBench`
//! are public; `main` is only present when this file is the root.

const std = @import("std");
const zrocks = @import("zrocks");

const DB = zrocks.db.DB;
const Options = zrocks.options.Options;
const Cache = zrocks.options.Cache;
const RealEnv = zrocks.env.RealEnv;

// ---------------------------------------------------------------------------
// BenchConfig — all workload parameters in one plain struct
// ---------------------------------------------------------------------------

pub const BenchConfig = struct {
    /// Total key-space size: keys are drawn uniformly from [0, keys).
    keys: usize = 100_000,
    /// Number of write operations.
    writes: ?usize = null,
    /// Number of read operations.
    reads: ?usize = null,
    /// Value payload size in bytes.
    value_size: usize = 128,
    /// LRU block cache capacity in bytes (0 = disabled).
    block_cache_bytes: usize = 0,
    /// PRNG seed for deterministic / reproducible runs.
    seed: u64 = 0xdeadbeef,
    /// DB path. Caller owns the memory; must outlive the call.
    db_path: []const u8 = "bench.db",

    fn writeCount(self: BenchConfig) usize {
        return self.writes orelse self.keys;
    }
    fn readCount(self: BenchConfig) usize {
        return self.reads orelse self.keys;
    }
};

// ---------------------------------------------------------------------------
// BenchResult — per-phase throughput numbers
// ---------------------------------------------------------------------------

pub const BenchResult = struct {
    writes: usize,
    write_ns: u64,
    reads: usize,
    read_ns: u64,
    found: usize,

    pub fn writeOpsPerSec(self: BenchResult) u64 {
        if (self.write_ns == 0) return 0;
        return @intCast((@as(u128, self.writes) * std.time.ns_per_s) / self.write_ns);
    }
    pub fn readOpsPerSec(self: BenchResult) u64 {
        if (self.read_ns == 0) return 0;
        return @intCast((@as(u128, self.reads) * std.time.ns_per_s) / self.read_ns);
    }
};

// ---------------------------------------------------------------------------
// runBench — the core workload; reusable from tests
// ---------------------------------------------------------------------------

/// Run a parameterised bench against a DB opened at `cfg.db_path` on a
/// `RealEnv` rooted at `base_dir`.  Returns per-phase throughput numbers.
/// The caller is responsible for cleaning up `cfg.db_path` in `base_dir`.
///
/// IMPORTANT: `io` must be a live `std.Io` instance (use `std.Io.Threaded`
/// from a real `main`, or the global `std.testing.io` from tests).
pub fn runBench(
    gpa: std.mem.Allocator,
    io: std.Io,
    base_dir: std.Io.Dir,
    cfg: BenchConfig,
) !BenchResult {
    // --- block cache (optional) -------------------------------------------
    var cache_storage: Cache = undefined;
    var cache_ptr: ?*Cache = null;
    if (cfg.block_cache_bytes > 0) {
        cache_storage = Cache.init(gpa, cfg.block_cache_bytes);
        cache_ptr = &cache_storage;
    }
    defer if (cache_ptr) |cp| cp.deinit();

    // --- env + open DB -------------------------------------------------------
    var re = RealEnv.init(io, base_dir);
    const e = re.env();

    const db = try DB.open(gpa, e, cfg.db_path, .{
        .create_if_missing = true,
        .block_cache = cache_ptr,
    });
    defer db.close();

    // --- PRNG ----------------------------------------------------------------
    var prng = std.Random.DefaultPrng.init(cfg.seed);
    const rng = prng.random();

    const n_writes = cfg.writeCount();
    const n_reads = cfg.readCount();
    const key_space: u64 = @intCast(cfg.keys);

    // Pre-allocate value buffer.
    const val_buf = try gpa.alloc(u8, cfg.value_size);
    defer gpa.free(val_buf);

    // Key buffer: enough for "key<20-digit decimal>" plus NUL.
    var key_buf: [32]u8 = undefined;

    // ---- WRITE PHASE --------------------------------------------------------
    const t_write_start = std.Io.Timestamp.now(io, .awake);
    {
        var i: usize = 0;
        while (i < n_writes) : (i += 1) {
            const k_idx = rng.uintLessThan(u64, key_space);
            const k = std.fmt.bufPrint(&key_buf, "{d}", .{k_idx}) catch unreachable;
            rng.bytes(val_buf);
            try db.put(.{}, k, val_buf);
        }
    }
    const write_ns = elapsedNs(t_write_start, std.Io.Timestamp.now(io, .awake));

    // ---- READ PHASE ---------------------------------------------------------
    const t_read_start = std.Io.Timestamp.now(io, .awake);
    var found: usize = 0;
    {
        var i: usize = 0;
        while (i < n_reads) : (i += 1) {
            const k_idx = rng.uintLessThan(u64, key_space);
            const k = std.fmt.bufPrint(&key_buf, "{d}", .{k_idx}) catch unreachable;
            if (try db.get(.{}, k)) |v| {
                found += 1;
                gpa.free(v);
            }
        }
    }
    const read_ns = elapsedNs(t_read_start, std.Io.Timestamp.now(io, .awake));

    return BenchResult{
        .writes = n_writes,
        .write_ns = write_ns,
        .reads = n_reads,
        .read_ns = read_ns,
        .found = found,
    };
}

/// Nonnegative nanoseconds from `start` to `end` (same clock).
fn elapsedNs(start: std.Io.Timestamp, end: std.Io.Timestamp) u64 {
    const d = start.durationTo(end).nanoseconds;
    return if (d > 0) @intCast(d) else 0;
}

// ---------------------------------------------------------------------------
// CLI main — only compiled when this file IS the root source
// ---------------------------------------------------------------------------

pub fn main(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    const io = init.io;

    var arg_it = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer arg_it.deinit();

    var args: std.ArrayListUnmanaged([]const u8) = .empty;
    defer args.deinit(gpa);
    while (arg_it.next()) |a| try args.append(gpa, a);

    var cfg = BenchConfig{};
    var db_path_buf: [256]u8 = undefined;

    var i: usize = 1;
    while (i < args.items.len) : (i += 1) {
        const arg = args.items[i];
        if (std.mem.eql(u8, arg, "--keys")) {
            i += 1;
            if (i >= args.items.len) return cliErr("--keys requires a value");
            cfg.keys = std.fmt.parseInt(usize, args.items[i], 10) catch return cliErr("--keys: not an integer");
        } else if (std.mem.eql(u8, arg, "--writes")) {
            i += 1;
            if (i >= args.items.len) return cliErr("--writes requires a value");
            cfg.writes = std.fmt.parseInt(usize, args.items[i], 10) catch return cliErr("--writes: not an integer");
        } else if (std.mem.eql(u8, arg, "--reads")) {
            i += 1;
            if (i >= args.items.len) return cliErr("--reads requires a value");
            cfg.reads = std.fmt.parseInt(usize, args.items[i], 10) catch return cliErr("--reads: not an integer");
        } else if (std.mem.eql(u8, arg, "--value-size")) {
            i += 1;
            if (i >= args.items.len) return cliErr("--value-size requires a value");
            cfg.value_size = std.fmt.parseInt(usize, args.items[i], 10) catch return cliErr("--value-size: not an integer");
        } else if (std.mem.eql(u8, arg, "--block-cache-mb")) {
            i += 1;
            if (i >= args.items.len) return cliErr("--block-cache-mb requires a value");
            const mb = std.fmt.parseInt(usize, args.items[i], 10) catch return cliErr("--block-cache-mb: not an integer");
            cfg.block_cache_bytes = mb * 1024 * 1024;
        } else if (std.mem.eql(u8, arg, "--seed")) {
            i += 1;
            if (i >= args.items.len) return cliErr("--seed requires a value");
            cfg.seed = std.fmt.parseInt(u64, args.items[i], 10) catch return cliErr("--seed: not an integer");
        } else if (std.mem.eql(u8, arg, "--db-path")) {
            i += 1;
            if (i >= args.items.len) return cliErr("--db-path requires a value");
            const s = args.items[i];
            if (s.len >= db_path_buf.len) return cliErr("--db-path: path too long");
            @memcpy(db_path_buf[0..s.len], s);
            cfg.db_path = db_path_buf[0..s.len];
        } else {
            std.debug.print("unknown argument: {s}\n", .{arg});
            printUsage();
            return 2;
        }
    }
    const result = try runBench(gpa, io, std.Io.Dir.cwd(), cfg);

    var buf: [512]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf,
        \\bench results:
        \\  writes: {d}  time: {d} ms  throughput: {d} ops/sec
        \\  reads:  {d}  time: {d} ms  throughput: {d} ops/sec  found: {d}
        \\
    , .{
        result.writes,
        result.write_ns / std.time.ns_per_ms,
        result.writeOpsPerSec(),
        result.reads,
        result.read_ns / std.time.ns_per_ms,
        result.readOpsPerSec(),
        result.found,
    }) catch {
        std.debug.print("(result too large for buffer)\n", .{});
        return 0;
    };
    std.Io.File.stdout().writeStreamingAll(io, msg) catch {};
    return 0;
}

fn cliErr(msg: []const u8) u8 {
    std.debug.print("error: {s}\n", .{msg});
    printUsage();
    return 2;
}

fn printUsage() void {
    std.debug.print(
        \\usage: zrocks-bench [OPTIONS]
        \\  --keys N            key-space size (default 100000)
        \\  --writes N          write operations (default = keys)
        \\  --reads  N          read  operations (default = keys)
        \\  --value-size B      value size in bytes (default 128)
        \\  --block-cache-mb M  block cache size in MiB (default 0 = off)
        \\  --seed S            PRNG seed (default 0xdeadbeef)
        \\  --db-path P         DB directory path (default bench.db)
        \\
    , .{});
}

// ---------------------------------------------------------------------------
// Tests — verify the bench harness compiles and runs without errors
// ---------------------------------------------------------------------------

test "bench: small write+read workload completes without errors" {
    const gpa = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const result = try runBench(gpa, std.testing.io, tmp.dir, .{
        .keys = 100,
        .value_size = 32,
        .seed = 42,
        .db_path = "bench_test_basic",
    });

    // Sanity: wrote 100 keys, attempted 100 reads.
    try std.testing.expectEqual(@as(usize, 100), result.writes);
    try std.testing.expectEqual(@as(usize, 100), result.reads);
    // found <= writes
    try std.testing.expect(result.found <= result.writes);
    // BenchResult helpers don't panic
    _ = result.writeOpsPerSec();
    _ = result.readOpsPerSec();
}

test "bench: block cache wired via Options (non-null cache path)" {
    const gpa = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // 1 MiB block cache — exercises the block_cache != null path in runBench.
    const result = try runBench(gpa, std.testing.io, tmp.dir, .{
        .keys = 200,
        .value_size = 64,
        .block_cache_bytes = 1 * 1024 * 1024,
        .seed = 0xabcdef,
        .db_path = "bench_test_cache",
    });

    try std.testing.expectEqual(@as(usize, 200), result.writes);
    try std.testing.expectEqual(@as(usize, 200), result.reads);
}

test "bench: explicit writes/reads override keys" {
    const gpa = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const result = try runBench(gpa, std.testing.io, tmp.dir, .{
        .keys = 1000,
        .writes = 50,
        .reads = 30,
        .value_size = 16,
        .seed = 0x1234,
        .db_path = "bench_test_override",
    });

    try std.testing.expectEqual(@as(usize, 50), result.writes);
    try std.testing.expectEqual(@as(usize, 30), result.reads);
}

test "bench: BenchResult.writeOpsPerSec and readOpsPerSec are safe with zero duration" {
    const r = BenchResult{
        .writes = 1000,
        .write_ns = 0,
        .reads = 500,
        .read_ns = 0,
        .found = 0,
    };
    try std.testing.expectEqual(@as(u64, 0), r.writeOpsPerSec());
    try std.testing.expectEqual(@as(u64, 0), r.readOpsPerSec());
}

test "bench: BenchConfig defaults are sane" {
    const cfg = BenchConfig{};
    try std.testing.expectEqual(@as(usize, 100_000), cfg.keys);
    try std.testing.expectEqual(@as(usize, 128), cfg.value_size);
    try std.testing.expectEqual(@as(usize, 0), cfg.block_cache_bytes);
    // writeCount/readCount default to keys.
    try std.testing.expectEqual(@as(usize, 100_000), cfg.writeCount());
    try std.testing.expectEqual(@as(usize, 100_000), cfg.readCount());
}
