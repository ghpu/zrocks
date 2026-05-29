const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Public module: consumers import this as "zrocks".
    const mod = b.addModule("zrocks", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Static library artifact — the primary deliverable.
    const lib = b.addLibrary(.{
        .name = "zrocks",
        .root_module = mod,
        .linkage = .static,
    });
    b.installArtifact(lib);

    // CLI executable: a small command-line tool over a real on-disk DB.  It
    // imports the library via `@import("zrocks")`.
    const exe = b.addExecutable(.{
        .name = "zrocks",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zrocks", .module = mod }},
        }),
    });
    b.installArtifact(exe);

    // `zig build run -- <args>` runs the CLI.
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run the zrocks CLI");
    run_step.dependOn(&run_cmd.step);

    // Bench executable: ReleaseFast harness with parameterised workloads.
    const bench_exe = b.addExecutable(.{
        .name = "zrocks-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/bench_main.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{.{ .name = "zrocks", .module = mod }},
        }),
    });
    b.installArtifact(bench_exe);

    // `zig build bench -- [ARGS]` runs the bench harness.
    const bench_run = b.addRunArtifact(bench_exe);
    bench_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| bench_run.addArgs(args);
    const bench_step = b.step("bench", "Run the zrocks bench harness");
    bench_step.dependOn(&bench_run.step);

    // Test step — runs both the in-src tests and the public-API integration
    // test (rooted at tests/integration_db_test.zig, importing the "zrocks"
    // module) plus the bench harness tests.
    const mod_tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(mod_tests);

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration_db_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zrocks", .module = mod }},
        }),
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);

    // D1c self-consistency interop regression gate (rooted at
    // tests/interop_selfconsistency_test.zig, importing the "zrocks" module):
    // locks in the RealEnv path model + kNewFile4 MANIFEST round-trip + a real
    // on-disk write/reopen/read cycle.
    const interop_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/interop_selfconsistency_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zrocks", .module = mod }},
        }),
    });
    const run_interop_tests = b.addRunArtifact(interop_tests);

    // Wave A external-LevelDB read-interop gate (rooted at
    // tests/leveldb_interop_test.zig, importing the "zrocks" module): generates
    // a byte-valid external-LevelDB fixture (CURRENT + MANIFEST + WAL, no SSTs)
    // and opens it read_only to prove non-destructive foreign-DB reads.
    const leveldb_interop_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/leveldb_interop_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zrocks", .module = mod }},
        }),
    });
    const run_leveldb_interop_tests = b.addRunArtifact(leveldb_interop_tests);

    // Wave B core real-RocksDB read-interop gate (rooted at
    // tests/rocksdb_interop_test.zig, importing the "zrocks" module): opens a
    // committed genuine RocksDB v11.4.0 database read_only and reads every live
    // key.  The absolute fixture path is injected as a build option so the test
    // is independent of the process working directory.
    const rocksdb_fixture_opts = b.addOptions();
    rocksdb_fixture_opts.addOption(
        []const u8,
        "rocksdb_fixture_path",
        b.pathFromRoot("tests/fixtures/rocksdb/basic"),
    );
    const rocksdb_interop_mod = b.createModule(.{
        .root_source_file = b.path("tests/rocksdb_interop_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "zrocks", .module = mod }},
    });
    rocksdb_interop_mod.addOptions("build_options", rocksdb_fixture_opts);
    const rocksdb_interop_tests = b.addTest(.{ .root_module = rocksdb_interop_mod });
    const run_rocksdb_interop_tests = b.addRunArtifact(rocksdb_interop_tests);

    // rocksdb-write CI-safe self-consistency gate (rooted at
    // tests/rocksdb_write_interop_test.zig, importing the "zrocks" module):
    // writes a fully-flushed DB in the opt-in `.rocksdb` dialect, re-reads it via
    // a fresh zrocks open (round-trip through the RocksDB-read path), and asserts
    // the on-disk SST/MANIFEST carry the RocksDB form.  No external RocksDB.
    const rocksdb_write_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/rocksdb_write_interop_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zrocks", .module = mod }},
        }),
    });
    const run_rocksdb_write_tests = b.addRunArtifact(rocksdb_write_tests);

    // Bench harness tests (bench_main.zig imports zrocks and has embedded tests).
    const bench_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/bench_main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zrocks", .module = mod }},
        }),
    });
    const run_bench_tests = b.addRunArtifact(bench_tests);

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_tests.step);
    test_step.dependOn(&run_integration_tests.step);
    test_step.dependOn(&run_interop_tests.step);
    test_step.dependOn(&run_leveldb_interop_tests.step);
    test_step.dependOn(&run_rocksdb_interop_tests.step);
    test_step.dependOn(&run_rocksdb_write_tests.step);
    test_step.dependOn(&run_bench_tests.step);
}
