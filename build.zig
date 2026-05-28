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

    // Test step — runs both the in-src tests and the public-API integration
    // test (rooted at tests/integration_db_test.zig, importing the "zrocks"
    // module).
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

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_tests.step);
    test_step.dependOn(&run_integration_tests.step);
}
