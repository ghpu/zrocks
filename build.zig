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

    // Test step — wire more per-phase test runs here in future milestones.
    const mod_tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(mod_tests);
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_tests.step);
}
