const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zio = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });

    const mod = b.addModule("polyrole_cs", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zio", .module = zio.module("zio") },
        },
    });

    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/test/test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zio", .module = zio.module("zio") },
            .{ .name = "polyrole_cs", .module = mod },
        },
    });

    const mod_tests = b.addTest(.{
        .root_module = test_mod,
        .use_llvm = true,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    // 模块内部测试：收集 polyrole_cs 模块内（root.zig/runner.zig 等文件内）
    // 的 test 块——可访问私有函数，如 Mux 的 dispatchFrames。
    const internal_tests = b.addTest(.{
        .root_module = mod,
        .use_llvm = true,
    });

    const run_internal_tests = b.addRunArtifact(internal_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_internal_tests.step);
}
