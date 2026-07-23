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

    const mod_tests = b.addTest(.{
        .root_module = mod,
        .use_llvm = true,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    const graph_chat = b.addExecutable(.{
        .name = "graph_chat",
        .use_llvm = true,
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/graph_chat.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "polyrole_cs", .module = mod },
            },
        }),
    });

    const run_graph_chat = b.addRunArtifact(graph_chat);
    const graph_step = b.step("graph", "Generate chat protocol state graphs (DOT format)");
    graph_step.dependOn(&run_graph_chat.step);
}
