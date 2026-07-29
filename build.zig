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

    // ── Library tests ───────────────────────────────────────────────
    const mod_tests = b.addTest(.{
        .root_module = mod,
        .use_llvm = true,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    // ── Graph tool ──────────────────────────────────────────────────
    const graph = b.addExecutable(.{
        .name = "graph",
        .use_llvm = true,
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/graph.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "polyrole_cs", .module = mod },
            },
        }),
    });

    const run_graph = b.addRunArtifact(graph);
    const graph_step = b.step("graph", "Generate protocol state graphs (DOT)");
    graph_step.dependOn(&run_graph.step);

    inline for (.{ "tls", "net_monitor" }) |name| {
        const dot_to_png = b.addSystemCommand(&.{ "dot", "-Tpng", "-o", "docs/graphs/" ++ name ++ ".png", "docs/graphs/" ++ name ++ ".dot" });
        dot_to_png.step.dependOn(&run_graph.step);
        graph_step.dependOn(&dot_to_png.step);
    }
}
