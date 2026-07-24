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

    // ── Chat example module ─────────────────────────────────────────
    const chat_example = b.createModule(.{
        .root_source_file = b.path("examples/chat/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zio", .module = zio.module("zio") },
            .{ .name = "polyrole_cs", .module = mod },
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

    // ── Chat example tests ──────────────────────────────────────────
    const chat_example_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/chat/test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zio", .module = zio.module("zio") },
                .{ .name = "polyrole_cs", .module = mod },
            },
        }),
        .use_llvm = true,
    });

    var run_chat_tests = b.addRunArtifact(chat_example_tests);
    test_step.dependOn(&run_chat_tests.step);

    // ── Graph tool ──────────────────────────────────────────────────
    const graph_chat = b.addExecutable(.{
        .name = "graph_chat",
        .use_llvm = true,
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/graph_chat.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "polyrole_cs", .module = mod },
                .{ .name = "chat_example", .module = chat_example },
            },
        }),
    });

    const run_graph_chat = b.addRunArtifact(graph_chat);
    const graph_step = b.step("graph", "Generate chat protocol state graphs (DOT + PNG)");
    graph_step.dependOn(&run_graph_chat.step);

    inline for (.{ "chat_init", "chat_say", "chat_push", "tls", "net_monitor" }) |name| {
        const dot_to_png = b.addSystemCommand(&.{ "dot", "-Tpng", "-o", "docs/graphs/" ++ name ++ ".png", "docs/graphs/" ++ name ++ ".dot" });
        dot_to_png.step.dependOn(&run_graph_chat.step);
        graph_step.dependOn(&dot_to_png.step);
    }
}
