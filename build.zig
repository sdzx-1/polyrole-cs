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

    // ── 库测试 ─────────────────────────────────────────────────────
    const mod_tests = b.addTest(.{
        .root_module = mod,
        .use_llvm = true,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    // ── 聊天室 demo ─────────────────────────────────────────────────
    const chat_server = b.addExecutable(.{
        .name = "chat-server",
        .use_llvm = true,
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/chat/server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "polyrole_cs", .module = mod },
                .{ .name = "zio", .module = zio.module("zio") },
            },
        }),
    });
    b.installArtifact(chat_server);

    const chat_client = b.addExecutable(.{
        .name = "chat-client",
        .use_llvm = true,
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/chat/client.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "polyrole_cs", .module = mod },
                .{ .name = "zio", .module = zio.module("zio") },
            },
        }),
    });
    b.installArtifact(chat_client);

    const chat_loadtest = b.addExecutable(.{
        .name = "chat-loadtest",
        .use_llvm = true,
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/chat_loadtest.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "polyrole_cs", .module = mod },
                .{ .name = "zio", .module = zio.module("zio") },
                .{ .name = "chat", .module = b.createModule(.{
                    .root_source_file = b.path("examples/chat/protocol.zig"),
                    .target = target,
                    .optimize = optimize,
                    .imports = &.{
                        .{ .name = "polyrole_cs", .module = mod },
                        .{ .name = "zio", .module = zio.module("zio") },
                    },
                }) },
            },
        }),
    });
    b.installArtifact(chat_loadtest);

    const chat_tests = b.addTest(.{
        .use_llvm = true,
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/chat/test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "polyrole_cs", .module = mod },
                .{ .name = "zio", .module = zio.module("zio") },
            },
        }),
    });
    const run_chat_tests = b.addRunArtifact(chat_tests);
    test_step.dependOn(&run_chat_tests.step);

    // ── 状态图工具 ──────────────────────────────────────────────────
    const chat_protocol = b.createModule(.{
        .root_source_file = b.path("examples/chat/protocol.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "polyrole_cs", .module = mod },
            .{ .name = "zio", .module = zio.module("zio") },
        },
    });

    const graph = b.addExecutable(.{
        .name = "graph",
        .use_llvm = true,
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/graph.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "polyrole_cs", .module = mod },
                .{ .name = "zio", .module = zio.module("zio") },
                .{ .name = "chat", .module = chat_protocol },
            },
        }),
    });

    const run_graph = b.addRunArtifact(graph);
    const graph_step = b.step("graph", "Generate protocol state graphs (DOT)");
    graph_step.dependOn(&run_graph.step);

    inline for (.{ "tls", "net_monitor", "chat_ctrl", "chat_push" }) |name| {
        const dot_to_png = b.addSystemCommand(&.{ "dot", "-Tpng", "-o", "docs/graphs/" ++ name ++ ".png", "docs/graphs/" ++ name ++ ".dot" });
        dot_to_png.step.dependOn(&run_graph.step);
        graph_step.dependOn(&dot_to_png.step);
    }
}
