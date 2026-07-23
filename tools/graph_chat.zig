const std = @import("std");
const polyrole = @import("polyrole_cs");
const Graph = polyrole.Graph;
const init = polyrole.chat_proto.init;
const say = polyrole.chat_proto.say;
const push = polyrole.chat_proto.push;

pub fn main(main_init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    const cwd = std.Io.Dir.cwd();

    {
        var graph = try Graph.initWithFsm(allocator, init.Send);
        defer graph.deinit();
        const file = try cwd.createFile(main_init.io, "chat_init.dot", .{});
        defer file.close(main_init.io);
        var w = file.writer(main_init.io, &.{});
        try graph.generateDot(null, &w.interface);
        try w.interface.flush();
    }

    {
        var graph = try Graph.initWithFsm(allocator, say.Say);
        defer graph.deinit();
        const file = try cwd.createFile(main_init.io, "chat_say.dot", .{});
        defer file.close(main_init.io);
        var w = file.writer(main_init.io, &.{});
        try graph.generateDot(null, &w.interface);
        try w.interface.flush();
    }

    {
        var graph = try Graph.initWithFsm(allocator, push.Sync);
        defer graph.deinit();
        const file = try cwd.createFile(main_init.io, "chat_push.dot", .{});
        defer file.close(main_init.io);
        var w = file.writer(main_init.io, &.{});
        try graph.generateDot(null, &w.interface);
        try w.interface.flush();
    }
}
