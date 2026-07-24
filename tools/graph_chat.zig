const std = @import("std");
const polyrole = @import("polyrole_cs");
const Graph = polyrole.Graph;
const chat = @import("chat_example");

pub fn main(main_init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    const cwd = std.Io.Dir.cwd();
    const out_dir = "docs/graphs";
    cwd.createDir(main_init.io, out_dir, .default_dir) catch {};

    inline for (.{
        .{ "chat_init", chat.init.Send },
        .{ "chat_say", chat.say.Say },
        .{ "chat_push", chat.push.Sync },
        .{ "tls", polyrole.tls.ClientHello },
        .{ "net_monitor", polyrole.net_monitor.PingQuery },
    }) |entry| {
        const name, const State = entry;
        var graph = try Graph.initWithFsm(allocator, State);
        defer graph.deinit();

        const dot_path = out_dir ++ "/" ++ name ++ ".dot";
        const file = try cwd.createFile(main_init.io, dot_path, .{});
        defer file.close(main_init.io);
        var w = file.writer(main_init.io, &.{});
        try graph.generateDot(.{}, &w.interface);
        try w.interface.flush();
    }
}
