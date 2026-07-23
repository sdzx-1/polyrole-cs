const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;
const push = @import("push.zig");

pub const Info = ProtocolInfo("chat", ClientContext, ServerContext);

pub const ClientContext = struct {
    input_ch: *zio.Channel([]const u8),
};

pub const ServerContext = struct {
    gpa: std.mem.Allocator,
    board: *push.SharedBoard,
    username: []const u8,
};

pub const Message = push.Message;
pub const MsgPayload = struct { text: []const u8 };

/// Persistent loop: Say.send → Say.send → ... → input_ch closed → Say.quit
pub const Say = union(enum) {
    send: Data(MsgPayload, @This()),
    quit: Data(void, Exit),

    pub const info: Info = .{ .agent = .client, .name = "Say" };

    pub fn process(ctx: *ClientContext) @This() {
        const text = ctx.input_ch.receive() catch return .quit;
        return .{ .send = .{ .data = .{ .text = text } } };
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        switch (result) {
            .send => |d| {
                const from = ctx.gpa.dupe(u8, ctx.username) catch return;
                const text = ctx.gpa.dupe(u8, d.data.text) catch {
                    ctx.gpa.free(from);
                    return;
                };
                ctx.board.append(.{ .kind = push.KIND_MSG, .from = from, .text = text });
            },
            .quit => {},
        }
    }
};
