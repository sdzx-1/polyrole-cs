const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;

pub const Info = ProtocolInfo("push", ClientContext, ServerContext);

pub const ClientContext = struct {
    gpa: std.mem.Allocator,
    recv: *std.ArrayList(Message),
};

pub const ServerContext = struct {
    board_ch: *zio.Channel(Message),
    kick: bool = false,
};

pub const Message = struct { kind: u8, from: []const u8, text: []const u8 };
pub const Kind = u8;
pub const KIND_MSG: u8 = 1;
pub const KIND_JOIN: u8 = 2;

pub const ItemPayload = struct { kind: u8, from: []const u8, text: []const u8 };

/// Persistent loop: Push.item → Ack.ok → Push.item → ... → board_ch closed → Push.kick
pub const Push = union(enum) {
    item: Data(ItemPayload, Ack),
    kick: Data(void, Exit),

    pub const info: Info = .{ .agent = .server, .name = "Push" };

    pub fn process(ctx: *ServerContext) @This() {
        if (ctx.kick) return .kick;
        const msg = ctx.board_ch.receive() catch return .kick;
        return .{ .item = .{ .data = .{ .kind = msg.kind, .from = msg.from, .text = msg.text } } };
    }

    pub fn preprocess(ctx: *ClientContext, result: @This()) void {
        switch (result) {
            .item => |d| {
                const from = ctx.gpa.dupe(u8, d.data.from) catch return;
                const text = ctx.gpa.dupe(u8, d.data.text) catch return;
                ctx.recv.append(ctx.gpa, .{ .kind = d.data.kind, .from = from, .text = text }) catch {};
            },
            .kick => {},
        }
    }
};

pub const Ack = union(enum) {
    ok: Data(void, Push),

    pub const info: Info = .{ .agent = .client, .name = "Ack" };

    pub fn process(ctx: *ClientContext) @This() {
        _ = ctx;
        return .ok;
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        _ = ctx;
        _ = result;
    }
};
