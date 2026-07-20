const std = @import("std");
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
    msg: Message,
};

pub const Message = struct {
    kind: u8,
    from: []const u8,
    text: []const u8,
};

pub const Kind = u8;
pub const KIND_MSG: u8 = 1;
pub const KIND_JOIN: u8 = 2;
pub const KIND_LEAVE: u8 = 3;

pub const ItemPayload = struct { kind: u8, from: []const u8, text: []const u8 };

/// Server pushes one message, client acks.
pub const Push = union(enum) {
    item: Data(ItemPayload, Ack),
    kick: Data(void, Exit),

    pub const info: Info = .{ .agent = .server, .name = "Push" };

    pub fn process(ctx: *ServerContext) @This() {
        return .{ .item = .{ .data = .{ .kind = ctx.msg.kind, .from = ctx.msg.from, .text = ctx.msg.text } } };
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
    ok: Data(void, Exit),

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
