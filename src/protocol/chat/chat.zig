const std = @import("std");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;

pub const Info = ProtocolInfo("chat", ClientContext, ServerContext);
pub const MaxTextLen = 256;

pub const ClientContext = struct {
    /// Next message text to send
    pending_text: ?[]const u8 = null,
    /// Set to true to exit the chat loop
    done: bool = false,
};

pub const ServerContext = struct {
    gpa: std.mem.Allocator,
    messages: *std.ArrayList(Message),
    username: []const u8 = "",
};

pub const Message = struct {
    from: []const u8,
    text: []const u8,
};

pub const MsgPayload = struct {
    text: [MaxTextLen]u8,
    text_len: usize,
};

pub const Say = union(enum) {
    send: Data(MsgPayload, Ack),
    quit: Data(void, Exit),

    pub const info: Info = .{ .agent = .client, .name = "Say" };

    pub fn process(ctx: *ClientContext) @This() {
        if (ctx.done) return .quit;
        if (ctx.pending_text) |text| {
            var buf: [MaxTextLen]u8 = undefined;
            const copy_len = @min(text.len, MaxTextLen);
            @memcpy(buf[0..copy_len], text[0..copy_len]);
            ctx.pending_text = null;
            return .{ .send = .{ .data = .{ .text = buf, .text_len = copy_len } } };
        }
        return .quit;
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        switch (result) {
            .send => |d| {
                const text = d.data.text[0..d.data.text_len];
                const from_dup = ctx.gpa.dupe(u8, ctx.username) catch return;
                const text_dup = ctx.gpa.dupe(u8, text) catch return;
                ctx.messages.append(ctx.gpa, .{
                    .from = from_dup,
                    .text = text_dup,
                }) catch {};
            },
            .quit => {},
        }
    }
};

pub const Ack = union(enum) {
    ok: Data(void, Say),

    pub const info: Info = .{ .agent = .server, .name = "Ack" };

    pub fn process(ctx: *ServerContext) @This() {
        _ = ctx;
        return .ok;
    }

    pub fn preprocess(ctx: *ClientContext, result: @This()) void {
        _ = ctx;
        _ = result;
    }
};
