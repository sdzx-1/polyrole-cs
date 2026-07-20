const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;

pub const Info = ProtocolInfo("init", ClientContext, ServerContext);

pub const ClientContext = struct {
    username: []const u8,
    accepted: bool = false,
};

pub const ServerContext = struct {
    users: *std.StringHashMap(void),
    mu: *zio.Mutex,
    pending_name: []const u8 = "",
};

pub const NamePayload = struct { name: []const u8 };

pub const Send = union(enum) {
    propose: Data(NamePayload, Reply),
    quit: Data(void, Exit),

    pub const info: Info = .{ .agent = .client, .name = "Send" };

    pub fn process(ctx: *ClientContext) @This() {
        if (ctx.username.len == 0) return .quit;
        return .{ .propose = .{ .data = .{ .name = ctx.username } } };
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        switch (result) {
            .propose => |d| ctx.pending_name = d.data.name,
            .quit => {},
        }
    }
};

pub const Reply = union(enum) {
    accept: Data(void, Exit),
    reject: Data(void, Exit),

    pub const info: Info = .{ .agent = .server, .name = "Reply" };

    pub fn process(ctx: *ServerContext) @This() {
        ctx.mu.lockUncancelable();
        defer ctx.mu.unlock();
        if (ctx.users.contains(ctx.pending_name)) return .reject;
        ctx.users.put(ctx.pending_name, {}) catch unreachable;
        return .accept;
    }

    pub fn preprocess(ctx: *ClientContext, result: @This()) void {
        switch (result) {
            .accept => ctx.accepted = true,
            .reject => {},
        }
    }
};
