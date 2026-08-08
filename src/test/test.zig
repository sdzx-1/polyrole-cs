// 测试根：收集 src/test/ 下所有测试文件。
// 由 build.zig 的 `zig build test` 目标编译运行。
comptime {
    _ = @import("codec_test.zig");
    _ = @import("quickstart_test.zig");
    _ = @import("channel_test.zig");
    _ = @import("runner_test.zig");
    _ = @import("tls_test.zig");
    _ = @import("net_monitor_test.zig");
}
