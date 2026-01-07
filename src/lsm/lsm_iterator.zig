const std = @import("std");
const TwoMergeIterator = @import("iterator.zig").TwoMergeIterator;
const Kv = @import("kv.zig").Kv;

pub const LsmIterator = struct {
    inner: TwoMergeIterator,
    end: []const u8,

    pub fn init(iterator: TwoMergeIterator, end: []const u8) LsmIterator {
        return LsmIterator{
            .inner = iterator,
            .end = end,
        };
    }

    pub fn deinit(self: *LsmIterator) void {
        self.inner.deinit();
    }

    pub fn next(self: *LsmIterator) !?Kv {
        var res = try self.inner.next();
        while (res != null) : (res = try self.inner.next()) {
            if (std.mem.order(u8, res.?.key, self.end) != .lt) {
                return null;
            }
            if (res.?.val.len > 0) {
                return res;
            }
        }
        return null;
    }
};
