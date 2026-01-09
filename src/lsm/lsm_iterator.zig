const std = @import("std");
const TwoMergeIterator = @import("iterator.zig").TwoMergeIterator;

pub const LsmIterator = struct {
    inner: TwoMergeIterator,
    end: []const u8,

    pub fn init(iterator: TwoMergeIterator, end: []const u8) !LsmIterator {
        var itr = LsmIterator{
            .inner = iterator,
            .end = end,
        };
        try itr.next();
        return itr;
    }

    pub fn deinit(self: *LsmIterator) void {
        self.inner.deinit();
    }

    pub fn key(self: *LsmIterator) []const u8 {
        return self.inner.key();
    }

    pub fn val(self: *LsmIterator) []const u8 {
        return self.inner.val();
    }

    pub fn is_valid(self: *LsmIterator) bool {
        return std.mem.order(u8, self.inner.key(), self.end) == .lt and self.inner.is_valid();
    }

    pub fn next(self: *LsmIterator) !void {
        if (std.mem.order(u8, self.inner.key(), self.end) != .lt) {
            return;
        }
        try self.inner.next();
        while (self.inner.is_valid()) : (try self.inner.next()) {
            if (std.mem.order(u8, self.inner.key(), self.end) != .lt) {
                return;
            }
            if (self.inner.val().len > 0) {
                return;
            }
        }
    }
};
