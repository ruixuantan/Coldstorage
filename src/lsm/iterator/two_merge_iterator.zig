const std = @import("std");
const Iterator = @import("iterator.zig").Iterator;
const IteratorError = Iterator.IteratorError;

pub const TwoMergeIterator = struct {
    a: *Iterator,
    b: *Iterator,
    gpa: std.mem.Allocator,

    pub fn init(a: Iterator, b: Iterator, gpa: std.mem.Allocator) IteratorError!TwoMergeIterator {
        const a_ptr = try gpa.create(Iterator);
        a_ptr.* = a;
        const b_ptr = try gpa.create(Iterator);
        b_ptr.* = b;
        return .{ .a = a_ptr, .b = b_ptr, .gpa = gpa };
    }

    pub fn deinit(self: *TwoMergeIterator) void {
        self.a.deinit(self.gpa);
        self.b.deinit(self.gpa);
        self.gpa.destroy(self.a);
        self.gpa.destroy(self.b);
    }

    fn get_return_itr(self: TwoMergeIterator) *Iterator {
        std.debug.assert(self.a.is_valid() or self.b.is_valid());
        if (self.a.is_valid() and !self.b.is_valid()) {
            return self.a;
        } else if (!self.a.is_valid() and self.b.is_valid()) {
            return self.b;
        }
        const order = std.mem.order(u8, self.a.key(), self.b.key());
        switch (order) {
            .lt, .eq => return self.a,
            .gt => return self.b,
        }
    }

    pub fn key(self: TwoMergeIterator) []const u8 {
        const itr = self.get_return_itr();
        return itr.key();
    }

    pub fn val(self: TwoMergeIterator) []const u8 {
        const itr = self.get_return_itr();
        return itr.val();
    }

    pub fn is_valid(self: TwoMergeIterator) bool {
        return self.a.is_valid() or self.b.is_valid();
    }

    pub fn next(self: *TwoMergeIterator) IteratorError!void {
        if (!self.a.is_valid() and !self.b.is_valid()) {
            return;
        } else if (!self.a.is_valid()) {
            try self.b.next();
        } else if (!self.b.is_valid()) {
            try self.a.next();
        } else {
            const order = std.mem.order(u8, self.a.key(), self.b.key());
            switch (order) {
                .lt => try self.a.next(),
                .gt => try self.b.next(),
                .eq => {
                    try self.a.next();
                    try self.b.next();
                },
            }
        }
    }
};
