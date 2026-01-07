const std = @import("std");
const Iterator = @import("iterator.zig").Iterator;
const Kv = @import("../kv.zig").Kv;

pub const TwoMergeIterator = struct {
    const Node = ?Kv;

    a: *Iterator,
    b: *Iterator,
    a_node: Node = null,
    b_node: Node = null,
    gpa: std.mem.Allocator,

    pub fn init(a: Iterator, b: Iterator, gpa: std.mem.Allocator) !TwoMergeIterator {
        const a_ptr = try gpa.create(Iterator);
        a_ptr.* = a;
        const b_ptr = try gpa.create(Iterator);
        b_ptr.* = b;
        return TwoMergeIterator{ .a = a_ptr, .b = b_ptr, .gpa = gpa };
    }

    pub fn deinit(self: *TwoMergeIterator) void {
        self.a.deinit(self.gpa);
        self.b.deinit(self.gpa);
        self.gpa.destroy(self.a);
        self.gpa.destroy(self.b);
    }

    pub fn next(self: *TwoMergeIterator) !?Kv {
        self.a_node = try self.a.next();
        self.b_node = try self.b.next();
        if (self.a_node == null and self.b_node == null) {
            return null;
        } else if (self.a_node == null) {
            return self.b_node;
        } else if (self.b_node == null) {
            return self.a_node;
        }
        const order = std.mem.order(u8, self.a_node.?.key, self.b_node.?.key);
        switch (order) {
            .lt => {
                const output = self.a_node;
                self.a_node = try self.a.next();
                return output;
            },
            .gt => {
                const output = self.b_node;
                self.b_node = try self.b.next();
                return output;
            },
            .eq => {
                const output = self.a_node;
                self.a_node = try self.a.next();
                self.b_node = try self.b.next();
                return output;
            },
        }
    }
};
