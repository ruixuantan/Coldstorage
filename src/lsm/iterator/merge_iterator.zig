const std = @import("std");
const mem = std.mem;
const memtable = @import("../memtable.zig");
const MemtableIterator = memtable.Memtable.MemtableIterator;
const Iterator = @import("iterator.zig").Iterator;
const Kv = @import("../kv.zig").Kv;

pub const MergeIterator = struct {
    const HeapNode = struct { itr_idx: usize, kv: Kv };

    fn compare(_: void, a: HeapNode, b: HeapNode) std.math.Order {
        const key_order = mem.order(u8, a.kv.key, b.kv.key);
        if (key_order != .eq) {
            return key_order;
        }
        return std.math.order(a.itr_idx, b.itr_idx);
    }

    const Heap = std.PriorityQueue(HeapNode, void, compare);

    gpa: mem.Allocator,
    heap: Heap,
    iterators: []Iterator,

    pub fn init(gpa: mem.Allocator, iterators: []Iterator) !MergeIterator {
        var heap = Heap.init(gpa, {});
        for (iterators, 0..) |*it, idx| {
            const n = try it.*.next();
            if (n) |kv| {
                heap.add(HeapNode{
                    .itr_idx = idx,
                    .kv = kv,
                }) catch @panic("PriorityQueue allocation failed");
            }
        }
        return MergeIterator{
            .gpa = gpa,
            .heap = heap,
            .iterators = iterators,
        };
    }

    pub fn deinit(self: *MergeIterator) void {
        for (self.iterators) |*it| {
            it.deinit(self.gpa);
        }
        self.gpa.free(self.iterators);
        self.heap.deinit();
    }

    // k-way merge
    pub fn next(self: *MergeIterator) !?Kv {
        if (self.heap.items.len == 0) {
            return null;
        }
        const min = self.heap.remove();
        var dup = self.heap.peek();
        while (dup != null and mem.eql(u8, min.kv.key, dup.?.kv.key)) {
            const rm = self.heap.remove();
            dup = self.heap.peek();
            const n = try self.iterators[rm.itr_idx].next();
            if (n) |kv| {
                try self.heap.add(HeapNode{ .itr_idx = rm.itr_idx, .kv = kv });
            }
        }
        const n = try self.iterators[min.itr_idx].next();
        if (n) |kv| {
            try self.heap.add(HeapNode{ .itr_idx = min.itr_idx, .kv = kv });
        }

        return Kv.init(min.kv.key, min.kv.val);
    }
};

test "MergeIterator: next" {
    const test_gpa = std.testing.allocator;
    var memtable1 = try memtable.Memtable.init(1, 1024, test_gpa);
    defer memtable1.deinit();
    var memtable2 = try memtable.Memtable.init(2, 1024, test_gpa);
    defer memtable2.deinit();
    var memtable3 = try memtable.Memtable.init(3, 1024, test_gpa);
    defer memtable3.deinit();

    try memtable1.put("b", "");
    try memtable1.put("c", "4");
    try memtable1.put("d", "5");
    try memtable2.put("a", "1");
    try memtable2.put("b", "2");
    try memtable2.put("c", "3");
    try memtable3.put("e", "4");

    const memtable_iter1_ptr = try test_gpa.create(MemtableIterator);
    memtable_iter1_ptr.* = memtable1.scan("a", "f");
    const memtable_iter2_ptr = try test_gpa.create(MemtableIterator);
    memtable_iter2_ptr.* = memtable2.scan("a", "f");
    const memtable_iter3_ptr = try test_gpa.create(MemtableIterator);
    memtable_iter3_ptr.* = memtable3.scan("a", "f");
    const itr1 = Iterator{ .memtable_iterator = memtable_iter1_ptr };
    const itr2 = Iterator{ .memtable_iterator = memtable_iter2_ptr };
    const itr3 = Iterator{ .memtable_iterator = memtable_iter3_ptr };
    var itrs = [_]Iterator{ itr1, itr2, itr3 };
    var merge_itr = try MergeIterator.init(test_gpa, try test_gpa.dupe(Iterator, itrs[0..]));
    defer merge_itr.deinit();

    const a = try merge_itr.next();
    try std.testing.expectEqualStrings("1", a.?.val);
    const b = try merge_itr.next();
    try std.testing.expectEqualStrings("", b.?.val);
    const c = try merge_itr.next();
    try std.testing.expectEqualStrings("4", c.?.val);
    const d = try merge_itr.next();
    try std.testing.expectEqualStrings("5", d.?.val);
    const e = try merge_itr.next();
    try std.testing.expectEqualStrings("4", e.?.val);
    try std.testing.expectEqual(null, try merge_itr.next());
}
