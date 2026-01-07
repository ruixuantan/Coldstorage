const std = @import("std");
const BlockIterator = @import("../block.zig").iterator.BlockIterator;
const SsTableIterator = @import("../table.zig").SsTableIterator;
const MemtableIterator = @import("../memtable.zig").Memtable.MemtableIterator;
const ConcatIterator = @import("concat_iterator.zig").ConcatIterator;
const MergeIterator = @import("merge_iterator.zig").MergeIterator;
const TwoMergeIterator = @import("two_merge_iterator.zig").TwoMergeIterator;
const Kv = @import("../kv.zig").Kv;

pub const Iterator = union(enum) {
    block_iterator: *BlockIterator,
    ss_table_iterator: *SsTableIterator,
    memtable_iterator: *MemtableIterator,
    concat_iterator: *ConcatIterator,
    merge_iterator: *MergeIterator,
    two_merge_iterator: *TwoMergeIterator,

    pub fn deinit(self: *Iterator, gpa: std.mem.Allocator) void {
        switch (self.*) {
            .block_iterator => |_| {},
            .ss_table_iterator => |it| it.deinit(),
            .memtable_iterator => |_| {},
            .concat_iterator => |it| it.deinit(),
            .merge_iterator => |it| it.deinit(),
            .two_merge_iterator => |it| it.deinit(),
        }
        switch (self.*) {
            inline else => |it| gpa.destroy(it),
        }
    }

    pub fn next(self: *Iterator) anyerror!?Kv {
        return switch (self.*) {
            inline else => |it| it.next(),
        };
    }
};
