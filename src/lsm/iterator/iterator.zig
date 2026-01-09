const std = @import("std");
const BlockIterator = @import("../block.zig").iterator.BlockIterator;
const SsTableIterator = @import("../table.zig").SsTableIterator;
const SsTableError = @import("../table.zig").SsTable.SsTableError;
const MemtableIterator = @import("../memtable.zig").Memtable.MemtableIterator;
const ConcatIterator = @import("concat_iterator.zig").ConcatIterator;
const ConcatIteratorError = ConcatIterator.ConcatIteratorError;
const MergeIterator = @import("merge_iterator.zig").MergeIterator;
const TwoMergeIterator = @import("two_merge_iterator.zig").TwoMergeIterator;

pub const Iterator = union(enum) {
    block_iterator: *BlockIterator,
    ss_table_iterator: *SsTableIterator,
    memtable_iterator: *MemtableIterator,
    concat_iterator: *ConcatIterator,
    merge_iterator: *MergeIterator,
    two_merge_iterator: *TwoMergeIterator,

    pub const IteratorError = error{OutOfMemory} || SsTableError || ConcatIteratorError;

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

    pub fn key(self: *Iterator) []const u8 {
        return switch (self.*) {
            inline else => |it| it.key(),
        };
    }

    pub fn val(self: *Iterator) []const u8 {
        return switch (self.*) {
            inline else => |it| it.val(),
        };
    }

    pub fn is_valid(self: Iterator) bool {
        return switch (self) {
            inline else => |it| it.is_valid(),
        };
    }

    pub fn next(self: *Iterator) IteratorError!void {
        return switch (self.*) {
            .block_iterator => |it| it.next(),
            .ss_table_iterator => |it| try it.next(),
            .memtable_iterator => |it| it.next(),
            .concat_iterator => |it| try it.next(),
            .merge_iterator => |it| try it.next(),
            .two_merge_iterator => |it| try it.next(),
        };
    }
};
