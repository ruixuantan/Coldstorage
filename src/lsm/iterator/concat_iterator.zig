const std = @import("std");
const mem = std.mem;
const SsTable = @import("../table.zig").SsTable;
const SsTableError = SsTable.SsTableError;
const SsTableIterator = @import("../table.zig").SsTableIterator;

pub const ConcatIterator = struct {
    itr: ?SsTableIterator,
    next_ss_table_index: usize = 0,
    ss_tables: []*SsTable,

    pub const ConcatIteratorError = error{OutOfMemory} || SsTableError;

    pub fn create_and_seek_to_first(ss_tables: []*SsTable) ConcatIteratorError!ConcatIterator {
        if (ss_tables.len == 0) {
            return ConcatIterator{
                .itr = null,
                .ss_tables = ss_tables,
            };
        }
        const itr = try SsTableIterator.create_and_seek_to_first(ss_tables[0]);
        return ConcatIterator{
            .itr = itr,
            .ss_tables = ss_tables,
            .next_ss_table_index = 1,
        };
    }

    fn find_sst_index(ss_tables: []*SsTable, k: []const u8) usize {
        var lo: usize = 0;
        var hi: usize = ss_tables.len;

        while (lo < hi) {
            const mid: usize = lo + @divFloor((hi - lo), 2);
            const ord = mem.order(u8, ss_tables[mid].first_key, k);
            switch (ord) {
                .eq => return mid,
                .lt => {
                    if (mem.order(u8, ss_tables[mid].last_key, k) != .lt) {
                        return mid;
                    }
                    lo = mid + 1;
                },
                .gt => hi = mid,
            }
        }
        return lo;
    }

    pub fn create_and_seek_to_key(
        ss_tables: []*SsTable,
        k: []const u8,
    ) ConcatIteratorError!ConcatIterator {
        const sst_index = ConcatIterator.find_sst_index(ss_tables, k);
        std.debug.assert(sst_index <= ss_tables.len);
        if (sst_index == ss_tables.len) {
            return ConcatIterator{
                .itr = null,
                .ss_tables = ss_tables,
            };
        }
        const itr = try SsTableIterator.create_and_seek_to_key(ss_tables[sst_index], k);
        return ConcatIterator{
            .itr = itr,
            .ss_tables = ss_tables,
            .next_ss_table_index = sst_index + 1,
        };
    }

    pub fn key(self: ConcatIterator) []const u8 {
        std.debug.assert(self.itr != null);
        return self.itr.?.key();
    }

    pub fn val(self: ConcatIterator) []const u8 {
        std.debug.assert(self.itr != null);
        return self.itr.?.val();
    }

    pub fn is_valid(self: ConcatIterator) bool {
        if (self.itr) |itr| {
            return itr.is_valid();
        }
        return false;
    }

    pub fn next(self: *ConcatIterator) ConcatIteratorError!void {
        std.debug.assert(self.itr != null);
        try self.itr.?.next();
        if (self.itr.?.is_valid()) return;

        if (self.next_ss_table_index >= self.ss_tables.len) {
            self.itr = null;
            return;
        }
        self.itr = try SsTableIterator.create_and_seek_to_first(
            self.ss_tables[self.next_ss_table_index],
        );
        self.next_ss_table_index += 1;
    }
};

test "ConcatIterator: create_and_seek_to_first, next" {
    const test_gpa = std.testing.allocator;
    const test_table = @import("../table/test_table.zig");
    const ss_table1 = try test_gpa.create(SsTable);
    ss_table1.* = try test_table.create_sst_test("concat_iterator_sst1.sst", test_gpa);
    const ss_table2 = try test_gpa.create(SsTable);
    ss_table2.* = try test_table.create_other_sst_test("concat_iterator_sst2.sst", test_gpa);
    var ss_tables: [2]*SsTable = .{ ss_table1, ss_table2 };
    defer {
        test_table.close_sst_test(ss_tables[0], "concat_iterator_sst1.sst");
        test_table.close_sst_test(ss_tables[1], "concat_iterator_sst2.sst");
        test_gpa.destroy(ss_table1);
        test_gpa.destroy(ss_table2);
    }
    var concat_itr = try ConcatIterator.create_and_seek_to_first(&ss_tables);
    try std.testing.expect(concat_itr.is_valid());
    try std.testing.expectEqualStrings("val1", concat_itr.val());
    try concat_itr.next();
    try std.testing.expect(concat_itr.is_valid());
    try std.testing.expectEqualStrings("val2", concat_itr.val());
    try concat_itr.next();
    try std.testing.expect(concat_itr.is_valid());
    try std.testing.expectEqualStrings("val3", concat_itr.val());
    try concat_itr.next();
    try std.testing.expect(concat_itr.is_valid());
    try std.testing.expectEqualStrings("val4", concat_itr.val());
    try concat_itr.next();
    try std.testing.expect(concat_itr.is_valid());
    try std.testing.expectEqualStrings("val5", concat_itr.val());
    try concat_itr.next();
    try std.testing.expect(concat_itr.is_valid());
    try std.testing.expectEqualStrings("val7", concat_itr.val());
    try concat_itr.next();
    try std.testing.expect(concat_itr.is_valid());
    try std.testing.expectEqualStrings("val8", concat_itr.val());
    try concat_itr.next();
    try std.testing.expect(!concat_itr.is_valid());
}

test "ConcatIterator: create_and_seek_to_first on empty ss_tables" {
    var concat_itr = try ConcatIterator.create_and_seek_to_first(&[_]*SsTable{});
    try std.testing.expect(!concat_itr.is_valid());
}

test "ConcatIterator: create_and_seek_to_key, next" {
    const test_gpa = std.testing.allocator;
    const test_table = @import("../table/test_table.zig");
    const ss_table1 = try test_gpa.create(SsTable);
    ss_table1.* = try test_table.create_sst_test(
        "concat_iterator_sst1.sst",
        test_gpa,
    );
    const ss_table2 = try test_gpa.create(SsTable);
    ss_table2.* = try test_table.create_other_sst_test(
        "concat_iterator_sst2.sst",
        test_gpa,
    );
    defer {
        test_table.close_sst_test(ss_table1, "concat_iterator_sst1.sst");
        test_table.close_sst_test(ss_table2, "concat_iterator_sst2.sst");
        test_gpa.destroy(ss_table1);
        test_gpa.destroy(ss_table2);
    }
    var ss_tables = [_]*SsTable{ ss_table1, ss_table2 };
    var concat_itr = try ConcatIterator.create_and_seek_to_key(&ss_tables, "key6");
    try std.testing.expect(concat_itr.is_valid());
    try std.testing.expectEqualStrings("val7", concat_itr.val());
    try concat_itr.next();
    try std.testing.expect(concat_itr.is_valid());
    try std.testing.expectEqualStrings("val8", concat_itr.val());
    try concat_itr.next();
    try std.testing.expect(!concat_itr.is_valid());
}

test "ConcatIterator: create_and_seek_to_key on empty ss_tables" {
    var concat_itr = try ConcatIterator.create_and_seek_to_key(&[_]*SsTable{}, "key1");
    try std.testing.expect(!concat_itr.is_valid());
}
