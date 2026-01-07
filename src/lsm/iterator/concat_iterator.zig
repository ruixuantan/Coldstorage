const std = @import("std");
const mem = std.mem;
const SsTable = @import("../table.zig").SsTable;
const SsTableIterator = @import("../table.zig").SsTableIterator;
const Kv = @import("../kv.zig").Kv;

pub const ConcatIterator = struct {
    itr: ?SsTableIterator,
    next_ss_table_index: usize = 0,
    ss_tables: []SsTable,

    pub fn deinit(self: ConcatIterator) void {
        if (self.itr) |i| {
            i.deinit();
        }
    }

    pub fn create_and_seek_to_first(ss_tables: []SsTable) !ConcatIterator {
        if (ss_tables.len == 0) {
            return ConcatIterator{
                .itr = null,
                .ss_tables = ss_tables,
            };
        }
        const itr = try SsTableIterator.create_and_seek_to_first(&ss_tables[0]);
        return ConcatIterator{
            .itr = itr,
            .ss_tables = ss_tables,
            .next_ss_table_index = 1,
        };
    }

    fn find_sst_index(ss_tables: []SsTable, key: []const u8) usize {
        var lo: usize = 0;
        var hi: usize = ss_tables.len;

        while (lo < hi) {
            const mid: usize = lo + @divFloor((hi - lo), 2);
            const ord = mem.order(u8, ss_tables[mid].first_key, key);
            switch (ord) {
                .eq => return mid,
                .lt => {
                    if (mem.order(u8, ss_tables[mid].last_key, key) != .lt) {
                        return mid;
                    }
                    lo = mid + 1;
                },
                .gt => hi = mid,
            }
        }
        return lo;
    }

    pub fn create_and_seek_to_key(ss_tables: []SsTable, key: []const u8) !ConcatIterator {
        const sst_index = ConcatIterator.find_sst_index(ss_tables, key);
        std.debug.assert(sst_index <= ss_tables.len);
        if (sst_index == ss_tables.len) {
            return ConcatIterator{
                .itr = null,
                .ss_tables = ss_tables,
            };
        }
        const itr = try SsTableIterator.create_and_seek_to_key(&ss_tables[sst_index], key);
        return ConcatIterator{
            .itr = itr,
            .ss_tables = ss_tables,
            .next_ss_table_index = sst_index + 1,
        };
    }

    pub fn next(self: *ConcatIterator) !?Kv {
        if (self.itr == null) {
            return null;
        }
        const next_kv = try self.itr.?.next();
        if (next_kv) |kv| {
            return Kv.init(kv.key, kv.val);
        } else {
            self.itr.?.deinit();
            if (self.next_ss_table_index >= self.ss_tables.len) {
                self.itr = null;
                return null;
            }
            self.itr = try SsTableIterator.create_and_seek_to_first(&self.ss_tables[self.next_ss_table_index]);
            self.next_ss_table_index += 1;
            return try self.next();
        }
    }
};

test "ConcatIterator: create_and_seek_to_first, next" {
    const test_gpa = std.testing.allocator;
    const test_table = @import("../table/test_table.zig");
    const ss_table1 = try test_table.create_sst_test("concat_iterator_sst1.sst", test_gpa);
    const ss_table2 = try test_table.create_other_sst_test("concat_iterator_sst2.sst", test_gpa);
    var ss_tables: [2]SsTable = .{ ss_table1, ss_table2 };
    defer {
        test_table.close_sst_test(&ss_tables[0], "concat_iterator_sst1.sst");
        test_table.close_sst_test(&ss_tables[1], "concat_iterator_sst2.sst");
    }
    var concat_itr = try ConcatIterator.create_and_seek_to_first(&ss_tables);
    defer concat_itr.deinit();
    const kv1 = try concat_itr.next();
    try std.testing.expectEqualStrings("val1", kv1.?.val);
    const kv2 = try concat_itr.next();
    try std.testing.expectEqualStrings("val2", kv2.?.val);
    const kv3 = try concat_itr.next();
    try std.testing.expectEqualStrings("val3", kv3.?.val);
    const kv4 = try concat_itr.next();
    try std.testing.expectEqualStrings("val4", kv4.?.val);
    const kv5 = try concat_itr.next();
    try std.testing.expectEqualStrings("val5", kv5.?.val);
    const kv7 = try concat_itr.next();
    try std.testing.expectEqualStrings("val7", kv7.?.val);
    const kv8 = try concat_itr.next();
    try std.testing.expectEqualStrings("val8", kv8.?.val);
    try std.testing.expectEqual(null, try concat_itr.next());
}

test "ConcatIterator: create_and_seek_to_first on empty ss_tables" {
    var concat_itr = try ConcatIterator.create_and_seek_to_first(&[_]SsTable{});
    defer concat_itr.deinit();
    try std.testing.expectEqual(null, try concat_itr.next());
}

test "ConcatIterator: create_and_seek_to_key, next" {
    const test_gpa = std.testing.allocator;
    const test_table = @import("../table/test_table.zig");
    var ss_table1 = try test_table.create_sst_test(
        "concat_iterator_sst1.sst",
        test_gpa,
    );
    var ss_table2 = try test_table.create_other_sst_test(
        "concat_iterator_sst2.sst",
        test_gpa,
    );
    defer {
        test_table.close_sst_test(&ss_table1, "concat_iterator_sst1.sst");
        test_table.close_sst_test(&ss_table2, "concat_iterator_sst2.sst");
    }
    var ss_tables = [_]SsTable{ ss_table1, ss_table2 };
    var concat_itr = try ConcatIterator.create_and_seek_to_key(&ss_tables, "key6");
    defer concat_itr.deinit();
    const kv1 = try concat_itr.next();
    try std.testing.expectEqualStrings("val7", kv1.?.val);
    const kv2 = try concat_itr.next();
    try std.testing.expectEqualStrings("val8", kv2.?.val);
    try std.testing.expectEqual(null, try concat_itr.next());
}

test "ConcatIterator: create_and_seek_to_key on empty ss_tables" {
    var concat_itr = try ConcatIterator.create_and_seek_to_key(&[_]SsTable{}, "key1");
    defer concat_itr.deinit();
    try std.testing.expectEqual(null, try concat_itr.next());
}
