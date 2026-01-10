const std = @import("std");
const fs = std.fs;
const SsTable = @import("table.zig").SsTable;
const SsTableBuilder = @import("builder.zig").SsTableBuilder;

pub fn create_sst_test(path: []const u8, gpa: std.mem.Allocator) !SsTable {
    const create_file = try fs.cwd().createFile(path, .{});
    var builder = try SsTableBuilder.init(32, gpa);
    try builder.add("key1", "val1");
    try builder.add("key2", "val2");
    try builder.add("key3", "val3");
    try builder.add("key4", "val4");
    try builder.add("key5", "val5");
    var built_table = try builder.build(0, create_file);
    built_table.close();

    const file = try fs.cwd().openFile(path, .{});
    return try SsTable.open(0, file, gpa);
}

pub fn create_other_sst_test(path: []const u8, gpa: std.mem.Allocator) !SsTable {
    const create_file = try fs.cwd().createFile(path, .{});
    var builder = try SsTableBuilder.init(32, gpa);
    try builder.add("key7", "val7");
    try builder.add("key8", "val8");
    var built_table = try builder.build(1, create_file);
    built_table.close();

    const file = try fs.cwd().openFile(path, .{});
    return try SsTable.open(1, file, gpa);
}

pub fn close_sst_test(sst: *SsTable, path: []const u8) void {
    fs.cwd().deleteTree(path) catch {};
    sst.close();
}

test "Check TestSsTable" {
    const test_gpa = std.testing.allocator;
    const path = "sst_table_builder_test.sst";
    var sst = try create_sst_test(path, test_gpa);
    defer close_sst_test(&sst, path);
    try std.testing.expectEqual(3, sst.block_metas.len);
    try std.testing.expectEqual(88, sst.block_meta_offset);
    try std.testing.expectEqual(0, sst.id);
    try std.testing.expectEqualStrings("key1", sst.first_key);
    try std.testing.expectEqualStrings("key5", sst.last_key);
    try std.testing.expect(!sst.bloom.may_contain("key100"));
    try std.testing.expect(sst.bloom.may_contain("key1"));
    _ = try sst.read_block_cached(0);
}
