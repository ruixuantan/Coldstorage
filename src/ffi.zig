const std = @import("std");
const lsm = @import("lsm.zig");
const Lsm = lsm.Lsm;
const LsmIterator = lsm.LsmIterator;
const LsmStorageOptions = lsm.LsmStorageOptions;

var debug_allocator = std.heap.DebugAllocator(.{}){};
const gpa = debug_allocator.allocator();

var coldstorage: Lsm = undefined;
var options: LsmStorageOptions = .{};

export fn cs_set_options(
    block_size: u32,
    target_sst_size: u32,
    num_memtable_limit: u32,
    enable_wal: bool,
) callconv(.c) void {
    options = .{
        .block_size = block_size,
        .target_sst_size = target_sst_size,
        .num_memtable_limit = num_memtable_limit,
        .enable_wal = enable_wal,
    };
}

export fn cs_open(path: [*:0]u8) callconv(.c) void {
    const path_str = std.mem.span(path);
    coldstorage = Lsm.open(path_str, options, gpa) catch {
        @panic("Error opening database");
    };
}

export fn cs_close() callconv(.c) void {
    coldstorage.close() catch @panic("Error closing database");
}

export fn cs_put(key: [*:0]u8, val: [*:0]u8) callconv(.c) void {
    const key_str = std.mem.span(key);
    const val_str = std.mem.span(val);
    coldstorage.put(key_str, val_str) catch @panic("Error putting key-value pair");
}

export fn cs_get(key: [*:0]u8, buffer: [*:0]u8) u32 {
    const key_str = std.mem.span(key);
    const buffer_str = std.mem.span(buffer);
    const actual = coldstorage.get(key_str, buffer_str) catch
        @panic("Error getting key-value pair");
    if (actual) |a| {
        return @intCast(a.len);
    }
    return 0;
}

export fn cs_remove(key: [*:0]u8) callconv(.c) void {
    const key_str = std.mem.span(key);
    coldstorage.del(key_str) catch @panic("Error deleting key-value pair");
}

export fn cs_scan(lower: [*:0]u8, upper: [*:0]u8) *anyopaque {
    const lower_str = std.mem.span(lower);
    const upper_str = std.mem.span(upper);
    const iterator = gpa.create(LsmIterator) catch @panic("Not enough memory creating iterator");
    iterator.* = coldstorage.scan(lower_str, upper_str) catch
        @panic("Error scanning key-value pairs");
    return iterator;
}

export fn cs_iterator_deinit(iterator: *anyopaque) callconv(.c) void {
    const ptr: *LsmIterator = @ptrCast(@alignCast(iterator));
    ptr.deinit();
    gpa.destroy(ptr);
}

export fn cs_iterator_key(iterator: *anyopaque, buffer: [*:0]u8) u32 {
    const ptr: *LsmIterator = @ptrCast(@alignCast(iterator));
    const key_str = ptr.key();
    const buffer_str = std.mem.span(buffer);
    @memcpy(buffer_str[0..key_str.len], key_str);
    return @intCast(key_str.len);
}

export fn cs_iterator_val(iterator: *anyopaque, buffer: [*:0]u8) u32 {
    const ptr: *LsmIterator = @ptrCast(@alignCast(iterator));
    const val_str = ptr.val();
    const buffer_str = std.mem.span(buffer);
    @memcpy(buffer_str[0..val_str.len], val_str);
    return @intCast(val_str.len);
}

export fn cs_iterator_is_valid(iterator: *anyopaque) bool {
    const ptr: *LsmIterator = @ptrCast(@alignCast(iterator));
    return ptr.is_valid();
}

export fn cs_iterator_next(iterator: *anyopaque) void {
    const ptr: *LsmIterator = @ptrCast(@alignCast(iterator));
    ptr.next() catch @panic("Error advancing iterator");
}

export fn cs_detect_leaks() bool {
    return debug_allocator.detectLeaks();
}
