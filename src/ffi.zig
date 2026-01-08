const std = @import("std");
const lsm = @import("lsm.zig");
const Lsm = lsm.Lsm;
const LsmStorageOptions = lsm.LsmStorageOptions;

var debug_allocator = std.heap.DebugAllocator(.{}){};
const gpa = debug_allocator.allocator();

var coldstorage: Lsm = undefined;
var options: LsmStorageOptions = .{};

export fn set_options(
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

export fn open(path: [*:0]u8) callconv(.c) void {
    const path_str = std.mem.span(path);
    coldstorage = Lsm.open(path_str, options, gpa) catch {
        @panic("Error opening database");
    };
}

export fn close() callconv(.c) void {
    coldstorage.close() catch @panic("Error closing database");
}

export fn put(key: [*:0]u8, val: [*:0]u8) callconv(.c) void {
    const key_str = std.mem.span(key);
    const val_str = std.mem.span(val);
    coldstorage.put(key_str, val_str) catch @panic("Error putting key-value pair");
}

export fn get(key: [*:0]u8, buffer: [*:0]u8) u32 {
    const key_str = std.mem.span(key);
    const buffer_str = std.mem.span(buffer);
    const actual = coldstorage.get(key_str, buffer_str) catch
        @panic("Error getting key-value pair");
    if (actual) |a| {
        return @intCast(a.len);
    }
    return 0;
}

export fn remove(key: [*:0]u8) callconv(.c) void {
    const key_str = std.mem.span(key);
    coldstorage.del(key_str) catch @panic("Error deleting key-value pair");
}

export fn detect_leaks() bool {
    return debug_allocator.detectLeaks();
}
