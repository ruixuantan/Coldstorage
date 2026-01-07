const std = @import("std");
const db = @import("db.zig");
const Db = db.Db;
const Cursor = db.Cursor;

var debug_allocator = std.heap.DebugAllocator(.{}){};
const gpa = debug_allocator.allocator();

var colderstorage: Db = undefined;
var cursor: Cursor = undefined; // only 1 cursor at a time, not thread safe

export fn open(path: [*:0]u8) callconv(.c) void {
    const path_str = std.mem.span(path);
    colderstorage = Db.open(path_str, .{}, gpa) catch {
        @panic("Error opening database. Not enough memory");
    };
}

export fn close() callconv(.c) void {
    colderstorage.close() catch @panic("Error closing database");
}

export fn execute(sql: [*:0]u8) bool {
    const str = std.mem.span(sql);
    cursor = colderstorage.execute_sql(str) catch |err| {
        if (err == Db.DbError.SqlError) {
            return false;
        }
        @panic("Error executing sql statement");
    };
    return true;
}

export fn list_tables() callconv(.c) void {
    colderstorage.list_tables() catch @panic("Error listing tables");
}

export fn display_table(table_name: [*:0]u8) callconv(.c) bool {
    colderstorage.display_table(std.mem.span(table_name)) catch |err| {
        if (err == Db.DbError.TableDoesNotExist) {
            return false;
        }
        @panic("Error displaying table");
    };
    return true;
}

export fn fetch() bool {
    return cursor.next() catch @panic("Error fetching sql result");
}

export fn cursor_write_schema() callconv(.c) void {
    cursor.write_schema();
}

export fn close_cursor() callconv(.c) void {
    cursor.close();
}

export fn get_buffer() [*:0]const u8 {
    return colderstorage.get_result().ptr;
}

export fn get_buffer_len() u32 {
    return @intCast(colderstorage.get_result_len());
}

export fn detect_leaks() bool {
    return debug_allocator.detectLeaks();
}
