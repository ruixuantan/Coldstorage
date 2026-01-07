pub const Kv = struct {
    key: []const u8,
    val: []const u8,

    pub fn init(key: []const u8, val: []const u8) Kv {
        return .{ .key = key, .val = val };
    }
};
