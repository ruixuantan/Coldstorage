const std = @import("std");
const mem = std.mem;
const DynamicBitSet = std.DynamicBitSet;

pub const Bloom = struct {
    filter: DynamicBitSet,
    k: u8,

    pub fn init_empty(bits: usize, k: u8, gpa: mem.Allocator) !Bloom {
        var bit_length = bits;
        while (bit_length % 8 != 0) : (bit_length += 1) {}
        return Bloom{
            .filter = try DynamicBitSet.initEmpty(gpa, bit_length),
            .k = k,
        };
    }

    pub fn deinit(self: *Bloom) void {
        self.filter.deinit();
    }

    pub fn decode(buf: []const u8, gpa: mem.Allocator) !Bloom {
        var filter = try DynamicBitSet.initEmpty(gpa, (buf.len - 1) * 8);
        const k = buf[buf.len - 1];
        for (buf[0 .. buf.len - 1], 0..) |byte, i| {
            var mask = byte;
            // const offset = buf.len - 2 - i;
            for (0..8) |bit_index| {
                filter.setValue(i * 8 + bit_index, mask & 1 != 0);
                mask >>= 1;
            }
        }
        return Bloom{ .filter = filter, .k = k };
    }

    pub fn encode(self: Bloom, gpa: mem.Allocator) ![]const u8 {
        var buf = try gpa.alloc(u8, self.filter.capacity() / 8 + 1);
        for (0..self.filter.capacity()) |i| {
            const byte_index = @divFloor(i, 8);
            const bit_index = @mod(i, 8);
            const mask: u8 = @as(u8, 1) << @as(u3, @intCast(bit_index));
            if (self.filter.isSet(i)) {
                buf[byte_index] |= mask;
            } else {
                buf[byte_index] &= ~mask;
            }
        }
        buf[buf.len - 1] = @intCast(self.k);
        return buf;
    }

    pub fn bloom_bits_per_key(false_positive_rate: f64) usize {
        const bits = -1.0 * std.math.log(f64, std.math.e, false_positive_rate) / (std.math.ln2 * std.math.ln2);
        return @as(usize, @intFromFloat(@ceil(bits)));
    }

    pub fn build_from_keys(keys: []const []const u8, bits_per_key: usize, gpa: mem.Allocator) !Bloom {
        const k: u8 = @intFromFloat(@ceil(@as(f64, @floatFromInt(bits_per_key)) * std.math.ln2));
        const n_bits = keys.len * bits_per_key;
        var bloom = try Bloom.init_empty(n_bits, k, gpa);
        for (keys) |key| {
            for (0..k) |i| {
                const hash = std.hash.Murmur3_32.hashWithSeed(key, @intCast(i));
                const bit_index: usize = @mod(hash, bloom.filter.capacity());
                bloom.filter.set(bit_index);
            }
        }
        return bloom;
    }

    pub fn may_contain(self: Bloom, key: []const u8) bool {
        for (0..self.k) |i| {
            const hash = std.hash.Murmur3_32.hashWithSeed(key, @intCast(i));
            const bit_index: usize = @mod(hash, self.filter.capacity());
            if (!self.filter.isSet(bit_index)) {
                return false;
            }
        }
        return true;
    }
};

test "Bloom: decode" {
    const test_gpa = std.testing.allocator;
    const buffer: [2]u8 = [_]u8{ 0b10110010, 3 };
    var bloom = try Bloom.decode(buffer[0..], test_gpa);
    defer bloom.deinit();

    try std.testing.expect(bloom.k == 3);
    try std.testing.expect(bloom.filter.isSet(0) == false);
    try std.testing.expect(bloom.filter.isSet(1) == true);
    try std.testing.expect(bloom.filter.isSet(2) == false);
    try std.testing.expect(bloom.filter.isSet(3) == false);
    try std.testing.expect(bloom.filter.isSet(4) == true);
    try std.testing.expect(bloom.filter.isSet(5) == true);
    try std.testing.expect(bloom.filter.isSet(6) == false);
    try std.testing.expect(bloom.filter.isSet(7) == true);
}

test "Bloom: encode" {
    const test_gpa = std.testing.allocator;
    var bloom = try Bloom.decode(&[_]u8{ 0b00001111, 0b10110010, 3 }, test_gpa);
    defer bloom.deinit();

    const buffer = try bloom.encode(test_gpa);
    defer test_gpa.free(buffer);
    try std.testing.expect(buffer[0] == 0b00001111);
    try std.testing.expect(buffer[1] == 0b10110010);
    try std.testing.expect(buffer[2] == 3);
}

test "Bloom: buildFromKeys, mayContain" {
    const test_gpa = std.testing.allocator;
    const keys: [4][]const u8 = .{ "apple", "banana", "carrot", "durian" };
    const bits_per_key = Bloom.bloom_bits_per_key(0.01);
    var bloom = try Bloom.build_from_keys(&keys, bits_per_key, test_gpa);
    defer bloom.deinit();

    try std.testing.expect(!bloom.may_contain("guava"));
    try std.testing.expect(!bloom.may_contain("orange"));
    try std.testing.expect(bloom.may_contain("apple"));
    try std.testing.expect(bloom.may_contain("banana"));
    try std.testing.expect(bloom.may_contain("carrot"));
    try std.testing.expect(bloom.may_contain("durian"));
}
