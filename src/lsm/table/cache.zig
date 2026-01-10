const std = @import("std");
const mem = std.mem;
const Block = @import("../block/block.zig").Block;

pub const Cache = struct {
    pub const CacheKey = struct { sst_id: usize, block_index: usize };

    const Node = struct {
        key: CacheKey,
        block: Block,
        prev: ?*Node = null,
        next: ?*Node = null,
    };

    gpa: mem.Allocator,
    head: ?*Node = null,
    tail: ?*Node = null,
    map: std.AutoHashMap(CacheKey, *Node),
    capacity: usize,
    size: usize = 0,

    pub fn init(gpa: mem.Allocator, capacity: usize) Cache {
        return .{
            .gpa = gpa,
            .map = std.AutoHashMap(CacheKey, *Node).init(gpa),
            .capacity = capacity,
        };
    }

    pub fn deinit(self: *Cache) void {
        var current = self.head;
        while (current) |node| {
            const next = node.next;
            node.block.deinit();
            self.gpa.destroy(node);
            current = next;
        }
        self.map.deinit();
    }

    pub fn get(self: *Cache, key: CacheKey) ?Block {
        if (self.map.get(key)) |node| {
            // Move to front
            self.move_to_front(node);
            return node.block;
        }
        return null;
    }

    pub fn put(self: *Cache, key: CacheKey, block: Block) !void {
        if (self.map.get(key)) |node| {
            // Update existing node
            node.block = block;
            self.move_to_front(node);
        } else {
            // Create new node
            if (self.size >= self.capacity) {
                // Remove least recently used (tail)
                if (self.tail) |tail_node| {
                    self.remove_node(tail_node);
                    _ = self.map.remove(tail_node.key);
                    tail_node.block.deinit();
                    self.gpa.destroy(tail_node);
                    self.size -= 1;
                }
            }

            const new_node = try self.gpa.create(Node);
            new_node.* = Node{
                .key = key,
                .block = block,
            };

            try self.map.put(key, new_node);
            self.insert_at_front(new_node);
            self.size += 1;
        }
    }

    pub fn len(self: Cache) usize {
        return self.size;
    }

    fn move_to_front(self: *Cache, node: *Node) void {
        if (node == self.head) return;
        self.remove_node(node);
        self.insert_at_front(node);
    }

    fn insert_at_front(self: *Cache, node: *Node) void {
        node.prev = null;
        node.next = self.head;

        if (self.head) |head| {
            head.prev = node;
        }
        self.head = node;

        if (self.tail == null) {
            self.tail = node;
        }
    }

    fn remove_node(self: *Cache, node: *Node) void {
        if (node.prev) |prev| {
            prev.next = node.next;
        } else {
            self.head = node.next;
        }

        if (node.next) |next| {
            next.prev = node.prev;
        } else {
            self.tail = node.prev;
        }
    }
};

test "Cache" {
    const test_gpa = std.testing.allocator;
    var lru = Cache.init(test_gpa, 3);
    defer lru.deinit();

    const block1 = Block{
        .data = try test_gpa.alloc(u8, 1),
        .offsets = try test_gpa.alloc(u16, 1),
        .gpa = test_gpa,
    };
    const block2 = Block{
        .data = try test_gpa.alloc(u8, 2),
        .offsets = try test_gpa.alloc(u16, 2),
        .gpa = test_gpa,
    };
    const block3 = Block{
        .data = try test_gpa.alloc(u8, 3),
        .offsets = try test_gpa.alloc(u16, 3),
        .gpa = test_gpa,
    };
    const block4 = Block{
        .data = try test_gpa.alloc(u8, 4),
        .offsets = try test_gpa.alloc(u16, 4),
        .gpa = test_gpa,
    };

    try lru.put(.{ .sst_id = 0, .block_index = 1 }, block1);
    try lru.put(.{ .sst_id = 0, .block_index = 2 }, block2);
    try lru.put(.{ .sst_id = 0, .block_index = 3 }, block3);

    try std.testing.expectEqual(3, lru.len());
    try std.testing.expectEqual(block3, lru.get(.{ .sst_id = 0, .block_index = 3 }));
    try std.testing.expectEqual(block1, lru.get(.{ .sst_id = 0, .block_index = 1 }));
    try std.testing.expectEqual(block2, lru.get(.{ .sst_id = 0, .block_index = 2 }));

    try lru.put(.{ .sst_id = 0, .block_index = 4 }, block4); // This should evict key 3
    try std.testing.expectEqual(3, lru.len());
    try std.testing.expectEqual(null, lru.get(.{ .sst_id = 0, .block_index = 3 }));
    try std.testing.expectEqual(block4, lru.get(.{ .sst_id = 0, .block_index = 4 }));
}
