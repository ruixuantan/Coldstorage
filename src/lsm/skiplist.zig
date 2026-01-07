const std = @import("std");
const mem = std.mem;

fn SkiplistType(comptime max_height: usize, comptime p: u8) type {
    return struct {
        const Skiplist = @This();

        pub const Node = struct {
            next: [max_height]*Node,
            key: []const u8,
            val: []const u8,
            height: u8,
        };

        pub const list_max_height = max_height;

        arena: std.heap.ArenaAllocator,
        rng: std.Random.DefaultPrng,
        head: *Node,
        tail: *Node,
        size: usize = 0,

        pub fn init(gpa: mem.Allocator) !Skiplist {
            std.debug.assert(p < 8);
            var arena = std.heap.ArenaAllocator.init(gpa);
            var allocator = arena.allocator();
            const tail = try allocator.create(Node);
            tail.*.height = 0;
            const head = try allocator.create(Node);
            head.* = Node{
                .next = .{tail} ** max_height,
                .key = undefined,
                .val = undefined,
                .height = max_height,
            };
            return Skiplist{
                .arena = arena,
                .head = head,
                .tail = tail,
                .rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp())),
            };
        }

        pub fn deinit(self: Skiplist) void {
            self.arena.deinit();
        }

        fn node_height(self: *Skiplist) u8 {
            var level: u8 = 1;
            const rand = self.rng.random();
            while (rand.intRangeAtMost(u8, 0, p - 1) > 0 and level < max_height) : (level += 1) {}
            return level;
        }

        fn greater_or_equal_to(self: Skiplist, key: []const u8, prevs: *[max_height]*Node) *Node {
            var prev = self.head;
            var next: *Node = undefined;
            var h = max_height;
            while (h > 0) {
                h -= 1;
                next = prev.next[h];
                while (next != self.tail and mem.order(u8, next.key, key) == .lt) {
                    prev = next;
                    next = next.next[h];
                }
                prevs[h] = prev;
            }
            return next;
        }

        pub fn get_greater_or_equal_to(self: Skiplist, key: []const u8) *Node {
            var prev = self.head;
            var next: *Node = undefined;
            var h = max_height;
            while (h > 0) {
                h -= 1;
                next = prev.next[h];
                while (next != self.tail and mem.order(u8, next.key, key) == .lt) {
                    prev = next;
                    next = next.next[h];
                }
            }
            return next;
        }

        pub fn get(self: Skiplist, key: []const u8) ?[]const u8 {
            const node_index = self.get_greater_or_equal_to(key);
            if (node_index != self.tail and mem.eql(u8, node_index.key, key)) {
                return node_index.val;
            } else {
                return null;
            }
        }

        pub fn put(self: *Skiplist, key: []const u8, val: []const u8) !void {
            var prevs: [max_height]*Node = undefined;
            const node = self.greater_or_equal_to(key, &prevs);
            if (node != self.tail and mem.eql(u8, node.key, key)) {
                node.val = val;
                return;
            }

            const new_node = try self.arena.allocator().create(Node);
            new_node.* = Node{
                .next = undefined,
                .key = key,
                .val = val,
                .height = self.node_height(),
            };
            for (0..new_node.height) |h| {
                new_node.*.next[h] = prevs[h].next[h];
                prevs[h].*.next[h] = new_node;
            }
            self.size += 1;
        }
    };
}

pub const DefaultSkiplist = SkiplistType(16, 2);

test "Skiplist: put, get, iter" {
    const test_gpa = std.testing.allocator;
    var skiplist = try SkiplistType(4, 2).init(test_gpa);
    defer skiplist.deinit();

    try skiplist.put("key1", "value1");
    try skiplist.put("key2", "value2");
    try skiplist.put("key3", "value3");
    try std.testing.expectEqualStrings("value1", skiplist.get("key1").?);
    try std.testing.expectEqualStrings("value2", skiplist.get("key2").?);
    try std.testing.expectEqualStrings("value3", skiplist.get("key3").?);
    try std.testing.expect(skiplist.get("key4") == null);
    try skiplist.put("key2", "value2_updated");
    try std.testing.expectEqualStrings("value2_updated", skiplist.get("key2").?);
}
