/// Arena allocator — LevelDB/RocksDB semantics.
///
/// # Memory accounting (`memoryUsage`)
/// We count the **sum of the raw byte lengths of every block** handed out by
/// the backing allocator.  That is:
///   - Normal blocks: each is exactly `kBlockSize` (4096) bytes.
///   - Large blocks:  each is exactly the requested size PLUS any alignment
///     padding we prepended (at most `alignment - 1` bytes).
///
/// We do NOT count any ArrayList bookkeeping overhead; we count only the bytes
/// belonging to the actual data blocks.
///
/// This mirrors what RocksDB's `arena.cc` tracks in `blocks_memory_`.

const std = @import("std");

pub const kBlockSize: usize = 4096;

pub const Arena = struct {
    backing: std.mem.Allocator,
    /// Every block we have allocated (we own them; freed on deinit).
    blocks: std.ArrayListUnmanaged([]u8),
    /// Pointer into the *current* block where the next bump allocation begins.
    alloc_ptr: [*]u8,
    /// How many bytes remain in the current block starting at `alloc_ptr`.
    alloc_bytes_remaining: usize,
    /// Sum of all block lengths (see module-level doc for exact accounting).
    memory_usage: usize,

    /// Create an Arena that will draw memory from `backing`.
    pub fn init(backing: std.mem.Allocator) Arena {
        return Arena{
            .backing = backing,
            .blocks = .empty,
            // alloc_ptr and alloc_bytes_remaining are initialised to 0 so the
            // very first allocation triggers allocFallback immediately.
            .alloc_ptr = @ptrFromInt(1), // non-null sentinel; never dereferenced
            .alloc_bytes_remaining = 0,
            .memory_usage = 0,
        };
    }

    /// Free all blocks and the block-list itself.  After this call the Arena
    /// must not be used again.
    pub fn deinit(self: *Arena) void {
        for (self.blocks.items) |block| {
            self.backing.free(block);
        }
        self.blocks.deinit(self.backing);
    }

    /// Error set for allocation operations.
    pub const AllocError = std.mem.Allocator.Error;

    /// Bump-allocate `bytes` bytes with default (byte) alignment.
    pub fn alloc(self: *Arena, bytes: usize) AllocError![]u8 {
        return self.allocAligned(bytes, 1);
    }

    /// Bump-allocate `bytes` bytes with at least `alignment`-byte alignment.
    /// `alignment` must be a power of two and > 0.
    pub fn allocAligned(self: *Arena, bytes: usize, alignment: usize) AllocError![]u8 {
        std.debug.assert(bytes > 0);
        std.debug.assert(alignment > 0);
        std.debug.assert(std.math.isPowerOfTwo(alignment));

        // Align the current pointer forward.
        const current_addr = @intFromPtr(self.alloc_ptr);
        const aligned_addr = std.mem.alignForward(usize, current_addr, alignment);
        const slop = aligned_addr - current_addr;

        if (slop + bytes <= self.alloc_bytes_remaining) {
            // Fast path: fits in the current block.
            const ptr: [*]u8 = @ptrFromInt(aligned_addr);
            self.alloc_ptr = ptr + bytes;
            self.alloc_bytes_remaining -= slop + bytes;
            return ptr[0..bytes];
        }

        // Slow path.
        return self.allocFallback(bytes, alignment);
    }

    /// Total bytes allocated from the backing allocator (sum of block lengths).
    pub fn memoryUsage(self: *const Arena) usize {
        return self.memory_usage;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    fn allocFallback(self: *Arena, bytes: usize, alignment: usize) AllocError![]u8 {
        if (bytes > kBlockSize / 4) {
            // Large allocation: give it its own exact-size block so we don't
            // waste the current block's remaining space.  The current block is
            // left as-is for future small allocations.
            return self.allocNewBlock(bytes, alignment);
        }

        // Start a fresh standard block.  The leftover in the old block is
        // abandoned (matches RocksDB behaviour).
        const block = try self.backing.alloc(u8, kBlockSize);
        try self.blocks.append(self.backing, block);
        self.memory_usage += kBlockSize;
        self.alloc_ptr = block.ptr;
        self.alloc_bytes_remaining = kBlockSize;

        // Now serve from the new block (one recursion — guaranteed to fit
        // because bytes <= kBlockSize/4 < kBlockSize).
        return self.allocAligned(bytes, alignment);
    }

    fn allocNewBlock(self: *Arena, bytes: usize, alignment: usize) AllocError![]u8 {
        // Over-allocate by (alignment - 1) so we can always find an aligned
        // offset within the raw block.  We store the full raw slice in `blocks`
        // so deinit frees it correctly.
        const pad = if (alignment <= 1) 0 else alignment - 1;
        const raw = try self.backing.alloc(u8, bytes + pad);
        errdefer self.backing.free(raw); // roll back if append fails
        try self.blocks.append(self.backing, raw);
        self.memory_usage += raw.len;

        // Find the aligned offset within the raw block.
        const raw_addr = @intFromPtr(raw.ptr);
        const aligned_addr = std.mem.alignForward(usize, raw_addr, alignment);
        const ptr: [*]u8 = @ptrFromInt(aligned_addr);

        // The new block is NOT set as the current bump-allocation block;
        // the current block is untouched for future small allocations.
        return ptr[0..bytes];
    }
};

// =============================================================================
// Tests
// =============================================================================

test "many small allocs stay within one block" {
    // kBlockSize = 4096.  Two 100-byte allocs together (200 bytes) fit in one
    // 4096-byte block; memoryUsage must equal exactly kBlockSize.
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    const s1 = try arena.alloc(100);
    const s2 = try arena.alloc(100);

    try std.testing.expectEqual(@as(usize, 100), s1.len);
    try std.testing.expectEqual(@as(usize, 100), s2.len);
    try std.testing.expectEqual(@as(usize, kBlockSize), arena.memoryUsage());
}

test "memory usage grows in block-sized steps" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    // 10 × 400 = 4000 bytes — fits in one 4096-byte block (96 bytes leftover).
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        _ = try arena.alloc(400);
    }
    try std.testing.expectEqual(@as(usize, kBlockSize), arena.memoryUsage());

    // One more 400-byte alloc overflows the first block → second block.
    _ = try arena.alloc(400);
    try std.testing.expectEqual(@as(usize, 2 * kBlockSize), arena.memoryUsage());
}

test "large alloc gets its own block" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    const big: usize = 5000; // > kBlockSize/4 = 1024
    const s = try arena.alloc(big);
    try std.testing.expectEqual(@as(usize, big), s.len);
    // No padding for alignment=1, so the raw block is exactly `big` bytes.
    try std.testing.expectEqual(@as(usize, big), arena.memoryUsage());
}

test "large alloc does not disturb current normal block" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    // Prime: one small alloc triggers the first normal block.
    _ = try arena.alloc(100);
    try std.testing.expectEqual(@as(usize, kBlockSize), arena.memoryUsage());

    // Large alloc: own block added, normal block intact.
    _ = try arena.alloc(5000);
    try std.testing.expectEqual(@as(usize, kBlockSize + 5000), arena.memoryUsage());

    // Another small alloc still served from the first normal block (no new block).
    _ = try arena.alloc(100);
    try std.testing.expectEqual(@as(usize, kBlockSize + 5000), arena.memoryUsage());
}

test "allocAligned returns correctly aligned pointers" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    // Misalign the bump pointer with a few 1-byte allocations.
    _ = try arena.alloc(1);
    _ = try arena.alloc(1);
    _ = try arena.alloc(1);

    const a8 = try arena.allocAligned(32, 8);
    try std.testing.expectEqual(@as(usize, 32), a8.len);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(a8.ptr) % 8);

    const a16 = try arena.allocAligned(64, 16);
    try std.testing.expectEqual(@as(usize, 64), a16.len);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(a16.ptr) % 16);
}

test "allocAligned large with alignment" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    // Large allocation (own block) with 16-byte alignment.
    const s = try arena.allocAligned(5000, 16);
    try std.testing.expectEqual(@as(usize, 5000), s.len);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(s.ptr) % 16);
}

test "write pattern then read back — no corruption between slices" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    const s1 = try arena.alloc(64);
    const s2 = try arena.alloc(64);
    const s3 = try arena.alloc(64);

    @memset(s1, 0xAA);
    @memset(s2, 0xBB);
    @memset(s3, 0xCC);

    for (s1) |b| try std.testing.expectEqual(@as(u8, 0xAA), b);
    for (s2) |b| try std.testing.expectEqual(@as(u8, 0xBB), b);
    for (s3) |b| try std.testing.expectEqual(@as(u8, 0xCC), b);
}

test "write pattern large block — no corruption" {
    var arena = Arena.init(std.testing.allocator);
    defer arena.deinit();

    const small = try arena.alloc(100);
    const large = try arena.alloc(5000);
    const small2 = try arena.alloc(100);

    @memset(small, 0x11);
    @memset(large, 0x22);
    @memset(small2, 0x33);

    for (small) |b| try std.testing.expectEqual(@as(u8, 0x11), b);
    for (large) |b| try std.testing.expectEqual(@as(u8, 0x22), b);
    for (small2) |b| try std.testing.expectEqual(@as(u8, 0x33), b);
}

test "deinit with testing.allocator — zero leaks" {
    // std.testing.allocator reports any un-freed allocation as a test failure.
    var arena = Arena.init(std.testing.allocator);
    _ = try arena.alloc(100);
    _ = try arena.alloc(5000);
    _ = try arena.allocAligned(256, 8);
    arena.deinit();
}
