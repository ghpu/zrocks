//! log_writer.zig — WAL/MANIFEST log record writer (legacy LevelDB format).
//!
//! See `log_format.zig` for the byte layout. The writer fragments an opaque
//! payload across `kBlockSize` blocks and emits one physical record per
//! fragment.
//!
//! Buffering note: the real `WritableFile.append` performs exactly one write
//! per call with no userspace buffering, so each physical record (7-byte header
//! plus its fragment) is built in a single contiguous buffer and handed to
//! `append` in one call. The block trailer (zero padding) is likewise emitted
//! as a single `append`.

const std = @import("std");
const env = @import("../env/env.zig");
const format = @import("log_format.zig");

const kBlockSize = format.kBlockSize;
const kHeaderSize = format.kHeaderSize;

pub const Writer = struct {
    file: env.WritableFile,
    /// Current write offset within the active block (0..kBlockSize).
    block_offset: usize = 0,

    pub fn init(file: env.WritableFile) Writer {
        return .{ .file = file, .block_offset = 0 };
    }

    /// Append one logical record. The payload is opaque bytes (may be empty).
    ///
    /// Fragments `data` across block boundaries: a single `full` fragment when
    /// the whole record fits in the current block's remaining space, otherwise
    /// `first` (+ `middle`*) + `last`. An empty payload still emits exactly one
    /// zero-length fragment so the record round-trips.
    pub fn addRecord(self: *Writer, gpa: std.mem.Allocator, data: []const u8) !void {
        var left = data;
        // `begin` distinguishes the first fragment of this logical record from
        // subsequent ones, so we pick first/full vs middle/last correctly.
        var begin = true;
        while (true) {
            std.debug.assert(self.block_offset <= kBlockSize);
            const leftover = kBlockSize - self.block_offset;
            if (leftover < kHeaderSize) {
                // Not enough room for even a header: zero-fill the trailer and
                // start a new block. (leftover is in 0..kHeaderSize-1, so the
                // pad buffer is tiny — fits comfortably on the stack.)
                if (leftover > 0) {
                    var pad: [kHeaderSize]u8 = .{0} ** kHeaderSize;
                    try self.file.append(pad[0..leftover]);
                }
                self.block_offset = 0;
            }

            // Space available for fragment payload in the current block.
            const avail = kBlockSize - self.block_offset - kHeaderSize;
            const fragment_len = @min(left.len, avail);

            const record_type: format.RecordType = blk: {
                const end = fragment_len == left.len;
                if (begin and end) break :blk .full;
                if (begin) break :blk .first;
                if (end) break :blk .last;
                break :blk .middle;
            };

            try self.emitPhysicalRecord(gpa, record_type, left[0..fragment_len]);

            left = left[fragment_len..];
            begin = false;
            if (left.len == 0) break;
        }
    }

    /// Build `[checksum:u32 LE][length:u16 LE][type:u8] ++ fragment` in one
    /// buffer and append it with a single `WritableFile.append` call.
    fn emitPhysicalRecord(
        self: *Writer,
        gpa: std.mem.Allocator,
        record_type: format.RecordType,
        fragment: []const u8,
    ) !void {
        std.debug.assert(fragment.len <= std.math.maxInt(u16));
        std.debug.assert(self.block_offset + kHeaderSize + fragment.len <= kBlockSize);

        const total = kHeaderSize + fragment.len;
        const buf = try gpa.alloc(u8, total);
        defer gpa.free(buf);

        // [0..4] checksum (masked CRC32C over [type] ++ fragment), LE.
        const crc = format.checksum(record_type, fragment);
        std.mem.writeInt(u32, buf[0..4], crc, .little);
        // [4..6] length, LE.
        std.mem.writeInt(u16, buf[4..6], @intCast(fragment.len), .little);
        // [6] type.
        buf[6] = @intFromEnum(record_type);
        // [7..] payload.
        @memcpy(buf[kHeaderSize..], fragment);

        try self.file.append(buf);
        self.block_offset += total;
    }
};
