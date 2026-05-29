//! log_writer.zig — WAL/MANIFEST log record writer.
//!
//! See `log_format.zig` for the byte layout. The writer fragments an opaque
//! payload across `kBlockSize` blocks and emits one physical record per
//! fragment. It supports two on-disk formats:
//!
//!   - legacy (default): 7-byte header, fragment types full/first/middle/last.
//!   - recyclable: 11-byte header carrying a log_number, fragment types
//!     recyclable_full/first/middle/last. Enabled via `initRecyclable`.
//!
//! Buffering note: the real `WritableFile.append` performs exactly one write
//! per call with no userspace buffering, so each physical record (header plus
//! its fragment) is built in a single contiguous buffer and handed to `append`
//! in one call. The block trailer (zero padding) is likewise emitted as a
//! single `append`.

const std = @import("std");
const env = @import("../env/env.zig");
const format = @import("log_format.zig");

const kBlockSize = format.kBlockSize;
const kHeaderSize = format.kHeaderSize;
const kRecyclableHeaderSize = format.kRecyclableHeaderSize;

pub const Writer = struct {
    file: env.WritableFile,
    /// Current write offset within the active block (0..kBlockSize).
    block_offset: usize = 0,
    /// When true, emit recyclable records (11-byte header with `log_number`).
    recyclable: bool = false,
    /// Log number stamped into recyclable headers; ignored when `recyclable`
    /// is false.
    log_number: u32 = 0,

    pub fn init(file: env.WritableFile) Writer {
        return .{ .file = file, .block_offset = 0 };
    }

    /// Construct a writer that emits recyclable records stamped with
    /// `log_number`. Use this when writing into a recycled (reused) log file so
    /// that stale records from the previous life of the file can be rejected by
    /// the reader.
    pub fn initRecyclable(file: env.WritableFile, log_number: u32) Writer {
        return .{ .file = file, .block_offset = 0, .recyclable = true, .log_number = log_number };
    }

    /// Continue writing into an existing log mid-block.  `block_offset` is the
    /// current write position within the active block, i.e. `file_size %
    /// kBlockSize`.  WAL records are always written whole, so resuming at that
    /// offset preserves the block-fragmentation invariant.
    pub fn initWithOffset(file: env.WritableFile, block_offset: usize) Writer {
        std.debug.assert(block_offset < kBlockSize);
        return .{ .file = file, .block_offset = block_offset };
    }

    /// Recyclable variant of `initWithOffset`.
    pub fn initRecyclableWithOffset(file: env.WritableFile, log_number: u32, block_offset: usize) Writer {
        std.debug.assert(block_offset < kBlockSize);
        return .{
            .file = file,
            .block_offset = block_offset,
            .recyclable = true,
            .log_number = log_number,
        };
    }

    /// The header size used by this writer's active format.
    fn headerSize(self: *const Writer) usize {
        return if (self.recyclable) kRecyclableHeaderSize else kHeaderSize;
    }

    /// Append one logical record. The payload is opaque bytes (may be empty).
    ///
    /// Fragments `data` across block boundaries: a single `full` fragment when
    /// the whole record fits in the current block's remaining space, otherwise
    /// `first` (+ `middle`*) + `last`. An empty payload still emits exactly one
    /// zero-length fragment so the record round-trips.
    pub fn addRecord(self: *Writer, gpa: std.mem.Allocator, data: []const u8) !void {
        const header_size = self.headerSize();
        var left = data;
        // `begin` distinguishes the first fragment of this logical record from
        // subsequent ones, so we pick first/full vs middle/last correctly.
        var begin = true;
        while (true) {
            std.debug.assert(self.block_offset <= kBlockSize);
            const leftover = kBlockSize - self.block_offset;
            if (leftover < header_size) {
                // Not enough room for even a header: zero-fill the trailer and
                // start a new block. (leftover is in 0..header_size-1, so the
                // pad buffer is tiny — fits comfortably on the stack.)
                if (leftover > 0) {
                    var pad: [kRecyclableHeaderSize]u8 = .{0} ** kRecyclableHeaderSize;
                    try self.file.append(pad[0..leftover]);
                }
                self.block_offset = 0;
            }

            // Space available for fragment payload in the current block.
            const avail = kBlockSize - self.block_offset - header_size;
            const fragment_len = @min(left.len, avail);

            const end = fragment_len == left.len;
            const record_type: format.RecordType = if (self.recyclable) blk: {
                if (begin and end) break :blk .recyclable_full;
                if (begin) break :blk .recyclable_first;
                if (end) break :blk .recyclable_last;
                break :blk .recyclable_middle;
            } else blk: {
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

    /// Build a physical record (header ++ fragment) in one buffer and append it
    /// with a single `WritableFile.append` call. The header layout depends on
    /// whether the record type is recyclable:
    ///
    ///   legacy:      [checksum:u32 LE][length:u16 LE][type:u8] ++ fragment
    ///   recyclable:  [checksum:u32 LE][length:u16 LE][type:u8]
    ///                [log_number:u32 LE] ++ fragment
    fn emitPhysicalRecord(
        self: *Writer,
        gpa: std.mem.Allocator,
        record_type: format.RecordType,
        fragment: []const u8,
    ) !void {
        std.debug.assert(fragment.len <= std.math.maxInt(u16));
        const header_size = self.headerSize();
        std.debug.assert(self.block_offset + header_size + fragment.len <= kBlockSize);

        const total = header_size + fragment.len;
        const buf = try gpa.alloc(u8, total);
        defer gpa.free(buf);

        // [4..6] length, LE; [6] type. Common to both layouts.
        std.mem.writeInt(u16, buf[4..6], @intCast(fragment.len), .little);
        buf[6] = @intFromEnum(record_type);

        if (self.recyclable) {
            // [7..11] log_number, LE; [11..] payload.
            std.mem.writeInt(u32, buf[7..11], self.log_number, .little);
            @memcpy(buf[kRecyclableHeaderSize..], fragment);
            const crc = format.recyclableChecksum(record_type, self.log_number, fragment);
            std.mem.writeInt(u32, buf[0..4], crc, .little);
        } else {
            // [7..] payload.
            @memcpy(buf[kHeaderSize..], fragment);
            const crc = format.checksum(record_type, fragment);
            std.mem.writeInt(u32, buf[0..4], crc, .little);
        }

        try self.file.append(buf);
        self.block_offset += total;
    }
};
