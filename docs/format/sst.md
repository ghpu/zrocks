# SST (block-based table) format

Target: RocksDB block-based table `format_version = 5`. See [ADR-000](../adr/000-target-format.md).

This document is filled in with exact byte layouts and golden vectors as the Phase 3 milestones
(M3.0–M3.5) are implemented. Summary of the layout to match:

```
<beginning_of_file>
[data block 1]
[data block 2]
...
[data block N]
[meta block: filter]
[meta block: ... ]
[metaindex block]
[index block]
[footer (53 bytes)]      <- fixed size, at end of file
<end_of_file>
```

- Each block on disk is followed by a 5-byte trailer: `[compression_type:u8][crc32c:u32 LE]`,
  where the checksum is the masked CRC32C of (block contents + compression-type byte).
- Footer (format_version ≥ 1): `[checksum_type:1][metaindex_handle:varint]
  [index_handle:varint][pad → 40 bytes][format_version:u32 LE][magic:u64 LE]`.
- Magic `0x88e241b785f4cff7`. Checksum type `kCRC32c = 1`.

TODO (per milestone): data block restart-array layout, filter block layout, metaindex keys,
index entry encoding, golden vectors.
