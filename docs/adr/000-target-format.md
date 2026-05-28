# ADR-000 — Pin the RocksDB on-disk format target

Status: accepted · 2026-05-28

## Context

zrocks aims to be byte-compatible with RocksDB on disk so it can (eventually) read/write
real RocksDB databases, and so each on-disk format milestone has an external golden-vector
oracle. RocksDB's formats are versioned and have evolved; we must pin concrete targets.

## Decision

Target the **RocksDB 9.x** format line, with these specific choices:

### Block-based table (SST)
- `format_version = 5` (stable; pre-v6, so the simpler 53-byte footer, no base-context checksum).
- Block-based table magic number `kBlockBasedTableMagicNumber = 0x88e241b785f4cff7`.
- Footer (format_version ≥ 1): `[checksum_type:1][metaindex_handle:varint][index_handle:varint]
  [pad to 40 bytes][format_version:u32 LE][magic:u64 LE]` = 53 bytes.
- Per-block trailer: `[compression_type:1][checksum:u32 LE]` (5 bytes).
- Checksum type `kCRC32c = 1`; checksum is masked CRC32C over (block contents + compression byte).
- Compression `kNoCompression = 0` only (compression deferred per project plan).
- BlockHandle = `{offset:varint64, size:varint64}`.

### Write-ahead log (WAL) and MANIFEST log
- LevelDB-derived legacy record format first; recyclable-log format deferred.
- Block size `kBlockSize = 32768`.
- Record header (7 bytes): `[checksum:u32 LE][length:u16 LE][type:u8]`.
- Record types: `kZeroType=0, kFullType=1, kFirstType=2, kMiddleType=3, kLastType=4`.
- Checksum = masked CRC32C over `[type byte][payload]`.

### CRC32C mask (shared by SST and log)
```
kMaskDelta = 0xa282ead8
mask(crc)   = ((crc >> 15) | (crc << 17)) +% kMaskDelta
unmask(m)   = let rot = m -% kMaskDelta in (rot >> 17) | (rot << 15)
```

### MANIFEST
- The MANIFEST is itself a log file (same record format as WAL) of serialized `VersionEdit`s.
- VersionEdit tag set (subset, RocksDB-compatible): `kComparator=1, kLogNumber=2,
  kNextFileNumber=3, kLastSequence=4, kCompactPointer=5, kDeletedFile=6, kNewFile4=100,
  kPrevLogNumber=9, kColumnFamily=200, kColumnFamilyAdd=201, kColumnFamilyDrop=202,
  kMaxColumnFamily=203`.
- A `CURRENT` file holds the name of the active MANIFEST.
- Real RocksDB always records a default column family; minimal default-CF support lands in
  recovery (M5.1) to enable full-DB read interop before the full CF milestone (M7.0).

### Internal key
- `InternalKey = user_key + (sequence << 8 | value_type):u64 LE` appended (8-byte footer).
- Value types: `kTypeDeletion=0, kTypeValue=1` (more added as features land:
  `kTypeMerge`, `kTypeSingleDeletion`, `kTypeRangeDeletion`, ...).

## Consequences

- Each format milestone embeds golden byte vectors taken from RocksDB source/spec to lock the
  encoding. Where a detail here proves inaccurate against real RocksDB output, the golden
  vector wins and this ADR is corrected.
- format_version 6+ features (base-context checksum, new index encodings) and recyclable WAL
  are explicitly out of scope until revisited.
