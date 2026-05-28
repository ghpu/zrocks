# Write-ahead log (WAL) format

Target: LevelDB-derived legacy record format. See [ADR-000](../adr/000-target-format.md).

```
block := record* trailer?
record := [checksum:u32 LE][length:u16 LE][type:u8][payload: length bytes]
```

- Block size `kBlockSize = 32768`. Records never span blocks without fragmentation.
- A record header is 7 bytes. If < 7 bytes remain in a block, they are zero-filled (trailer).
- `type ∈ {kFullType=1, kFirstType=2, kMiddleType=3, kLastType=4}` (kZeroType=0 reserved).
  A logical record is one kFullType, or kFirstType (kMiddleType)* kLastType.
- Checksum = masked CRC32C over `[type:u8][payload]`.

The MANIFEST uses this same record format (payload = serialized VersionEdit). Recyclable-log
format (kRecyclable* types) is deferred.

TODO (M2.2): golden vectors for single/fragmented/boundary records.
