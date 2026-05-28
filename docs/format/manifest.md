# MANIFEST / VersionEdit format

See [ADR-000](../adr/000-target-format.md). The MANIFEST is a log file (see [wal.md](wal.md))
whose records are serialized `VersionEdit`s. A `CURRENT` file contains the active MANIFEST's
filename.

VersionEdit is a sequence of `[tag:varint32][tag-specific fields]`. Tags (subset):

| tag | name | fields |
|-----|------|--------|
| 1 | kComparator | length-prefixed comparator name |
| 2 | kLogNumber | varint64 |
| 3 | kNextFileNumber | varint64 |
| 4 | kLastSequence | varint64 |
| 5 | kCompactPointer | varint32 level + length-prefixed internal key |
| 6 | kDeletedFile | varint32 level + varint64 file number |
| 9 | kPrevLogNumber | varint64 |
| 100 | kNewFile4 | level, file#, file size, smallest/largest internal key, + custom fields |
| 200 | kColumnFamily | varint32 cf id |
| 201 | kColumnFamilyAdd | varint32 cf id + length-prefixed name |
| 202 | kColumnFamilyDrop | (cf id from kColumnFamily) |
| 203 | kMaxColumnFamily | varint32 |

TODO (M5.0/M5.1): exact kNewFile4 custom-field encoding, golden vectors, default-CF records.
