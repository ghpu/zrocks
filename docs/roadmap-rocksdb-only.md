# zrocks — RocksDB-only format migration

Directive: byte-exact RocksDB becomes the ONLY on-disk format; native/"clean" formats removed (real RocksDB must open everything zrocks writes). Designed + adversarially verified by a workflow (9 components → 9 verdicts → synthesis). Oracle: real RocksDB v11.4.0 at `/home/ghpu/rocksdb-interop/` (`verify_open`).

## Already byte-exact (little/no work)
properties block + single-level index + footer + crc — byte-exact (opt-in); FastLocalBloom algorithm byte-exact; WriteBatch default-CF records + legacy WAL match RocksDB default; the read path (transcode + external-fixture reading) is unaffected.

## Features being DROPPED (not made byte-exact — native is going away)
- **Partitioned (two-level) index** — disproportionate to make byte-exact; remove the feature (`partitioned_index.zig` + `index_type` option + its tests).
- **Recyclable-WAL "parts 3&4"** — false-premise extras; drop.
- **Block-based LevelDB bloom WRITE path** — drop (KEEP the read side for LevelDB interop).
- Bespoke range-tombstone serialization (replaced by RocksDB range_del block).

## Sequenced milestones (serial unless noted; each gated by full `zig build test` AND real-RocksDB `verify_open`)
1. **oracle-widen** (S) — widen `verify_open` to point-Get + Seek/range-scan + iterate (so filters/range-dels are exercised, not just SeekToFirst); reframe the interop test as a two-track (CI self-consistency + dev-loop real-RocksDB) gate; baseline OPEN_OK=11.
2. **properties-always** (M) — make `finishRocksDb` the unconditional finish; `rocksdb.properties` on every SST.
3. **sst-default-flip** (L, core) — delete `Options.sst_output` enum + the native finish/index path; RocksDB index + `addFile4` unconditional; reader always transcodes (drop the `metaindexHasRocksDbProperties` discriminator + native two-level reader). *Non-atomic across ~5 files — do it as one milestone.*
4. **manifest-rocksdb-exact** (M, core) — **fix the latent bug**: compaction outputs still emit legacy `kNewFile=7`; add seqno fields to `compaction.zig` `Output`/`EmitCtx` and emit `kNewFile4`(103) for compaction + flush on every edit; verify a MANIFEST after flush+compaction (multi-edit, deletes) is RocksDB-openable.
5. **filter-rocksdb-only** (M) — emit FastLocalBloom full-filter in SSTs + wire `readFilter` probe; drop the block-based bloom write path + clean prefix filter.
6. **range-del-rocksdb** (M) — RocksDB `range_del` meta-block format (fragmented, IKC-sorted); drop the bespoke format.
7. **wal-writebatch-cf** (M, core) — RocksDB CF value-type bytes (drop the `kColumnFamilyTag=0x10` prefix); drop recyclable parts 3&4. Makes a non-flushed zrocks DB RocksDB-openable.
8. **partitioned-index-drop** (M) — remove the two-level index feature + `index_type` option + its 7 tests.
9. **remove-native-and-tests** (L, core) — delete remaining native code, invert/rewrite golden-vector tests to RocksDB form, reconcile SharedManifest single-writer.

## Cross-cutting risks (from adversarial verify)
- **Irreversible**: removing native breaks existing on-disk zrocks DBs (intended). The default flip is non-atomic across ~5 files — keep it one milestone, gated.
- **Compaction seqno plumbing**: `Output` has no seqno fields today; `addFile4` with zeros → Corruption. Extend seqnos from the parsed internal keys.
- **SharedManifest second-writer** under the unconditional comparator/CF path.
- **Broad test breakage**; don't remove `addRangeTombstone` before range-del-rocksdb lands.
- Filter read-probe becomes mandatory; the real-RocksDB gate is dev-loop-only (librocksdb not at CI) — CI keeps the self-consistency gate.
