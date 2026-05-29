# zrocks — Next-Directions Roadmap (post planned-scope)

Produced by a multi-agent workflow (9 codebase-grounded design specs → 9 adversarial verifications → synthesis). 24 milestones across 9 waves, spanning the 3 directions: **D1 real-RocksDB byte interop**, **D2 production hardening (concurrency/GC)**, **D3 performance**.

Full per-milestone design specs + verdicts: workflow transcript under
`.claude/projects/.../subagents/workflows/wf_14591179-b07/`.

## Hard rule (carried from the build)
Milestones touching any of **db.zig / compaction.zig / version_set.zig / write_batch.zig** are *core-contended* and CANNOT share a parallel worktree wave — serialize them. Only disjoint non-core files (table_cache.zig, log_format.zig, snappy.zig, full_filter.zig, lock_manager.zig, bench, options-only) may parallelize.

## Key adversarial corrections (why the raw specs changed)
1. **D1a (SST bloom interop) was INFEASIBLE as written** — it reused the legacy LevelDB 32-bit double-hash filter. Real RocksDB FastLocalBloom needs a **64-bit XXPH3 hash (absent from the codebase), FastRange32 cache-line selection, rotate-7 probe walk, and a 5-byte TRAILER**. Re-scoped: split out an XXPH3 prerequisite + re-derive the layout from `filter_policy.cc` with golden vectors BEFORE writing Zig; reclassify the table wiring as core-contended (bloom policy threads through flush/compaction/version_set/table_cache/db) and note it silently breaks filters on existing on-disk SSTs unless gated.
2. **D2c GC dependency was INVERTED** — synchronous obsolete-file deletion is safe ONLY single-threaded; it becomes use-after-free once D2a background threads land. **GC must ship BEFORE D2a** (D2a then converts deletion to a refcount/pending-deletion queue).
3. **D2a is XL, not M/L** — it glossed: (a) the immutable memtable is freed by the flush worker while readers hold a captured pointer (needs refcount/epoch pinning); (b) the lock-free `table_cache` map is mutated by `findTable` on both the read path and background workers (data race); (c) `version_set.logAndApply` has no internal lock — MANIFEST safety relies on single-flush + single-compact + flush-then-compact sequencing (must be made explicit).
4. **io-vs-VTable collision** — threading `io` through `Cache` is impossible without touching `table_reader.readBlockCached` and the generic iterator VTable (no io slot). Decide store-io-at-construction vs VTable rewrite before D2b-3 / block-cache opts.
5. **Duplicates merged** — merge-GetContext (D3a-M2 == D3c-2) and zero-copy pinned block cache (D3a-M3 == D3c-1).
6. **D1c honestly re-scoped** to a self-consistency regression gate; true "open a real RocksDB DB" needs kNewFile4 + FastLocalBloom + recyclable WAL all correct first. (Also: zrocks WAL uses a custom `kColumnFamilyTag=0x10`, not RocksDB `kTypeColumnFamilyValue` — true write-interop is a further milestone.)

## Sequenced waves
- **Wave 1 [PARALLEL, no core contention]** — `TableCache.evict` (factor per-entry teardown; unblocks GC) · recyclable-WAL record format (log_format/writer/reader) · ReleaseFast bench harness (new bench_main) · pure-Zig Snappy codec (new file).
- **Wave 2 [serial, db.zig]** — SST obsolete-file GC after compaction + FIFO (compaction.zig/db.zig; evict from DB.table_cache) → single-CF WAL GC (delete old .log after flush). *Before D2a.*
- **Wave 3 [serial]** — D1-PRE: XXPH3 64-bit hash + FastLocalBloom re-derivation (golden vectors first) · D1b-M1: kNewFile4 encode/decode (version_edit + version_set/compaction/flush — FileMetaData shared, no deinit).
- **Wave 4 [serial]** — D1b-M3: CF lifecycle tags 200-203 + default-CF kColumnFamilyAdd · D3b-M2: wire Snappy into TableBuilder/Reader (BlockHandle.size = COMPRESSED length).
- **Wave 5 [serial]** — D3a-M1: skip range-tombstone aggregator when none (db.zig) · D3b-M3: per-level compression config + flush/compaction wiring.
- **Wave 6 [serial]** — merge GetContext operand accumulation (probeFile → forward accumulating scan, L) · D1b-M4: shared-MANIFEST multi-CF (XL keystone; db.zig openCf can no longer own a MANIFEST).
- **Wave 7 [PARALLEL]** — FastLocalBloom full-filter (new full_filter.zig, uses wave-3 hash, golden-gated) · zero-copy pinned-handle block cache (table_reader/cache; audit all readBlockCached sites).
- **Wave 8 [serial, XL concurrency keystone]** — D2a-1 io+DB write mutex → D2a-2 background flush worker + imm refcount/epoch pinning + table_cache serialization → D2a-3 background compaction worker + single-writer-MANIFEST invariant → D2a-4 write stalls / L0 throttling. (After GC; convert sync deletion to pending-deletion queue.)
- **Wave 9 [serial + some sub-parallel]** — D2b blocking pessimistic locks (HEAP-allocate LockEntry — Condition-by-value in a hashmap is unsound) + per-shard cache mutex (resolve io-vs-VTable first) + skiplist SWMR audit · fragmented range-tombstone iterator · partitioned index/filter (OPTIONAL stretch) · D1c self-consistency interop gate.

## Recommended first milestone — `D2c-M-GC2: TableCache.evict`
File: `src/version/table_cache.zig` only (no core-contended files, zero deps, effort S). Unblocks all obsolete-file GC.
- Add `pub fn evict(self: *TableCache, file_number: u64) void`: `fetchRemove(file_number)` → on hit `entry.value.table.deinit(); entry.value.file.close() catch {}; gpa.destroy(entry.value);` (exact teardown order from the deinit loop); miss = no-op. Don't touch `findTable` insertion.
- TDD: open 2 SSTs → evict one → count drops → re-`findTable` re-opens cleanly (no dangling state); evict-then-deleteFile → `findTable` returns `error.FileNotFound`; evict-uncached = no-op; zero leaks; full ~378-test suite stays green.

## Execution result (2026-05-29)
Executed via a gated sequential workflow: **26/27 milestones landed, suite 378 -> 508 tests, main always green**. The XL concurrency keystone (background flush+compaction workers, imm pinning, write stalls), shared-MANIFEST multi-CF, FastLocalBloom full-filter, XXPH3 hash, kNewFile4, Snappy, GC, and the interop self-consistency gate all merged. ONLY DEFERRED: `partitioned-idx` (Wave 9, explicitly OPTIONAL) — declined because byte-exact RocksDB fv5 partitioned index needs an out-of-scope base single-level-index format rework first; a self-consistent (non-byte-exact) two-level index remains possible later.
