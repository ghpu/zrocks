---
project: zrocks
zig_binary: /home/ghpu/zig/zig
stdlib: /home/ghpu/zig/lib/std
target_rocksdb: "9.x line; block-based table format_version 5; legacy WAL/MANIFEST log (see docs/adr/000-target-format.md)"
active_phase: P0
active_milestone: M0.5
last_completed: M0.0, M0.1, M0.2, M0.3, M0.4, M0.6 (Phase 0 foundation minus Options)
branch: milestone/m0.5-options
worktree: /home/ghpu/projets/zig/zrocks-wt/m0.5-options
test_command: "/home/ghpu/zig/zig build test"
test_count: 88
updated: 2026-05-28
---

# zrocks Development State

Plan of record: `/home/ghpu/.claude/plans/very-ambitious-project-transient-acorn.md`
RocksDB reference: https://github.com/facebook/rocksdb/wiki

## Roadmap progress   [ ] todo   [~] active   [x] merged

### Phase 0 — Foundation
- [x] M0.0 Build & harness            (merged d17befc)
- [x] M0.1 Slice & Status             (merged f4f605f)
- [x] M0.2 Coding (varint + fixed)    (merged 9eaa0b0)
- [x] M0.3 Comparator                 (merged 60bdb88)
- [x] M0.4 Arena                      (merged 1abcd38)
- [~] M0.5 Options  <-- ACTIVE
- [x] M0.6 CRC32C                     (merged 352d647)

### Phase 1 — Environment
- [ ] M1.0 Env over std.Io (+ MemEnv)

### Phase 2 — Write durability core
- [ ] M2.0 InternalKey
- [ ] M2.1 WriteBatch
- [ ] M2.2 WAL (log writer + reader)
- [ ] M2.3 Skiplist
- [ ] M2.4 MemTable

### Phase 3 — Block-based table (SST)
- [ ] M3.0 Block builder/reader
- [ ] M3.1 Bloom filter + filter block
- [ ] M3.2 Footer & BlockHandle
- [ ] M3.3 TableBuilder
- [ ] M3.4 TableReader
- [ ] M3.5 LRU block cache + table cache

### Phase 4 — Iterators & in-memory DB
- [ ] M4.0 Iterator interface + merging/two-level
- [ ] M4.1 In-memory DB (Put/Get/Delete/Write/iter)

### Phase 5 — Persistence: Version/MANIFEST + recovery
- [ ] M5.0 VersionEdit
- [ ] M5.1 Version / VersionSet / MANIFEST
- [ ] M5.2 Recovery (+ RocksDB DB read-interop gate)

### Phase 6 — Compaction → full embedded KV store
- [ ] M6.0 Flush (memtable → L0)
- [ ] M6.1 Leveled compaction
- [ ] M6.2 Snapshots (full)
- [ ] GATE: LevelDB-equivalent core complete (+ CLI, integration test)

### Phase 7 — RocksDB extensions
- [ ] M7.0 Column Families
- [ ] M7.1 MergeOperator
- [ ] M7.2 Prefix bloom & prefix seek
- [ ] M7.3 Universal + FIFO compaction
- [ ] M7.4 CompactionFilter
- [ ] M7.5 DeleteRange (range tombstones)
- [ ] M7.6 Transactions (optimistic + pessimistic)
- [ ] M7.7 Checkpoints

## Active milestone: M0.5 — Options
- TDD state: not started
- Files in flight: src/options.zig
- Depends on: M0.3 Comparator (Options.comparator holds a `comparator.Comparator`).
- Acceptance checklist:
  - [ ] Options / ReadOptions / WriteOptions with sane no-compression defaults
  - [ ] default comparator = comparator.bytewise
  - [ ] instantiable as plain value types; zig build test green, zero leaks

## Next steps (ordered)
1. Dispatch M0.5 (Options) subagent into worktree milestone/m0.5-options.
2. Merge → wire src/options.zig into root.zig → `zig build test` green → update this file → remove worktree.
3. Phase 0 complete after M0.5. Then Phase 1: M1.0 Env over std.Io (+ MemEnv) — the first I/O milestone (Opus); isolates all the 0.16 Io threading.

## Decision log (ADR pointers)
- ADR-000: RocksDB format target pinned (format_version 5 SST, legacy WAL/MANIFEST, CRC32C mask). docs/adr/000-target-format.md
- Interface convention (from M0.3): runtime vtable = `struct { ctx: *const anyopaque, vtable: *const VTable }` with thin method wrappers calling `self.vtable.fn(self.ctx, ...)`. Reuse this for all runtime-swappable capabilities (comparator, filter policy, env files, iterators). See src/util/comparator.zig.
- Parallel-batch workflow validated: independent foundation milestones built in 5 concurrent worktrees, each adding only its own file (verified standalone via `zig test <file>`), root.zig wiring done once at integration. Branches merged without conflict.

## 0.16 API gotchas (do not regress)
1. No std.fs.File/Dir/cwd — use std.Io.Dir/std.Io.File; every call takes `io` (from std.Io.Threaded.io()).
2. Positional I/O: file.readPositionalAll(io, buf, off) / writePositionalAll(io, bytes, off). fsync = file.sync(io). No pread/pwrite.
3. No GeneralPurposeAllocator — std.heap.DebugAllocator(.{}), `var gpa: ... = .init;`, `gpa.deinit() == .ok`.
4. No std.Thread.Mutex — std.Io.Mutex/Condition/RwLock (lock/unlock take io). std.atomic.Value(T) ok; AtomicOrder lowercase.
5. std.ArrayList is unmanaged — `.empty`, methods take allocator, deinit(gpa). Managed = std.array_list.Managed(T).
6. std.testing.expectEqual(expected, actual) — 2 args. expectEqualSlices/expectEqualStrings for bytes. std.testing.allocator detects leaks.
7. std.mem.readInt(T, *[N]u8, endian)/writeInt take a fixed-size array pointer, not a slice.
8. Build module-first: addExecutable/addTest/addLibrary take root_module: *Module. .zon .name & .fingerprint are enum literals.
9. CRC32C = std.hash.crc.Crc32Iscsi (not Crc32) + RocksDB mask.
10. std.Io.Reader/Writer have no takeInt/writeInt — combine with std.mem. Varint hand-rolled (NOT std.leb128).
