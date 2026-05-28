---
project: zrocks
zig_binary: /home/ghpu/zig/zig
stdlib: /home/ghpu/zig/lib/std
target_rocksdb: "9.x line; block-based table format_version 5; legacy WAL/MANIFEST log (see docs/adr/000-target-format.md)"
active_phase: P1
active_milestone: M1.0
last_completed: M0.0–M0.6 (Phase 0 foundation COMPLETE)
branch: milestone/m1.0-env
worktree: /home/ghpu/projets/zig/zrocks-wt/m1.0-env
test_command: "/home/ghpu/zig/zig build test"
test_count: 98
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
- [x] M0.5 Options                    (merged f074f04)
- [x] M0.6 CRC32C                     (merged 352d647)

### Phase 1 — Environment
- [~] M1.0 Env over std.Io (+ MemEnv)  <-- ACTIVE

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

## Active milestone: M1.0 — Env (filesystem over std.Io) + MemEnv
- TDD state: in progress (Opus subagent in worktree milestone/m1.0-env)
- Files in flight: src/env/env.zig (+ possibly src/env/mem_env.zig)
- Architectural: isolates ALL 0.16 std.Io filesystem churn behind one capability interface
  (Env + WritableFile/SequentialFile/RandomAccessFile vtable handles); real OS env + MemEnv
  test double share one contract test.
- Acceptance checklist:
  - [ ] MemEnv passes the env contract (write/sync/read seq+random/rename/delete/size/notfound)
  - [ ] RealEnv passes the same contract against a temp dir, via std.Io.Threaded io
  - [ ] all FS through std.Io (no std.fs.*); zero leaks

## Next steps (ordered)
1. Integrate M1.0 when the subagent finishes: merge → wire src/env into root.zig → `zig build test` → update this file → remove worktree. Capture the exact std.Io/Dir/File signatures it used into the gotchas list below for reuse.
2. Phase 2 — Write durability core. Likely-parallel batch: M2.0 InternalKey (S) and M2.1 WriteBatch (S, depends on M2.0) on coding; M2.2 WAL (O, depends on Env+crc32c); M2.3 Skiplist (O, depends on Arena+Comparator); M2.4 MemTable (O, depends on Skiplist+InternalKey). Order: M2.0→M2.1 serial; M2.2/M2.3 can parallel after Env; M2.4 after M2.3+M2.0.

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
