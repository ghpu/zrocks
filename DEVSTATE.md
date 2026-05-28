---
project: zrocks
zig_binary: /home/ghpu/zig/zig
stdlib: /home/ghpu/zig/lib/std
target_rocksdb: "9.x line; block-based table format_version 5; legacy WAL/MANIFEST log (see docs/adr/000-target-format.md)"
active_phase: P0
active_milestone: M0.0
last_completed: bootstrap (repo skeleton on main)
branch: milestone/m0.0-harness
worktree: /home/ghpu/projets/zig/zrocks-wt/m0.0-harness
test_command: "/home/ghpu/zig/zig build test"
updated: 2026-05-28
---

# zrocks Development State

Plan of record: `/home/ghpu/.claude/plans/very-ambitious-project-transient-acorn.md`
RocksDB reference: https://github.com/facebook/rocksdb/wiki

## Roadmap progress   [ ] todo   [~] active   [x] merged

### Phase 0 — Foundation
- [~] M0.0 Build & harness  <-- ACTIVE
- [ ] M0.1 Slice & Status
- [ ] M0.2 Coding (varint + fixed)
- [ ] M0.3 Comparator
- [ ] M0.4 Arena
- [ ] M0.5 Options
- [ ] M0.6 CRC32C

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

## Active milestone: M0.0 — Build & test harness
- TDD state: not started
- Files in flight: build.zig, build.zig.zon, src/root.zig, src/main.zig
- Acceptance checklist:
  - [ ] `zig build` produces the static library artifact
  - [ ] `zig build test` discovers & runs a trivial passing test (via refAllDecls in root.zig)
  - [ ] module-first wiring; per-phase test steps stubbed (test, test:util, ...)
  - [ ] zero leaks under std.testing.allocator

## Next steps (ordered)
1. Dispatch M0.0 subagent into worktree milestone/m0.0-harness.
2. Review → `zig build test` green → merge --no-ff to main → update this file → remove worktree.
3. Proceed through Phase 0 (M0.1–M0.6), running independent milestones in parallel worktrees where safe.

## Decision log (ADR pointers)
- ADR-000: RocksDB format target pinned (format_version 5 SST, legacy WAL/MANIFEST, CRC32C mask). docs/adr/000-target-format.md

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
