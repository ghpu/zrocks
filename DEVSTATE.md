---
project: zrocks
zig_binary: /home/ghpu/zig/zig
stdlib: /home/ghpu/zig/lib/std
target_rocksdb: "9.x line; block-based table format_version 5; legacy WAL/MANIFEST log (see docs/adr/000-target-format.md)"
active_phase: P7 (not started)
active_milestone: "LEVELDB-EQUIVALENT CORE COMPLETE (Phases 0–6). Next: Phase 7 RocksDB extensions."
last_completed: M6.3 Snapshots + CLI + integration gate (Phase 6 COMPLETE)
worktrees: "(none — clean)"
test_command: "/home/ghpu/zig/zig build test"
test_count: 296
artifacts: "zig build -> zig-out/lib/libzrocks.a + zig-out/bin/zrocks (CLI). CLI verified end-to-end (put/get/scan/bench, durable across processes)."
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
- [x] M1.0 Env over std.Io (+ MemEnv)  (merged; src/env/{env,real_env,mem_env}.zig)

### Phase 2 — Write durability core
- [x] M2.0 InternalKey                (src/format/internal_key.zig)
- [x] M2.1 WriteBatch                 (src/format/write_batch.zig)
- [x] M2.2 WAL (log writer + reader)  (src/format/log_{format,writer,reader}.zig)
- [x] M2.3 Skiplist                   (src/memtable/skiplist.zig)
- [x] M2.4 MemTable                   (src/memtable/memtable.zig)

### Phase 3 — Block-based table (SST)
- [x] M3.0 Block builder/reader        (src/format/block.zig)
- [x] M3.1 Bloom filter + filter block (src/format/bloom.zig, filter_block.zig)
- [x] M3.2 Footer & BlockHandle        (src/format/footer.zig)
- [x] M3.3 TableBuilder                (src/format/table_builder.zig)
- [x] M3.4 TableReader                 (src/format/table_reader.zig)
- [x] M3.5 LRU block cache             (src/util/cache.zig + table_reader integration; file-handle/table cache deferred to M5.1)

### Phase 4 — Iterators & in-memory DB
- [x] M4.0 Iterator interface + merging/two-level  (src/iterator/*.zig)
- [x] M4.1 In-memory DB (Put/Get/Delete/Write/iter)  (src/db/*.zig)

### Phase 5 — Persistence: Version/MANIFEST + recovery
- [x] M5.0 VersionEdit                  (src/version/version_edit.zig)
- [x] M5.1 Version / VersionSet / MANIFEST  (src/version/{version_set,filename}.zig)
- [x] M5.2 Recovery  (src/db/recovery.zig + db.zig durable open; Env.newAppendableFile)

### Phase 6 — Compaction → full embedded KV store
- [x] M6.0 SST read path (src/version/table_cache.zig + Iterator.deinit + Version.get/iters; DB reads memtable+SSTs)
- [x] M6.1 Flush (immutable memtable → L0 SST + VersionEdit + log switch + write_buffer trigger)  (src/db/flush.zig)
- [x] M6.2 Leveled compaction (src/db/compaction.zig; picker + merge + tombstone/overwrite drop)
- [x] M6.3 Snapshots (full SnapshotList; compaction respects oldest) + CLI + integration gate
- [x] GATE: LevelDB-equivalent core COMPLETE — durable, crash-recoverable, leveled-compacting LSM KV store + CLI

### Phase 7 — RocksDB extensions (not started; each independent on the core)
- [ ] M7.0 Column Families
- [ ] M7.1 MergeOperator
- [ ] M7.2 Prefix bloom & prefix seek
- [ ] M7.3 Universal + FIFO compaction
- [ ] M7.4 CompactionFilter
- [ ] M7.5 DeleteRange (range tombstones)
- [ ] M7.6 Transactions (optimistic + pessimistic)
- [ ] M7.7 Checkpoints
- [ ] (revisit) RocksDB real-DB read-interop gate (now feasible: SST read path + manifest exist; needs RocksDB kNewFile4 manifest tag + full-filter format)

### Phase 7 — RocksDB extensions
- [ ] M7.0 Column Families
- [ ] M7.1 MergeOperator
- [ ] M7.2 Prefix bloom & prefix seek
- [ ] M7.3 Universal + FIFO compaction
- [ ] M7.4 CompactionFilter
- [ ] M7.5 DeleteRange (range tombstones)
- [ ] M7.6 Transactions (optimistic + pessimistic)
- [ ] M7.7 Checkpoints

## STATUS: LevelDB-equivalent core COMPLETE (Phases 0–6, 296 tests). Next: Phase 7 (RocksDB extensions).

zrocks is a working, durable, crash-recoverable, leveled-compacting LSM key-value store with byte-compatible on-disk formats and a CLI. `zig build` → `libzrocks.a` + `zrocks` binary. To resume: pick a Phase 7 milestone from the checklist above, create a worktree `../zrocks-wt/<slug>` off main, dispatch a subagent with the standard TDD + capability-style + 0.16-gotchas brief, integrate (merge --no-ff, wire root.zig, `zig build test`, update this file, remove worktree).

## (historical) Active: Phase 6 — Compaction → full LSM (4 milestones)

### Phase 6 plan
- M6.0 SST read path (O) — needs version_set + table_reader + cache + iterator. New: src/version/table_cache.zig; modify db.zig read path + add Version.get / Version.newIterators. FIRST add an optional `deinit: ?*const fn(ctx)void` to the `iterator.Iterator` vtable and have Merging/TwoLevel call children's deinit (the gap noted below). table_cache opens/caches Table handles by file number (filename.tableFileName). DB.get: check memtable, then current Version's files (L0 newest-first by file number, then levels 1.. by key range); DB.newIterator merges memtable iter + per-file table iters. Tested by building SSTs via TableBuilder, registering them with a VersionEdit, reading through DB.
- M6.1 Flush (O) — needs M6.0. Immutable-memtable switch when active memtable exceeds write_buffer_size; build an L0 SST (TableBuilder) from the immutable memtable; logAndApply a VersionEdit adding the file + new log_number; open a fresh WAL + memtable. Background or synchronous trigger (synchronous is fine first; behind Env). Reopen then recovers via SSTs (manifest) + remaining WAL.
- M6.2 Leveled compaction (O) — needs M6.1. Size/level picker; compaction job merges input SSTs (MergingIterator over table iters) into output level; drop tombstones/overwritten keys below the oldest snapshot; grandparent-overlap output splitting; install edits. Gate: randomized op-log vs reference AutoHashMap after compactions + reopen.
- M6.3 Snapshots (O) — full SnapshotList pinning sequences; compaction respects the oldest live snapshot.
- GATE: add main.zig CLI (put/get/scan/bench) + tests/integration_db_test.zig.

## Next steps (ordered)
1. Phase 6: M6.0 read path → M6.1 flush → M6.2 leveled compaction → M6.3 snapshots → CLI + integration gate = "LevelDB-equivalent core complete".
2. Then revisit the RocksDB real-DB read-interop gate (now feasible with the SST read path).
3. Phase 7 — RocksDB extensions (column families, merge operator, prefix seek, universal/FIFO compaction, compaction filter, DeleteRange, transactions, checkpoints).

## Engine capabilities so far (on main, 264 tests, Phases 0–5 COMPLETE)
Foundation · Env capability over std.Io (+MemEnv, append) · WAL (byte-compat) · Skiplist · MemTable (snapshot+tombstone get) · WriteBatch · full block-based SST (byte-compat, CRC-verified, round-trips) · sharded LRU block cache · generic Iterator/Merging/TwoLevel · VersionEdit + VersionSet/MANIFEST (write→recover) · **durable DB: open/put/get/delete/write/newIterator/snapshot over MemTable+WAL, recovers across reopen (reuse-logs)**. Not yet: data is single-memtable + WAL only (no SST flush/compaction; reads don't consult SSTs yet).

## Decision log (ADR pointers)
- ADR-000: RocksDB format target pinned (format_version 5 SST, legacy WAL/MANIFEST, CRC32C mask). docs/adr/000-target-format.md
- BLOCK COMPARATOR (from M6.0, fix in M6.1): `BlockBuilder.add` (src/format/block.zig:89-91) asserts BYTEWISE non-decreasing order. Internal-key SSTs are ordered by InternalKeyComparator (user asc, seq DESC) which is NOT bytewise — so flushing multi-version keys trips it. M6.1 must parameterize BlockBuilder/TableBuilder with the comparator (use IKC for internal-key SSTs) so the data-block order assertion + in-block binary search use IKC. SSTs must be BUILT and OPENED with the InternalKeyComparator (table_cache already opens with IKC). M6.0 sidestepped this by using one internal key per user key per SST.
- RESOLVED (was M4.0 gap): the `iterator.Iterator` vtable now has optional `deinit` (M6.0); Merging/TwoLevel propagate it.
- (historical) KNOWN GAP (from M4.0): the generic `iterator.Iterator` vtable has NO deinit/close. Memtable iters are arena-backed (no per-iter alloc) so the in-memory DB is fine, but table/SST iterators allocate block buffers — before merging SSTs into reads/compaction (Phase 5/6), add an optional `deinit: ?*const fn(ctx) void` to the Iterator vtable and have Merging/TwoLevel call it on children. TwoLevelIterator already documents that 2nd-level sources must own cleanup.
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

### CLI / process / time signatures (verified in M6.3, src/main.zig)
- `pub fn main(init: std.process.Init) !u8` — 0.16 populates `init` with `gpa`, `io` (a ready `std.Io.Threaded` — NO manual init/deinit), `arena`, `minimal.args`. Build a RealEnv with `RealEnv.init(init.io, std.Io.Dir.cwd())`.
- Args: `std.process.args`/`argsAlloc` are GONE. Use `std.process.Args.Iterator.initAllocator(init.minimal.args, gpa)` then `.next()`/`.deinit()`. Iterator-owned slices valid until `.deinit()`.
- stdout: no ambient buffered stdout — `std.Io.File.stdout().writeStreamingAll(io, bytes)`. `std.debug.print` still ok for stderr/usage.
- Timing: `std.time.Timer` is GONE. Use `std.Io.Timestamp.now(io, .awake)` and `start.durationTo(end).nanoseconds` (an i96).

### std.Io filesystem signatures (verified in M1.0 — use via the Env capability, src/env/env.zig)
- Obtain io: `std.Io.Threaded.init(gpa, opts)` then `.io()`; `.deinit()` to tear down. Tests: global `std.testing.io` + `std.testing.tmpDir(.{})` (cleanup with `defer tmp.cleanup()`).
- Dir (io is 2nd arg EXCEPT rename): `Dir.cwd()`; `createFile(dir, io, sub_path, CreateFileOptions{.truncate=true,...})`; `openFile(dir, io, sub_path, OpenFileOptions{.mode=.read_only|.write_only|.read_write})`; `openDir/createDir/createDirPath(dir, io, sub_path, ...)`; `deleteFile(dir, io, sub_path)`; `statFile(dir, io, sub_path, .{}) -> Stat{.size:u64}` (error.FileNotFound); `deleteTree(dir, io, sub_path)`.
- **`Dir.rename(old_dir, old_sub_path, new_dir, new_sub_path, io)` — io is the LAST arg (gotcha). Same-dir atomic rename: pass root for both dirs.**
- File positional I/O (no pread/pwrite/fsync): `writePositionalAll(file, io, bytes, offset)`; `readPositionalAll(file, io, buf, offset) -> usize` (short/0 read = EOF); `sync(file, io)` (fsync); `close(file, io)`; `stat(file, io)`. Lower-level `read/writePositional` take iovec `[]const []u8`.
- Errors are large platform unions (OpenError ~25, RenameError ~18) — map down via small switches. Env maps to `env.Error{NotFound,AlreadyExists,IoError,PermissionDenied,NotSupported} || Allocator.Error`.
- Env interface convention: file handles are vtable structs `{ptr,vtable}` (WritableFile.append/flush/sync/close, SequentialFile.read/skip/close, RandomAccessFile.readAt/close). NOTE for WAL: RealWritable.append currently does one writePositionalAll per call with no userspace buffer (flush is a no-op) — WAL layer should buffer 32KB blocks itself.

### Standalone-test rule (IMPORTANT for subagent briefs)
- `zig test src/foo/bar.zig` roots the module at `bar.zig`'s DIRECTORY, so any `@import("../...")` is rejected (`error: import of file outside module path`). This hits every file under src/format, src/memtable, src/db, etc.
- Files MUST keep relative PATH imports (e.g. `@import("../util/coding.zig")`) — these are what the single `src`-rooted `zrocks` module + `root.zig` wiring use. Do NOT switch to named `-M`/`@import("coding")` modules (breaks `zig build test`). [M2.0 regressed this; fixed in 5ddbc10.]
- Canonical standalone-verify for a `../`-importing file (subagents use this for TDD; remove the temp file after):
  `printf 'test { _ = @import("format/internal_key.zig"); }' > src/_verify.zig && /home/ghpu/zig/zig test src/_verify.zig && rm src/_verify.zig`
- The authoritative check is always `zig build test` on main after root.zig wiring (orchestrator runs it at every integration).
