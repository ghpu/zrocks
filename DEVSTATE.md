---
project: zrocks
zig_binary: /home/ghpu/zig/zig
stdlib: /home/ghpu/zig/lib/std
target_rocksdb: "9.x line; block-based table format_version 5; legacy WAL/MANIFEST log (see docs/adr/000-target-format.md)"
active_phase: P5
active_milestone: "M5.1 Version / VersionSet / MANIFEST"
last_completed: M5.0 VersionEdit
worktrees: "m5.1-version-set"
test_command: "/home/ghpu/zig/zig build test"
test_count: 243
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
- [~] M5.1 Version / VersionSet / MANIFEST  <-- ACTIVE
- [ ] M5.2 Recovery   (RocksDB real-DB read-interop gate deferred to after Phase 6 SST read path)

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

## Active: PHASE 4 COMPLETE — next is Phase 5 (persistence/recovery)

### Phase 5 plan — Version/MANIFEST + recovery (sequential-ish)
- M5.0 VersionEdit (S) — encode/decode the MANIFEST VersionEdit tag set (comparator name, log#, next-file#, last-seq, deleted/added files per level). New: src/version/version_edit.zig. Deps: coding. Golden bytes. (Independent — can run alone first.)
- M5.1 Version / VersionSet / MANIFEST (O) — needs M5.0 + table_reader + log_writer/reader. New: src/version/{version_set,table_cache}.zig. FileMetaData, per-level file lists, apply edits, write/read MANIFEST log (reuse the WAL log format), CURRENT file, file-number allocation. Minimal default-CF records for interop.
- M5.2 Recovery (O) — needs M5.1 + db.zig. DB.open: read CURRENT→MANIFEST→VersionSet, replay WAL(s) into a fresh memtable, restore last_sequence. Wire into the existing db.zig open path (which currently starts empty). INTEROP GATE: open a small DB written by real RocksDB (default CF) and read it back.

## Next steps (ordered)
1. Phase 5: dispatch M5.0 (VersionEdit) first; then M5.1 (VersionSet/MANIFEST); then M5.2 (recovery + db.open wiring + RocksDB read-interop gate).
2. Phase 6 — flush (memtable→L0 SST) + leveled compaction + full snapshots → "LevelDB-equivalent core complete" (add CLI in main.zig + integration test).
3. Phase 7 — RocksDB extensions (column families, merge operator, prefix seek, universal/FIFO compaction, compaction filter, DeleteRange, transactions, checkpoints).

## Engine capabilities so far (on main, 235 tests, Phases 0–4 COMPLETE)
Foundation (slice/status/coding/comparator/arena/crc32c/options) · Env capability over std.Io (+MemEnv) · WAL (byte-compat) · Skiplist · MemTable (snapshot+tombstone get) · WriteBatch · full block-based SST (block/bloom/filter/footer/TableBuilder/TableReader, byte-compat, CRC-verified, round-trips) · sharded LRU block cache · generic Iterator/Merging/TwoLevel · **in-memory DB: open/put/get/delete/write(batch)/newIterator/snapshot over MemTable+WAL (no persistence/recovery yet)**.

## Decision log (ADR pointers)
- ADR-000: RocksDB format target pinned (format_version 5 SST, legacy WAL/MANIFEST, CRC32C mask). docs/adr/000-target-format.md
- KNOWN GAP (from M4.0): the generic `iterator.Iterator` vtable has NO deinit/close. Memtable iters are arena-backed (no per-iter alloc) so the in-memory DB is fine, but table/SST iterators allocate block buffers — before merging SSTs into reads/compaction (Phase 5/6), add an optional `deinit: ?*const fn(ctx) void` to the Iterator vtable and have Merging/TwoLevel call it on children. TwoLevelIterator already documents that 2nd-level sources must own cleanup.
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
