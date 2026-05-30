# zrocks

WARNING : this project was entirely vibe-coded by Claude Code (dynamic workflows), to assess their porting capabilities.

A pure-Zig (Zig 0.16) reimplementation of [RocksDB](https://github.com/facebook/rocksdb/wiki) — an LSM-tree embedded key-value storage engine. No C dependencies.

Goals:
- On-disk format **byte-compatible** with RocksDB (SST, WAL, MANIFEST) — interoperable and a strong correctness oracle.
- Idiomatic, **capability-based** Zig API (allocator, `Io`, `Env`, comparator passed explicitly; no global/ambient authority).
- Built incrementally under strict TDD, LevelDB-equivalent core first, then RocksDB extensions.

## Status

See [DEVSTATE.md](DEVSTATE.md) for current development state and the milestone roadmap.

## Build & test

```sh
zig build          # build the library
zig build test     # run all tests
```

Requires Zig 0.16.0.

## License

Licensed under the [Apache License 2.0](LICENSE) — the same license used by upstream [RocksDB](https://github.com/facebook/rocksdb).
