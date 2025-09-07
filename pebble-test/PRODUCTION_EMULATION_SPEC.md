# Production-Accurate Benchmark: Design and Implementation Spec

This document explains how `pebble-test/benchmark_test.go` emulates a production Canopy environment and specifies the structure and rationale for each storage implementation measured:

- Badger (Managed MVCC with LSS/HSS pattern)
- DualTimeline Pebble (LSS/HSS on Pebble)
- SplitTimeline Pebble (LSS/HSS in separate Pebble DBs)
- SeekLT Pebble (single-stream inverted-version keys)

It describes the defaults, fairness knobs, and why the design produces representative results for Canopy workloads.

---

## 1) What “Production-Accurate” Means for Canopy

Canopy’s store is architected around two timelines:

- Latest State Store (LSS): hot path reads and writes to the latest state. In Badger production, LSS lives at timestamp `MaxUint64`.
- Historical State Store (HSS): immutable historical views partitioned by block height (version), used for historical queries and proofs.

Key requirements to emulate production:

- Durability parity (WAL + fsync enabled) so write/flush/compaction costs are realistic.
- LSS/HSS behavior: latest queries over a single logical version, historical scans over a height-partitioned view.
- Realistic key shapes and prefix usage to exercise Bloom filters, block cache, and iterator bounds the way Canopy does.
- Comparable memory/compaction budgets across DBs to avoid skewing results by configuration alone.

---

## 2) How `benchmark_test.go` Emulates Production Canopy

File: `pebble-test/benchmark_test.go`

### Defaults chosen for realism and practicality

- KEY_SHAPE: `realistic` by default.
  - Keys are distributed across families to mimic Canopy’s workload:
    - `account_`, `validator_`, `committee_`, `hash_` (see key generation in `Benchmark_SystemLevel_FullTest`).
- NUM_KEYS: `10_000`; NUM_VERSIONS: `50`.
  - Represents a moderate working set across many versions for iterators.
- VALUE_BYTES: `32` (inline values by default).
  - Can be increased (e.g., 4096) to exercise Badger’s value-log (vlog) path.
- Durability parity: WAL/fsync enabled by default for both Pebble and Badger.
  - Pebble: `DisableWAL: false`, `Commit(pebble.Sync)` is the default behavior in the harness.
  - Badger: `SyncWrites=true` by default.
- Steady-state latest reads (QUIESCE): By default the benchmarks precondition the latest range before timing to emulate steady-state hot-path performance (can be disabled by setting `QUIESCE=0`).
  - DualTimeline: `db.Flush()` + `db.Compact(["s/", "t/") , parallel=true]` before measuring latest scans.
  - SplitTimeline: `lssDB.Flush()` + `lssDB.Compact(["s/", "t/") , parallel=true]` before measuring latest scans.
  - SeekLT latest (Get): `db.Flush()` + `db.Compact(nil, nil, parallel=true)` before timing.

All of the above can be overridden with environment variables, but reasonable production-like defaults are used out-of-the-box so no flags are required.

### Workloads measured (system-level)

- Write throughput across versions (LSS + HSS for Badger/DualTimeline; single-stream for SeekLT).
- Latest scans over the latest state.
- Historical scans:
  - Full-state historical iterator (entire height partition).
  - Prefix-specific historical iterator (e.g., `account_`, `validator_`, `committee_`) when `KEY_SHAPE=realistic`.

### Iterator semantics and bounds

- Badger:
  - Latest: transaction at `MaxUint64` over LSS prefix `s/`.
  - Historical: transaction at specific height over HSS prefix `h/`.
- DualTimeline Pebble:
  - Latest: `LowerBound: []byte("s/")`, `UpperBound: []byte("t/")` (tight ASCII range).
  - Historical: `boundsForHeight(height)` → `[h/<height>/, h/<height+1>/)`.
- SeekLT Pebble:
  - Historical/latest iterators over single-stream inverted-version keys use `IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)}` and single-pass key deduplication to select the newest visible version ≤ queried height.

### Copy semantics

- All iterators return copied `Key()`/`Value()` slices (mirrors Canopy safety guarantees and avoids aliasing bugs).

### Fairness knobs

- Pebble options (DualTimeline)
  - `MemTableSize: 256MB`, `Cache: 512MB`, `L0CompactionThreshold: 20`, `L0StopWritesThreshold: 40`, `MaxOpenFiles: 5000`.
- Pebble options (SplitTimeline)
  - Per-DB resource isolation with env tuning (defaults favor LSS):
    - `LSS_CACHE_MB` (1024), `HSS_CACHE_MB` (256)
    - `LSS_MAX_COMPACTIONS` (`NumCPU`), `HSS_MAX_COMPACTIONS` (1)
  - Allows biasing LSS latency under HSS load without changing semantics.
- Badger options
  - Managed mode, `SyncWrites=true`, `ValueThreshold` configurable (default 1024), multiple memtables and large base sizes to match Canopy tuning.
- SeekLT options
  - WAL enabled (even in-memory), synchronous commits by default, iterators over DB snapshot (not batch snapshots), bounded seeks, and always-copy semantics.

---

## 3) Implementation Specs

### 3.1 Badger (Managed MVCC with LSS/HSS pattern)

File: `benchmark_test.go` — type `BadgerVersionedStore`

- DB mode: `badger.OpenManaged(opts)`
  - Tuning mirrors production: compression off, multiple/larger memtables, compactor threads, `SyncWrites` on.
- Layout emulation:
  - LSS writes: prefix `s/` at `lssVersion = MaxUint64`.
  - HSS writes: prefix `h/` at the actual block height.
  - Latest reads: `NewTransactionAt(MaxUint64, false)` over `s/`.
  - Historical reads: `NewTransactionAt(height, false)` over `h/`.
- Iteration:
  - Prefix-bounded iterators with `badger.IteratorOptions`.
  - Values are copied via `ValueCopy(nil)`, as in production.
- Rationale:
  - This mirrors Canopy’s latest/historical split and Badger’s MVCC-managed timestamps.
  - Write batches emulate single-block atomic commit to both LSS and HSS.

### 3.2 DualTimeline Pebble (LSS/HSS on Pebble)

Files:
- Store: `benchmark_test.go` — type `DualTimelinePebbleStore`
- Key helpers: `dual_timeline_pebble.go` (`keyLSS`, `keyHSS`, `boundsForHeight`)

- Layout:
  - LSS: `s/<userKey>` — single entry per logical key for latest.
  - HSS: `h/<heightBE>/<userKey>` — per-height partition; contiguous lexicographic ranges.
- Options (production-friendly by default):
  - WAL on; `MemTableSize: 256MB`; `Cache: 512MB` (block cache); `L0CompactionThreshold: 20`; `L0StopWritesThreshold: 40`; `MaxOpenFiles: 5000`; `FormatMajorVersion: pebble.FormatNewest`.
- Iteration:
  - Latest: `LowerBound: "s/"`, `UpperBound: "t/"`.
  - Historical: `[h/<height>/, h/<height+1>/)` via `boundsForHeight(height)`.
- Batching:
  - `db.NewBatch()` (not `NewIndexedBatch`) — no read-your-writes needed during bench.
- Rationale:
  - Exactly mirrors Canopy’s LSS/HSS semantics on Pebble. Perfect height-bounded historical scans with no per-key version resolution, and latest scans over one entry per key.

### 3.3 SeekLT Pebble (Single-stream inverted-version layout)

File: `seeklt_pebble_store.go` — type `SeekLTPebbleStore`

- Key/value format:
  - Key: `[userKey][8-byte ^version]` (big-endian inverted version so newest sorts first).
  - Value: `[1-byte tombstone][value-bytes]`.
- Get (point lookups):
  - Bounded iterator over `[key, prefixEnd(key))`, position with `SeekLT(key|version+1)`, return first visible ≤ version.
- Iterator:
  - Single-pass dedupe by userKey; skip tombstoned; respects query version.
- Options/behavior:
  - WAL enabled even in-memory; `Commit()` sync by default; writes via `NewIndexedBatch()`; iterators created from `db.NewIter(...)` (DB snapshot, not batch) to avoid snapshot artifacts.
  - Always-copy iterator key/value outputs.
- Rationale:
  - This is the best-faith version of the Issue-196/SeekLT approach: correct, bounded seeks, production durability, realistic iterator cost.

### 3.4 SplitTimeline Pebble (Separate DBs for LSS vs HSS)

File: `split_timeline_pebble.go` — type `SplitTimelinePebbleStore`

- Layout:
  - LSS DB: only `s/<userKey>` entries (hot path).
  - HSS DB: only `h/<heightBE>/<userKey>` entries (historical path).
- Core rationale:
  - Isolate hot latest reads/writes from archival churn (compactions, scans) and allow independent tuning/policy/device placement per timeline.
- Options and env tunables (production-safe):
  - Defaults favor LSS for latency.
    - `LSS_CACHE_MB` (default 1024)
    - `HSS_CACHE_MB` (default 256)
    - `LSS_MAX_COMPACTIONS` (default `NumCPU`)
    - `HSS_MAX_COMPACTIONS` (default `1`)
  - WAL enabled and `Commit(pebble.Sync)` by default for parity.
- Iterator semantics:
  - Latest: `lssDB.NewIter(LowerBound: "s/", UpperBound: "t/")`.
  - Historical: `hssDB.NewIter(boundsForHeight(height))`.
- Summary of results in this suite:
  - Historical scans: SplitTimeline tends to outperform DualTimeline due to clean height partitions and isolation.
  - Latest scans: With steady-state conditioning and the defaults above, SplitTimeline was not materially faster than DualTimeline in the system-level tests. DualTimeline remained faster for latest at large scales, though SplitTimeline can win latest in stress tests when LSS is more aggressively favored (e.g., larger LSS cache, lower HSS compaction concurrency).

---

## 4) Why These Design Choices Reflect Canopy

- Latest vs Historical separation matches Canopy’s API and usage patterns.
- Prefix-bounded iterators ensure scans are realistic and exercise Bloom filters and the block cache.
- Durability settings reflect actual node behavior (no WAL-off or NoSync shortcuts).
- Realistic keys (families) emulate how Canopy stores accounts, validators, committees, and hashed artifacts.
- Batch semantics and copy semantics match real safety constraints.

---

## 5) How to Run

- Default, no flags (production-like):

```bash
cd pebble-test
go test -bench='SystemLevel_FullTest$' -run '^$' -benchtime=1x -count=1
```

- Larger values to exercise Badger’s vlog path:

```bash
VALUE_BYTES=4096 go test -bench='SystemLevel_FullTest$' -run '^$' -benchtime=1x -count=1
```

- Larger datasets to stress compactions:

```bash
NUM_KEYS=100000 go test -bench='SystemLevel_FullTest$' -run '^$' -benchtime=1x -count=1
```

---

## 6) Interpreting Results (Rule of Thumb)

- Writes: SeekLT often has the edge (single-stream). DualTimeline and SplitTimeline write both LSS and HSS per update.
- Latest scans: With steady-state conditioning (QUIESCE) and production-like options, DualTimeline is competitive or fastest in system-level tests; SplitTimeline was not materially faster for latest by default.
- Historical scans: SplitTimeline often leads due to isolation, with DualTimeline also very strong thanks to perfect height partitioning.
- Allocation/bytes: SeekLT scanners do more per-key work (dedupe + version checks), which shows up as higher allocs/bytes.

---

## 7) Extensions and Fairness

- You can turn on larger `VALUE_BYTES` to cover Badger’s vlog. DualTimeline and SeekLT continue to write values directly in LSM.
- To equalize read-side tuning further, you can mirror Pebble options (cache/L0 thresholds) on SeekLT as well. The iterator cost remains higher due to layout, but cache parity isolates variables.

---

## 8) Sources and Key Code References

- Benchmark harness: `pebble-test/benchmark_test.go`
- DualTimeline helpers: `pebble-test/dual_timeline_pebble.go` (`keyLSS`, `keyHSS`, `boundsForHeight`)
- SplitTimeline store: `pebble-test/split_timeline_pebble.go`
- SeekLT store: `pebble-test/seeklt_pebble_store.go`

These files contain the concrete bindings and options described above.
