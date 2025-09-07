# Pebble Test Results and Recommendation

## Overview

This document captures the latest, production-parity benchmark results across four implementations and provides a clear recommendation for Canopy:

- Badger (Managed MVCC with LSS/HSS pattern)
- DualTimeline Pebble (LSS/HSS in a single Pebble DB)
- SplitTimeline Pebble (LSS/HSS in separate Pebble DBs)
- SeekLT Pebble (single-stream inverted-version keys; Issue‑196 style)

The benchmark harness mirrors production durability and iterator semantics and, by default, preconditions the latest range before timing (QUIESCE) to emulate steady-state hot-path performance. Key knobs:

- PEBBLE_SYNC: control durable commits (default: 1/true)
- BADGER_SYNC_WRITES: `SyncWrites=true` parity (default: 1/true)
- BADGER_VALUE_THRESHOLD: inline vs vlog placement (default: 1024)
- BADGER_PREFETCH: iterator prefetch (default: 0/false for production parity)
- QUIESCE: precondition latest range (default: 1/true)

## Test Suite

- System-Level Full Test (Writes, Latest, Historical)
  - Workload: NUM_KEYS=100000, NUM_VERSIONS=50
  - Config: PEBBLE_SYNC=1, BADGER_SYNC_WRITES=1, BADGER_VALUE_THRESHOLD=1024, BADGER_PREFETCH=0, QUIESCE=1
- Architectural Comparison: Dual Timeline Pebble vs Issue‑196 (Pebble‑only)
  - Workload scales: 1k keys and 200k keys, 4 versions
  - Shows iterator/scan behavior differences independent of Badger

## Methodology: Production fidelity and feature parity

**Badger (production emulation)**

- Dual-write semantics matching Canopy: latest under `s/<key>` at `ts=MaxUint64` and historical under `h/<height>/<key>` at `ts=height` using Badger Managed Mode (`OpenManaged`, `NewWriteBatchAt`, `NewTransactionAt`).
- Durability parity: `SyncWrites=true` (via `BADGER_SYNC_WRITES=1`).
- Iterator semantics: latest scans by prefix `s/` and historical reads by versioned transaction; `PrefetchValues=false` (via `BADGER_PREFETCH=0`) matches production behavior.
- Value placement: `ValueThreshold=1024` (via `BADGER_VALUE_THRESHOLD`) keeps sub‑1KB values inline in the LSM as in production.
- Other tunables mirror production defaults in `store/store.go` (e.g., MemTableSize=256MB, Compression=None).

**Dual Timeline Pebble (feature parity)**

- Same dual‑write model and key layout: latest `s/<key>`, historical `h/<heightBE>/<key>`.
- Durability parity: WAL enabled with sync commits (`PEBBLE_SYNC=1`) to match Badger’s `SyncWrites=true`.
- Iterator semantics: bounded range scans using `LowerBound/UpperBound` over `s/` for latest and `h/<height>/` for historical; reverse iteration supported via `Last()/Prev()` within the same bounds.
- Archive/all‑versions queries map to scanning HSS ranges (height windows) rather than relying on MVCC.

**SplitTimeline Pebble (isolation option)**

- Same LSS/HSS layout as DualTimeline, but with two separate Pebble DBs and per-DB resource tunings. Defaults prioritize LSS latency (e.g., larger LSS cache, limited HSS compactions). Useful where archival churn would otherwise contend with latest reads/writes.

## System-Level Results (v50)

Command:

```bash
NUM_KEYS=100000 NUM_VERSIONS=50 \
PEBBLE_SYNC=1 BADGER_SYNC_WRITES=1 BADGER_VALUE_THRESHOLD=1024 BADGER_PREFETCH=0 QUIESCE=1 \
go test -bench='SystemLevel_FullTest$' -run '^$' -benchtime=1x -count=1
```

Summary:

- Latest (read): DualTimeline fastest; SplitTimeline close; both beat Badger by a wide margin; SeekLT far behind.
- Historical (read): SplitTimeline fastest; DualTimeline also very fast; Badger and SeekLT much slower.
- Writes: SplitTimeline slightly faster than DualTimeline; both beat Badger; SeekLT fastest on writes (single-stream layout).

Measured results (abbrev.):

```
Latest
  DualTimelinePebble  ~0.013s
  SplitTimelinePebble ~0.029s
  Badger              ~0.103s
  SeekLTPebble        ~9.374s

Historical
  SplitTimelinePebble ~0.010s
  DualTimelinePebble  ~0.037s
  Badger              ~0.911s
  SeekLTPebble        ~1.544s

Write
  SplitTimelinePebble ~17.447s
  DualTimelinePebble  ~18.768s
  Badger              ~36.885s
  SeekLTPebble        ~13.864s
```

Interpretation:

- DualTimeline beats Badger and SeekLT in every read metric (latest and historical) and is simpler to integrate and operate than SplitTimeline.
- SplitTimeline offers operational isolation and leads historical scans, but was not materially faster than DualTimeline for latest in this suite, while adding the complexity of a second DB.
- SeekLT is significantly slower on read paths due to per-key version resolution and is not recommended as the primary engine.

Recommendation: Choose DualTimeline as the default Pebble integration for Canopy.

---

## Latest Results (additional context)

### A) System-Level Iteration Stress Test (production parity)

Command:

```bash
STRESS_KEYS=100000 STRESS_VERSIONS=50 \
PEBBLE_SYNC=1 BADGER_SYNC_WRITES=1 BADGER_VALUE_THRESHOLD=1024 BADGER_PREFETCH=0 \
go test -bench=Benchmark_IterationStressTest -benchmem -timeout=15m
```

Results (Iteration Stress, Latest view over 100k keys × 50 versions):

```
Badger (Latest)         3241203325 ns/op   772619648 B/op   1163738 allocs/op
Dual Timeline Pebble    263861008  ns/op     5613248 B/op     200042 allocs/op
```

Observations:

- DualTimeline Pebble is much faster and more memory-efficient than Badger on latest scans.
- With aggressive LSS bias, SplitTimeline can become the fastest latest scanner under stress; however, in system-level defaults DualTimeline is preferred for simplicity and consistent wins vs Badger/SeekLT on reads.

### B) Architectural Comparison: Dual Timeline Pebble vs Issue‑196

Command:

```bash
go test -bench=Benchmark_DualTimelinePebble_vs_Issue196 -benchmem -timeout=15m
```

Results (Pebble‑only):

```
# 1k keys, 4 versions
DualTimelinePebble-Latest-Iter/1000           97616    ns/op      5 B/op        2 allocs/op
DualTimelinePebble-Historical-Iter/1000      109707    ns/op     32 B/op        2 allocs/op
Issue196-Latest-SeekLT/1000                 1446479    ns/op  24038 B/op     1001 allocs/op

# 200k keys, 4 versions
DualTimelinePebble-Latest-Iter/200000      25160814    ns/op    665 B/op        4 allocs/op
DualTimelinePebble-Historical-Iter/200000  23279879    ns/op    201 B/op        4 allocs/op
Issue196-Latest-SeekLT/200000             391614396    ns/op 4809285 B/op   200014 allocs/op
```

Notes:

- Issue‑196 here is evaluated as a Pebble‑only design (no Badger involved) for a fair architectural comparison.
- We previously did not include Issue‑196 in “integration options” because its current in‑repo form is a Badger/Pebble hybrid; it isn’t a viable integration target as-is. For this benchmark, we isolate its core approach in Pebble to compare iterator strategies.

## Why Dual Timeline Pebble is faster and lower‑memory

- Iterator buffer reuse and contiguous range scans:
  - Dual Timeline Pebble stores latest keys under a single contiguous prefix `s/` and historical under `h/<heightBE>/…`.
  - Pebble iterators efficiently scan bounded ranges with stable buffers and minimal copying.
- Avoiding per‑item value copies:
  - Badger’s API typically returns copies per item (e.g., `ValueCopy`), increasing allocations; Pebble returns slices referencing internal buffers unless explicitly copied.
- Layout synergy with bounds:
  - LowerBound/UpperBound over `s/` and height partitions drastically reduces overhead vs per‑key lookups.

## Why Issue‑196 is slower than Dual Timeline Pebble (architectural)

- Per‑key SeekLT pattern:
  - Issue‑196 requires a `SeekLT` to find the latest version for each logical key, incurring extra seeks and iterator state transitions per item.
- Pre‑enumeration of logical keys:
  - Extracting unique logical keys first, then seeking for each, adds an up‑front scan and additional memory pressure.
- Lack of contiguous latest range:
  - Without a single `s/` partition for latest state, you lose the benefits of tight, sequential iteration that Pebble optimizes well.

## Implications for Canopy

- Block processing:
  - Faster latest‑state scans reduce time to apply transactions and compute commitments per height.
- RPC queries and archival scans:
  - Lower memory and allocation rates support more concurrent queries and reduce GC churn.
- Node synchronization:
  - Predictable memory usage and efficient height‑bounded historical scans (`h/<height>/…`) improve sync reliability.
- Operational parity:
  - With durable commits enabled for both engines, Dual Timeline Pebble maintains its advantage under production‑realistic settings.

## Conclusion
DualTimeline Pebble is the recommended engine for Canopy:

- Beats Badger and SeekLT in every read metric in our production-accurate system-level suite.
- Simpler integration and operations than SplitTimeline (single DB), while delivering top latest-read performance.
- SplitTimeline remains a viable operational option where isolation is critical (e.g., heavy archival churn), and it leads historical scans—but it is not materially faster than DualTimeline for latest by default.

The core LSS/HSS layout is validated, durability parity is enforced, and the harness reflects steady-state read conditions via QUIESCE. Proceed with DualTimeline as the default path; retain SplitTimeline as an optional mode if operational isolation is required.
