# Block 09 Closure Decision

Date: 2026-04-16  
Status: pre-Block10 execution decision  
Decision path: `A2`

## Verdict

`BLOCK_09_NOT_FORMALLY_CLOSED`

Current evidence is strong enough to support:

- Block 07 progression
- Block 08 progression
- pre-Block10 readiness work

Current evidence is **not** strict enough to claim formal Block 09 closure without a representative-month-like bounded execution.

## Why Block 09 is not formally closed yet

Block 09 in the master spec is explicitly framed around:

- one representative sample month
- real storage measurement
- full-period sizing extrapolation from that representative slice

The current project evidence already includes:

- controlled real slice on real data
- storage measurement from that slice
- replay-safe validation
- loader stress evidence

That evidence is enough to prove:

- correctness of the `S3 -> loader -> private ClickHouse` contour
- wallet-centric projection correctness on real data
- bounded-memory single-worker loader behavior
- replay-safe canonical load semantics

It does **not** fully substitute for a representative-month-like bounded run, because the accepted master-spec wording for Block 09 still expects a month-like sample for storage sizing and operational confidence before Block 10.

## What remains required for formal Block 09 closure

Run one representative-month-like bounded execution before Block 10 approval.

## Exact representative-month-like bounded run plan

### Window

- start UTC: `2023-11-03T00:00:00Z`
- end UTC: `2023-12-03T00:00:00Z`
- time semantics: half-open interval `[2023-11-03T00:00:00Z, 2023-12-03T00:00:00Z)`

### Frozen runtime assumptions

- exact-tree deploy only
- extractor -> S3 -> loader -> private ClickHouse contour
- `LOADER_CONCURRENCY=1`
- `LOADER_RECORD_BATCH_SIZE=25000`
- single disposable ClickHouse schema for this run
- same replay-safe loader path already accepted in Block 06/06B

### Exact inputs

- current approved exact-tree workspace artifact
- frozen TRON start boundary for `2023-11-03T00:00:00Z`
- bounded extractor end boundary resolved for `2023-12-03T00:00:00Z`
- current stage raw bucket / prefix discipline
- current private ClickHouse stage endpoint

### Required output reports

The run must produce one explicit evidence bundle containing at least:

- `load_summary.json`
- `validation.json`
- `replay.json`
- `storage_measurement.json`
- `operator_summary.md`
- loader SQLite ledger snapshot
- exact-tree deploy metadata
- artifact sha256

### Required acceptance checks

The representative-month-like run is accepted only if all checks below pass:

1. manifest counts match loaded canonical counts
2. `legs == 2 * events`
3. replay of the same run inserts `0` new canonical rows
4. no `failed` or `quarantined` segment remains unresolved
5. wallet-centric query surface works on the resulting disposable schema
6. storage report includes:
   - `rows`
   - `bytes_on_disk`
   - `data_compressed_bytes`
   - `data_uncompressed_bytes`
7. full-period sizing projection is recomputed from this representative-month-like run

### Expected operator evidence bundle

The operator bundle must include:

- run id
- exact UTC window
- exact-tree artifact sha256
- disposable schema name
- segment count
- canonical event count
- canonical leg count
- replay result
- storage totals
- projected full-period storage estimate
- explicit verdict

## What this decision means

- Block 07 remains materially progressed and practically validated
- Block 08 remains materially progressed and practically validated
- Block 09 is **not** formally closed yet
- the representative-month-like bounded run above is the remaining formal Block 09 closure step before manual approval into Block 10
