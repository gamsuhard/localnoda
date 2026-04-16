# Block 06 - Frankfurt Loader / Normalizer

## Goal

Close the path from frozen raw S3 segments into the local analytical schema:

`S3 buffer -> loader -> staging tables -> canonical tables`

This block keeps the extractor and node runtime fully separate from the analytical read path.

## Deliverables

1. TRON USDT transfer normalizer from raw `solidityLogTrigger` NDJSON into canonical event rows and wallet-leg rows
2. Loader state schema for replay-safe run/segment tracking
3. Loader runtime that:
    - reads run and segment manifests from S3
    - records explicit per-segment work items (`pending -> claimed -> loading -> merged -> validated | failed | quarantined`)
    - enforces `loader concurrency = 1` while staging tables remain global
    - treats `LOADER_PYTHON_BIN` from the loader venv as the canonical interpreter for secret hydration and loader-side scripts
    - reads raw segment payloads from S3
    - downloads raw segments to local temp storage and normalizes them in bounded batches
    - writes batches into staging tables first
    - merges into canonical tables with dedupe on `event_id` and `leg_id`
    - records `load_audit`
4. Validation runtime for row-count and replay assertions
5. Replay runtime for idempotent re-load of the same run
6. Local synthetic end-to-end test against fake S3 + fake ClickHouse target

## Frozen rules

- no direct reads from node DB
- no direct first-hop writes into canonical tables
- canonical merge only after staging insert
- loader work selection must stay operator-visible in SQLite, not hidden in an in-process queue
- current staging layout is single-worker only until per-worker staging isolation exists
- replay must not duplicate canonical events or legs
- `load_audit` must record every segment load attempt
- system `python3` is not part of the canonical interpreter contract for the loader path

## Acceptance criteria

1. A synthetic S3-backed run loads into staged events and legs
2. Canonical tables receive one event row per logical transfer and two leg rows per event
3. `load_audit` rows exist for both target tables
4. Validation confirms `legs == 2 * events`
5. Replay of the same run does not create duplicate canonical rows

## Pre-bulk gate

Before any real historical load, Block 06 must pass an explicit three-part gate:

1. fresh server-side disposable-schema validation from the exact packaged tree
2. medium-size performance rehearsal on real `S3 -> private ClickHouse`
3. bulk-run checklist freeze with explicit loader constraints, operator checks, and artifact sha256

Runbook: `PRE_BULK_GATE.md`
