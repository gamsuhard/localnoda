# Block 10 Operator Checklist

Date: 2026-04-16  
Status: pre-start checklist for the first bounded bulk

## Before any start command

1. Confirm Singapore extractor restore is complete and no non-resumable restore process is still active.
2. Confirm at least one real source-side bounded run is available for the Block 09 closure step.
3. Confirm exact-tree artifact sha256 matches the frozen approval package.
4. Confirm the deployed workspace on active hosts matches the same artifact sha256.
5. Confirm `LOADER_CONCURRENCY=2`.
6. Confirm `LOADER_RECORD_BATCH_SIZE=350000`.
7. Confirm `TRON_FILE_SINK_SEGMENT_MAX_RECORDS=750000`.
8. Confirm `TRON_FILE_SINK_SEGMENT_MAX_BYTES=805306368`, `TRON_FILE_SINK_FLUSH_EVERY_RECORDS=5000`, and fast-gzip knobs (`TRON_FILE_SINK_GZIP_LEVEL=1`, `TRON_FILE_SINK_GZIP_BUFFER_BYTES=65536`).
9. Confirm `CLICKHOUSE_DATABASE` is an explicit non-disposable target schema for Block 10.
10. Confirm `LOADER_PYTHON_BIN` points to the pinned loader venv.
11. Confirm private ClickHouse probe succeeds from the Frankfurt loader host.

## Extractor-side checks

1. Confirm `run_state.sqlite` is readable and not reporting unresolved `failed` or `quarantined` segments from older runs.
2. Confirm the current bounded run has a frozen `RUN_RESOLVED_END_BLOCK`.
3. Confirm `RUNTIME_MANIFEST_JAVA_TRON_VERSION`, `RUNTIME_MANIFEST_CONFIG_SHA256`, and `RUNTIME_MANIFEST_PLUGIN_BUILD_ID` are populated.
4. Confirm S3 bucket, prefix root, SSE mode, and KMS key match the approved stage contour.
5. Confirm raw output root has enough free disk for current bounded execution.

## Loader-side checks

1. Confirm loader runtime lock table is empty or held only by the current operator-approved owner.
2. Confirm SQLite ledger tables exist and are writable.
3. Confirm no stale `claimed` or `loading` rows remain from an abandoned run unless intentionally resumed.
4. Confirm staging tables are empty before the first segment of the new bounded run.
5. Confirm the target schema exists and has the expected canonical/staging tables.

## S3 / manifest checks

1. Confirm run manifest naming and prefix discipline still match `providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/<run_id>/...`.
2. Confirm every visible segment object has a matching segment manifest.
3. Confirm checksum files and runtime manifest can be written under the run prefix.
4. Confirm object verification still uses `sha256` metadata and size equality.

## SQL / operator checks before proceeding

Run and inspect:

1. count rows in target canonical tables
2. count rows in staging tables
3. count `pending / claimed / loading / merged / validated / failed / quarantined` in loader ledger
4. check the current runtime lock row
5. confirm private endpoint query latency is within expected range for a tiny probe

## Immediate no-start conditions

Do **not** start Block 10 if any one of the following is true:

- Singapore restore still running
- representative-month-like bounded run has not been accepted or explicitly waived
- exact-tree artifact sha256 is unknown or mismatched
- loader runtime lock is inconsistent
- any unresolved `quarantined` segment exists
- ClickHouse private probe fails
- target schema name is still disposable or ambiguous
