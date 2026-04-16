# Controlled Real Slice Operator Summary

- Run ID: `real-slice-20230901t0000z-20260416t114407z`
- Final decision: `READY_FOR_BLOCK_10`
- Exact tree artifact sha256: `d6619d951ae08da50a1a4fe503dd27490c946fced1481786817587fe75f74edc`
- Git commit: `2dd27f78efeaba342d9c4a943a62510feebb9aca`
- Disposable schema: `tron_usdt_slice_20230901_20260416t114407z`
- Slice window: `2023-09-01T00:00:00Z` .. `2023-09-01T01:00:00Z`
- Blocks: `54298720` .. `54299918`
- Loader constraints: `LOADER_CONCURRENCY=1`, `LOADER_RECORD_BATCH_SIZE=1000`

## Acceptance checks

- Canonical events: `34690`
- Address legs: `69380`
- Legs == 2 * events: `True`
- Replay new canonical rows: `0`
- Replay new leg rows: `0`
- Load audit rows: `4`
- Segment status summary: `{"validated": 1}`

## Performance and storage

- Segment s3_read_ms / normalize_ms / stage_ms / merge_ms / validation_ms: `137` / `1459` / `21909` / `1452` / `307`
- Rows total in disposable schema: `104074`
- data_compressed_bytes: `14132253`
- rows_per_day_estimate: `832560`
- projected_full_period_compressed_bytes: `324928760976`

## Recommendation

- Controlled real slice passed on real data with single-worker constraints preserved. Block 10 can move to manual approval for the first bounded bulk run.

## Notes

- This evidence is from the exact deployed tree and disposable schema only; it is not itself a full bounded historical run approval.
