# ClickHouse Loader Stress Test Summary (20260416t112926z)

- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`
- artifact source: `s3://goldusdt-v2-stage-913378704801-ops/codex/local-tron-usdt-backfill/workspace-20260416t112926z.tar.gz`
- recommendation: `READY_FOR_BLOCK_10`
- chosen batch size: `25000`
- chosen records/segment target: `250000`

## Run Comparison

| Wave | Label | Status | Batch | Segments | Records/Segment | Rows/sec | Segments/sec | Dominant bottleneck | Replay delta events | Replay delta legs |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| batch-size-sweep | batch-1000 | passed | 1000 | 10 | 10000 | 1356.1 | 0.1356 | stage_insert | 0 | 0 |
| batch-size-sweep | batch-5000 | passed | 5000 | 10 | 10000 | 2393.2 | 0.2393 | merge | 0 | 0 |
| batch-size-sweep | batch-10000 | passed | 10000 | 10 | 10000 | 2721.83 | 0.2722 | merge | 0 | 0 |
| batch-size-sweep | batch-25000 | passed | 25000 | 10 | 10000 | 2771.39 | 0.2771 | merge | 0 | 0 |
| batch-size-sweep | batch-50000 | passed | 50000 | 10 | 10000 | 2569.04 | 0.2569 | merge | 0 | 0 |
| segment-count-sweep | segments-10 | passed | 10000 | 10 | 10000 | 2510.1 | 0.251 | merge | 0 | 0 |
| segment-count-sweep | segments-50 | passed | 10000 | 50 | 10000 | 2564.62 | 0.2565 | merge | 0 | 0 |
| segment-count-sweep | segments-100 | passed | 10000 | 100 | 10000 | 2449.65 | 0.245 | merge | 0 | 0 |
| segment-size-sweep | segment-size-10000 | passed | 10000 | 10 | 10000 | 2425.48 | 0.2425 | merge | 0 | 0 |
| segment-size-sweep | segment-size-50000 | passed | 10000 | 10 | 50000 | 5244.66 | 0.1049 | stage_insert | 0 | 0 |
| segment-size-sweep | segment-size-100000 | passed | 10000 | 10 | 100000 | 5625.78 | 0.0563 | stage_insert | 0 | 0 |
| segment-size-sweep | segment-size-250000 | passed | 10000 | 10 | 250000 | 5797.38 | 0.0232 | stage_insert | 0 | 0 |
| replay-fault | medium-initial | passed | 10000 | 10 | 10000 | 2834.39 | 0.2834 | merge | 0 | 0 |
| replay-fault | medium-restart-replay | passed | 10000 | 10 | 10000 | 0 | 0 |  | 0 | 0 |
| replay-fault | medium-missing-ledger-row | passed | 10000 | 10 | 10000 | 0 | 0 |  | 0 | 0 |
| replay-fault | medium-corrupted-segment | passed | 10000 | 10 | 10000 | 0 | 0 |  | 0 | 0 |
| real-controlled-slice | real-slice-initial | passed | 10000 | 0 | 0 | 3934.89 | 0.1134 | stage_insert | 0 | 0 |
| real-controlled-slice | real-slice-restart-replay | passed | 10000 | 0 | 0 | 0 | 0 |  | 0 | 0 |

## Audit Focus Answers

1. Dominant measured bottleneck across passed synthetic runs: stage_insert.
2. clickhouse-client subprocess overhead is measurable if query/process counts rise faster than rows/sec; best batch run used 150 client invocations.
3. Current merge degradation check: merge_ms/row 0.147740 at 10 segments vs 0.157582 at 100 segments.
4. Single-worker path stayed explicit and replay-safe across passed runs; keep LOADER_CONCURRENCY=1.
5. Highest passed synthetic throughput came from batch 25000 and records/segment 250000.

## Recommendation

- current merge strategy acceptable: `True`
- synchronous clickhouse-client path acceptable: `True`
- note: Stress evidence improves approval confidence but does not auto-start full bulk load.
