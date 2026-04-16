# Stress Run: batch-1000

- wave: `batch-size-sweep`
- run_id: `stress-batch-1000-20260416t112926z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_stress_batch_1000_202604`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `73741`
- dominant bottleneck: `stage_insert`
- throughput rows/sec: `1356.1`
- throughput bytes/sec: `31368.64`
- throughput segments/sec: `0.1356`

## Replay

- wall_ms: `71080`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"validated": 10}`
- clickhouse counts: `{"audit": {"audit_rows": 40}, "canonical": {"events": 100000, "unique_events": 100000}, "legs": {"legs": 200000, "unique_legs": 200000}}`
