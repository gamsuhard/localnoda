# Stress Run: segment-size-250000

- wave: `segment-size-sweep`
- run_id: `stress-segsize-250000-20260416t112926z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_stress_segsize_250000_20`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `431229`
- dominant bottleneck: `stage_insert`
- throughput rows/sec: `5797.38`
- throughput bytes/sec: `133915.14`
- throughput segments/sec: `0.0232`

## Replay

- wall_ms: `408300`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"validated": 10}`
- clickhouse counts: `{"audit": {"audit_rows": 40}, "canonical": {"events": 2500000, "unique_events": 2500000}, "legs": {"legs": 5000000, "unique_legs": 5000000}}`
