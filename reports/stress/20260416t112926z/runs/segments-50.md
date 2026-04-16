# Stress Run: segments-50

- wave: `segment-count-sweep`
- run_id: `stress-segments-50-20260416t112926z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_stress_segments_50_20260`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `194961`
- dominant bottleneck: `merge`
- throughput rows/sec: `2564.62`
- throughput bytes/sec: `59324.3`
- throughput segments/sec: `0.2565`

## Replay

- wall_ms: `189550`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"validated": 50}`
- clickhouse counts: `{"audit": {"audit_rows": 200}, "canonical": {"events": 500000, "unique_events": 500000}, "legs": {"legs": 1000000, "unique_legs": 1000000}}`
