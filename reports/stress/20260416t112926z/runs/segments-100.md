# Stress Run: segments-100

- wave: `segment-count-sweep`
- run_id: `stress-segments-100-20260416t112926z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_stress_segments_100_2026`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `408222`
- dominant bottleneck: `merge`
- throughput rows/sec: `2449.65`
- throughput bytes/sec: `56665.13`
- throughput segments/sec: `0.245`

## Replay

- wall_ms: `386080`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"validated": 100}`
- clickhouse counts: `{"audit": {"audit_rows": 400}, "canonical": {"events": 1000000, "unique_events": 1000000}, "legs": {"legs": 2000000, "unique_legs": 2000000}}`
