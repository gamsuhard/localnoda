# Stress Run: segments-10

- wave: `segment-count-sweep`
- run_id: `stress-segments-10-20260416t112926z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_stress_segments_10_20260`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `39839`
- dominant bottleneck: `merge`
- throughput rows/sec: `2510.1`
- throughput bytes/sec: `58062.58`
- throughput segments/sec: `0.251`

## Replay

- wall_ms: `33142`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"validated": 10}`
- clickhouse counts: `{"audit": {"audit_rows": 40}, "canonical": {"events": 100000, "unique_events": 100000}, "legs": {"legs": 200000, "unique_legs": 200000}}`
