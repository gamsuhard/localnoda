# Stress Run: segment-size-100000

- wave: `segment-size-sweep`
- run_id: `stress-segsize-100000-20260416t112926z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_stress_segsize_100000_20`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `177753`
- dominant bottleneck: `stage_insert`
- throughput rows/sec: `5625.78`
- throughput bytes/sec: `129961.26`
- throughput segments/sec: `0.0563`

## Replay

- wall_ms: `167326`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"validated": 10}`
- clickhouse counts: `{"audit": {"audit_rows": 40}, "canonical": {"events": 1000000, "unique_events": 1000000}, "legs": {"legs": 2000000, "unique_legs": 2000000}}`
