# Stress Run: medium-initial

- wave: `replay-fault`
- run_id: `stress-replay-medium-20260416t112926z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_stress_replay_medium_202`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `35281`
- dominant bottleneck: `merge`
- throughput rows/sec: `2834.39`
- throughput bytes/sec: `65563.76`
- throughput segments/sec: `0.2834`

## Replay

- wall_ms: `32207`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"validated": 10}`
- clickhouse counts: `{"audit": {"audit_rows": 40}, "canonical": {"events": 100000, "unique_events": 100000}, "legs": {"legs": 200000, "unique_legs": 200000}}`
