# Stress Run: real-slice-initial

- wave: `real-controlled-slice`
- run_id: `real-slice-20230901t0000z-20260416t114407z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_real_slice_20230901t0000`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `8816`
- dominant bottleneck: `stage_insert`
- throughput rows/sec: `3934.89`
- throughput bytes/sec: `358867.63`
- throughput segments/sec: `0.1134`

## Replay

- wall_ms: `7194`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"validated": 1}`
- clickhouse counts: `{"audit": {"audit_rows": 4}, "canonical": {"events": 34690, "unique_events": 34690}, "legs": {"legs": 69380, "unique_legs": 69380}}`
