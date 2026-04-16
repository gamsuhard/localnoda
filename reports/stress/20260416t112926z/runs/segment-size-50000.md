# Stress Run: segment-size-50000

- wave: `segment-size-sweep`
- run_id: `stress-segsize-50000-20260416t112926z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_stress_segsize_50000_202`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `95335`
- dominant bottleneck: `stage_insert`
- throughput rows/sec: `5244.66`
- throughput bytes/sec: `121177.82`
- throughput segments/sec: `0.1049`

## Replay

- wall_ms: `87008`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"validated": 10}`
- clickhouse counts: `{"audit": {"audit_rows": 40}, "canonical": {"events": 500000, "unique_events": 500000}, "legs": {"legs": 1000000, "unique_legs": 1000000}}`
