# Stress Run: medium-corrupted-segment

- wave: `replay-fault`
- run_id: `stress-corrupt-20260416t112926z`
- status: `passed`
- database: `tron_usdt_stress_20260416t112926z_stress_corrupt_20260416t`
- artifact sha256: `a945c595b1a99b42565c1a4b68a167057e29a8827d4875948f3c3722256b7abf`

## Initial Load

- wall_ms: `0`
- dominant bottleneck: ``
- throughput rows/sec: `0`
- throughput bytes/sec: `0`
- throughput segments/sec: `0`

## Replay

- wall_ms: `0`
- new canonical event rows: `0`
- new leg rows: `0`

## Loader State

- segment status summary: `{"failed": 1, "pending": 8, "validated": 1}`
- clickhouse counts: `{"audit": {"audit_rows": 2}, "canonical": {"events": 10000, "unique_events": 10000}, "legs": {"legs": 20000, "unique_legs": 20000}}`

## Error

- `RuntimeError: segment sha256 mismatch for stress-corrupt-20260416t112926z-seg-000002: expected 34c83a6a276a3e4126bab19958f96e4635555644ca1732b1339186a3bb8aafc0 got 1774b0a2420357a368070ab9c3a8f1f3421854b0c700861ca745e978fef43aee`
