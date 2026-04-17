# BLOCK_03 - Extraction boundaries and config freeze

Date: 2026-04-15
Status: completed locally, pending node-side render/use
Scope: one-off USDT-on-TRON historical backfill

---

## 1. Goal

Resolve the exact bounded extraction interval and freeze the runtime values that
must be used later by the event-service config and extractor runtime.

---

## 2. Resolved boundary

Frozen full bounded interval:

- target start timestamp: `2023-11-03T00:00:00Z`
- exact start block: `56112550`
- exact start block timestamp: `2023-11-03T00:00:00Z`
- previous start block: `56112549`
- previous start block timestamp: `2023-11-02T23:59:57Z`
- target end timestamp (exclusive): `2026-02-01T00:00:00Z`
- exact end block exclusive: `79746536`
- exact end block exclusive timestamp: `2026-02-01T00:00:00Z`
- resolved inclusive end block: `79746535`
- resolved inclusive end block timestamp: `2026-01-31T23:59:57Z`
- time semantics: half-open interval `[2023-11-03T00:00:00Z, 2026-02-01T00:00:00Z)`

Resolution rule:

- use the first TRON block with `block_timestamp >= target_start_timestamp_utc`
- use the first TRON block with `block_timestamp >= target_end_timestamp_utc` as the exclusive upper boundary

Resolution method used for this freeze:

- public TRON API boundary discovery
- binary search over block heights
- retry/backoff to avoid public endpoint rate limits

---

## 3. Frozen runtime values

- `event.subscribe.version = 1`
- `startSyncBlockNum = 56112550`
- `contractAddress = [TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t]`
- `contractTopic = [ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef]`
- trigger posture: `soliditylog`
- `native.useNativeQueue = false`

---

## 4. End boundary posture

The bounded execution interval is now frozen explicitly:

- start UTC: `2023-11-03T00:00:00Z`
- end UTC exclusive: `2026-02-01T00:00:00Z`
- inclusive end block: `79746535`

No broad unbounded “forever sync” posture is introduced.

---

## 5. Produced artifacts

- `artifacts/manifests/block_03_boundary.freeze.json`
- `artifacts/manifests/block_03_runtime_config.freeze.json`
- `artifacts/manifests/block_03_filter_manifest.json`
- `scripts/ops/30_resolve_tron_start_block.py`

---

## 6. Acceptance

Block 03 is accepted when:

- the exact start and exclusive end block boundaries are recorded in manifests
- the runtime config manifest is frozen
- the contract/topic filter manifest is frozen
- extractor env placeholders for start block and transfer topic are gone
- no broad “all TRC20” subscription remains in the project context
