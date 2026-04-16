# BLOCK_03 - Extraction boundaries and config freeze

Date: 2026-04-15
Status: completed locally, pending node-side render/use
Scope: one-off USDT-on-TRON historical backfill

---

## 1. Goal

Resolve the exact bounded extraction start and freeze the runtime values that
must be used later by the event-service config and extractor runtime.

---

## 2. Resolved boundary

Frozen extraction start:

- target timestamp: `2023-09-01T00:00:00Z`
- exact start block: `54298720`
- exact start block timestamp: `2023-09-01T00:00:00Z`
- previous block: `54298719`
- previous block timestamp: `2023-08-31T23:59:57Z`

Resolution rule:

- use the first TRON block with `block_timestamp >= 2023-09-01T00:00:00Z`

Resolution method used for this freeze:

- public TRON API boundary discovery
- binary search over block heights
- retry/backoff to avoid public endpoint rate limits

---

## 3. Frozen runtime values

- `event.subscribe.version = 1`
- `startSyncBlockNum = 54298720`
- `contractAddress = [TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t]`
- `contractTopic = [ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef]`
- trigger posture: `soliditylog`
- `native.useNativeQueue = false`

---

## 4. End boundary posture

No exact end block is frozen in Block 03.

Frozen rule instead:

- phase 1 remains a bounded historical backfill
- the end block is resolved later from the approved solidified tip at run start
- no broad unbounded “forever sync” posture is introduced

This keeps Block 03 aligned with the one-off extraction contour while avoiding
freezing a moving target before the node is ready.

---

## 5. Produced artifacts

- `artifacts/manifests/block_03_boundary.freeze.json`
- `artifacts/manifests/block_03_runtime_config.freeze.json`
- `artifacts/manifests/block_03_filter_manifest.json`
- `scripts/ops/30_resolve_tron_start_block.py`

---

## 6. Acceptance

Block 03 is accepted when:

- the exact start block is recorded in a manifest
- the runtime config manifest is frozen
- the contract/topic filter manifest is frozen
- extractor env placeholders for start block and transfer topic are gone
- no broad “all TRC20” subscription remains in the project context
