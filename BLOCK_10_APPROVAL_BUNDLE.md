# Block 10 Approval Bundle

Date: 2026-04-21
Status: historical approval and execution record

## Goal

Record the evidence package that justified and now explains the first bounded
bulk USDT run.

This file is no longer a pre-start placeholder. It now documents the actual
execution outcome.

## Approval verdict state

`APPROVED_FOR_FIRST_BOUNDED_BULK`

Historical note:

- the approval state above is now historical
- the first bounded bulk was actually executed
- the USDT bounded run completed source upload and loader reconciliation

## Executed bounded run

- Run ID: `tron-usdt-backfill-20231103-20260201-20260417t221647z`
- Window start block: `56112550`
- Resolved end block: `79746535`
- Last block with a USDT transfer: `79743883`
- Uploaded source segments: `4168`
- Source compressed raw size: `292669935182` bytes (`272.57 GiB`)

## Final reconciliation

- Final loader ledger observed before shutdown:
  - `validated = 3681`
  - `skipped = 487`
  - `processed total = 4168`
- Canonical counts observed before shutdown:
  - events: `1755555770`
  - legs: `3511111540`
  - `legs == 2 * events`

## Bundle contents for the executed run

### Frozen runtime / readiness docs

- `BLOCK_09_CLOSURE_DECISION.md`
- `BLOCK_10_RUNTIME_FREEZE.md`
- `BLOCK_10_OPERATOR_CHECKLIST.md`
- `BLOCK_10_STOP_POLICY.md`

### Pre-run evidence that remained relevant

- pre-bulk gate reports
- loader stress summary and per-run reports
- controlled real slice evidence bundle
- real multi-segment canary evidence bundle

### Executed run closure bundle

- [load_summary.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/load_summary.json)
- [validation.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/validation.json)
- [replay.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/replay.json)
- [storage_measurement.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/storage_measurement.json)
- [operator_summary.md](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/operator_summary.md)

## Remaining limitations

- The Frankfurt loader-host was intentionally shut down after the run finished.
- A fresh post-shutdown re-probe on `2026-04-21` was blocked because
  `StartInstances` returned AWS account status `Blocked`.
- The executed-run bundle therefore uses the final live loader/canonical probes
  captured before shutdown, plus durable S3 and Singapore SQLite evidence.

## Current truthful state

Current truthful state is:

`APPROVED_FOR_FIRST_BOUNDED_BULK`

with the following historical interpretation:

- the first bounded bulk was approved
- the first bounded bulk was executed
- the USDT bounded run completed materially successfully
- post-run work now belongs to validation / audit / handoff, not to approval
