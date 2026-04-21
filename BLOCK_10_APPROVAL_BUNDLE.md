# Block 10 Approval Bundle

Date: 2026-04-21
Status: historical approval and execution record corrected after post-hoc validation

## Goal

Record the evidence package that justified and now explains the first bounded
bulk USDT run.

This file is no longer a pre-start placeholder. It now documents the actual
execution outcome.

Corrective addendum:

- [posthoc_legs_validation_addendum.md](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/posthoc_legs_validation_addendum.md)

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
- Canonical counts after post-hoc manual ClickHouse verification on `2026-04-21`:
  - events: `1755555770`
  - actual legs: `283854684`
  - expected legs if complete: `3511111540`
  - `legs != 2 * events`

This means:

- `trc20_transfer_events` materialization completed materially successfully
- `address_transfer_legs` did **not** complete
- the earlier `legs == 2 * events` claim in the initial closure bundle was
  incorrect because it reused a watcher-derived synthetic field instead of a
  real `address_transfer_legs` query

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
- [posthoc_legs_validation_addendum.md](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/posthoc_legs_validation_addendum.md)

## Remaining limitations

- The Frankfurt loader-host was intentionally shut down after the run finished.
- A fresh post-shutdown re-probe on `2026-04-21` was blocked because
  `StartInstances` returned AWS account status `Blocked`.
- The executed-run bundle therefore uses the final live loader/canonical probes
  captured before shutdown, plus durable S3 and Singapore SQLite evidence.
- S3 raw payload has since been deleted, so any legs recovery now depends on
  the preserved local raw archive on Singapore plus the retained S3 metadata.

## Current truthful state

Current truthful state is:

`APPROVED_FOR_FIRST_BOUNDED_BULK`

with the following historical interpretation:

- the first bounded bulk was approved
- the first bounded bulk was executed
- source upload completed
- `trc20_transfer_events` completed materially successfully
- `address_transfer_legs` remains incomplete and still requires rebuild or
  explicit waiver
- post-run work therefore still includes validation/remediation, not only
  audit/handoff
