# Block 09 Closure Decision

Date: 2026-04-21
Status: corrected after post-hoc ClickHouse verification
Decision path: `A1`

## Verdict

`BLOCK_09_SOURCE_RAW_AND_EVENT_TABLE_STORAGE_EVIDENCE_CLOSED`

Corrective addendum:

- [posthoc_legs_validation_addendum.md](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/posthoc_legs_validation_addendum.md)

## Why Block 09 is now closed

The original Block 09 gate existed to avoid blind sizing guesses by forcing one
representative bounded run before the first bulk execution.

That surrogate is no longer needed for **source raw sizing** and the
`trc20_transfer_events` table because the project now has stronger evidence:

- the actual full bounded USDT run completed source upload
- the full bounded run produced exact source-side row and byte totals
- the loader reconciled all `4168` uploaded segments before shutdown
- canonical event rows were observed on the production target schema before shutdown
- replay-safe semantics were already accepted on the same loader path through:
  - the controlled real slice
  - the real two-segment canary
  - the loader restart/replay stress drills

What changed after the initial closure:

- post-hoc manual ClickHouse verification on `2026-04-21` showed that
  `address_transfer_legs` was only partially materialized
- the prior `legs == 2 * events` claim came from a synthetic watcher field that
  computed `count(events) * 2`, not from a real query against
  `address_transfer_legs`
- therefore the earlier full-canonical-pair storage claim is withdrawn

## Final bounded-run facts used for closure

- Run ID: `tron-usdt-backfill-20231103-20260201-20260417t221647z`
- Window start block: `56112550`
- Resolved end block: `79746535`
- Last block with a USDT transfer in this run: `79743883`
- Eventless tail after the last transfer: `2652` blocks
- Uploaded source segments: `4168`
- Source rows emitted: `1972922649`
- Source compressed raw bytes in S3: `292669935182` (`272.57 GiB`)
- Final loader reconciliation observed before the Frankfurt shutdown:
  - `validated = 3681`
  - `skipped = 487`
  - `processed total = 4168`
- Post-hoc ClickHouse facts from manual console verification on `2026-04-21`:
  - events: `1755555770`
  - actual legs: `283854684`
  - expected legs if complete: `3511111540`
  - missing legs: `3227256856`
  - `legs != 2 * events`

## Storage conclusion

Block 09 required real storage evidence. The full bounded run now provides that
for source raw and the event table:

- exact source compressed raw size: `272.57 GiB`
- exact canonical event-table footprint from post-hoc table metadata:
  - `249776592038` bytes
  - `232.62 GiB`
- current observed partial leg-table footprint:
  - `36581490855` bytes
  - `34.07 GiB`
- current observed total footprint of the two tables as they exist now:
  - `286358082893` bytes
  - `266.69 GiB`

Important correction:

- the current `266.69 GiB` total is **not** a valid final full-canonical budget
  because `address_transfer_legs` is incomplete
- no corrected full `events + legs` per-replica storage recommendation is
  asserted here until legs are rebuilt or explicitly waived

## Documented limitations that do not keep this corrected Block 09 claim open

- The Frankfurt loader-host was intentionally shut down after completion.
- On `2026-04-21`, a fresh re-probe was blocked because `StartInstances`
  returned AWS account status `Blocked`.
- The closure therefore relies on:
  - durable S3 run artifacts
  - durable extractor SQLite state on Singapore
  - the final live loader/canonical probes captured before shutdown
  - the post-hoc manual ClickHouse verification performed on `2026-04-21`

These limitations do **not** outweigh the fact that an actual full bounded run
was executed and reconciled for source upload and canonical events. That is
materially stronger evidence than the original representative-month surrogate
for the parts of Block 09 that are still being claimed here.

## Closure artifacts

The final bounded-run closure bundle lives at:

- [load_summary.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/load_summary.json)
- [validation.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/validation.json)
- [replay.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/replay.json)
- [storage_measurement.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/storage_measurement.json)
- [operator_summary.md](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/operator_summary.md)
- [posthoc_legs_validation_addendum.md](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/posthoc_legs_validation_addendum.md)

## What this means

- Block 09 is closed only for:
  - exact source raw sizing
  - exact `trc20_transfer_events` sizing
- the prior full `events + legs` storage claim is superseded by the corrective
  addendum
- Block 10 does not depend on a representative-month waiver, but full
  validation still depends on resolving or waiving the incomplete legs table
