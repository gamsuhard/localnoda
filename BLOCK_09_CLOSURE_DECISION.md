# Block 09 Closure Decision

Date: 2026-04-21
Status: final post-Block10 closure decision
Decision path: `A1`

## Verdict

`BLOCK_09_FORMALLY_CLOSED`

## Why Block 09 is now closed

The original Block 09 gate existed to avoid blind sizing guesses by forcing one
representative bounded run before the first bulk execution.

That surrogate is no longer needed because the project now has **stronger**
evidence:

- the actual full bounded USDT run completed source upload
- the full bounded run produced exact source-side row and byte totals
- the loader reconciled all `4168` uploaded segments before shutdown
- canonical counts were observed on the production target schema before shutdown
- replay-safe semantics were already accepted on the same loader path through:
  - the controlled real slice
  - the real two-segment canary
  - the loader restart/replay stress drills

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
- Canonical counts observed before the Frankfurt shutdown:
  - events: `1755555770`
  - legs: `3511111540`
  - `legs == 2 * events`

## Storage conclusion

Block 09 required real storage evidence. The full bounded run now provides that:

- exact source compressed raw size: `272.57 GiB`
- exact canonical event-table `bytes_on_disk` observed before shutdown:
  - `249776555862` bytes
  - `232.62 GiB`
- estimated final total canonical footprint:
  - `287000858134` bytes
  - `267.29 GiB`

Recommended disk budget going forward:

- minimum `350 GiB` per replica
- comfortable `500 GiB` per replica

## Documented limitations that do not keep Block 09 open

- The Frankfurt loader-host was intentionally shut down after completion.
- On `2026-04-21`, a fresh re-probe was blocked because `StartInstances`
  returned AWS account status `Blocked`.
- The closure therefore relies on:
  - durable S3 run artifacts
  - durable extractor SQLite state on Singapore
  - the final live loader/canonical probes captured before shutdown

These limitations do **not** outweigh the fact that an actual full bounded run
was executed and reconciled. That is materially stronger evidence than the
original representative-month surrogate.

## Closure artifacts

The final bounded-run closure bundle lives at:

- [load_summary.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/load_summary.json)
- [validation.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/validation.json)
- [replay.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/replay.json)
- [storage_measurement.json](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/storage_measurement.json)
- [operator_summary.md](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/operator_summary.md)

## What this means

- Block 09 is formally closed.
- Block 10 no longer depends on a representative-month waiver.
- Further work moves to post-load validation / audit / handoff, not to storage
  uncertainty.
