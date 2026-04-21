# Full Bounded USDT Operator Summary

- Run ID: `tron-usdt-backfill-20231103-20260201-20260417t221647z`
- Git commit in current workspace: `7c71c83cce60c8555321e4265330438eceb85050`
- ClickHouse database: `tron_usdt_reprmonth_20231103_20231203_20260417t221647z`
- Verdict: `BLOCK_09_FORMALLY_CLOSED` and `BLOCK_10_EXECUTED_FOR_USDT`

## Bounded run result

- Window start block: `56112550`
- Resolved end block: `79746535`
- Last block with a USDT transfer in this run: `79743883`
- Eventless tail after the last transfer: `2652` blocks
- Source segments uploaded: `4168`
- Source rows emitted: `1972922649`
- Source compressed raw size in S3: `292669935182` bytes (`272.57 GiB`)

## Canonical load result

- Final loader reconciliation observed before the Frankfurt host shutdown:
  - `validated = 3681`
  - `skipped = 487`
  - `processed total = 4168`
- Canonical event rows observed: `1755555770`
- Canonical leg rows observed: `3511111540`
- `legs == 2 * events`: `true`

## Operational notes

- Segment numbering reached `seg-004179`, but the authoritative uploaded segment count is `4168`.
- The `11` numbering gaps are explained by controlled restart residue:
  - `10` orphaned partial files remained under the run directory
  - `1` live `.partial` advanced to `seg-004180` after the uploaded run had already closed
- One extractor upload-state row (`seg-002366`) kept `uploaded_at = null` while also holding a verified S3 key and `last_verified_at`. This was treated as SQLite bookkeeping drift, not missing source data.

## Storage conclusion

- Exact event-table `bytes_on_disk` observed before shutdown: `249776555862` bytes (`232.62 GiB`)
- Estimated final leg-table `bytes_on_disk`: `37224302272` bytes (`34.67 GiB`)
- Estimated total canonical on-disk footprint: `287000858134` bytes (`267.29 GiB`)
- Recommended disk budget:
  - minimum `350 GiB` per replica
  - comfortable `500 GiB` per replica

## Limits that remain documented but non-blocking

- A fresh post-shutdown Frankfurt re-probe was not possible on `2026-04-21` because `StartInstances` returned AWS account status `Blocked`.
- The bounded run is therefore closed from the final live probes captured before shutdown, plus durable source-side artifacts in `S3` and extractor SQLite.
- Replay safety is accepted from prior evidence on the same loader path:
  - controlled real slice replay
  - real two-segment canary replay
  - loader stress restart/replay drills

## Closure decision

- Block 09 is formally closed because the actual full bounded run is stronger evidence than the previously planned representative-month surrogate.
- Block 10 is closed for the USDT bounded run because source upload completed and loader reconciliation reached `4168 / 4168` processed segments before shutdown.
