# Full Bounded USDT Operator Summary

- Run ID: `tron-usdt-backfill-20231103-20260201-20260417t221647z`
- Git commit in current workspace: `7c71c83cce60c8555321e4265330438eceb85050`
- ClickHouse database: `tron_usdt_reprmonth_20231103_20231203_20260417t221647z`
- Verdict: `source upload complete; trc20_transfer_events validated; address_transfer_legs incomplete`

Corrective addendum:

- [posthoc_legs_validation_addendum.md](/G:/CODEX/LOCALNODA/local-tron-usdt-backfill/reports/full-bounded-usdt/tron-usdt-backfill-20231103-20260201-20260417t221647z/posthoc_legs_validation_addendum.md)

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
- Canonical leg rows observed after post-hoc manual verification: `283854684`
- Expected leg rows if complete: `3511111540`
- Missing leg rows: `3227256856`
- `legs == 2 * events`: `false`

## Operational notes

- Segment numbering reached `seg-004179`, but the authoritative uploaded segment count is `4168`.
- The `11` numbering gaps are explained by controlled restart residue:
  - `10` orphaned partial files remained under the run directory
  - `1` live `.partial` advanced to `seg-004180` after the uploaded run had already closed
- One extractor upload-state row (`seg-002366`) kept `uploaded_at = null` while also holding a verified S3 key and `last_verified_at`. This was treated as SQLite bookkeeping drift, not missing source data.

## Storage conclusion

- Exact event-table footprint from post-hoc table metadata: `249776592038` bytes (`232.62 GiB`)
- Current observed partial leg-table footprint: `36581490855` bytes (`34.07 GiB`)
- Current observed total footprint of the two tables as they exist now: `286358082893` bytes (`266.69 GiB`)
- No corrected final full `events + legs` disk budget is asserted here because `address_transfer_legs` is incomplete.

## Limits that remain documented but non-blocking

- A fresh post-shutdown Frankfurt re-probe was not possible on `2026-04-21` because `StartInstances` returned AWS account status `Blocked`.
- The bounded run is therefore closed from the final live probes captured before shutdown, plus durable source-side artifacts in `S3` and extractor SQLite.
- Replay safety is accepted from prior evidence on the same loader path:
  - controlled real slice replay
  - real two-segment canary replay
  - loader stress restart/replay drills

## Recovery viability for legs

- Local raw still exists on Singapore:
  - `/srv/local-tron-usdt-backfill/raw -> /mnt/raw-ebs/raw`
- The raw volume still contains this run locally.
- Local `segments/*.ndjson.gz` count for this run is `4169`, while the authoritative uploaded run count is `4168`.
- This means a legs rebuild is still possible, but it must be scoped by the authoritative run manifest/checkpoint and not by local raw file count alone.

## Closure decision

- Block 09 remains usable only for source raw and event-table storage evidence.
- Block 10 is not fully closed because `address_transfer_legs` remains incomplete.
