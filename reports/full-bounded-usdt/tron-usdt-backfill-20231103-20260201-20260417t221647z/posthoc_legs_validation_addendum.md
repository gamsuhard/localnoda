# Post-Hoc Legs Validation Addendum

Date: 2026-04-21
Status: corrective addendum after manual ClickHouse verification

## Why this addendum exists

After the initial Block 09/10 closure bundle was written, manual ClickHouse
console verification on `2026-04-21` showed that the prior `legs == 2 * events`
claim was incorrect.

The earlier bundle had reused a watcher-derived synthetic field from
`scripts/ops/22_watch_pipeline_eta.py`:

- `clickhouse_legs = count(trc20_transfer_events) * 2`

That field did **not** query `address_transfer_legs` directly.

## Corrected ClickHouse facts

Database:

- `tron_usdt_reprmonth_20231103_20231203_20260417t221647z`

Manual post-hoc verification showed:

- `trc20_transfer_events` rows: `1755555770`
- `address_transfer_legs` actual rows: `283854684`
- expected legs if complete: `3511111540`
- missing leg rows: `3227256856`
- `legs != 2 * events`

Current table metadata from the same manual verification:

- `trc20_transfer_events.total_bytes = 249776592038` (`232.62 GiB`)
- `address_transfer_legs.total_bytes = 36581490855` (`34.07 GiB`)
- current observed combined footprint = `286358082893` (`266.69 GiB`)

Additional sanity checks from the same verification:

- only one matching database existed for the bounded run suffix
- `system.mutations` was empty
- partition distribution showed `address_transfer_legs` present only
  partially/sporadically across the range, not fully materialized

## Status impact

- Source upload / S3 reconciliation: complete
- Loader processed-segment reconciliation: complete
- `trc20_transfer_events`: validated
- `address_transfer_legs`: incomplete

Therefore:

- Block 09 remains usable only for source raw and event-table sizing evidence
- the earlier full-canonical-pair storage claim is withdrawn
- Block 10 full validation/handoff remains open until legs are rebuilt or
  explicitly waived

## Recovery viability

Raw payload was deleted from S3 after the initial closure, but recovery is still
possible because Singapore retains the local raw archive:

- `/srv/local-tron-usdt-backfill/raw -> /mnt/raw-ebs/raw`
- current raw archive size on Singapore: about `470G`

Local run presence on Singapore was verified on `2026-04-21`:

- run directory exists under:
  - `/srv/local-tron-usdt-backfill/raw/runs/tron-usdt-backfill-20231103-20260201-20260417t221647z`
- local raw segment files for this run:
  - `4169` `*.ndjson.gz`

Authoritative uploaded run facts remain:

- uploaded source segments: `4168`
- S3 payload for those segments has been deleted
- manifests/checkpoint/checksums remain in S3

Interpretation:

- a legs rebuild is still possible from Singapore local raw plus retained S3
  metadata
- rebuild scoping must follow the authoritative run manifest/checkpoint, not the
  local raw file count alone, because local residue contains at least one extra
  post-close segment beyond the authoritative uploaded set
