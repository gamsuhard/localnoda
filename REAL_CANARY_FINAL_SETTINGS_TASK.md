# Real Canary Final Settings Task

Date: 2026-04-16  
Status: execution-ready task before Block 10

## Goal

Run one short real-data canary on the final frozen loader settings, without starting the full bounded bulk run.

This canary exists to prove that the final chosen settings are valid on real data, not only on synthetic stress evidence.

## Frozen runtime settings

- `LOADER_CONCURRENCY=1`
- `LOADER_RECORD_BATCH_SIZE=25000`
- `records/segment target=250000`
- exact-tree deploy only
- current single-worker loader contour only
- disposable ClickHouse schema only

## Exact run scope

### Start boundary

- start UTC: `2023-09-01T00:00:00Z`

### End policy

- bounded canary run
- stop when exactly **2 contiguous real sealed segments** have completed the full path:
  - sealed
  - uploaded
  - verified
  - merged
  - validated

### Hard caps

- do not allow more than `3` real segments in this canary
- do not widen scope beyond the first contiguous canary slice
- do not reuse this canary as implicit approval for full Block 10

## Required execution contour

- exact-tree artifact package
- exact-tree deploy to active hosts
- extractor -> S3 buffer -> loader -> private ClickHouse
- single loader worker only
- fresh disposable schema for the canary
- one replay of the exact same canary run after initial success

## Required outputs

The canary must produce a separate evidence bundle containing:

- exact-tree artifact sha256
- deploy metadata
- canary run manifest
- upload summary
- load summary
- validation report
- replay report
- loader ledger snapshot
- operator summary

## Acceptance criteria

The canary is accepted only if all conditions below are true:

1. exactly 2 contiguous real segments complete successfully
2. no segment ends in `failed` or `quarantined`
3. canonical event count matches manifests
4. canonical leg count equals `2 * events`
5. replay inserts `0` new canonical rows and `0` new leg rows
6. no unresolved upload verification mismatch exists
7. no unresolved loader ledger inconsistency exists
8. all timing fields are present for the completed segments
9. operator summary ends with an explicit canary verdict

## Stop conditions

Stop the canary immediately if any of the following occurs:

- first segment enters `failed`
- first segment enters `quarantined`
- upload verification mismatch
- segment sha256 mismatch
- replay produces new canonical rows
- replay produces new leg rows
- loader runtime lock becomes inconsistent
- more than 3 segments would be required to complete the canary

## Manual approval boundary

Even a passing canary does **not** authorize the full Block 10 run by itself.

A passing canary only adds one more evidence item to the operator approval package.  
Separate manual approval remains required before the first full bounded bulk execution.
