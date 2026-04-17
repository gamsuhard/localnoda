# Block 10 Stop Policy

Date: 2026-04-16  
Status: operator stop policy for first bounded bulk

## Principle

The first bounded bulk must favor transparency and data safety over persistence.

If the run enters a state where correctness or operator visibility is no longer
clear, stop the bounded bulk and preserve evidence immediately.

## Immediate stop conditions

Stop the run immediately if any one of the following occurs:

1. any segment enters `quarantined`
2. upload verification mismatch is detected
3. sha256 mismatch is detected on loader download
4. loader runtime lock becomes inconsistent
5. replay-safe invariants are violated
6. manifest counts drift against canonical counts
7. a ClickHouse query returns an integrity error for canonical merge

## Repeated-failure stop conditions

Stop the run if any one of the following occurs:

1. the same segment enters `failed` twice
2. two consecutive segments enter `failed`
3. the same loader step repeats with the same error after one clean retry

## Throughput / latency stop conditions

For the first bounded bulk, use the conservative thresholds below:

- warn if `stage_ms > 60000` for a segment
- stop if `stage_ms > 180000` for two consecutive segments
- warn if total `seconds_per_segment > 300`
- stop if total `seconds_per_segment > 600` for two consecutive segments

These thresholds are intentionally conservative relative to the validated
real-slice and stress evidence. They are operational stop guards, not tuning targets.

## Upload-lag stop conditions

Stop the run if:

1. the number of `sealed` but not `uploaded + verified` segments keeps increasing across two operator checks
2. upload verification repeatedly fails for the same segment
3. source-side lag materially exceeds the operator’s expected window without a clear network explanation

## Operator actions on stop

When a stop condition triggers:

1. stop creating new work
2. preserve loader ledger snapshot
3. preserve current deploy metadata
4. preserve exact-tree artifact sha256
5. preserve S3/manifests/checkpoint evidence
6. write one stop summary with:
   - run id
   - last successful segment
   - first failing segment
   - exact stop reason
   - next safe resume point

## Explicit non-actions

When stopping the first bounded bulk:

- do not widen scope
- do not enable async inserts
- do not enable multi-worker loader execution
- do not bypass the ledger or runtime lock
