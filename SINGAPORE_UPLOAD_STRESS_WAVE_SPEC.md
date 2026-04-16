# Singapore Upload Stress Wave Spec

Date: 2026-04-16  
Status: execution spec for cost-critical extractor upload contour

## Goal

Measure and judge the cost-critical path:

`Singapore extractor -> Frankfurt S3 buffer`

This wave is not about ClickHouse correctness.  
It exists to measure how long the expensive Singapore extractor remains needed before segments become `uploaded + verified`.

## Exact metrics

Every scenario in the wave must record at minimum:

- `upload_ms`
- `verify_ms`
- `sealed_to_uploaded_ms`
- `bytes_per_second`
- `peak_rss_kb`
- `head_object_count`
- `put_object_count`
- `retry_count`
- total wall clock until all segments reach `uploaded + verified`

## Exact test matrix

The wave must include the following baseline matrix.

### Segment size sweep

Fixed `segment_count = 10`

- `32 MB`
- `128 MB`
- `512 MB`

### Segment count sweep

Fixed `segment_size = 128 MB`

- `10`
- `50`
- `100`

## Required evidence bundle

The wave must produce:

- exact-tree artifact sha256
- deploy metadata
- extractor tooling bootstrap proof
- one JSON report per scenario
- one summary JSON
- one operator markdown summary

Each per-scenario report must include:

- run id
- upload mode
- region
- bucket / prefix
- segment size
- segment count
- all required metrics
- explicit success / failure outcome

## Operator checklist

Before the wave:

- confirm current Singapore extractor restore is not interrupted
- confirm wave runs in a separate workspace/runtime path
- confirm cleanup policy for temporary uploaded test objects
- confirm same Frankfurt bucket / prefix / KMS path used by the real project

After the wave:

- confirm every scenario reached a visible terminal state
- confirm no hidden backlog exists
- confirm all temporary uploaded objects were removed if cleanup was enabled
- confirm summary report contains one explicit decision

## Acceptance / reject criteria

### Current simple upload path is acceptable only if all conditions below are true

1. all baseline scenarios complete successfully
2. no uploaded object fails verification
3. `peak_rss_kb <= 256000` on heavy cases:
   - `512 MB x 10`
   - `128 MB x 50`
   - `128 MB x 100`
4. `bytes_per_second >= 20971520` on all heavy cases
5. `retry_count / (head_object_count + put_object_count) <= 2.0`
6. total wall clock for `128 MB x 100` stays within `15 minutes`

### Reject current simple upload path if any one of the following is true

- heavy-case RSS exceeds the acceptance bound
- heavy-case throughput falls below the acceptance bound
- retries escalate materially above the acceptance bound
- wall clock for `128 MB x 100` exceeds the acceptance bound
- any scenario shows upload/verify instability

## Required next action depending on outcome

### If current simple upload path is acceptable

- freeze current uploader path for the first bounded bulk
- carry the wave summary into the Block 10 approval package

### If current simple upload path is rejected

Perform one focused optimization step only:

- multipart upload / `TransferConfig` / tuned concurrency

Then rerun only the heavy comparison subset:

- `512 MB x 10`
- `128 MB x 50`
- `128 MB x 100`

## Explicit non-goals

Do **not** do the following in response to this wave:

- no async architectural redesign
- no queue redesign
- no multi-worker redesign
- no new subsystem such as Kafka / Redis / Postgres queue

The first candidate optimization is only:

- multipart upload
- tuned `TransferConfig`
- tuned concurrency
