# Block 10 Runtime Freeze

Date: 2026-04-16  
Status: readiness freeze before first bounded bulk

## Current execution status

`READY_FOR_BLOCK_10_PENDING_SOURCE_SIDE_DATA_AVAILABILITY_AND_MANUAL_APPROVAL`

This means:

- loader/materialization path is considered execution-ready for the first bounded bulk
- source-side evidence is still waiting on Singapore restore completion and additional real segments
- starting Block 10 still requires explicit manual approval

## Frozen runtime settings

### Loader

- `LOADER_CONCURRENCY=2`
- `LOADER_RECORD_BATCH_SIZE=350000`
- exact-tree deploy only
- two-worker loader only with isolated per-slot staging
- runtime lock and shared SQLite ledger must remain enabled

### Extractor / segment target

- `TRON_FILE_SINK_SEGMENT_MAX_RECORDS=500000`
- `TRON_FILE_SINK_SEGMENT_MAX_BYTES=671088640`
- extractor -> S3 -> loader -> private ClickHouse contour only

### Extractor upload mode

Public-tree frozen uploader mode remains the current baseline code path in
`extractor/supervisor/10_upload_sealed_segments.py`.

Do **not** treat any unpublished local upload optimization result as the public
runtime default until the code and evidence are pushed and reviewed.

### Exact-tree artifact

- artifact path: `PRIVATE_AUDIT_ARCHIVE/pre-block10-readiness-20260416t154559z/artifact/workspace-20260416t154559z.tar.gz`
- sha256: `37d5311a1badfd12d2f38dbe703ca21362e364baf8b52c795c743d26f456fedc`

### Schema naming rule

- disposable validation schema: `tron_usdt_<purpose>_<utcstamp>`
- representative-month-like schema: `tron_usdt_month_<yyyymm>_<utcstamp>`
- first bounded bulk target schema must be explicit and stable
- never run the first bounded bulk into `tron_usdt_local`
- never reuse a disposable schema name for the first bounded bulk

## Exact frozen evidence references

- latest public loader stress recommendation remains the basis for the current contour, but live runtime has since been tuned upward under operator supervision:
  - `LOADER_RECORD_BATCH_SIZE=350000`
  - `records/segment target=500000`
- current live contour keeps:
  - deferred legs in hot path disabled
  - per-segment canonical recounts skipped on initial pass
  - two-worker isolated staging on the Frankfurt loader host

## What this freeze does not do

This freeze does **not**:

- close Block 09 formally
- authorize Block 10 automatically
- waive the representative-month-like bounded run
- override the manual approval boundary
