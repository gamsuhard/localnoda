# Block 05 - Sealed Segment Uploader

## Goal

Close the extractor-side path from local sealed raw segments into the frozen S3 buffer contour:

`local raw segments -> S3 buffer`

This block does not write to ClickHouse and does not require a live FullNode run.

## Deliverables

1. SQLite upload-state extension for per-segment S3 tracking
2. Uploader that only consumes `segments.status = sealed`
3. Local writers for:
   - `manifests/run.json`
   - `checkpoints/extraction.json`
   - `checksums/SHA256SUMS`
4. Remote upload of:
   - segment object
   - segment manifest
   - run manifest
   - extraction checkpoint
   - checksums file
5. Upload verification pass against the frozen S3 prefix
6. Local unit test for the `sealed -> uploaded` path with fake S3

## Frozen behavior

- uploader never touches `open` segments
- uploader marks a segment `uploaded` only after the segment object and segment manifest are safely present in S3
- uploader uses head-object idempotency on `sha256` and `file_size_bytes`
- uploader writes run/checkpoint/checksum sidecars under the canonical run prefix
- uploader writes `resolved_end_block` from the frozen run boundary, never from the last uploaded segment
- uploader refuses to synthesize production runtime manifests from `__UNSET__` placeholders
- uploader does not mutate ClickHouse and does not depend on the live node

## Remote prefix layout

For run `tron-usdt-backfill-YYYYMMDD-HHMMSSZ`:

- `providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/<run_id>/segments/<segment_file>`
- `providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/<run_id>/manifests/segments/<segment_manifest_file>`
- `providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/<run_id>/manifests/run.json`
- `providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/<run_id>/checkpoints/extraction.json`
- `providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/<run_id>/checksums/SHA256SUMS`

## Acceptance criteria

1. A sealed local segment uploads to S3 under the frozen prefix
2. SQLite records `s3_bucket`, `s3_key`, `uploaded_at`, `etag`, `upload_attempts`, and `last_upload_error`
3. Local `segments.status` transitions from `sealed` to `uploaded`
4. Re-running the uploader is idempotent when remote objects already match local `sha256` and `file_size_bytes`
5. The unit test passes without AWS access
