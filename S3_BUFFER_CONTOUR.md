# S3 buffer contour

Date: 2026-04-15  
Status: frozen for one-off extractor delivery

---

## 1. Goal

Provide a safe bridge between:

- `ap-southeast-1` temporary extractor host
- `eu-central-1` stage VPC loader
- private ClickHouse endpoint in Frankfurt

The extractor must not write directly to ClickHouse.

---

## 2. Frozen bucket choice

Primary buffer bucket:

- `goldusdt-v2-stage-913378704801-raw`

Reason:

- already in `eu-central-1`
- already private
- already encrypted with SSE-KMS
- already versioned
- already reachable from the stage VPC through the existing S3 gateway endpoint

Do not introduce a public ClickHouse ingress path for this project.

---

## 3. Prefix convention

Frozen prefix root:

- `providers/tron-usdt-backfill/usdt-transfer-oneoff`

Per-run root:

- `providers/tron-usdt-backfill/usdt-transfer-oneoff/runs/<run_id>/`

Required layout:

- `segments/usdt_transfer_000001.ndjson.gz`
- `segments/usdt_transfer_000002.ndjson.gz`
- `manifests/run.json`
- `manifests/runtime.json`
- `manifests/segments/usdt_transfer_000001.manifest.json`
- `manifests/segments/usdt_transfer_000002.manifest.json`
- `checkpoints/extraction.json`
- `checksums/SHA256SUMS`
- `reports/coverage.json`
- `reports/load/load-summary.json`

Recommended `run_id` shape:

- `tron-usdt-backfill-YYYYMMDD-HHMMSSZ`

---

## 4. Segment manifest contract

Each segment manifest must include:

- `manifest_version`
- `kind`
- `segment_id`
- `run_id`
- `segment_seq`
- `stream_name`
- `trigger_name`
- `topic0`
- `contract_address`
- `block_from`
- `block_to`
- `first_event_key`
- `last_event_key`
- `first_tx_hash`
- `last_tx_hash`
- `record_count`
- `file_size_bytes`
- `sha256`
- `codec`
- `local_path`
- `relative_path`
- `s3_bucket`
- `s3_key`
- `extractor_instance_id`
- `created_at_utc`
- `closed_at_utc`
- `status`

Frozen status vocabulary:

- `open`
- `sealed`
- `uploaded`
- `validated`
- `loaded`
- `failed`
- `quarantined`

Do not introduce alternate names like `closed`, `done`, or `complete`.

---

## 5. Runtime manifest contract

`manifests/runtime.json` must include:

- `manifest_version`
- `kind = runtime_manifest`
- `run_id`
- `java_tron_version`
- `event_framework_version`
- `config_sha256`
- `plugin_type = custom-file-sink`
- `plugin_build_id`
- `sink_codec`
- `segment_target_bytes`
- `s3_bucket`
- `s3_prefix_root`
- `extractor_region`
- `created_at_utc`

---

## 6. Run manifest contract

`manifests/run.json` must include:

- `manifest_version`
- `kind = run_manifest`
- `run_id`
- `stream_name`
- `contract_address`
- `topic0`
- `start_block`
- `end_policy`
- `resolved_end_block`
- `segment_count`
- `segments_prefix`
- `runtime_manifest_s3_key`
- `created_at_utc`
- `status`

---

## 7. Extraction checkpoint contract

`checkpoints/extraction.json` must include at least:

- `run_id`
- `last_uploaded_segment_id`
- `last_uploaded_block_number`
- `next_start_block_number`
- `updated_at`

This checkpoint is for operator recovery and loader coordination.
It is not a substitute for local extractor state in SQLite.

---

## 8. IAM scope freeze

Extractor-side role:

- `local-tron-usdt-backfill-ssm-role`

Minimum required scope:

- bucket list access only for the frozen project prefix
- object read/write only under the frozen project prefix
- multipart upload support for large segment uploads
- KMS usage only for the stage S3 encryption key

Frankfurt loader side can keep using existing stage roles that already have access to the stage artifact buckets.

---

## 9. Runtime note

Attaching the new S3/KMS policy to the running extractor instance role does **not** require:

- reboot
- instance stop/start
- restore restart

EC2 instance profile credentials refresh automatically through IMDS after policy attachment propagates.
The policy attachment itself does not require a restart, but fresh permissions can appear with a short delay until the next credential refresh.
