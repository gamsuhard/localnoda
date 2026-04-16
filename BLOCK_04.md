# Block 04 - Custom Raw Sink Plugin

## Goal

Freeze and scaffold the minimal custom PF4J plugin that receives TRON event-service callbacks and writes bounded raw NDJSON segments for the one-off USDT backfill.

This block does **not** introduce direct ClickHouse writes, does **not** read node databases directly, and does **not** replace the `Singapore extractor -> S3 buffer -> Frankfurt loader -> private ClickHouse` contour.

## Deliverables

1. Gradle-based plugin project under `extractor/plugin/`
2. Compile-only local copy of `IPluginEventListener`
3. Custom file-sink plugin that:
   - consumes TRON callback payloads as JSON strings
   - writes bounded NDJSON or NDJSON+gzip segment files
   - rotates by record count and approximate byte count
   - writes one manifest JSON per sealed segment
   - keeps no dependency on Kafka or MongoDB
4. Host-side build script that can package `artifacts/plugins/plugin-file-sink.zip`
5. Extractor env wiring so the FullNode service can expose plugin settings through `EnvironmentFile`
6. Local Windows build validation against a Java 8 target bytecode level

## Runtime contract freeze

- Primary trigger for this project: `solidityLogTrigger`
- Expected contract/topic filtering remains in FullNode `event.subscribe.filter`
- Plugin receives JSON strings from java-tron and preserves them as raw NDJSON lines
- Plugin config comes from environment variables, not from direct node DB access
- Segment manifests are written locally first; SQLite updates and S3 upload orchestration remain downstream responsibilities

## Local output contract

For a run id like `tron-usdt-backfill-20260415-120000Z`, the plugin writes:

- `raw/runs/<run_id>/segments/usdt_transfer_000001.ndjson.gz`
- `raw/runs/<run_id>/manifests/usdt_transfer_000001.manifest.json`

Each manifest includes:

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

## Partial-file safety policy

- orphan `.partial` detected at plugin start: rename to `.orphaned.<timestamp>` and fail startup
- existing `.partial` path for a new segment: fail startup instead of overwrite

## Not in scope for Block 04

- S3 uploader process
- SQLite checkpoint reconciliation
- loader / normalizer implementation
- ClickHouse integration

## Build command on the Linux extractor host

```bash
scripts/run/15_build_file_sink_plugin.sh
```

Expected result:

- `artifacts/plugins/plugin-file-sink.zip`

## Local Windows build command

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run/16_build_file_sink_plugin_windows.ps1
```

Expected result:

- `artifacts/plugins/plugin-file-sink.zip`
- plugin jar contains `META-INF/extensions.idx`
- compiled classes target Java major version `52`

## Acceptance criteria

1. Plugin project layout exists and matches the PF4J loading model
2. FullNode service wiring can expose `TRON_FILE_SINK_*` env vars without changing the current restore flow
3. Segment and manifest filenames are deterministic and run-scoped
4. The plugin can be built later on the Linux host without introducing external control-plane services
5. Local Windows build validation succeeds and produces Java 8 compatible bytecode
6. Plugin startup fails instead of overwriting abandoned `.partial` data
7. Linux runtime validation still remains a later host-side step after restore and FullNode startup
