# Custom file sink plugin

This directory now contains the `Block 04` skeleton for the custom PF4J file sink used by the bounded TRON USDT backfill.

## What it does

- implements `org.tron.common.logsfilter.IPluginEventListener`
- accepts TRON event-service callbacks as JSON strings
- writes raw NDJSON segments under a run-scoped local directory
- rotates segments by approximate byte count and record count
- writes one sidecar manifest JSON per sealed segment

## What it does not do

- upload to S3 directly
- write to ClickHouse
- update SQLite checkpoints
- query any node database directly

Those responsibilities remain outside the plugin so the runtime contour stays:

`Singapore extractor -> S3 buffer -> Frankfurt loader -> private ClickHouse`

## Layout

- `api/`
  - compile-only copy of `IPluginEventListener`
- `filesinkplugin/`
  - PF4J plugin implementation
- `build.gradle`, `settings.gradle`, `gradle.properties`
  - package `plugin-file-sink.zip`

## Build on the Linux extractor host

```bash
scripts/run/15_build_file_sink_plugin.sh
```

Expected output:

- `artifacts/plugins/plugin-file-sink.zip`

## Build on this Windows machine

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run/16_build_file_sink_plugin_windows.ps1
```

The current workstation already has a verified local build path:

- JDK: Temurin 17
- Gradle: portable `8.7` under `G:\CODEX\LOCALNODA\_tools\gradle-8.7`
- validated artifact: `artifacts/plugins/plugin-file-sink.zip`
- validated bytecode level: Java 8 (`major version 52`)

## Runtime env contract

The plugin reads `TRON_FILE_SINK_*` environment variables from the FullNode service environment.

Required inputs:

- `TRON_FILE_SINK_RUN_ID`
- `TRON_FILE_SINK_OUTPUT_ROOT`
- `TRON_FILE_SINK_STREAM_NAME`
- `TRON_FILE_SINK_COMPRESSION`
- `TRON_FILE_SINK_EXPECTED_TRIGGER_NAME`
- `TRON_FILE_SINK_CONTRACT_ADDRESS`
- `TRON_FILE_SINK_TOPIC0`
- `TRON_FILE_SINK_EXTRACTOR_INSTANCE_ID`
- `TRON_FILE_SINK_SEGMENT_MAX_BYTES`
- `TRON_FILE_SINK_SEGMENT_MAX_RECORDS`
- `TRON_FILE_SINK_FLUSH_EVERY_RECORDS`
- `TRON_FILE_SINK_MAX_QUEUE_RECORDS`
- `TRON_FILE_SINK_S3_BUCKET`
- `TRON_FILE_SINK_S3_PREFIX_ROOT`

Conditional inputs:

- `TRON_FILE_SINK_SSE_MODE`
- `TRON_FILE_SINK_KMS_KEY_ARN`

Runtime rules:

- no auto-generated `RUN_ID`
- no fallback `OUTPUT_ROOT`
- no fallback from `serverAddress`
- compression must be `gzip` or `none`
- queue capacity is bounded by `TRON_FILE_SINK_MAX_QUEUE_RECORDS`
- queue overflow is fatal and stops the listener instead of dropping triggers
- writer exceptions are fatal and stop the listener instead of silently continuing
- if `TRON_FILE_SINK_SSE_MODE=aws:kms`, then `TRON_FILE_SINK_KMS_KEY_ARN` is required
- abandoned `.partial` files are renamed to `.orphaned.<timestamp>` and startup fails

## Current limitation

The plugin is now verified to compile locally, but it is **not yet runtime-validated inside the Linux FullNode process**. That check remains for the host-side step after restore completes.
