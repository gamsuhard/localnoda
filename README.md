# local-tron-usdt-backfill

Minimal local-only workspace for a bounded one-off historical backfill of **USDT on TRON**.

This repository is intentionally small:

- temporary TRON FullNode later in Block 02
- event replay and custom file sink in Block 04
- ClickHouse as final analytical store
- SQLite as extractor-side operational state
- no GitHub remote required

## Current status

This workspace currently covers:

- repo skeleton
- config templates
- SQLite run-state schema draft
- ClickHouse schema draft
- helper scripts
- Block 02 provision/bootstrap scripts for the temporary extractor host
- Block 04 PF4J custom file sink plugin scaffold
- Block 04 local Windows build validation for `plugin-file-sink.zip`
- Block 05 sealed-segment uploader, sidecar writers, and fake-S3 unit test
- Block 06A synthetic demo generator and demo-chain test
- Block 06 loader/normalizer, replay-safe loader state, and fake-ClickHouse tests
- Block 06B explicit loader ledger, single-worker lock, disposable ClickHouse schema support, and bounded-memory batch load path
- pre-bulk gate scripts for exact-tree staging rollout, real disposable-schema validation, medium rehearsal, and checklist freeze
- strict P0/P1 hardening for path contracts, manifest contracts, and private ClickHouse loader defaults

## Working order

1. Review `ARCHITECTURE.md`
2. Review `MASTER_SPEC_BY_BLOCKS.md`
3. Review `BLOCK_01.md`
4. Review `S3_BUFFER_CONTOUR.md` before wiring extractor-to-loader transfer
5. Review `CLICKHOUSE_INTEGRATION_MEMORY.md` before any ClickHouse work
6. Initialize local git: `bash scripts/00_init_local_git.sh`
7. Prepare local directories: `bash scripts/01_prepare_workspace.sh`
8. Initialize SQLite state DB: `bash scripts/02_init_run_state_sqlite.sh`
9. Review `BLOCK_02.md`
10. Copy env examples and fill values when Block 02 begins
11. Use `scripts/providers/10_sync_provider_api_keys.py` when helper provider access is needed
12. Review `BLOCK_04.md` before building or wiring the custom plugin
13. Review `BLOCK_05.md` before wiring S3 uploader flow
14. Review `BLOCK_06.md` before wiring loader-side runtime
15. Review `PRE_BULK_GATE.md` before any server-side validation or bulk-run discussion
16. Keep live provider keys only in AWS Secrets Manager or a local runtime-only env outside git
17. Use `runtime/provider_api.env.example` only as a blank local runtime template

## Important boundaries

- No snapshot files in git
- No raw NDJSON in git
- No runtime SQLite DB in git
- No GitHub remote by default
- No permanent FullNode implied by this repo
- No Terraform implied by this repo
- No AWS secrets stored in the repo
- Loader-side runtime is reproducible only through `LOADER_PYTHON_BIN` from the pinned loader venv, not through system `python3`
- No real bulk historical load should be treated as approved until a fresh server-side disposable-schema validation is rerun from this exact tree
- No pre-bulk gate evidence should be treated as durable unless the workspace artifact sha256 is captured alongside the reports

## Pre-Block 10

The project is now at the manual-approval boundary immediately before Block 10.  
The required execution artifacts before the first full bounded bulk are:

- `BLOCK_09_CLOSURE_DECISION.md`
- `REAL_CANARY_FINAL_SETTINGS_TASK.md`
- `SINGAPORE_UPLOAD_STRESS_WAVE_SPEC.md`

## Files to touch first in later blocks

- `configs/fullnode/config.conf.overlay.template`
- `configs/fullnode/fullnode.env.example`
- `configs/extractor/extractor.env.example`
- `configs/extractor/extractor_s3_buffer_iam_policy.json`
- `configs/providers/providers.env.example`
- `runtime/provider_api.env.example`
- `configs/loader/clickhouse.env.example`
- `BLOCK_04.md`
- `extractor/plugin/README.md`
- `S3_BUFFER_CONTOUR.md`
- `sql/sqlite/001_run_state.sql`
- `sql/clickhouse/010_core_schema.sql`
- `CLICKHOUSE_INTEGRATION_MEMORY.md`
- `scripts/providers/`
- `scripts/provision/`
- `scripts/run/`
- `scripts/snapshot/`
- `scripts/validate/`
- `scripts/run/16_build_file_sink_plugin_windows.ps1`
