#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${RUN_ID:?RUN_ID is required}"
: "${CLICKHOUSE_SECRET_NAME:?CLICKHOUSE_SECRET_NAME is required}"
: "${CLICKHOUSE_DATABASE:?CLICKHOUSE_DATABASE is required}"
: "${AWS_REGION:=eu-central-1}"
: "${S3_BUFFER_BUCKET:=goldusdt-v2-stage-913378704801-raw}"
: "${S3_BUFFER_PREFIX_ROOT:=providers/tron-usdt-backfill/usdt-transfer-oneoff}"
: "${LOADER_DB_PATH:=$WORKSPACE_ROOT/runtime/loader_state.sqlite}"
: "${LOADER_SCHEMA_PATH:=$WORKSPACE_ROOT/loader/sql/020_loader_state.sql}"
: "${REPORT_DIR:=$WORKSPACE_ROOT/reports/stress}"
: "${STRESS_LABEL:=}"
: "${STRESS_REPLAY:=0}"
: "${STRESS_SKIP_INITIAL_LOAD:=0}"
: "${STRESS_EXPECT_FAILURE:=0}"
: "${STRESS_COLLECT_STORAGE:=1}"
: "${STRESS_COLLECT_QUERY_SURFACE:=0}"
: "${TREE_ARTIFACT_SHA256:=}"
: "${TREE_ARTIFACT_SOURCE:=}"

export WORKSPACE_ROOT AWS_REGION CLICKHOUSE_SECRET_NAME CLICKHOUSE_DATABASE

bash "$WORKSPACE_ROOT/scripts/run/35_prepare_loader_runtime.sh" >/dev/null
bash "$WORKSPACE_ROOT/scripts/04_apply_clickhouse_schema.sh" >/dev/null

set -a
# shellcheck disable=SC1090
source "$WORKSPACE_ROOT/configs/loader/clickhouse.env"
set +a

mkdir -p "$REPORT_DIR"
OUTPUT_PATH="$REPORT_DIR/${RUN_ID}-stress-run.json"

ARGS=(
  "$WORKSPACE_ROOT/scripts/validate/90_execute_loader_stress_run.py"
  --run-id "$RUN_ID"
  --bucket "$S3_BUFFER_BUCKET"
  --prefix-root "$S3_BUFFER_PREFIX_ROOT"
  --loader-db-path "$LOADER_DB_PATH"
  --loader-schema-path "$LOADER_SCHEMA_PATH"
  --region "$AWS_REGION"
  --label "$STRESS_LABEL"
  --loader-env-file "$WORKSPACE_ROOT/configs/loader/clickhouse.env"
  --workspace-artifact-sha256 "$TREE_ARTIFACT_SHA256"
  --workspace-artifact-source "$TREE_ARTIFACT_SOURCE"
  --output "$OUTPUT_PATH"
  --stdout-summary-only
)

if [ "$STRESS_REPLAY" = "1" ]; then
  ARGS+=(--replay)
fi

if [ "$STRESS_SKIP_INITIAL_LOAD" = "1" ]; then
  ARGS+=(--skip-initial-load)
fi

if [ "$STRESS_EXPECT_FAILURE" = "1" ]; then
  ARGS+=(--expect-failure)
fi

if [ "$STRESS_COLLECT_STORAGE" = "1" ]; then
  ARGS+=(--collect-storage)
fi

if [ "$STRESS_COLLECT_QUERY_SURFACE" = "1" ]; then
  ARGS+=(--collect-query-surface)
fi

if [ -n "${STRESS_DELETE_LOADED_SEGMENT_ID:-}" ]; then
  ARGS+=(--delete-loaded-segment-id "$STRESS_DELETE_LOADED_SEGMENT_ID")
fi

if [ -n "${SLICE_START_UTC:-}" ]; then
  ARGS+=(--slice-start-utc "$SLICE_START_UTC")
fi

if [ -n "${SLICE_END_UTC:-}" ]; then
  ARGS+=(--slice-end-utc "$SLICE_END_UTC")
fi

if [ -n "${FULL_PERIOD_START_UTC:-}" ]; then
  ARGS+=(--full-period-start-utc "$FULL_PERIOD_START_UTC")
fi

if [ -n "${FULL_PERIOD_END_UTC:-}" ]; then
  ARGS+=(--full-period-end-utc "$FULL_PERIOD_END_UTC")
fi

"$LOADER_PYTHON_BIN" "${ARGS[@]}"
