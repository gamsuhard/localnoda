#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${LOADER_ENV_FILE:=$WORKSPACE_ROOT/configs/loader/clickhouse.env}"
: "${LOADER_REGION:=eu-central-1}"

if [ ! -f "$LOADER_ENV_FILE" ]; then
  echo "LOADER_ENV_FILE does not exist: $LOADER_ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$LOADER_ENV_FILE"
set +a

: "${LOADER_PYTHON_BIN:?LOADER_PYTHON_BIN is required}"
: "${LOADER_DB_PATH:=$WORKSPACE_ROOT/runtime/loader_state.sqlite}"
: "${LOADER_SCHEMA_PATH:=$WORKSPACE_ROOT/loader/sql/020_loader_state.sql}"
: "${LOADER_RUN_ID:=${TRON_FILE_SINK_RUN_ID:-${S3_BUFFER_RUN_ID:-}}}"
: "${S3_BUFFER_BUCKET:?S3_BUFFER_BUCKET is required}"
: "${S3_BUFFER_PREFIX_ROOT:?S3_BUFFER_PREFIX_ROOT is required}"

if [ -z "$LOADER_RUN_ID" ]; then
  echo "LOADER_RUN_ID is required" >&2
  exit 1
fi

if [ ! -x "$LOADER_PYTHON_BIN" ]; then
  echo "LOADER_PYTHON_BIN is not executable: $LOADER_PYTHON_BIN" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOADER_DB_PATH")" "$WORKSPACE_ROOT/reports/load"

exec "$LOADER_PYTHON_BIN" "$WORKSPACE_ROOT/loader/run/10_load_run_from_s3.py" \
  --run-id "$LOADER_RUN_ID" \
  --bucket "$S3_BUFFER_BUCKET" \
  --prefix-root "$S3_BUFFER_PREFIX_ROOT" \
  --loader-db-path "$LOADER_DB_PATH" \
  --loader-schema-path "$LOADER_SCHEMA_PATH" \
  --region "$LOADER_REGION"
