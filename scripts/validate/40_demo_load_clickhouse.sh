#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${LOADER_ENV_FILE:=$WORKSPACE_ROOT/configs/loader/clickhouse.env}"

if [ -f "$LOADER_ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$LOADER_ENV_FILE"
  set +a
fi

: "${RUN_ID:?RUN_ID is required}"
: "${S3_BUFFER_BUCKET:?S3_BUFFER_BUCKET is required}"
: "${S3_BUFFER_PREFIX_ROOT:?S3_BUFFER_PREFIX_ROOT is required}"
: "${AWS_REGION:=eu-central-1}"
: "${RUNTIME_DIR:=$WORKSPACE_ROOT/runtime}"
: "${REPORT_DIR:=$WORKSPACE_ROOT/reports}"
: "${LOADER_PYTHON_BIN:=$WORKSPACE_ROOT/.venvs/loader/bin/python}"

LOADER_DB_PATH="$RUNTIME_DIR/loader_state.sqlite"
LOADER_SCHEMA_PATH="$WORKSPACE_ROOT/loader/sql/020_loader_state.sql"
REPORT_PATH="$REPORT_DIR/load/${RUN_ID}-demo-load-report.json"

mkdir -p "$(dirname "$REPORT_PATH")" "$RUNTIME_DIR"

load_started="$(date -u +%s)"
load_output="$("$LOADER_PYTHON_BIN" "$WORKSPACE_ROOT/loader/run/10_load_run_from_s3.py" \
  --run-id "$RUN_ID" \
  --bucket "$S3_BUFFER_BUCKET" \
  --prefix-root "$S3_BUFFER_PREFIX_ROOT" \
  --loader-db-path "$LOADER_DB_PATH" \
  --loader-schema-path "$LOADER_SCHEMA_PATH" \
  --region "$AWS_REGION")"
load_finished="$(date -u +%s)"

validate_output="$("$LOADER_PYTHON_BIN" "$WORKSPACE_ROOT/loader/run/20_validate_loaded_run.py" \
  --run-id "$RUN_ID" \
  --loader-db-path "$LOADER_DB_PATH" \
  --loader-schema-path "$LOADER_SCHEMA_PATH")"
validate_finished="$(date -u +%s)"

replay_output="$("$LOADER_PYTHON_BIN" "$WORKSPACE_ROOT/loader/run/30_replay_run.py" \
  --run-id "$RUN_ID" \
  --bucket "$S3_BUFFER_BUCKET" \
  --prefix-root "$S3_BUFFER_PREFIX_ROOT" \
  --loader-db-path "$LOADER_DB_PATH" \
  --loader-schema-path "$LOADER_SCHEMA_PATH" \
  --region "$AWS_REGION")"
replay_finished="$(date -u +%s)"

"$LOADER_PYTHON_BIN" - "$REPORT_PATH" "$RUN_ID" "$load_started" "$load_finished" "$validate_finished" "$replay_finished" "$load_output" "$validate_output" "$replay_output" <<'PY'
import json
import sys

report_path, run_id, load_started, load_finished, validate_finished, replay_finished, load_output, validate_output, replay_output = sys.argv[1:]
report = {
    "run_id": run_id,
    "load_seconds": int(load_finished) - int(load_started),
    "validate_seconds": int(validate_finished) - int(load_finished),
    "replay_seconds": int(replay_finished) - int(validate_finished),
    "load_result": json.loads(load_output),
    "validate_result": json.loads(validate_output),
    "replay_result": json.loads(replay_output),
}
with open(report_path, "w", encoding="utf-8") as handle:
    json.dump(report, handle, indent=2)
print(json.dumps(report, indent=2))
PY
