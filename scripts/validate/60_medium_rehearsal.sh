#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${AWS_REGION:=eu-central-1}"
: "${RUN_ID:?RUN_ID is required}"
: "${CLICKHOUSE_SECRET_NAME:?CLICKHOUSE_SECRET_NAME is required}"
: "${CLICKHOUSE_DATABASE:?CLICKHOUSE_DATABASE is required}"
: "${REPORT_DIR:=$WORKSPACE_ROOT/reports}"
: "${LOADER_PYTHON_BIN:=$WORKSPACE_ROOT/.venvs/loader/bin/python}"

LOADER_ENV_FILE="$WORKSPACE_ROOT/configs/loader/clickhouse.env"
LOADER_DB_PATH="$WORKSPACE_ROOT/runtime/loader_state.sqlite"
LOADER_SCHEMA_PATH="$WORKSPACE_ROOT/loader/sql/020_loader_state.sql"
REPORT_PATH="$REPORT_DIR/gates/${RUN_ID}-medium-rehearsal.json"

mkdir -p "$(dirname "$REPORT_PATH")"

export WORKSPACE_ROOT AWS_REGION CLICKHOUSE_SECRET_NAME CLICKHOUSE_DATABASE LOADER_ENV_FILE

bash "$WORKSPACE_ROOT/scripts/run/35_prepare_loader_runtime.sh" >/dev/null
bash "$WORKSPACE_ROOT/scripts/04_apply_clickhouse_schema.sh" >/dev/null

set -a
# shellcheck disable=SC1090
source "$LOADER_ENV_FILE"
set +a

load_started_ms="$("$LOADER_PYTHON_BIN" - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
load_output="$("$LOADER_PYTHON_BIN" "$WORKSPACE_ROOT/loader/run/10_load_run_from_s3.py" \
  --run-id "$RUN_ID" \
  --bucket "$S3_BUFFER_BUCKET" \
  --prefix-root "$S3_BUFFER_PREFIX_ROOT" \
  --loader-db-path "$LOADER_DB_PATH" \
  --loader-schema-path "$LOADER_SCHEMA_PATH" \
  --region "$AWS_REGION")"
load_finished_ms="$("$LOADER_PYTHON_BIN" - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"

validate_output="$("$LOADER_PYTHON_BIN" "$WORKSPACE_ROOT/loader/run/20_validate_loaded_run.py" \
  --run-id "$RUN_ID" \
  --loader-db-path "$LOADER_DB_PATH" \
  --loader-schema-path "$LOADER_SCHEMA_PATH")"
validate_finished_ms="$("$LOADER_PYTHON_BIN" - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"

"$LOADER_PYTHON_BIN" - "$REPORT_PATH" "$RUN_ID" "$CLICKHOUSE_DATABASE" "$load_started_ms" "$load_finished_ms" "$validate_finished_ms" "$load_output" "$validate_output" <<'PY'
import json
import sys
from pathlib import Path

report_path = Path(sys.argv[1])
run_id = sys.argv[2]
clickhouse_database = sys.argv[3]
load_started_ms = int(sys.argv[4])
load_finished_ms = int(sys.argv[5])
validate_finished_ms = int(sys.argv[6])
load_output = json.loads(sys.argv[7])
validate_output = json.loads(sys.argv[8])

segments = load_output["segments"]
aggregate = {
    "segment_count": len(segments),
    "bytes_read": 0,
    "record_count": 0,
    "event_rows_expected": 0,
    "leg_rows_expected": 0,
    "s3_read_ms": 0,
    "normalize_ms": 0,
    "stage_ms": 0,
    "merge_ms": 0,
    "audit_ms": 0,
    "validation_ms": 0,
}
for segment in segments:
    metrics = segment["metrics"]
    for key in aggregate:
        if key == "segment_count":
            continue
        aggregate[key] += int(metrics.get(key, 0))

load_wall_ms = max(load_finished_ms - load_started_ms, 1)
validate_wall_ms = max(validate_finished_ms - load_finished_ms, 0)
rows_per_second = round(aggregate["record_count"] / (load_wall_ms / 1000), 2)
bytes_per_second = round(aggregate["bytes_read"] / (load_wall_ms / 1000), 2)
segments_per_second = round(aggregate["segment_count"] / (load_wall_ms / 1000), 4)

report = {
    "gate": "medium_size_performance_rehearsal",
    "run_id": run_id,
    "clickhouse_database": clickhouse_database,
    "load_wall_ms": load_wall_ms,
    "validate_wall_ms": validate_wall_ms,
    "aggregate_metrics": aggregate,
    "throughput": {
        "rows_per_second": rows_per_second,
        "bytes_per_second": bytes_per_second,
        "segments_per_second": segments_per_second,
        "average_seconds_per_segment": round((load_wall_ms / 1000) / max(aggregate["segment_count"], 1), 4),
    },
    "load_result": load_output,
    "validate_result": validate_output,
    "status": "passed" if validate_output["status"] in {"ok", "validated"} else "failed",
}
report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(json.dumps(report, indent=2))
PY
