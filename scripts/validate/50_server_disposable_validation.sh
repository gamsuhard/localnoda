#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${AWS_REGION:=eu-central-1}"
: "${RUN_ID:?RUN_ID is required}"
: "${CLICKHOUSE_SECRET_NAME:?CLICKHOUSE_SECRET_NAME is required}"
: "${CLICKHOUSE_DATABASE:?CLICKHOUSE_DATABASE is required}"
: "${REPORT_DIR:=$WORKSPACE_ROOT/reports}"
: "${TREE_ARTIFACT_SHA256:=__UNSET__}"
: "${WORKSPACE_ARTIFACT_SOURCE:=__UNSET__}"
: "${LOADER_PYTHON_BIN:=$WORKSPACE_ROOT/.venvs/loader/bin/python}"

LOADER_ENV_FILE="$WORKSPACE_ROOT/configs/loader/clickhouse.env"
GATE_REPORT_PATH="$REPORT_DIR/gates/${RUN_ID}-disposable-validation.json"
DEMO_REPORT_PATH="$REPORT_DIR/load/${RUN_ID}-demo-load-report.json"

mkdir -p "$(dirname "$GATE_REPORT_PATH")" "$(dirname "$DEMO_REPORT_PATH")"

export WORKSPACE_ROOT AWS_REGION CLICKHOUSE_SECRET_NAME CLICKHOUSE_DATABASE LOADER_ENV_FILE REPORT_DIR RUN_ID

bash "$WORKSPACE_ROOT/scripts/run/35_prepare_loader_runtime.sh" >/dev/null
bash "$WORKSPACE_ROOT/scripts/04_apply_clickhouse_schema.sh" >/dev/null
probe_output="$(bash "$WORKSPACE_ROOT/scripts/validate/30_probe_clickhouse_private.sh")"
demo_output="$(bash "$WORKSPACE_ROOT/scripts/validate/40_demo_load_clickhouse.sh")"

set -a
# shellcheck disable=SC1090
source "$LOADER_ENV_FILE"
set +a

ARGS=(
  --host "$CLICKHOUSE_HOST"
  --port "${CLICKHOUSE_PORT:-9440}"
  --user "$CLICKHOUSE_USER"
)

if [ -n "${CLICKHOUSE_PASSWORD:-}" ]; then
  ARGS+=(--password "$CLICKHOUSE_PASSWORD")
fi

if [ "${CLICKHOUSE_SECURE:-1}" = "1" ]; then
  ARGS+=(--secure)
fi

query_output="$(
  clickhouse-client \
    "${ARGS[@]}" \
    --query "
      SELECT
        (SELECT count() FROM ${CLICKHOUSE_DATABASE}.trc20_transfer_events WHERE load_run_id = '${RUN_ID}') AS events,
        (SELECT count() FROM ${CLICKHOUSE_DATABASE}.address_transfer_legs WHERE load_run_id = '${RUN_ID}') AS legs,
        (SELECT count() FROM ${CLICKHOUSE_DATABASE}.load_audit WHERE run_id = '${RUN_ID}') AS audit_rows
      FORMAT JSONEachRow
    "
)"

"$LOADER_PYTHON_BIN" - "$GATE_REPORT_PATH" "$RUN_ID" "$CLICKHOUSE_DATABASE" "$TREE_ARTIFACT_SHA256" "$WORKSPACE_ARTIFACT_SOURCE" "$probe_output" "$demo_output" "$query_output" <<'PY'
import json
import sys
from pathlib import Path

report_path = Path(sys.argv[1])
run_id = sys.argv[2]
clickhouse_database = sys.argv[3]
tree_sha256 = sys.argv[4]
artifact_source = sys.argv[5]
probe_output = sys.argv[6]
demo_output = json.loads(sys.argv[7])
query_output = json.loads(sys.argv[8])
query_row = query_output[0] if isinstance(query_output, list) else query_output

report = {
    "gate": "fresh_server_side_disposable_validation",
    "run_id": run_id,
    "clickhouse_database": clickhouse_database,
    "tree_artifact_sha256": tree_sha256,
    "workspace_artifact_source": artifact_source,
    "probe_output": probe_output,
    "demo_report": demo_output,
    "sql_verification": {
        "events": int(query_row["events"]),
        "legs": int(query_row["legs"]),
        "audit_rows": int(query_row["audit_rows"]),
    },
    "status": (
        "passed"
        if demo_output["validate_result"]["status"] in {"ok", "validated"}
        and demo_output["replay_result"]["segments"][0]["status"] == "validated"
        else "failed"
    ),
}
report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(json.dumps(report, indent=2))
PY
