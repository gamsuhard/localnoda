#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${LOADER_ENV_FILE:=$WORKSPACE_ROOT/configs/loader/clickhouse.env}"
: "${AWS_REGION:=eu-central-1}"
: "${CLICKHOUSE_SECRET_NAME:?CLICKHOUSE_SECRET_NAME is required}"
: "${CLICKHOUSE_DATABASE:?CLICKHOUSE_DATABASE is required}"
: "${LOADER_VENV_DIR:=$WORKSPACE_ROOT/.venvs/loader}"
: "${LOADER_PYTHON_BIN:=$LOADER_VENV_DIR/bin/python}"
: "${LOADER_CONCURRENCY:=2}"
: "${LOADER_RECORD_BATCH_SIZE:=250000}"
: "${LOADER_BUILD_LEGS_IN_HOT_PATH:=0}"
: "${LOADER_SKIP_PER_SEGMENT_CANONICAL_COUNTS:=1}"
: "${S3_BUFFER_BUCKET:=goldusdt-v2-stage-913378704801-raw}"
: "${S3_BUFFER_PREFIX_ROOT:=providers/tron-usdt-backfill/usdt-transfer-oneoff}"

if [ ! -f "$LOADER_PYTHON_BIN" ]; then
  echo "LOADER_PYTHON_BIN does not exist: $LOADER_PYTHON_BIN" >&2
  exit 1
fi

if [ ! -x "$LOADER_PYTHON_BIN" ]; then
  echo "LOADER_PYTHON_BIN is not executable: $LOADER_PYTHON_BIN" >&2
  exit 1
fi

if ! LOADER_PYTHON_VERSION="$("$LOADER_PYTHON_BIN" --version 2>&1)"; then
  echo "LOADER_PYTHON_BIN failed --version check: $LOADER_PYTHON_BIN" >&2
  exit 1
fi

if ! "$LOADER_PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)'; then
  echo "LOADER_PYTHON_BIN must be Python >= 3.10; got: $LOADER_PYTHON_VERSION" >&2
  exit 1
fi

SECRET_JSON="$(aws secretsmanager get-secret-value \
  --secret-id "$CLICKHOUSE_SECRET_NAME" \
  --region "$AWS_REGION" \
  --query 'SecretString' \
  --output text)"

mapfile -t CLICKHOUSE_SECRET_FIELDS < <(
  printf '%s' "$SECRET_JSON" | "$LOADER_PYTHON_BIN" -c '
import json
import sys

payload = json.load(sys.stdin)
print(payload["private_dns_hostname"])
print(payload["username"])
print(payload["password"])
'
)

CLICKHOUSE_HOST="${CLICKHOUSE_SECRET_FIELDS[0]:-}"
CLICKHOUSE_USER="${CLICKHOUSE_SECRET_FIELDS[1]:-}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_SECRET_FIELDS[2]:-}"

for required_value in "$CLICKHOUSE_HOST" "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD"; do
  if [ -z "$required_value" ]; then
    echo "ClickHouse secret hydration returned an empty required value" >&2
    exit 1
  fi
done

mkdir -p "$(dirname "$LOADER_ENV_FILE")" "$WORKSPACE_ROOT/runtime" "$WORKSPACE_ROOT/reports/load"
cat >"$LOADER_ENV_FILE" <<EOF
CLICKHOUSE_HOST=$CLICKHOUSE_HOST
CLICKHOUSE_PORT=9440
CLICKHOUSE_USER=$CLICKHOUSE_USER
CLICKHOUSE_PASSWORD=$CLICKHOUSE_PASSWORD
CLICKHOUSE_DATABASE=$CLICKHOUSE_DATABASE
CLICKHOUSE_SECURE=1
LOADER_VENV_DIR=$LOADER_VENV_DIR
LOADER_PYTHON_BIN=$LOADER_PYTHON_BIN
LOADER_CONCURRENCY=$LOADER_CONCURRENCY
LOADER_RECORD_BATCH_SIZE=$LOADER_RECORD_BATCH_SIZE
LOADER_BUILD_LEGS_IN_HOT_PATH=$LOADER_BUILD_LEGS_IN_HOT_PATH
LOADER_SKIP_PER_SEGMENT_CANONICAL_COUNTS=$LOADER_SKIP_PER_SEGMENT_CANONICAL_COUNTS
LOADER_DB_PATH=$WORKSPACE_ROOT/runtime/loader_state.sqlite
LOADER_SCHEMA_PATH=$WORKSPACE_ROOT/loader/sql/020_loader_state.sql
AWS_REGION=$AWS_REGION
S3_BUFFER_BUCKET=$S3_BUFFER_BUCKET
S3_BUFFER_PREFIX_ROOT=$S3_BUFFER_PREFIX_ROOT
RAW_DIR=$WORKSPACE_ROOT/raw
REPORT_DIR=$WORKSPACE_ROOT/reports
RUNTIME_DIR=$WORKSPACE_ROOT/runtime
EOF

chmod 600 "$LOADER_ENV_FILE"
echo "Wrote loader runtime env: $LOADER_ENV_FILE"
