#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${LOADER_ENV_FILE:=$WORKSPACE_ROOT/configs/loader/clickhouse.env}"
: "${AWS_REGION:=eu-central-1}"
: "${CLICKHOUSE_SECRET_NAME:?CLICKHOUSE_SECRET_NAME is required}"
: "${CLICKHOUSE_DATABASE:?CLICKHOUSE_DATABASE is required}"
: "${LOADER_VENV_DIR:=$WORKSPACE_ROOT/.venvs/loader}"
: "${LOADER_PYTHON_BIN:=$LOADER_VENV_DIR/bin/python}"
: "${LOADER_CONCURRENCY:=1}"
: "${LOADER_RECORD_BATCH_SIZE:=1000}"
: "${S3_BUFFER_BUCKET:=goldusdt-v2-stage-913378704801-raw}"
: "${S3_BUFFER_PREFIX_ROOT:=providers/tron-usdt-backfill/usdt-transfer-oneoff}"

SECRET_JSON="$(aws secretsmanager get-secret-value \
  --secret-id "$CLICKHOUSE_SECRET_NAME" \
  --region "$AWS_REGION" \
  --query 'SecretString' \
  --output text)"

CLICKHOUSE_HOST="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["private_dns_hostname"])' <<<"$SECRET_JSON")"
CLICKHOUSE_USER="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["username"])' <<<"$SECRET_JSON")"
CLICKHOUSE_PASSWORD="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["password"])' <<<"$SECRET_JSON")"

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
AWS_REGION=$AWS_REGION
S3_BUFFER_BUCKET=$S3_BUFFER_BUCKET
S3_BUFFER_PREFIX_ROOT=$S3_BUFFER_PREFIX_ROOT
RAW_DIR=$WORKSPACE_ROOT/raw
REPORT_DIR=$WORKSPACE_ROOT/reports
RUNTIME_DIR=$WORKSPACE_ROOT/runtime
RUN_STATE_DB=$WORKSPACE_ROOT/runtime/run_state.sqlite
EOF

chmod 600 "$LOADER_ENV_FILE"
echo "Wrote loader runtime env: $LOADER_ENV_FILE"
