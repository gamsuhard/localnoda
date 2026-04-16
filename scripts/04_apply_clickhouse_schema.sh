#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
SQL_PATH="$ROOT_DIR/sql/clickhouse/010_core_schema.sql"
: "${CLICKHOUSE_ENV_FILE:=$ROOT_DIR/configs/loader/clickhouse.env}"

if [ -f "$CLICKHOUSE_ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$CLICKHOUSE_ENV_FILE"
  set +a
fi

: "${CLICKHOUSE_HOST:=__SET_PRIVATE_ENDPOINT_HOST__}"
: "${CLICKHOUSE_PORT:=9440}"
: "${CLICKHOUSE_USER:=default}"
: "${CLICKHOUSE_PASSWORD:=}"
: "${CLICKHOUSE_DATABASE:=tron_usdt_local}"
: "${CLICKHOUSE_SECURE:=1}"

if ! command -v clickhouse-client >/dev/null 2>&1; then
  echo "clickhouse-client is not installed" >&2
  exit 1
fi

CLICKHOUSE_ARGS=(
  --host "$CLICKHOUSE_HOST"
  --port "$CLICKHOUSE_PORT"
  --user "$CLICKHOUSE_USER"
  --multiquery
)

if [ -n "$CLICKHOUSE_PASSWORD" ]; then
  CLICKHOUSE_ARGS+=(--password "$CLICKHOUSE_PASSWORD")
fi

if [ "$CLICKHOUSE_SECURE" = "1" ]; then
  CLICKHOUSE_ARGS+=(--secure)
fi

if ! [[ "$CLICKHOUSE_DATABASE" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "CLICKHOUSE_DATABASE must match ^[A-Za-z0-9_]+$" >&2
  exit 1
fi

TMP_SQL="$(mktemp)"
trap 'rm -f "$TMP_SQL"' EXIT
sed "s/tron_usdt_local/${CLICKHOUSE_DATABASE}/g" "$SQL_PATH" > "$TMP_SQL"

clickhouse-client "${CLICKHOUSE_ARGS[@]}" < "$TMP_SQL"

echo "Applied ClickHouse schema from: $SQL_PATH"
echo "Target ClickHouse database: $CLICKHOUSE_DATABASE"
