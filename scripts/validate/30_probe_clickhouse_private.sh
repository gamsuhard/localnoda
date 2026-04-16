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

: "${CLICKHOUSE_HOST:?CLICKHOUSE_HOST is required}"
: "${CLICKHOUSE_PORT:=9440}"
: "${CLICKHOUSE_USER:?CLICKHOUSE_USER is required}"
: "${CLICKHOUSE_PASSWORD:=}"
: "${CLICKHOUSE_SECURE:=1}"

ARGS=(
  --host "$CLICKHOUSE_HOST"
  --port "$CLICKHOUSE_PORT"
  --user "$CLICKHOUSE_USER"
  --query "SELECT version(), currentUser(), currentDatabase() FORMAT TSV"
)

if [ -n "$CLICKHOUSE_PASSWORD" ]; then
  ARGS+=(--password "$CLICKHOUSE_PASSWORD")
fi

if [ "$CLICKHOUSE_SECURE" = "1" ]; then
  ARGS+=(--secure)
fi

clickhouse-client "${ARGS[@]}"
