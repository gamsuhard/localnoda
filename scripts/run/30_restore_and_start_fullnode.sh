#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${TRON_DATA_DIR:=/tron-data/java-tron}"
: "${TRON_LOG_DIR:=/var/log/tron}"

mkdir -p "$TRON_LOG_DIR"

echo "$(date -Is) restore_start" >>"$TRON_LOG_DIR/block02-bootstrap.status.log"
bash "$WORKSPACE_ROOT/scripts/snapshot/10_stream_restore_snapshot.sh"
echo "$(date -Is) restore_done" >>"$TRON_LOG_DIR/block02-bootstrap.status.log"

bash "$WORKSPACE_ROOT/scripts/run/20_start_fullnode.sh"
echo "$(date -Is) fullnode_start_requested" >>"$TRON_LOG_DIR/block02-bootstrap.status.log"
