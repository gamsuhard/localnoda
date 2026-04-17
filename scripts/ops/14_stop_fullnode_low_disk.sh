#!/usr/bin/env bash
set -euo pipefail

: "${TRON_LOG_DIR:=/var/log/tron}"

MARKER="$TRON_LOG_DIR/fullnode-stopped-low-disk.marker"

timestamp="$(date -Is)"
echo "$timestamp low_disk_stop_requested" | tee -a "$MARKER"

systemctl stop tron-fullnode.service
systemctl is-active tron-fullnode.service 2>/dev/null || true

echo "low_disk_stop_marker=$MARKER"
echo "low_disk_stop_timestamp=$timestamp"
