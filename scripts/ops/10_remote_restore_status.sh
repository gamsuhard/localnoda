#!/usr/bin/env bash
set -euo pipefail

: "${TRON_DATA_DIR:=/tron-data/java-tron}"
: "${TRON_HTTP_PORT:=8090}"
: "${TRON_LOG_DIR:=/var/log/tron}"

OUTPUT_DIR="$TRON_DATA_DIR/output-directory"
BOOTSTRAP_LOG="$TRON_LOG_DIR/block02-bootstrap.status.log"
FULLNODE_STDOUT="$TRON_LOG_DIR/fullnode.stdout.log"

echo "ts=$(date -Is)"
echo "host=$(hostname)"
echo "uptime=$(uptime -p 2>/dev/null || true)"

if systemctl is-active --quiet amazon-ssm-agent; then
  echo "ssm_agent=active"
else
  echo "ssm_agent=inactive"
fi

if [ -d "$OUTPUT_DIR" ]; then
  echo "output_dir_exists=true"
  echo "output_dir_bytes=$(du -sb "$OUTPUT_DIR" | awk '{print $1}')"
  echo "output_dir_human=$(du -sh "$OUTPUT_DIR" | awk '{print $1}')"
else
  echo "output_dir_exists=false"
fi

if [ -d /tron-data ]; then
  df -h /tron-data | awk 'NR==2 {print "tron_data_used="$3; print "tron_data_avail="$4; print "tron_data_usep="$5}'
fi

if [ -f "$BOOTSTRAP_LOG" ]; then
  echo "bootstrap_tail_begin"
  tail -n 5 "$BOOTSTRAP_LOG"
  echo "bootstrap_tail_end"
fi

RESTORE_PIDS="$(pgrep -f 'curl .*FullNode_output-directory.tgz|curl -fL .*FullNode_output-directory.tgz|tar -xz -C /tron-data/java-tron|tar -xz' || true)"
if [ -n "$RESTORE_PIDS" ]; then
  echo "restore_process_alive=true"
  RESTORE_PID_LIST="$(printf '%s\n' "$RESTORE_PIDS" | paste -sd, -)"
  ps -o pid=,ppid=,etimes=,%cpu=,%mem=,cmd= -p "$RESTORE_PID_LIST"
else
  echo "restore_process_alive=false"
fi

if systemctl list-unit-files tron-fullnode.service >/dev/null 2>&1; then
  echo "fullnode_service_enabled=$(systemctl is-enabled tron-fullnode.service 2>/dev/null || true)"
  echo "fullnode_service_active=$(systemctl is-active tron-fullnode.service 2>/dev/null || true)"
fi

FULLNODE_PIDS="$(pgrep -f 'FullNode-x64\.jar|java .*org\.tron|java .*FullNode' || true)"
if [ -n "$FULLNODE_PIDS" ]; then
  echo "fullnode_process_alive=true"
  pgrep -af 'FullNode-x64\.jar|java .*org\.tron|java .*FullNode'
else
  echo "fullnode_process_alive=false"
fi

if curl -sf --max-time 10 -H 'Content-Type: application/json' -d '{}' "http://127.0.0.1:${TRON_HTTP_PORT}/wallet/getnodeinfo" >/tmp/tron-nodeinfo.json 2>/dev/null; then
  python3 - <<'PY'
import json
from pathlib import Path
data = json.loads(Path("/tmp/tron-nodeinfo.json").read_text(encoding="utf-8"))
for key in ("beginSyncNum", "block", "solidityBlock"):
    if key in data:
        print(f"{key}={data[key]}")
PY
else
  echo "nodeinfo_ready=false"
fi

if [ -f "$FULLNODE_STDOUT" ]; then
  echo "fullnode_stdout_tail_begin"
  tail -n 20 "$FULLNODE_STDOUT"
  echo "fullnode_stdout_tail_end"
fi
