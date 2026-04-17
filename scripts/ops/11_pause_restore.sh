#!/usr/bin/env bash
set -euo pipefail

: "${TRON_LOG_DIR:=/var/log/tron}"

PAUSE_MARKER="$TRON_LOG_DIR/restore-paused.marker"
RESTORE_PIDS="$(pgrep -f 'curl .*FullNode_output-directory.tgz|curl -fL .*FullNode_output-directory.tgz|tar -xz -C /tron-data/java-tron|tar -xz' || true)"

echo "pause_ts=$(date -Is)"

if [ -z "$RESTORE_PIDS" ]; then
  echo "restore_pause_issued=false"
  echo "restore_pause_reason=no_restore_processes"
  exit 0
fi

RESTORE_PID_LIST="$(printf '%s\n' "$RESTORE_PIDS" | paste -sd, -)"
kill -STOP $RESTORE_PIDS
mkdir -p "$TRON_LOG_DIR"
printf '%s\n' "$(date -Is) restore_pause pid_list=$RESTORE_PID_LIST" >>"$PAUSE_MARKER"

echo "restore_pause_issued=true"
echo "restore_pause_pid_list=$RESTORE_PID_LIST"
ps -o pid=,state=,cmd= -p "$RESTORE_PID_LIST"
