#!/usr/bin/env bash
set -euo pipefail

: "${TRON_DATA_DIR:=/tron-data/java-tron}"
: "${TRON_LOG_DIR:=/var/log/tron}"

OUTPUT_DIR="$TRON_DATA_DIR/output-directory"
PIDFILE="$TRON_LOG_DIR/s3-backup-output-directory.pid"
LOGFILE="$TRON_LOG_DIR/s3-backup-output-directory.log"
ERRFILE="$TRON_LOG_DIR/s3-backup-output-directory.err.log"
LOW_DISK_MARKER="$TRON_LOG_DIR/fullnode-stopped-low-disk.marker"

echo "ts=$(date -Is)"
echo "host=$(hostname)"
echo "uptime=$(uptime -p 2>/dev/null || true)"

if [ -d "$OUTPUT_DIR" ]; then
  echo "local_total_bytes=$(du -sb "$OUTPUT_DIR" | awk '{print $1}')"
  echo "local_total_human=$(du -sh "$OUTPUT_DIR" | awk '{print $1}')"
fi

if [ -d /tron-data ]; then
  df -h /tron-data | awk 'NR==2 {print "tron_data_used="$3; print "tron_data_avail="$4; print "tron_data_usep="$5}'
  df -B1 /tron-data | awk 'NR==2 {print "tron_data_used_bytes="$3; print "tron_data_avail_bytes="$4}'
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

if [ -f "$LOW_DISK_MARKER" ]; then
  echo "low_disk_marker_exists=true"
else
  echo "low_disk_marker_exists=false"
fi

if [ -f "$PIDFILE" ]; then
  pid="$(cat "$PIDFILE")"
  echo "backup_pid=$pid"
  if kill -0 "$pid" 2>/dev/null; then
    echo "backup_process_alive=true"
    echo "backup_elapsed_seconds=$(ps -o etimes= -p "$pid" | tr -d ' ')"
    ps -o pid=,ppid=,etimes=,%cpu=,%mem=,cmd= -p "$pid"
  else
    echo "backup_process_alive=false"
  fi
else
  echo "backup_pidfile_exists=false"
fi

if [ -f "$LOGFILE" ]; then
  echo "backup_log_bytes=$(wc -c < "$LOGFILE" | tr -d ' ')"
  echo "backup_log_tail_begin"
  tail -n 10 "$LOGFILE"
  echo "backup_log_tail_end"
fi

if [ -f "$ERRFILE" ]; then
  echo "backup_err_bytes=$(wc -c < "$ERRFILE" | tr -d ' ')"
  echo "backup_err_lines=$(wc -l < "$ERRFILE" | tr -d ' ')"
  echo "backup_err_tail_begin"
  tail -n 10 "$ERRFILE"
  echo "backup_err_tail_end"
fi
