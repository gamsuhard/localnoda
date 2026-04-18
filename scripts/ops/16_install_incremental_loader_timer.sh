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

: "${LOADER_SERVICE_USER:=${SUDO_USER:-$(id -un)}}"
: "${LOADER_SERVICE_GROUP:=$LOADER_SERVICE_USER}"
: "${LOADER_LOG_DIR:=$WORKSPACE_ROOT/logs/loader}"
: "${LOADER_TIMER_INTERVAL:=2min}"
: "${LOADER_RUN_ID:?LOADER_RUN_ID is required}"
: "${LOADER_DB_PATH:=$WORKSPACE_ROOT/runtime/loader_state.sqlite}"
: "${LOADER_SCHEMA_PATH:=$WORKSPACE_ROOT/loader/sql/020_loader_state.sql}"
: "${LOADER_REGION:=eu-central-1}"
: "${LOADER_CONCURRENCY:=2}"

if [ "$LOADER_CONCURRENCY" -lt 1 ] || [ "$LOADER_CONCURRENCY" -gt 2 ]; then
  echo "LOADER_CONCURRENCY must be between 1 and 2" >&2
  exit 1
fi

SERVICE_PATH="/etc/systemd/system/local-tron-incremental-loader@.service"
TIMER_PATH="/etc/systemd/system/local-tron-incremental-loader@.timer"

install -d -m 0755 "$LOADER_LOG_DIR" "$(dirname "$LOADER_DB_PATH")"
chown "$LOADER_SERVICE_USER:$LOADER_SERVICE_GROUP" "$LOADER_LOG_DIR" "$(dirname "$LOADER_DB_PATH")"

for slot in 1 2; do
  LOG_FILE="$LOADER_LOG_DIR/incremental-loader-w${slot}.log"
  touch "$LOG_FILE"
  chown "$LOADER_SERVICE_USER:$LOADER_SERVICE_GROUP" "$LOG_FILE"
  chmod 0644 "$LOG_FILE"
done

cat >"$SERVICE_PATH" <<EOF
[Unit]
Description=Periodic TRON incremental loader cycle (worker %i)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$LOADER_SERVICE_USER
Group=$LOADER_SERVICE_GROUP
WorkingDirectory=$WORKSPACE_ROOT
Environment=WORKSPACE_ROOT=$WORKSPACE_ROOT
Environment=LOADER_ENV_FILE=$LOADER_ENV_FILE
Environment=LOADER_RUN_ID=$LOADER_RUN_ID
Environment=LOADER_DB_PATH=$LOADER_DB_PATH
Environment=LOADER_SCHEMA_PATH=$LOADER_SCHEMA_PATH
Environment=LOADER_REGION=$LOADER_REGION
Environment=LOADER_WORKER_SLOT=%i
ExecStart=$WORKSPACE_ROOT/scripts/run/36_run_incremental_loader_cycle.sh
StandardOutput=append:$LOADER_LOG_DIR/incremental-loader-w%i.log
StandardError=append:$LOADER_LOG_DIR/incremental-loader-w%i.log
EOF

cat >"$TIMER_PATH" <<EOF
[Unit]
Description=Run TRON incremental loader worker %i every $LOADER_TIMER_INTERVAL

[Timer]
OnBootSec=2min
OnUnitActiveSec=$LOADER_TIMER_INTERVAL
AccuracySec=30s
Persistent=true
Unit=local-tron-incremental-loader@%i.service

[Install]
WantedBy=timers.target
EOF

if systemctl list-unit-files | grep -q '^local-tron-incremental-loader\.timer'; then
  systemctl disable --now local-tron-incremental-loader.timer || true
fi
if systemctl list-unit-files | grep -q '^local-tron-incremental-loader\.service'; then
  systemctl stop local-tron-incremental-loader.service || true
fi

systemctl daemon-reload

for slot in 1 2; do
  if [ "$slot" -le "$LOADER_CONCURRENCY" ]; then
    systemctl enable --now "local-tron-incremental-loader@${slot}.timer"
    systemctl --no-pager --full status "local-tron-incremental-loader@${slot}.timer" || true
  else
    systemctl disable --now "local-tron-incremental-loader@${slot}.timer" || true
    systemctl stop "local-tron-incremental-loader@${slot}.service" || true
  fi
done
