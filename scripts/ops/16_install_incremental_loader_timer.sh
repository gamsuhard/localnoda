#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${LOADER_ENV_FILE:=$WORKSPACE_ROOT/configs/loader/clickhouse.env}"
: "${LOADER_SERVICE_USER:=${SUDO_USER:-$(id -un)}}"
: "${LOADER_SERVICE_GROUP:=$LOADER_SERVICE_USER}"
: "${LOADER_LOG_DIR:=$WORKSPACE_ROOT/logs/loader}"
: "${LOADER_LOG_FILE:=$LOADER_LOG_DIR/incremental-loader.log}"
: "${LOADER_TIMER_INTERVAL:=2min}"
: "${LOADER_RUN_ID:?LOADER_RUN_ID is required}"
: "${LOADER_DB_PATH:=$WORKSPACE_ROOT/runtime/loader_state.sqlite}"
: "${LOADER_SCHEMA_PATH:=$WORKSPACE_ROOT/loader/sql/020_loader_state.sql}"
: "${LOADER_REGION:=eu-central-1}"

SERVICE_PATH="/etc/systemd/system/local-tron-incremental-loader.service"
TIMER_PATH="/etc/systemd/system/local-tron-incremental-loader.timer"

install -d -m 0755 "$LOADER_LOG_DIR" "$(dirname "$LOADER_DB_PATH")"
touch "$LOADER_LOG_FILE"
chown "$LOADER_SERVICE_USER:$LOADER_SERVICE_GROUP" "$LOADER_LOG_DIR" "$LOADER_LOG_FILE" "$(dirname "$LOADER_DB_PATH")"
chmod 0644 "$LOADER_LOG_FILE"

cat >"$SERVICE_PATH" <<EOF
[Unit]
Description=Periodic TRON incremental loader cycle
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
ExecStart=$WORKSPACE_ROOT/scripts/run/36_run_incremental_loader_cycle.sh
StandardOutput=append:$LOADER_LOG_FILE
StandardError=append:$LOADER_LOG_FILE
EOF

cat >"$TIMER_PATH" <<EOF
[Unit]
Description=Run TRON incremental loader every $LOADER_TIMER_INTERVAL

[Timer]
OnBootSec=2min
OnUnitActiveSec=$LOADER_TIMER_INTERVAL
AccuracySec=30s
Persistent=true
Unit=local-tron-incremental-loader.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now local-tron-incremental-loader.timer
systemctl --no-pager --full status local-tron-incremental-loader.timer || true
