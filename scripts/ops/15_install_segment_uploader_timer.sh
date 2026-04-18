#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${EXTRACTOR_ENV_FILE:=$WORKSPACE_ROOT/configs/extractor/extractor.env}"
: "${UPLOADER_SERVICE_USER:=tron}"
: "${UPLOADER_SERVICE_GROUP:=$UPLOADER_SERVICE_USER}"
: "${UPLOADER_LOG_DIR:=$WORKSPACE_ROOT/logs/extractor}"
: "${UPLOADER_LOG_FILE:=$UPLOADER_LOG_DIR/uploader-cycle.log}"
: "${UPLOADER_TIMER_INTERVAL:=2min}"

SERVICE_PATH="/etc/systemd/system/local-tron-segment-uploader.service"
TIMER_PATH="/etc/systemd/system/local-tron-segment-uploader.timer"

install -d -m 0755 "$UPLOADER_LOG_DIR"
touch "$UPLOADER_LOG_FILE"
chown "$UPLOADER_SERVICE_USER:$UPLOADER_SERVICE_GROUP" "$UPLOADER_LOG_DIR" "$UPLOADER_LOG_FILE"
chmod 0644 "$UPLOADER_LOG_FILE"

cat >"$SERVICE_PATH" <<EOF
[Unit]
Description=Periodic TRON segment uploader cycle
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$UPLOADER_SERVICE_USER
Group=$UPLOADER_SERVICE_GROUP
WorkingDirectory=$WORKSPACE_ROOT
Environment=WORKSPACE_ROOT=$WORKSPACE_ROOT
Environment=EXTRACTOR_ENV_FILE=$EXTRACTOR_ENV_FILE
ExecStart=$WORKSPACE_ROOT/scripts/run/25_start_segment_uploader.sh
StandardOutput=append:$UPLOADER_LOG_FILE
StandardError=append:$UPLOADER_LOG_FILE
EOF

cat >"$TIMER_PATH" <<EOF
[Unit]
Description=Run TRON segment uploader every $UPLOADER_TIMER_INTERVAL

[Timer]
OnBootSec=2min
OnUnitActiveSec=$UPLOADER_TIMER_INTERVAL
AccuracySec=30s
Persistent=true
Unit=local-tron-segment-uploader.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now local-tron-segment-uploader.timer
systemctl --no-pager --full status local-tron-segment-uploader.timer || true
