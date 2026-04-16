#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${TRON_USER:=tron}"
: "${TRON_HOME:=/opt/tron/fullnode}"
: "${TRON_DATA_DIR:=/tron-data/java-tron}"
: "${TRON_LOG_DIR:=/var/log/tron}"
: "${TRON_RELEASE_JAR:=FullNode-x64.jar}"
: "${TRON_CONFIG_PATH:=$TRON_HOME/config/config.conf}"
: "${TRON_JAVA_OPTS:=-Xms16G -Xmx48G -XX:+UseG1GC}"
: "${TRON_ENABLE_EVENT_SERVICE:=0}"
: "${EXTRACTOR_ENV_FILE:=$WORKSPACE_ROOT/configs/extractor/extractor.env}"

WRAPPER_PATH="$TRON_HOME/bin/run-fullnode.sh"
SERVICE_PATH="/etc/systemd/system/tron-fullnode.service"
DATA_OUTPUT_DIR="$TRON_DATA_DIR/output-directory"

mkdir -p "$TRON_LOG_DIR" "$DATA_OUTPUT_DIR"
chown -R "$TRON_USER:$TRON_USER" "$TRON_HOME" "$TRON_DATA_DIR" "$TRON_LOG_DIR" "$(dirname "$TRON_CONFIG_PATH")"

cat >"$WRAPPER_PATH" <<EOF
#!/usr/bin/env bash
set -euo pipefail
ulimit -n 1048576 || true
TRON_HOME="\${TRON_HOME:-$TRON_HOME}"
TRON_RELEASE_JAR="\${TRON_RELEASE_JAR:-$TRON_RELEASE_JAR}"
TRON_CONFIG_PATH="\${TRON_CONFIG_PATH:-$TRON_CONFIG_PATH}"
TRON_DATA_DIR="\${TRON_DATA_DIR:-$TRON_DATA_DIR}"
TRON_LOG_DIR="\${TRON_LOG_DIR:-$TRON_LOG_DIR}"
TRON_JAVA_OPTS="\${TRON_JAVA_OPTS:-$TRON_JAVA_OPTS}"
TRON_ENABLE_EVENT_SERVICE="\${TRON_ENABLE_EVENT_SERVICE:-$TRON_ENABLE_EVENT_SERVICE}"
DATA_OUTPUT_DIR="\${TRON_DATA_DIR}/output-directory"
mkdir -p "\$TRON_LOG_DIR" "\$DATA_OUTPUT_DIR"
cd "\$TRON_HOME"
EXTRA_ARGS=()
if [ "\${TRON_ENABLE_EVENT_SERVICE:-0}" = "1" ]; then
  EXTRA_ARGS+=(--es)
fi
exec /usr/bin/java \${TRON_JAVA_OPTS} -jar "\$TRON_HOME/bin/\$TRON_RELEASE_JAR" -c "\$TRON_CONFIG_PATH" --output-directory "\$DATA_OUTPUT_DIR" "\${EXTRA_ARGS[@]}" >>"\$TRON_LOG_DIR/fullnode.stdout.log" 2>>"\$TRON_LOG_DIR/fullnode.stderr.log"
EOF

chmod 0755 "$WRAPPER_PATH"
chown "$TRON_USER:$TRON_USER" "$WRAPPER_PATH"

cat >"$SERVICE_PATH" <<EOF
[Unit]
Description=TRON FullNode
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$TRON_USER
Group=$TRON_USER
EnvironmentFile=-$EXTRACTOR_ENV_FILE
ExecStart=$WRAPPER_PATH
Restart=always
RestartSec=10
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now tron-fullnode.service
systemctl --no-pager --full status tron-fullnode.service || true
