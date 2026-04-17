#!/usr/bin/env bash
set -euo pipefail

: "${TRON_USER:=tron}"
: "${TRON_HOME:=/opt/tron/fullnode}"
: "${TRON_LOG_DIR:=/var/log/tron}"
: "${TRON_RELEASE_TAG:=GreatVoyage-v4.8.1}"
: "${TRON_RELEASE_JAR:=FullNode-x64.jar}"
: "${TRON_CONFIG_URL:=https://raw.githubusercontent.com/tronprotocol/java-tron/GreatVoyage-v4.8.1/framework/src/main/resources/config.conf}"
: "${TRON_CONFIG_PATH:=$TRON_HOME/config/config.conf}"

dnf install -y \
  awscli \
  curl \
  gradle \
  gzip \
  java-1.8.0-amazon-corretto \
  procps-ng \
  python3 \
  tar \
  util-linux \
  wget \
  xfsprogs

id "$TRON_USER" >/dev/null 2>&1 || useradd --system --create-home --home-dir "$TRON_HOME" --shell /sbin/nologin "$TRON_USER"

mkdir -p "$TRON_HOME/bin" "$(dirname "$TRON_CONFIG_PATH")" "$TRON_LOG_DIR"
chown -R "$TRON_USER:$TRON_USER" "$TRON_HOME" "$TRON_LOG_DIR" "$(dirname "$TRON_CONFIG_PATH")"

curl --fail --location --retry 5 --retry-all-errors --connect-timeout 30 \
  "https://github.com/tronprotocol/java-tron/releases/download/${TRON_RELEASE_TAG}/${TRON_RELEASE_JAR}" \
  -o "$TRON_HOME/bin/${TRON_RELEASE_JAR}"
curl --fail --location --retry 5 --retry-all-errors --connect-timeout 30 \
  "$TRON_CONFIG_URL" \
  -o "$TRON_CONFIG_PATH"
# Official v4.8.1 config defaults to LEVELDB, but our snapshot is RocksDB.
sed -i 's/db\.engine = "LEVELDB"/db.engine = "ROCKSDB"/' "$TRON_CONFIG_PATH"
chmod 0755 "$TRON_HOME/bin/${TRON_RELEASE_JAR}"
chmod 0644 "$TRON_CONFIG_PATH"
chown -R "$TRON_USER:$TRON_USER" "$TRON_HOME" "$(dirname "$TRON_CONFIG_PATH")"

java -version
echo "Installed java-tron release ${TRON_RELEASE_TAG}"
