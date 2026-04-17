#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${TRON_HOME:=/opt/tron/fullnode}"
: "${TRON_DATA_DIR:=/tron-data/java-tron}"
: "${TRON_LOG_DIR:=/var/log/tron}"
: "${BUNDLE_BUCKET:?BUNDLE_BUCKET is required}"
: "${BUNDLE_PREFIX:?BUNDLE_PREFIX is required}"
: "${BUNDLE_STORAGE_CLASS:=STANDARD}"
: "${AWS_DEFAULT_REGION:=ap-southeast-1}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
STAGE_DIR="/tmp/tron-launch-ready-${STAMP}"
BUNDLE_PATH="/tmp/tron-launch-ready-${STAMP}.tar.gz"
MANIFEST_PATH="$STAGE_DIR/manifest.txt"

mkdir -p \
  "$STAGE_DIR/runtime/opt/tron/fullnode/bin" \
  "$STAGE_DIR/runtime/opt/tron/fullnode/config" \
  "$STAGE_DIR/runtime/etc/systemd/system" \
  "$STAGE_DIR/runtime/var/log/tron" \
  "$STAGE_DIR/runtime/srv/local-tron-usdt-backfill/configs/extractor" \
  "$STAGE_DIR/runtime/srv/local-tron-usdt-backfill/configs/fullnode" \
  "$STAGE_DIR/runtime/srv/local-tron-usdt-backfill/scripts/run"

cp -a "$TRON_HOME/bin/FullNode-x64.jar" "$STAGE_DIR/runtime/opt/tron/fullnode/bin/"
cp -a "$TRON_HOME/bin/run-fullnode.sh" "$STAGE_DIR/runtime/opt/tron/fullnode/bin/"
cp -a "$TRON_HOME/config/config.conf" "$STAGE_DIR/runtime/opt/tron/fullnode/config/"
cp -a /etc/systemd/system/tron-fullnode.service "$STAGE_DIR/runtime/etc/systemd/system/"

if [ -f "$WORKSPACE_ROOT/configs/extractor/extractor.env" ]; then
  cp -a "$WORKSPACE_ROOT/configs/extractor/extractor.env" "$STAGE_DIR/runtime/srv/local-tron-usdt-backfill/configs/extractor/"
fi

cp -a "$WORKSPACE_ROOT/configs/fullnode/." "$STAGE_DIR/runtime/srv/local-tron-usdt-backfill/configs/fullnode/"
cp -a "$WORKSPACE_ROOT/scripts/run/." "$STAGE_DIR/runtime/srv/local-tron-usdt-backfill/scripts/run/"

{
  echo "bundle_created_utc=$STAMP"
  echo "host=$(hostname)"
  echo "kernel=$(uname -srmo)"
  echo "java_version_begin"
  java -version 2>&1
  echo "java_version_end"
  echo "service_unit_begin"
  systemctl cat tron-fullnode.service
  echo "service_unit_end"
  echo "output_directory_bytes=$(du -sb "$TRON_DATA_DIR/output-directory" | awk '{print $1}')"
  echo "output_directory_human=$(du -sh "$TRON_DATA_DIR/output-directory" | awk '{print $1}')"
  echo "data_files=$(find "$TRON_DATA_DIR/output-directory" -type f | wc -l)"
} > "$MANIFEST_PATH"

tar -C "$STAGE_DIR" -czf "$BUNDLE_PATH" runtime manifest.txt

aws s3 cp "$BUNDLE_PATH" "s3://$BUNDLE_BUCKET/$BUNDLE_PREFIX/tron-launch-ready-${STAMP}.tar.gz" \
  --storage-class "$BUNDLE_STORAGE_CLASS" \
  --region "$AWS_DEFAULT_REGION"

aws s3 cp "$MANIFEST_PATH" "s3://$BUNDLE_BUCKET/$BUNDLE_PREFIX/tron-launch-ready-${STAMP}.manifest.txt" \
  --storage-class "$BUNDLE_STORAGE_CLASS" \
  --region "$AWS_DEFAULT_REGION"

echo "bundle_path=$BUNDLE_PATH"
echo "bundle_bucket=$BUNDLE_BUCKET"
echo "bundle_prefix=$BUNDLE_PREFIX"
echo "bundle_object=tron-launch-ready-${STAMP}.tar.gz"
echo "manifest_object=tron-launch-ready-${STAMP}.manifest.txt"
echo "bundle_storage_class=$BUNDLE_STORAGE_CLASS"

