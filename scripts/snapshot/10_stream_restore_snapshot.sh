#!/usr/bin/env bash
set -euo pipefail

: "${TRON_DATA_DIR:=/tron-data/java-tron}"
: "${TRON_USER:=tron}"
: "${TRON_SNAPSHOT_BASE_URL:=http://35.197.17.205}"
: "${TRON_SNAPSHOT_ARCHIVE:=FullNode_output-directory.tgz}"
: "${TRON_SNAPSHOT_URL:=}"

resolve_latest_snapshot_url() {
  TRON_SNAPSHOT_BASE_URL="$TRON_SNAPSHOT_BASE_URL" TRON_SNAPSHOT_ARCHIVE="$TRON_SNAPSHOT_ARCHIVE" python3 - <<'PY'
import os
import re
import urllib.request

base = os.environ["TRON_SNAPSHOT_BASE_URL"].rstrip("/") + "/"
archive = os.environ["TRON_SNAPSHOT_ARCHIVE"].strip()
req = urllib.request.Request(base, headers={"User-Agent": "Mozilla/5.0"})
with urllib.request.urlopen(req, timeout=30) as r:
    html = r.read().decode("utf-8", errors="ignore")
dirs = sorted(set(re.findall(r'href="(backup\d{8}/)"', html)))
if not dirs:
    raise SystemExit("no backup directories found")
print(base + dirs[-1] + archive)
PY
}

if [ -z "$TRON_SNAPSHOT_URL" ]; then
  TRON_SNAPSHOT_URL="$(resolve_latest_snapshot_url)"
fi

mkdir -p "$(dirname "$TRON_DATA_DIR")" "$TRON_DATA_DIR"
rm -rf "$TRON_DATA_DIR/output-directory"

echo "SNAPSHOT_BASE_URL=$TRON_SNAPSHOT_BASE_URL"
echo "SNAPSHOT_URL=$TRON_SNAPSHOT_URL"
echo "RESTORE_MODE=stream_non_resumable"

curl --fail --location --retry 10 --retry-all-errors --retry-delay 5 --connect-timeout 30 "$TRON_SNAPSHOT_URL" | tar -xz -C "$TRON_DATA_DIR"
chown -R "$TRON_USER:$TRON_USER" "$TRON_DATA_DIR"

echo "SNAPSHOT_URL=$TRON_SNAPSHOT_URL"
echo "RESTORE_PATH=$TRON_DATA_DIR/output-directory"
