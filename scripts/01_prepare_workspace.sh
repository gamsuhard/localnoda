#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p   "$ROOT_DIR/raw/incoming"   "$ROOT_DIR/raw/closed"   "$ROOT_DIR/raw/quarantine"   "$ROOT_DIR/logs/fullnode"   "$ROOT_DIR/logs/extractor"   "$ROOT_DIR/logs/loader"   "$ROOT_DIR/reports/validation"   "$ROOT_DIR/reports/load"   "$ROOT_DIR/runtime"   "$ROOT_DIR/artifacts/plugins"
find "$ROOT_DIR/scripts" -type f -name "*.sh" -exec chmod 0755 {} +

echo "Workspace directories prepared under: $ROOT_DIR"
