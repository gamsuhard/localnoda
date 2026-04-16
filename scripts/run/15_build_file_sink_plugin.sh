#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${PLUGIN_ROOT:=$WORKSPACE_ROOT/extractor/plugin}"
: "${PLUGIN_ZIP_TARGET:=$WORKSPACE_ROOT/artifacts/plugins/plugin-file-sink.zip}"

if ! command -v gradle >/dev/null 2>&1; then
  echo "gradle is required to build the custom file sink plugin" >&2
  exit 1
fi

mkdir -p "$(dirname "$PLUGIN_ZIP_TARGET")"

cd "$PLUGIN_ROOT"
gradle --no-daemon clean assembleWorkspacePlugin

if [ ! -f "$PLUGIN_ZIP_TARGET" ]; then
  echo "plugin build did not produce $PLUGIN_ZIP_TARGET" >&2
  exit 1
fi

ls -lh "$PLUGIN_ZIP_TARGET"
