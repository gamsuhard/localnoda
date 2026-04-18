#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${TRON_CONFIG_PATH:=/opt/tron/fullnode/config/config.conf}"
: "${TARGET_RUN_ROOT:=}"
: "${FILTER_FROM_BLOCK_OVERRIDE:=}"
: "${FILTER_TO_BLOCK_OVERRIDE:=}"

if [ -z "$TARGET_RUN_ROOT" ]; then
  TARGET_RUN_ROOT="$(ls -1dt "$WORKSPACE_ROOT"/raw/runs/* 2>/dev/null | head -n1 || true)"
fi

if [ -z "$TARGET_RUN_ROOT" ] || [ ! -d "$TARGET_RUN_ROOT" ]; then
  echo "run root not found" >&2
  exit 1
fi

export TARGET_RUN_ROOT TRON_CONFIG_PATH FILTER_FROM_BLOCK_OVERRIDE FILTER_TO_BLOCK_OVERRIDE

python3 - <<'PY'
import json
import os
import re
import shutil
from pathlib import Path

run_root = Path(os.environ["TARGET_RUN_ROOT"])
config_path = Path(os.environ["TRON_CONFIG_PATH"])
from_override = os.environ.get("FILTER_FROM_BLOCK_OVERRIDE", "").strip()
to_override = os.environ.get("FILTER_TO_BLOCK_OVERRIDE", "").strip()

manifest_paths = sorted((run_root / "manifests").glob("*.manifest.json"))
if not manifest_paths:
    raise SystemExit("no segment manifests found under run root")

max_block_to = None
for path in manifest_paths:
    payload = json.loads(path.read_text(encoding="utf-8"))
    block_to = payload.get("block_to")
    if block_to is None:
        continue
    if max_block_to is None or block_to > max_block_to:
        max_block_to = int(block_to)

if max_block_to is None:
    raise SystemExit("unable to determine max block_to from manifests")

run_manifest_path = run_root / "manifests" / "run.json"
run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8")) if run_manifest_path.exists() else {}
resolved_end_block = run_manifest.get("resolved_end_block")

from_block = int(from_override) if from_override else max_block_to + 1
to_block = int(to_override) if to_override else int(resolved_end_block) if resolved_end_block is not None else None

text = config_path.read_text(encoding="utf-8")
updated = re.sub(r'startSyncBlockNum\s*=\s*\d+', f'startSyncBlockNum = {from_block}', text, count=1)
updated = re.sub(r'fromblock\s*=\s*".*?"', f'fromblock = "{from_block}"', updated, count=1)
replacement_toblock = f'toblock = "{to_block}"' if to_block is not None else 'toblock = ""'
updated = re.sub(r'toblock\s*=\s*".*?"', replacement_toblock, updated, count=1)

if updated == text:
    raise SystemExit("config filter block was not updated")

backup_path = config_path.with_suffix(config_path.suffix + ".bak.resume")
shutil.copyfile(config_path, backup_path)
config_path.write_text(updated, encoding="utf-8")

print(
    json.dumps(
        {
            "run_root": str(run_root),
            "config_path": str(config_path),
            "backup_path": str(backup_path),
            "max_sealed_block_to": max_block_to,
            "fromblock": from_block,
            "toblock": to_block,
        }
    )
)
PY
