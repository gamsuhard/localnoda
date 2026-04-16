#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
if command -v tree >/dev/null 2>&1; then
  tree -a -I '.git'
else
  find . -mindepth 1 -maxdepth 4 | sort
fi
