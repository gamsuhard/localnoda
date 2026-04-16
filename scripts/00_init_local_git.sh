#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v git >/dev/null 2>&1; then
  echo "git is not installed" >&2
  exit 1
fi

if [ ! -d .git ]; then
  git init
fi

git config --local core.autocrlf input || true
git config --local pull.rebase false || true

echo "Repository initialized at: $ROOT_DIR"
echo "Remote list (should be empty for this project):"
git remote -v || true
