#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="$ROOT_DIR/runtime/run_state.sqlite"
SCHEMA_PATH="$ROOT_DIR/sql/sqlite/001_run_state.sql"

if ! command -v sqlite3 >/dev/null 2>&1; then
  if ! command -v python >/dev/null 2>&1; then
    echo "sqlite3 is not installed and python is not available for fallback" >&2
    exit 1
  fi

  export DB_PATH
  export SCHEMA_PATH
  python - <<'PY'
import os
import pathlib
import sqlite3

db_path = pathlib.Path(os.environ["DB_PATH"])
schema_path = pathlib.Path(os.environ["SCHEMA_PATH"])

db_path.parent.mkdir(parents=True, exist_ok=True)

with sqlite3.connect(db_path) as conn:
    conn.executescript(schema_path.read_text(encoding="utf-8"))
PY

  echo "Initialized SQLite state DB via python fallback: $DB_PATH"
  exit 0
fi

mkdir -p "$ROOT_DIR/runtime"
sqlite3 "$DB_PATH" < "$SCHEMA_PATH"

echo "Initialized SQLite state DB: $DB_PATH"
