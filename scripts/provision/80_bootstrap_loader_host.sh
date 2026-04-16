#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${LOADER_VENV_DIR:=$WORKSPACE_ROOT/.venvs/loader}"
: "${LOADER_REQUIREMENTS_FILE:=$WORKSPACE_ROOT/requirements-loader.txt}"

if command -v dnf >/dev/null 2>&1; then
  PKG_MGR=dnf
elif command -v yum >/dev/null 2>&1; then
  PKG_MGR=yum
else
  echo "Neither dnf nor yum is available" >&2
  exit 1
fi

sudo "$PKG_MGR" install -y python3.11 python3.11-pip jq tar gzip unzip awscli

if ! command -v curl >/dev/null 2>&1; then
  sudo "$PKG_MGR" install -y curl
fi

if ! command -v clickhouse-client >/dev/null 2>&1; then
  sudo tee /etc/yum.repos.d/clickhouse.repo >/dev/null <<'EOF'
[clickhouse]
name=ClickHouse
baseurl=https://packages.clickhouse.com/rpm/stable/
enabled=1
gpgcheck=0
repo_gpgcheck=0
EOF
  sudo "$PKG_MGR" install -y clickhouse-client
fi

if [ ! -f "$LOADER_REQUIREMENTS_FILE" ]; then
  echo "Loader requirements file not found: $LOADER_REQUIREMENTS_FILE" >&2
  exit 1
fi

PYTHON_BIN="$(command -v python3.11)"
if [ -z "$PYTHON_BIN" ]; then
  echo "python3.11 was not installed successfully" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOADER_VENV_DIR")"
rm -rf "$LOADER_VENV_DIR"
"$PYTHON_BIN" -m venv "$LOADER_VENV_DIR"
"$LOADER_VENV_DIR/bin/python" -m pip install --upgrade pip >/dev/null
"$LOADER_VENV_DIR/bin/pip" install --requirement "$LOADER_REQUIREMENTS_FILE" >/dev/null

echo "Loader host bootstrap complete"
clickhouse-client --version
"$LOADER_VENV_DIR/bin/python" --version
