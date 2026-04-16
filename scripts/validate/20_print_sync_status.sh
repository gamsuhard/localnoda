#!/usr/bin/env bash
set -euo pipefail

: "${TRON_HTTP_PORT:=8090}"

curl -sf --max-time 10 -H 'Content-Type: application/json' -d '{}' "http://127.0.0.1:${TRON_HTTP_PORT}/wallet/getnodeinfo" | python3 - <<'PY'
import json
import sys

data = json.load(sys.stdin)
for key in ['beginSyncNum', 'block', 'solidityBlock', 'cheatWitnessInfoMap']:
    if key in data:
        print(f'{key}={data[key]}')
PY
