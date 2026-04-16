#!/usr/bin/env bash
set -euo pipefail

: "${TRON_HTTP_PORT:=8090}"

systemctl is-active --quiet tron-fullnode.service
pgrep -af 'FullNode|java.*tron' >/dev/null
test -f /var/log/tron/fullnode.stdout.log

if curl -sf --max-time 10 -H 'Content-Type: application/json' -d '{}' "http://127.0.0.1:${TRON_HTTP_PORT}/wallet/getnodeinfo" >/tmp/tron-nodeinfo.json; then
  python3 - <<'PY'
import json
from pathlib import Path
data = json.loads(Path('/tmp/tron-nodeinfo.json').read_text(encoding='utf-8'))
print('nodeinfo_ok=true')
for key in ['beginSyncNum', 'block', 'solidityBlock']:
    if key in data:
        print(f'{key}={data[key]}')
PY
else
  echo "Node HTTP endpoint is not ready yet; service and process are alive."
fi

tail -n 20 /var/log/tron/fullnode.stdout.log || true
