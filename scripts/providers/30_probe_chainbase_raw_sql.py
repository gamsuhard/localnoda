#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from provider_clients import ChainbaseRawClient, DEFAULT_CHAINBASE_RAW_BASE_URL, read_env_file


DEFAULT_SQL = "\n".join(
    [
        "SELECT block_number, tx_id, log_index, timestamp, contract_address, from_address, to_address, value, __pk",
        "FROM tron.stable_coin_transfers",
        "LIMIT 1",
    ]
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).resolve().parents[2] / "runtime" / "provider_api.env"),
    )
    parser.add_argument("--sql", default=DEFAULT_SQL)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=2.0)
    args = parser.parse_args()

    env_values = read_env_file(Path(args.env_file))
    client = ChainbaseRawClient(
        base_url=env_values.get("CHAINBASE_RAW_BASE_URL", DEFAULT_CHAINBASE_RAW_BASE_URL),
        api_key=env_values.get("CHAINBASE_API_KEY", ""),
    )
    result = client.run_sql(
        sql=args.sql,
        poll_interval_seconds=args.poll_interval_seconds,
        timeout_seconds=args.timeout_seconds,
    )
    payload = ((result.get("results") or {}).get("payload") or {}) if isinstance(result, dict) else {}
    data_obj = payload.get("data", {}) if isinstance(payload, dict) else {}
    columns = data_obj.get("columns", []) if isinstance(data_obj, dict) else []
    rows = data_obj.get("data", []) if isinstance(data_obj, dict) else []
    print(
        json.dumps(
            {
                "execution_id": result.get("execution_id", ""),
                "final_status": result.get("final_status", ""),
                "execute_status_code": ((result.get("execute") or {}).get("status_code")),
                "status_status_code": ((result.get("status") or {}).get("status_code")),
                "results_status_code": ((result.get("results") or {}).get("status_code")),
                "column_count": len(columns) if isinstance(columns, list) else 0,
                "row_count": len(rows) if isinstance(rows, list) else 0,
                "first_row_preview": rows[0] if isinstance(rows, list) and rows else [],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
