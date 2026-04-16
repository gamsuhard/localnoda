#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from provider_clients import (
    DEFAULT_TRONGRID_API_KEY_HEADER,
    DEFAULT_TRONGRID_BASE_URL,
    TronGridClient,
    read_env_file,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).resolve().parents[2] / "runtime" / "provider_api.env"),
    )
    parser.add_argument("--address", default="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
    parser.add_argument("--limit", type=int, default=1)
    args = parser.parse_args()

    env_values = read_env_file(Path(args.env_file))
    client = TronGridClient(
        base_url=env_values.get("TRONGRID_BASE_URL", DEFAULT_TRONGRID_BASE_URL),
        api_key=env_values.get("TRONGRID_API_KEY", ""),
        api_key_header=env_values.get("TRONGRID_API_KEY_HEADER", DEFAULT_TRONGRID_API_KEY_HEADER),
    )
    response = client.trc20_transactions(address=args.address, limit=args.limit)
    payload = response.get("payload", {})
    data = payload.get("data", []) if isinstance(payload, dict) else []
    first = data[0] if isinstance(data, list) and data else {}
    print(
        json.dumps(
            {
                "status_code": response.get("status_code"),
                "address": args.address,
                "row_count": len(data) if isinstance(data, list) else 0,
                "first_item_keys": sorted(first.keys()) if isinstance(first, dict) else [],
                "success": int(response.get("status_code", 0)) < 400,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
