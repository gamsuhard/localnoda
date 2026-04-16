#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from provider_clients import DEFAULT_PROVIDER_SECRET_ARN, sync_provider_runtime_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[2] / "runtime" / "provider_api.env"),
    )
    parser.add_argument("--secret-arn", default=DEFAULT_PROVIDER_SECRET_ARN)
    parser.add_argument("--profile", default="ai-agents-dev")
    parser.add_argument("--region", default="eu-central-1")
    args = parser.parse_args()

    result = sync_provider_runtime_env(
        output_path=Path(args.output),
        secret_arn=args.secret_arn,
        profile=args.profile,
        region=args.region,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
