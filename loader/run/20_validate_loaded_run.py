#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


def load_loader_module():
    module_path = Path(__file__).with_name("10_load_run_from_s3.py")
    spec = importlib.util.spec_from_file_location("run_loader", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a loaded TRON USDT run.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--loader-db-path", required=True, type=Path)
    parser.add_argument("--loader-schema-path", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    loader = load_loader_module()
    result = loader.validate_loaded_run(
        run_id=args.run_id,
        loader_db_path=args.loader_db_path,
        loader_schema_path=args.loader_schema_path,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
