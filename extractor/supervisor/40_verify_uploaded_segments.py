#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


def load_uploader_module():
    module_path = Path(__file__).with_name("10_upload_sealed_segments.py")
    spec = importlib.util.spec_from_file_location("segment_uploader", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify uploaded segment objects and sidecars in S3.")
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--schema-path", required=True, type=Path)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix-root", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--region")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    uploader = load_uploader_module()
    result = uploader.verify_uploaded_segments(
        db_path=args.db_path,
        schema_path=args.schema_path,
        bucket=args.bucket,
        prefix_root=args.prefix_root,
        run_id=args.run_id,
        region=args.region,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
