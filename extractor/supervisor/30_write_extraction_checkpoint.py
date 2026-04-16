#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import sqlite3
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
    parser = argparse.ArgumentParser(description="Write local extraction checkpoint for a run.")
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    uploader = load_uploader_module()
    connection = sqlite3.connect(args.db_path)
    try:
        checkpoint_path = uploader.write_extraction_checkpoint(connection, args.run_root, args.run_id)
    finally:
        connection.close()
    print(checkpoint_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
