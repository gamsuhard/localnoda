#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GENERATOR_PATH = PROJECT_ROOT / "scripts" / "demo" / "10_generate_demo_tron_segment.py"
UPLOADER_PATH = PROJECT_ROOT / "extractor" / "supervisor" / "10_upload_sealed_segments.py"
SQLITE_SCHEMA_001 = PROJECT_ROOT / "sql" / "sqlite" / "001_run_state.sql"
SQLITE_SCHEMA_002 = PROJECT_ROOT / "sql" / "sqlite" / "002_segment_upload_state.sql"


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def initialize_sqlite(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(SQLITE_SCHEMA_001.read_text(encoding="utf-8"))
        connection.executescript(SQLITE_SCHEMA_002.read_text(encoding="utf-8"))
        connection.commit()
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a synthetic TRON USDT run and publish it into the frozen S3 buffer.")
    parser.add_argument("--run-id", default=f"prebulk-demo-{utc_now_compact().lower()}")
    parser.add_argument("--record-count", type=int, default=3)
    parser.add_argument("--base-block", type=int, default=54300001)
    parser.add_argument("--base-timestamp-ms", type=int, default=1693526400123)
    parser.add_argument("--bucket", default="goldusdt-v2-stage-913378704801-raw")
    parser.add_argument("--prefix-root", default="providers/tron-usdt-backfill/usdt-transfer-oneoff")
    parser.add_argument("--region", default="eu-central-1")
    parser.add_argument("--sse-mode")
    parser.add_argument("--kms-key-arn")
    parser.add_argument("--java-tron-version", default="synthetic-demo")
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--plugin-build-id", required=True)
    parser.add_argument("--extractor-region", default="eu-central-1")
    parser.add_argument("--resolved-end-block", type=int)
    parser.add_argument("--work-root", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    generator = load_module("demo_generator_publish", GENERATOR_PATH)
    uploader = load_module("segment_uploader_publish", UPLOADER_PATH)

    with tempfile.TemporaryDirectory() as temp_dir:
        base_root = args.work_root or Path(temp_dir)
        run_root = base_root / "raw" / "runs" / args.run_id
        db_path = base_root / "runtime" / "run_state.sqlite"
        initialize_sqlite(db_path)

        generated = generator.generate_demo_segment(
            run_root=run_root,
            run_id=args.run_id,
            db_path=db_path,
            record_count=args.record_count,
            base_block=args.base_block,
            base_timestamp_ms=args.base_timestamp_ms,
            bucket=args.bucket,
            prefix_root=args.prefix_root,
            extractor_instance_id=f"synthetic-{args.extractor_region}",
        )
        resolved_end_block = args.resolved_end_block or int(generated["block_to"])
        uploaded = uploader.upload_sealed_segments(
            db_path=db_path,
            schema_path=SQLITE_SCHEMA_002,
            bucket=args.bucket,
            prefix_root=args.prefix_root,
            run_id=args.run_id,
            sse_mode=args.sse_mode,
            kms_key_arn=args.kms_key_arn,
            region=args.extractor_region,
            java_tron_version=args.java_tron_version,
            config_sha256=args.config_sha256,
            plugin_build_id=args.plugin_build_id,
            resolved_end_block=resolved_end_block,
        )
        verified = uploader.verify_uploaded_segments(
            db_path=db_path,
            schema_path=SQLITE_SCHEMA_002,
            bucket=args.bucket,
            prefix_root=args.prefix_root,
            run_id=args.run_id,
            region=args.extractor_region,
        )
        result = {
            "run_id": args.run_id,
            "record_count": args.record_count,
            "bucket": args.bucket,
            "prefix_root": args.prefix_root,
            "resolved_end_block": resolved_end_block,
            "uploaded_segments": uploaded,
            "verified": verified,
        }
        print(json.dumps(result, indent=2))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
