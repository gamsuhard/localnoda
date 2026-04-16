#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
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
    parser = argparse.ArgumentParser(description="Write local runtime/run manifests and SHA256SUMS for a run.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix-root", required=True)
    parser.add_argument("--region")
    parser.add_argument("--java-tron-version", required=True)
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--plugin-build-id", required=True)
    parser.add_argument("--resolved-end-block", required=True, type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    uploader = load_uploader_module()
    uploader.write_runtime_manifest(
        args.run_root,
        args.run_id,
        args.bucket,
        args.prefix_root,
        args.region,
        java_tron_version=args.java_tron_version,
        config_sha256=args.config_sha256,
        plugin_build_id=args.plugin_build_id,
    )
    run_manifest_path = uploader.write_run_manifest(
        args.run_root,
        args.run_id,
        args.bucket,
        args.prefix_root,
        resolved_end_block=args.resolved_end_block,
    )
    checksums_path = uploader.write_checksums(args.run_root)
    print(run_manifest_path)
    print(checksums_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
