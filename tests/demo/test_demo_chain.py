from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GENERATOR_PATH = PROJECT_ROOT / "scripts" / "demo" / "10_generate_demo_tron_segment.py"
UPLOADER_PATH = PROJECT_ROOT / "extractor" / "supervisor" / "10_upload_sealed_segments.py"
SQLITE_SCHEMA_001 = PROJECT_ROOT / "sql" / "sqlite" / "001_run_state.sql"
SQLITE_SCHEMA_002 = PROJECT_ROOT / "sql" / "sqlite" / "002_segment_upload_state.sql"
RESOLVED_END_BLOCK = 60000000


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}

    def head_object(self, bucket: str, key: str) -> dict[str, object]:
        try:
            return self.objects[(bucket, key)]
        except KeyError as exc:
            raise FileNotFoundError(f"s3://{bucket}/{key}") from exc

    def put_object(
        self,
        bucket: str,
        key: str,
        local_path: Path,
        metadata: dict[str, str],
        sse_mode: str | None = None,
        kms_key_arn: str | None = None,
        content_type: str | None = None,
    ) -> dict[str, object]:
        payload = {
            "ContentLength": local_path.stat().st_size,
            "Metadata": metadata,
            "ETag": '"demo-etag"',
            "ContentType": content_type,
        }
        self.objects[(bucket, key)] = payload
        return payload


class DemoChainTest(unittest.TestCase):
    def setUp(self) -> None:
        self.generator = load_module("demo_generator", GENERATOR_PATH)
        self.uploader = load_module("segment_uploader_demo", UPLOADER_PATH)
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.run_id = "tron-usdt-backfill-20260415-130000Z"
        self.run_root = self.root / "raw" / "runs" / self.run_id
        self.db_path = self.root / "runtime" / "run_state.sqlite"
        self.bucket = "goldusdt-v2-stage-913378704801-raw"
        self.prefix_root = "providers/tron-usdt-backfill/usdt-transfer-oneoff"
        (self.root / "runtime").mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        try:
            connection.executescript(SQLITE_SCHEMA_001.read_text(encoding="utf-8"))
            connection.commit()
        finally:
            connection.close()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_generator_and_uploader_produce_demo_run(self) -> None:
        generated = self.generator.generate_demo_segment(self.run_root, self.run_id, self.db_path)
        self.assertTrue(Path(generated["segment_path"]).exists())
        self.assertTrue(Path(generated["manifest_path"]).exists())

        fake_s3 = FakeS3Client()
        uploaded = self.uploader.upload_sealed_segments(
            db_path=self.db_path,
            schema_path=SQLITE_SCHEMA_002,
            bucket=self.bucket,
            prefix_root=self.prefix_root,
            run_id=self.run_id,
            s3_client=fake_s3,
            sse_mode="aws:kms",
            kms_key_arn="arn:aws:kms:eu-central-1:913378704801:key/demo",
            region="eu-central-1",
            java_tron_version="GreatVoyage-v4.8.1",
            config_sha256="configsha-demo",
            plugin_build_id="plugin-build-demo",
            resolved_end_block=RESOLVED_END_BLOCK,
        )
        self.assertEqual(len(uploaded), 1)

        run_manifest = json.loads((self.run_root / "manifests" / "run.json").read_text(encoding="utf-8"))
        runtime_manifest = json.loads((self.run_root / "manifests" / "runtime.json").read_text(encoding="utf-8"))
        checkpoint = json.loads((self.run_root / "checkpoints" / "extraction.json").read_text(encoding="utf-8"))
        self.assertEqual(run_manifest["segment_count"], 1)
        self.assertEqual(run_manifest["status"], "uploaded")
        self.assertEqual(run_manifest["resolved_end_block"], RESOLVED_END_BLOCK)
        self.assertEqual(runtime_manifest["plugin_build_id"], "plugin-build-demo")
        self.assertEqual(checkpoint["last_uploaded_segment_id"], f"{self.run_id}-seg-000001")

        expected_keys = {
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/segments/usdt_transfer_000001.ndjson.gz"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/manifests/segments/usdt_transfer_000001.manifest.json"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/manifests/runtime.json"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/manifests/run.json"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/checkpoints/extraction.json"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/checksums/SHA256SUMS"),
        }
        self.assertTrue(expected_keys.issubset(set(fake_s3.objects.keys())))

    def test_multi_segment_generator_and_uploader_produce_demo_run(self) -> None:
        generated = self.generator.generate_demo_run(
            self.run_root,
            self.run_id,
            self.db_path,
            segment_count=4,
            records_per_segment=5,
        )
        self.assertEqual(generated["segment_count"], 4)
        self.assertEqual(generated["total_record_count"], 20)

        fake_s3 = FakeS3Client()
        uploaded = self.uploader.upload_sealed_segments(
            db_path=self.db_path,
            schema_path=SQLITE_SCHEMA_002,
            bucket=self.bucket,
            prefix_root=self.prefix_root,
            run_id=self.run_id,
            s3_client=fake_s3,
            sse_mode="aws:kms",
            kms_key_arn="arn:aws:kms:eu-central-1:913378704801:key/demo",
            region="eu-central-1",
            java_tron_version="GreatVoyage-v4.8.1",
            config_sha256="configsha-demo",
            plugin_build_id="plugin-build-demo",
            resolved_end_block=RESOLVED_END_BLOCK,
        )
        self.assertEqual(len(uploaded), 4)
        run_manifest = json.loads((self.run_root / "manifests" / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(run_manifest["segment_count"], 4)


if __name__ == "__main__":
    unittest.main()
