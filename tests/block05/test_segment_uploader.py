from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
UPLOADER_PATH = PROJECT_ROOT / "extractor" / "supervisor" / "10_upload_sealed_segments.py"
SQLITE_SCHEMA_001 = PROJECT_ROOT / "sql" / "sqlite" / "001_run_state.sql"
SQLITE_SCHEMA_002 = PROJECT_ROOT / "sql" / "sqlite" / "002_segment_upload_state.sql"
FIXTURE_TRIGGER = PROJECT_ROOT / "tests" / "block05" / "fixtures" / "sample_solidity_log_trigger.json"
RESOLVED_END_BLOCK = 60000000


def load_uploader_module():
    spec = importlib.util.spec_from_file_location("segment_uploader", UPLOADER_PATH)
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
        body = local_path.read_bytes()
        etag = hashlib.md5(body).hexdigest()  # noqa: S324 - test fake only
        payload = {
            "ContentLength": len(body),
            "Metadata": metadata,
            "ETag": f'"{etag}"',
            "Body": body,
            "ServerSideEncryption": sse_mode,
            "SSEKMSKeyId": kms_key_arn,
            "ContentType": content_type,
        }
        self.objects[(bucket, key)] = payload
        return payload


class SegmentUploaderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_uploader_module()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "runtime" / "run_state.sqlite"
        self.run_id = "tron-usdt-backfill-20260415-120000Z"
        self.bucket = "goldusdt-v2-stage-913378704801-raw"
        self.prefix_root = "providers/tron-usdt-backfill/usdt-transfer-oneoff"
        self.run_root = self.root / "raw" / "runs" / self.run_id
        (self.run_root / "segments").mkdir(parents=True, exist_ok=True)
        (self.run_root / "manifests").mkdir(parents=True, exist_ok=True)
        (self.root / "runtime").mkdir(parents=True, exist_ok=True)
        self._init_db()
        self.segment_path, self.manifest_path = self._write_demo_segment()
        self.fake_s3 = FakeS3Client()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _init_db(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.executescript(SQLITE_SCHEMA_001.read_text(encoding="utf-8"))
            connection.executescript(SQLITE_SCHEMA_002.read_text(encoding="utf-8"))
            connection.execute(
                """
                INSERT INTO runs (run_id, run_type, status, started_at)
                VALUES (?, 'extract', 'running', CURRENT_TIMESTAMP)
                """,
                (self.run_id,),
            )
            connection.commit()
        finally:
            connection.close()

    def _write_demo_segment(self) -> tuple[Path, Path]:
        trigger = json.loads(FIXTURE_TRIGGER.read_text(encoding="utf-8"))
        segment_path = self.run_root / "segments" / "usdt_transfer_000001.ndjson.gz"
        with gzip.open(segment_path, "wt", encoding="utf-8") as handle:
            handle.write(json.dumps(trigger))
            handle.write("\n")
        segment_sha256 = self.module.sha256_file(segment_path)
        manifest_path = self.run_root / "manifests" / "usdt_transfer_000001.manifest.json"
        manifest_payload = {
            "manifest_version": 1,
            "kind": "segment_manifest",
            "segment_id": f"{self.run_id}-seg-000001",
            "run_id": self.run_id,
            "segment_seq": 1,
            "stream_name": "usdt_transfer",
            "trigger_name": "solidityLogTrigger",
            "topic0": "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "contract_address": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            "block_from": 54300001,
            "block_to": 54300001,
            "first_event_key": "54300001-0-0",
            "last_event_key": "54300001-0-0",
            "first_tx_hash": trigger["transactionId"],
            "last_tx_hash": trigger["transactionId"],
            "record_count": 1,
            "file_size_bytes": segment_path.stat().st_size,
            "sha256": segment_sha256,
            "codec": "ndjson.gz",
            "local_path": str(segment_path),
            "relative_path": f"segments/{segment_path.name}",
            "s3_bucket": self.bucket,
            "s3_key": f"{self.prefix_root}/runs/{self.run_id}/segments/{segment_path.name}",
            "extractor_instance_id": "i-demo",
            "created_at_utc": "2026-04-15T12:00:00Z",
            "closed_at_utc": "2026-04-15T12:01:00Z",
            "status": "sealed",
        }
        manifest_path.write_text(json.dumps(manifest_payload, indent=2) + "\n", encoding="utf-8")
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                """
                INSERT INTO segments (
                    segment_id,
                    run_id,
                    segment_seq,
                    file_path,
                    status,
                    first_block,
                    last_block,
                    first_tx_hash,
                    last_tx_hash,
                    row_count,
                    byte_count,
                    sha256
                ) VALUES (?, ?, 1, ?, 'sealed', 54300001, 54300001, ?, ?, 1, ?, ?)
                """,
                (
                    manifest_payload["segment_id"],
                    self.run_id,
                    str(segment_path),
                    trigger["transactionId"],
                    trigger["transactionId"],
                    segment_path.stat().st_size,
                    segment_sha256,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        return segment_path, manifest_path

    def test_uploads_sealed_segment_and_sidecars_idempotently(self) -> None:
        uploaded = self.module.upload_sealed_segments(
            db_path=self.db_path,
            schema_path=SQLITE_SCHEMA_002,
            bucket=self.bucket,
            prefix_root=self.prefix_root,
            run_id=self.run_id,
            s3_client=self.fake_s3,
            sse_mode="aws:kms",
            kms_key_arn="arn:aws:kms:eu-central-1:913378704801:key/demo",
            region="eu-central-1",
            java_tron_version="GreatVoyage-v4.8.1",
            config_sha256="configsha-demo",
            plugin_build_id="plugin-build-demo",
            resolved_end_block=RESOLVED_END_BLOCK,
        )
        self.assertEqual(len(uploaded), 1)

        connection = sqlite3.connect(self.db_path)
        try:
            status = connection.execute(
                "SELECT status FROM segments WHERE run_id = ? AND segment_seq = 1",
                (self.run_id,),
            ).fetchone()[0]
            self.assertEqual(status, "uploaded")
            upload_state = connection.execute(
                """
                SELECT s3_bucket, s3_key, uploaded_at, etag, upload_attempts, last_upload_error
                FROM segment_upload_state
                WHERE segment_id = ?
                """,
                (f"{self.run_id}-seg-000001",),
            ).fetchone()
            self.assertEqual(upload_state[0], self.bucket)
            self.assertIn("/segments/usdt_transfer_000001.ndjson.gz", upload_state[1])
            self.assertIsNotNone(upload_state[2])
            self.assertIsNotNone(upload_state[3])
            self.assertEqual(upload_state[4], 1)
            self.assertIsNone(upload_state[5])
        finally:
            connection.close()

        manifest_payload = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest_payload["status"], "uploaded")
        runtime_manifest = json.loads((self.run_root / "manifests" / "runtime.json").read_text(encoding="utf-8"))
        run_manifest = json.loads((self.run_root / "manifests" / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(runtime_manifest["java_tron_version"], "GreatVoyage-v4.8.1")
        self.assertEqual(runtime_manifest["config_sha256"], "configsha-demo")
        self.assertEqual(runtime_manifest["plugin_build_id"], "plugin-build-demo")
        self.assertEqual(run_manifest["resolved_end_block"], RESOLVED_END_BLOCK)

        expected_keys = {
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/segments/{self.segment_path.name}"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/manifests/segments/{self.manifest_path.name}"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/manifests/runtime.json"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/manifests/run.json"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/checkpoints/extraction.json"),
            (self.bucket, f"{self.prefix_root}/runs/{self.run_id}/checksums/SHA256SUMS"),
        }
        self.assertTrue(expected_keys.issubset(set(self.fake_s3.objects.keys())))

        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                "UPDATE segments SET status = 'sealed' WHERE run_id = ? AND segment_seq = 1",
                (self.run_id,),
            )
            connection.commit()
        finally:
            connection.close()
        manifest_payload["status"] = "sealed"
        self.manifest_path.write_text(json.dumps(manifest_payload, indent=2) + "\n", encoding="utf-8")

        uploaded_again = self.module.upload_sealed_segments(
            db_path=self.db_path,
            schema_path=SQLITE_SCHEMA_002,
            bucket=self.bucket,
            prefix_root=self.prefix_root,
            run_id=self.run_id,
            s3_client=self.fake_s3,
            sse_mode="aws:kms",
            kms_key_arn="arn:aws:kms:eu-central-1:913378704801:key/demo",
            region="eu-central-1",
            java_tron_version="GreatVoyage-v4.8.1",
            config_sha256="configsha-demo",
            plugin_build_id="plugin-build-demo",
            resolved_end_block=RESOLVED_END_BLOCK,
        )
        self.assertEqual(len(uploaded_again), 1)

        verification = self.module.verify_uploaded_segments(
            db_path=self.db_path,
            schema_path=SQLITE_SCHEMA_002,
            bucket=self.bucket,
            prefix_root=self.prefix_root,
            run_id=self.run_id,
            s3_client=self.fake_s3,
            region="eu-central-1",
        )
        self.assertEqual(verification["uploaded_segments"], 1)
        self.assertEqual(verification["verified_runs"], [self.run_id])


if __name__ == "__main__":
    unittest.main()
