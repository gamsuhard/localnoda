from __future__ import annotations

import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GENERATOR_PATH = PROJECT_ROOT / "scripts" / "demo" / "10_generate_demo_tron_segment.py"
UPLOADER_PATH = PROJECT_ROOT / "extractor" / "supervisor" / "10_upload_sealed_segments.py"
LOADER_PATH = PROJECT_ROOT / "loader" / "run" / "10_load_run_from_s3.py"
SQLITE_SCHEMA_001 = PROJECT_ROOT / "sql" / "sqlite" / "001_run_state.sql"
SQLITE_SCHEMA_002 = PROJECT_ROOT / "sql" / "sqlite" / "002_segment_upload_state.sql"
LOADER_SCHEMA = PROJECT_ROOT / "loader" / "sql" / "020_loader_state.sql"
RESOLVED_END_BLOCK = 60000000


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeS3BufferClient:
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
            "Body": local_path.read_bytes(),
            "ContentType": content_type,
        }
        self.objects[(bucket, key)] = payload
        return payload

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        return sorted(key for (stored_bucket, key) in self.objects.keys() if stored_bucket == bucket and key.startswith(prefix))

    def get_bytes(self, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]["Body"]  # type: ignore[return-value]

    def get_json(self, bucket: str, key: str):
        import json

        return json.loads(self.get_bytes(bucket, key).decode("utf-8"))

    def download_to_file(self, bucket: str, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.get_bytes(bucket, key))


class FakeClickHouseTarget:
    def __init__(self) -> None:
        self.staging_events: list[dict[str, object]] = []
        self.staging_legs: list[dict[str, object]] = []
        self.events: dict[str, dict[str, object]] = {}
        self.legs: dict[str, dict[str, object]] = {}
        self.audit_rows: list[dict[str, object]] = []

    def reset_stage_tables(self) -> None:
        self.staging_events = []
        self.staging_legs = []

    def append_stage_rows(self, event_rows, leg_rows) -> None:
        self.staging_events.extend(event_rows)
        self.staging_legs.extend(leg_rows)

    def merge_segment(self, run_id: str, segment_id: str) -> dict[str, int]:
        events_inserted = 0
        legs_inserted = 0
        for row in self.staging_events:
            if row["event_id"] not in self.events:
                self.events[row["event_id"]] = row
                events_inserted += 1
        for row in self.staging_legs:
            if row["leg_id"] not in self.legs:
                self.legs[row["leg_id"]] = row
                legs_inserted += 1
        self.reset_stage_tables()
        return {"events_inserted": events_inserted, "legs_inserted": legs_inserted}

    def insert_load_audit(self, rows) -> None:
        self.audit_rows.extend(rows)

    def count_rows(self, table_name: str, where_clause: str) -> int:
        if table_name.endswith("trc20_transfer_events"):
            return len(self.events)
        if table_name.endswith("address_transfer_legs"):
            return len(self.legs)
        raise AssertionError(f"unexpected table_name {table_name}")


class LoaderE2EDemoTest(unittest.TestCase):
    def setUp(self) -> None:
        self.generator = load_module("demo_generator_block06", GENERATOR_PATH)
        self.uploader = load_module("segment_uploader_block06", UPLOADER_PATH)
        self.loader = load_module("run_loader_block06", LOADER_PATH)
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.run_id = "tron-usdt-backfill-20260415-140000Z"
        self.run_root = self.root / "raw" / "runs" / self.run_id
        self.runtime_db = self.root / "runtime" / "run_state.sqlite"
        self.loader_db = self.root / "runtime" / "loader_state.sqlite"
        self.bucket = "goldusdt-v2-stage-913378704801-raw"
        self.prefix_root = "providers/tron-usdt-backfill/usdt-transfer-oneoff"
        (self.root / "runtime").mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.runtime_db)
        try:
            connection.executescript(SQLITE_SCHEMA_001.read_text(encoding="utf-8"))
            connection.commit()
        finally:
            connection.close()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_loader_consumes_demo_run_and_replay_is_idempotent(self) -> None:
        self.generator.generate_demo_segment(self.run_root, self.run_id, self.runtime_db)
        fake_s3 = FakeS3BufferClient()
        self.uploader.upload_sealed_segments(
            db_path=self.runtime_db,
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

        target = FakeClickHouseTarget()
        load_result = self.loader.load_run_from_s3(
            run_id=self.run_id,
            bucket=self.bucket,
            prefix_root=self.prefix_root,
            loader_db_path=self.loader_db,
            loader_schema_path=LOADER_SCHEMA,
            storage_client=fake_s3,
            load_target=target,
            region="eu-central-1",
        )
        self.assertEqual(len(load_result["segments"]), 1)
        self.assertEqual(load_result["segments"][0]["status"], "validated")
        self.assertEqual(len(target.events), 3)
        self.assertEqual(len(target.legs), 6)
        self.assertEqual(len(target.audit_rows), 2)

        validation = self.loader.validate_loaded_run(
            run_id=self.run_id,
            loader_db_path=self.loader_db,
            loader_schema_path=LOADER_SCHEMA,
            load_target=target,
        )
        self.assertEqual(validation["status"], "ok")
        self.assertEqual(validation["events"], 3)
        self.assertEqual(validation["legs"], 6)
        self.assertEqual(validation["segment_status_counts"]["validated"], 1)

        replay_result = self.loader.load_run_from_s3(
            run_id=self.run_id,
            bucket=self.bucket,
            prefix_root=self.prefix_root,
            loader_db_path=self.loader_db,
            loader_schema_path=LOADER_SCHEMA,
            storage_client=fake_s3,
            load_target=target,
            region="eu-central-1",
            force_replay=True,
        )
        self.assertEqual(len(replay_result["segments"]), 1)
        self.assertEqual(replay_result["segments"][0]["events_inserted"], 0)
        self.assertEqual(replay_result["segments"][0]["legs_inserted"], 0)
        self.assertEqual(len(target.events), 3)
        self.assertEqual(len(target.legs), 6)


if __name__ == "__main__":
    unittest.main()
