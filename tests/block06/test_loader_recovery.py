from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

LOADER_PATH = PROJECT_ROOT / "loader" / "run" / "10_load_run_from_s3.py"
LOADER_SCHEMA = PROJECT_ROOT / "loader" / "sql" / "020_loader_state.sql"


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@contextmanager
def temporary_env(**updates: str):
    previous: dict[str, str | None] = {}
    for key, value in updates.items():
        previous[key] = os.environ.get(key)
        os.environ[key] = value
    try:
        yield
    finally:
        for key, original in previous.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original


def load_loader_module(module_name: str, **env: str):
    with temporary_env(**env):
        return load_module(module_name, LOADER_PATH)


def make_manifest(segment_id: str, sha256: str = "sha-demo") -> dict[str, str]:
    return {
        "segment_id": segment_id,
        "s3_key": f"providers/demo/runs/run-a/segments/{segment_id}.ndjson.gz",
        "sha256": sha256,
    }


class LoaderRecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "runtime" / "loader_state.sqlite"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _connection(self, loader):
        connection = loader.open_loader_connection(self.db_path)
        loader.ensure_loader_state_schema(connection, LOADER_SCHEMA)
        connection.commit()
        return connection

    def test_reclaims_stale_runtime_lock_without_pid(self) -> None:
        loader = load_loader_module(
            "run_loader_recovery_lock",
            LOADER_CONCURRENCY="2",
            LOADER_WORKER_SLOT="1",
            LOADER_RUNTIME_LOCK_STALE_SECONDS="1",
        )
        connection = self._connection(loader)
        try:
            connection.execute(
                """
                INSERT INTO loader_runtime_lock (lock_name, owner_id, clickhouse_database, acquired_at, released_at)
                VALUES (?, ?, ?, ?, NULL)
                """,
                (loader.RUNTIME_LOCK_NAME, "legacy-stale-owner", loader.CLICKHOUSE_DATABASE, "2000-01-01T00:00:00Z"),
            )
            connection.commit()
            owner_id = loader.acquire_runtime_lock(connection, "run-a")
            connection.commit()
            row = connection.execute(
                "SELECT owner_id, released_at FROM loader_runtime_lock WHERE lock_name = ?",
                (loader.RUNTIME_LOCK_NAME,),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], owner_id)
            self.assertIsNone(row[1])
        finally:
            connection.close()

    def test_requeues_stale_loading_segment_for_retry(self) -> None:
        loader = load_loader_module(
            "run_loader_recovery_loading",
            LOADER_CONCURRENCY="2",
            LOADER_WORKER_SLOT="1",
            LOADER_SEGMENT_STALE_SECONDS="1",
            LOADER_MAX_ATTEMPTS="5",
        )
        connection = self._connection(loader)
        try:
            connection.execute(
                """
                INSERT INTO loaded_segments (
                    run_id, segment_id, source_s3_key, source_sha256, status, claim_token, attempts, load_started_at
                ) VALUES (?, ?, ?, ?, 'loading', ?, ?, ?)
                """,
                ("run-a", "seg-000001", "s3://demo/seg-000001", "sha-demo", "legacy-stale-claim", 1, "2000-01-01T00:00:00Z"),
            )
            connection.commit()
            claimed = loader.claim_next_segment_manifest(connection, "run-a", {"seg-000001": make_manifest("seg-000001")})
            self.assertIsNotNone(claimed)
            row = connection.execute(
                """
                SELECT status, attempts, claim_token, claimed_at, load_started_at, merged_at, load_finished_at
                FROM loaded_segments
                WHERE run_id = ? AND segment_id = ?
                """,
                ("run-a", "seg-000001"),
            ).fetchone()
            self.assertEqual(row[0], "claimed")
            self.assertEqual(int(row[1]), 2)
            self.assertIsNotNone(row[2])
            self.assertIsNotNone(row[3])
            self.assertIsNone(row[4])
            self.assertIsNone(row[5])
            self.assertIsNone(row[6])
        finally:
            connection.close()

    def test_failed_segment_respects_retry_backoff(self) -> None:
        loader = load_loader_module(
            "run_loader_recovery_backoff",
            LOADER_CONCURRENCY="2",
            LOADER_WORKER_SLOT="1",
            LOADER_FAILED_RETRY_BACKOFF_SECONDS="3600",
            LOADER_MAX_ATTEMPTS="5",
        )
        connection = self._connection(loader)
        try:
            connection.execute(
                """
                INSERT INTO loaded_segments (
                    run_id, segment_id, source_s3_key, source_sha256, status, attempts, load_finished_at
                ) VALUES (?, ?, ?, ?, 'failed', ?, ?)
                """,
                ("run-a", "seg-000002", "s3://demo/seg-000002", "sha-demo", 1, loader.utc_now_iso()),
            )
            connection.commit()
            self.assertIsNone(
                loader.claim_next_segment_manifest(connection, "run-a", {"seg-000002": make_manifest("seg-000002")})
            )
            connection.execute(
                """
                UPDATE loaded_segments
                SET load_finished_at = ?
                WHERE run_id = ? AND segment_id = ?
                """,
                ("2000-01-01T00:00:00Z", "run-a", "seg-000002"),
            )
            connection.commit()
            claimed = loader.claim_next_segment_manifest(connection, "run-a", {"seg-000002": make_manifest("seg-000002")})
            self.assertIsNotNone(claimed)
            row = connection.execute(
                "SELECT status, attempts FROM loaded_segments WHERE run_id = ? AND segment_id = ?",
                ("run-a", "seg-000002"),
            ).fetchone()
            self.assertEqual(row[0], "claimed")
            self.assertEqual(int(row[1]), 2)
        finally:
            connection.close()

    def test_quarantines_stale_segment_after_max_attempts(self) -> None:
        loader = load_loader_module(
            "run_loader_recovery_quarantine",
            LOADER_CONCURRENCY="2",
            LOADER_WORKER_SLOT="1",
            LOADER_SEGMENT_STALE_SECONDS="1",
            LOADER_MAX_ATTEMPTS="3",
        )
        connection = self._connection(loader)
        try:
            connection.execute(
                """
                INSERT INTO loaded_segments (
                    run_id, segment_id, source_s3_key, source_sha256, status, claim_token, attempts, merged_at
                ) VALUES (?, ?, ?, ?, 'merged', ?, ?, ?)
                """,
                ("run-a", "seg-000003", "s3://demo/seg-000003", "sha-demo", "legacy-stale-claim", 3, "2000-01-01T00:00:00Z"),
            )
            connection.commit()
            claimed = loader.claim_next_segment_manifest(connection, "run-a", {"seg-000003": make_manifest("seg-000003")})
            self.assertIsNone(claimed)
            row = connection.execute(
                "SELECT status, load_finished_at, last_error FROM loaded_segments WHERE run_id = ? AND segment_id = ?",
                ("run-a", "seg-000003"),
            ).fetchone()
            self.assertEqual(row[0], "quarantined")
            self.assertIsNotNone(row[1])
            self.assertIn("max attempts reached", row[2])
        finally:
            connection.close()

    def test_two_workers_claim_distinct_segments_from_shared_sqlite(self) -> None:
        loader_slot1 = load_loader_module(
            "run_loader_recovery_slot1",
            LOADER_CONCURRENCY="2",
            LOADER_WORKER_SLOT="1",
            LOADER_SQLITE_BUSY_TIMEOUT_MS="5000",
        )
        loader_slot2 = load_loader_module(
            "run_loader_recovery_slot2",
            LOADER_CONCURRENCY="2",
            LOADER_WORKER_SLOT="2",
            LOADER_SQLITE_BUSY_TIMEOUT_MS="5000",
        )
        connection = self._connection(loader_slot1)
        try:
            connection.executemany(
                """
                INSERT INTO loaded_segments (run_id, segment_id, source_s3_key, source_sha256, status)
                VALUES (?, ?, ?, ?, 'pending')
                """,
                [
                    ("run-a", "seg-000010", "s3://demo/seg-000010", "sha-a"),
                    ("run-a", "seg-000011", "s3://demo/seg-000011", "sha-b"),
                ],
            )
            connection.commit()
        finally:
            connection.close()

        barrier = threading.Barrier(2)
        results: list[tuple[int, str | None]] = []
        errors: list[BaseException] = []
        manifests = {
            "seg-000010": make_manifest("seg-000010", sha256="sha-a"),
            "seg-000011": make_manifest("seg-000011", sha256="sha-b"),
        }

        def claim_with(loader, slot: int) -> None:
            try:
                claim_connection = self._connection(loader)
                try:
                    barrier.wait(timeout=5)
                    manifest = loader.claim_next_segment_manifest(claim_connection, "run-a", manifests)
                    results.append((slot, None if manifest is None else str(manifest["segment_id"])))
                finally:
                    claim_connection.close()
            except BaseException as exc:  # pragma: no cover - surfaced by assertion below
                errors.append(exc)

        threads = [
            threading.Thread(target=claim_with, args=(loader_slot1, 1)),
            threading.Thread(target=claim_with, args=(loader_slot2, 2)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        if errors:
            raise errors[0]
        self.assertEqual(len(results), 2)
        self.assertEqual({segment_id for _, segment_id in results}, {"seg-000010", "seg-000011"})
        check_connection = self._connection(loader_slot1)
        try:
            statuses = check_connection.execute(
                """
                SELECT segment_id, status, claim_token
                FROM loaded_segments
                WHERE run_id = ?
                ORDER BY segment_id
                """,
                ("run-a",),
            ).fetchall()
            self.assertEqual([row[1] for row in statuses], ["claimed", "claimed"])
            self.assertIn(":slot-1:", str(statuses[0][2]) + str(statuses[1][2]))
            self.assertIn(":slot-2:", str(statuses[0][2]) + str(statuses[1][2]))
        finally:
            check_connection.close()


if __name__ == "__main__":
    unittest.main()
