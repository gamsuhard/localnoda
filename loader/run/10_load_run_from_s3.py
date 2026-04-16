#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loader.normalizer.tron_usdt_transfer_normalizer import normalize_records


CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "tron_usdt_local")
LOADER_CONCURRENCY = int(os.environ.get("LOADER_CONCURRENCY", "1"))
LOADER_RECORD_BATCH_SIZE = int(os.environ.get("LOADER_RECORD_BATCH_SIZE", "1000"))
RUNTIME_LOCK_NAME = f"{CLICKHOUSE_DATABASE}:single-worker"

EVENTS_STAGE_TABLE = f"{CLICKHOUSE_DATABASE}.trc20_transfer_events_staging"
EVENTS_TABLE = f"{CLICKHOUSE_DATABASE}.trc20_transfer_events"
LEGS_STAGE_TABLE = f"{CLICKHOUSE_DATABASE}.address_transfer_legs_staging"
LEGS_TABLE = f"{CLICKHOUSE_DATABASE}.address_transfer_legs"
AUDIT_TABLE = f"{CLICKHOUSE_DATABASE}.load_audit"

SEGMENT_TERMINAL_STATUSES = {"validated", "failed", "quarantined", "skipped"}


@dataclass
class SegmentMetrics:
    bytes_read: int = 0
    record_count: int = 0
    event_rows_expected: int = 0
    leg_rows_expected: int = 0
    s3_read_ms: int = 0
    normalize_ms: int = 0
    stage_ms: int = 0
    merge_ms: int = 0
    audit_ms: int = 0
    validation_ms: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "bytes_read": self.bytes_read,
            "record_count": self.record_count,
            "event_rows_expected": self.event_rows_expected,
            "leg_rows_expected": self.leg_rows_expected,
            "s3_read_ms": self.s3_read_ms,
            "normalize_ms": self.normalize_ms,
            "stage_ms": self.stage_ms,
            "merge_ms": self.merge_ms,
            "audit_ms": self.audit_ms,
            "validation_ms": self.validation_ms,
        }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def elapsed_ms(started_at: float) -> int:
    return int((time.perf_counter() - started_at) * 1000)


def sql_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def load_json_bytes(payload: bytes) -> dict[str, Any]:
    return json.loads(payload.decode("utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_loader_state_schema(connection: sqlite3.Connection, schema_path: Path) -> None:
    connection.executescript(schema_path.read_text(encoding="utf-8"))


class Boto3S3BufferClient:
    def __init__(self, region_name: str | None = None) -> None:
        import boto3

        self._client = boto3.client("s3", region_name=region_name)

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                keys.append(item["Key"])
        return keys

    def get_json(self, bucket: str, key: str) -> dict[str, Any]:
        return load_json_bytes(self._client.get_object(Bucket=bucket, Key=key)["Body"].read())

    def download_to_file(self, bucket: str, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("wb") as handle:
            self._client.download_fileobj(bucket, key, handle)


class ClickHouseClientTarget:
    def __init__(self, host: str, port: int, user: str, password: str, secure: bool) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.secure = secure

    def _base_args(self) -> list[str]:
        args = [
            "clickhouse-client",
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--user",
            self.user,
        ]
        if self.password:
            args.extend(["--password", self.password])
        if self.secure:
            args.append("--secure")
        return args

    def _run_query(self, query: str, input_text: str | None = None) -> str:
        args = self._base_args() + ["--query", query]
        completed = subprocess.run(args, input=input_text, text=True, capture_output=True, check=True)
        return completed.stdout.strip()

    def insert_json_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        payload = "\n".join(json.dumps(row, ensure_ascii=True) for row in rows) + "\n"
        self._run_query(f"INSERT INTO {table_name} FORMAT JSONEachRow", input_text=payload)

    def count_rows(self, table_name: str, where_clause: str) -> int:
        result = self._run_query(f"SELECT count() FROM {table_name} WHERE {where_clause}")
        return int(result or "0")

    def reset_stage_tables(self) -> None:
        self._run_query(f"TRUNCATE TABLE {EVENTS_STAGE_TABLE}")
        self._run_query(f"TRUNCATE TABLE {LEGS_STAGE_TABLE}")

    def append_stage_rows(self, event_rows: list[dict[str, Any]], leg_rows: list[dict[str, Any]]) -> None:
        self.insert_json_rows(EVENTS_STAGE_TABLE, event_rows)
        self.insert_json_rows(LEGS_STAGE_TABLE, leg_rows)

    def merge_segment(self, run_id: str, segment_id: str) -> dict[str, int]:
        where_clause = f"load_run_id = {sql_quote(run_id)} AND source_segment_id = {sql_quote(segment_id)}"
        before_events = self.count_rows(EVENTS_TABLE, where_clause)
        before_legs = self.count_rows(LEGS_TABLE, where_clause)
        self._run_query(
            f"""
            INSERT INTO {EVENTS_TABLE}
            SELECT *
            FROM {EVENTS_STAGE_TABLE} s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {EVENTS_TABLE} c
                WHERE c.event_id = s.event_id
            )
            """
        )
        self._run_query(
            f"""
            INSERT INTO {LEGS_TABLE}
            SELECT *
            FROM {LEGS_STAGE_TABLE} s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {LEGS_TABLE} c
                WHERE c.leg_id = s.leg_id
            )
            """
        )
        after_events = self.count_rows(EVENTS_TABLE, where_clause)
        after_legs = self.count_rows(LEGS_TABLE, where_clause)
        self.reset_stage_tables()
        return {
            "events_inserted": after_events - before_events,
            "legs_inserted": after_legs - before_legs,
        }

    def insert_load_audit(self, rows: list[dict[str, Any]]) -> None:
        self.insert_json_rows(AUDIT_TABLE, rows)


def iter_segment_records(local_path: Path, codec: str) -> Iterator[dict[str, Any]]:
    if codec == "ndjson.gz":
        opener = gzip.open(local_path, "rt", encoding="utf-8")
    else:
        opener = local_path.open("rt", encoding="utf-8")
    with opener as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def dedupe_rows(rows: list[dict[str, Any]], key_field: str, seen: set[str]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    for row in rows:
        key = str(row[key_field])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def iter_record_batches(local_path: Path, codec: str, batch_size: int) -> Iterator[list[dict[str, Any]]]:
    records_batch: list[dict[str, Any]] = []
    for record in iter_segment_records(local_path, codec):
        records_batch.append(record)
        if len(records_batch) < batch_size:
            continue
        yield records_batch
        records_batch = []
    if records_batch:
        yield records_batch


def enforce_single_worker_mode() -> None:
    if LOADER_CONCURRENCY != 1:
        raise RuntimeError("LOADER_CONCURRENCY must be 1 while staging tables are global and single-worker only")
    if LOADER_RECORD_BATCH_SIZE <= 0:
        raise RuntimeError("LOADER_RECORD_BATCH_SIZE must be > 0")


def upsert_loader_run(
    connection: sqlite3.Connection,
    run_id: str,
    bucket: str,
    prefix_root: str,
    status: str,
    note: str = "",
) -> None:
    connection.execute(
        """
        INSERT INTO loader_runs (run_id, s3_bucket, s3_prefix_root, clickhouse_database, status, started_at, note)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            s3_bucket = excluded.s3_bucket,
            s3_prefix_root = excluded.s3_prefix_root,
            clickhouse_database = excluded.clickhouse_database,
            status = excluded.status,
            note = excluded.note
        """,
        (run_id, bucket, prefix_root, CLICKHOUSE_DATABASE, status, note),
    )


def acquire_runtime_lock(connection: sqlite3.Connection, run_id: str) -> str:
    owner_id = f"{run_id}:{os.getpid()}"
    connection.execute("DELETE FROM loader_runtime_lock WHERE lock_name = ? AND released_at IS NOT NULL", (RUNTIME_LOCK_NAME,))
    existing = connection.execute(
        "SELECT owner_id FROM loader_runtime_lock WHERE lock_name = ? AND released_at IS NULL",
        (RUNTIME_LOCK_NAME,),
    ).fetchone()
    if existing and existing[0] != owner_id:
        raise RuntimeError(f"loader runtime lock is already held by {existing[0]}")
    if not existing:
        connection.execute(
            """
            INSERT INTO loader_runtime_lock (lock_name, owner_id, clickhouse_database, acquired_at, released_at)
            VALUES (?, ?, ?, ?, NULL)
            """,
            (RUNTIME_LOCK_NAME, owner_id, CLICKHOUSE_DATABASE, utc_now_iso()),
        )
    return owner_id


def release_runtime_lock(connection: sqlite3.Connection, owner_id: str) -> None:
    connection.execute(
        "UPDATE loader_runtime_lock SET released_at = ? WHERE lock_name = ? AND owner_id = ?",
        (utc_now_iso(), RUNTIME_LOCK_NAME, owner_id),
    )


def sync_segment_work_items(connection: sqlite3.Connection, run_id: str, segment_manifests: list[dict[str, Any]]) -> None:
    for manifest in segment_manifests:
        connection.execute(
            """
            INSERT INTO loaded_segments (
                run_id,
                segment_id,
                source_s3_key,
                source_sha256,
                status
            ) VALUES (?, ?, ?, ?, 'pending')
            ON CONFLICT(run_id, segment_id) DO UPDATE SET
                source_s3_key = excluded.source_s3_key,
                source_sha256 = excluded.source_sha256,
                status = CASE
                    WHEN loaded_segments.source_sha256 != excluded.source_sha256 THEN 'pending'
                    ELSE loaded_segments.status
                END,
                last_error = CASE
                    WHEN loaded_segments.source_sha256 != excluded.source_sha256 THEN NULL
                    ELSE loaded_segments.last_error
                END
            """,
            (run_id, manifest["segment_id"], manifest["s3_key"], manifest["sha256"]),
        )


def get_segment_status(connection: sqlite3.Connection, run_id: str, segment_id: str) -> tuple[str, str] | None:
    row = connection.execute(
        """
        SELECT source_sha256, status
        FROM loaded_segments
        WHERE run_id = ? AND segment_id = ?
        """,
        (run_id, segment_id),
    ).fetchone()
    return None if row is None else (str(row[0]), str(row[1]))


def should_skip_segment(connection: sqlite3.Connection, run_id: str, segment_manifest: dict[str, Any], force_replay: bool) -> bool:
    if force_replay:
        return False
    state = get_segment_status(connection, run_id, segment_manifest["segment_id"])
    if state is None:
        return False
    source_sha256, status = state
    return source_sha256 == segment_manifest["sha256"] and status in {"validated", "skipped"}


def mark_segment_claimed(connection: sqlite3.Connection, run_id: str, segment_manifest: dict[str, Any], claim_token: str) -> None:
    connection.execute(
        """
        UPDATE loaded_segments
        SET status = 'claimed',
            claim_token = ?,
            attempts = attempts + 1,
            claimed_at = ?,
            last_error = NULL
        WHERE run_id = ? AND segment_id = ?
        """,
        (claim_token, utc_now_iso(), run_id, segment_manifest["segment_id"]),
    )


def mark_segment_loading(connection: sqlite3.Connection, run_id: str, segment_manifest: dict[str, Any]) -> None:
    connection.execute(
        """
        UPDATE loaded_segments
        SET status = 'loading',
            load_started_at = ?,
            last_error = NULL
        WHERE run_id = ? AND segment_id = ?
        """,
        (utc_now_iso(), run_id, segment_manifest["segment_id"]),
    )


def mark_segment_skipped(connection: sqlite3.Connection, run_id: str, segment_manifest: dict[str, Any]) -> None:
    connection.execute(
        """
        UPDATE loaded_segments
        SET status = 'skipped',
            load_finished_at = ?,
            last_error = NULL
        WHERE run_id = ? AND segment_id = ?
        """,
        (utc_now_iso(), run_id, segment_manifest["segment_id"]),
    )


def mark_segment_after_merge(
    connection: sqlite3.Connection,
    run_id: str,
    segment_manifest: dict[str, Any],
    metrics: SegmentMetrics,
) -> None:
    connection.execute(
        """
        UPDATE loaded_segments
        SET status = 'merged',
            bytes_read = ?,
            record_count = ?,
            event_rows = ?,
            leg_rows = ?,
            s3_read_ms = ?,
            normalize_ms = ?,
            stage_ms = ?,
            merge_ms = ?,
            audit_ms = ?,
            merged_at = ?,
            load_finished_at = ?,
            last_error = NULL
        WHERE run_id = ? AND segment_id = ?
        """,
        (
            metrics.bytes_read,
            metrics.record_count,
            metrics.event_rows_expected,
            metrics.leg_rows_expected,
            metrics.s3_read_ms,
            metrics.normalize_ms,
            metrics.stage_ms,
            metrics.merge_ms,
            metrics.audit_ms,
            utc_now_iso(),
            utc_now_iso(),
            run_id,
            segment_manifest["segment_id"],
        ),
    )


def mark_segment_validated(
    connection: sqlite3.Connection,
    run_id: str,
    segment_manifest: dict[str, Any],
    status: str,
    metrics: SegmentMetrics,
    last_error: str | None = None,
) -> None:
    connection.execute(
        """
        UPDATE loaded_segments
        SET status = ?,
            validation_ms = ?,
            load_finished_at = ?,
            last_error = ?
        WHERE run_id = ? AND segment_id = ?
        """,
        (status, metrics.validation_ms, utc_now_iso(), last_error, run_id, segment_manifest["segment_id"]),
    )


def mark_segment_failed(
    connection: sqlite3.Connection,
    run_id: str,
    segment_manifest: dict[str, Any],
    status: str,
    error_text: str,
) -> None:
    connection.execute(
        """
        UPDATE loaded_segments
        SET status = ?,
            load_finished_at = ?,
            last_error = ?
        WHERE run_id = ? AND segment_id = ?
        """,
        (status, utc_now_iso(), error_text, run_id, segment_manifest["segment_id"]),
    )


def build_audit_rows(
    run_id: str,
    segment_manifest: dict[str, Any],
    merge_counts: dict[str, int],
    metrics: SegmentMetrics,
) -> list[dict[str, Any]]:
    started_at = utc_now_iso()
    finished_at = utc_now_iso()
    note = json.dumps(metrics.as_dict(), separators=(",", ":"))
    return [
        {
            "load_batch_id": f"{run_id}:{segment_manifest['segment_id']}:events",
            "target_table": EVENTS_TABLE,
            "run_id": run_id,
            "segment_id": segment_manifest["segment_id"],
            "source_file": segment_manifest["s3_key"],
            "source_sha256": segment_manifest["sha256"],
            "source_row_count": segment_manifest["record_count"],
            "inserted_row_count": merge_counts["events_inserted"],
            "status": "loaded",
            "started_at": started_at,
            "finished_at": finished_at,
            "note": note,
        },
        {
            "load_batch_id": f"{run_id}:{segment_manifest['segment_id']}:legs",
            "target_table": LEGS_TABLE,
            "run_id": run_id,
            "segment_id": segment_manifest["segment_id"],
            "source_file": segment_manifest["s3_key"],
            "source_sha256": segment_manifest["sha256"],
            "source_row_count": segment_manifest["record_count"] * 2,
            "inserted_row_count": merge_counts["legs_inserted"],
            "status": "loaded",
            "started_at": started_at,
            "finished_at": finished_at,
            "note": note,
        },
    ]


def list_segment_manifest_keys(storage_client: Any, bucket: str, prefix_root: str, run_id: str) -> list[str]:
    prefix = f"{prefix_root.rstrip('/')}/runs/{run_id}/manifests/segments/"
    return sorted(storage_client.list_keys(bucket, prefix))


def default_clickhouse_target_from_env() -> ClickHouseClientTarget:
    host = os.environ.get("CLICKHOUSE_HOST", "__SET_PRIVATE_ENDPOINT_HOST__")
    port = int(os.environ.get("CLICKHOUSE_PORT", "9440"))
    user = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    secure = os.environ.get("CLICKHOUSE_SECURE", "1") == "1"
    return ClickHouseClientTarget(host=host, port=port, user=user, password=password, secure=secure)


def count_segment_rows(load_target: Any, run_id: str, segment_id: str) -> tuple[int, int]:
    where_clause = f"load_run_id = {sql_quote(run_id)} AND source_segment_id = {sql_quote(segment_id)}"
    events = load_target.count_rows(EVENTS_TABLE, where_clause)
    legs = load_target.count_rows(LEGS_TABLE, where_clause)
    return events, legs


def validate_segment_counts(load_target: Any, run_id: str, segment_manifest: dict[str, Any], metrics: SegmentMetrics) -> tuple[str, str | None]:
    actual_events, actual_legs = count_segment_rows(load_target, run_id, segment_manifest["segment_id"])
    if actual_events != metrics.event_rows_expected:
        return "quarantined", (
            f"segment {segment_manifest['segment_id']} canonical event count mismatch: "
            f"expected {metrics.event_rows_expected}, got {actual_events}"
        )
    if actual_legs != metrics.leg_rows_expected:
        return "quarantined", (
            f"segment {segment_manifest['segment_id']} canonical leg count mismatch: "
            f"expected {metrics.leg_rows_expected}, got {actual_legs}"
        )
    return "validated", None


def load_run_from_s3(
    run_id: str,
    bucket: str,
    prefix_root: str,
    loader_db_path: Path,
    loader_schema_path: Path,
    storage_client: Any | None = None,
    load_target: Any | None = None,
    region: str | None = None,
    force_replay: bool = False,
) -> dict[str, Any]:
    enforce_single_worker_mode()
    storage = storage_client or Boto3S3BufferClient(region_name=region)
    target = load_target or default_clickhouse_target_from_env()
    connection = sqlite3.connect(loader_db_path)
    lock_owner: str | None = None
    try:
        ensure_loader_state_schema(connection, loader_schema_path)
        upsert_loader_run(connection, run_id, bucket, prefix_root, "discovering")
        connection.commit()

        run_manifest_key = f"{prefix_root.rstrip('/')}/runs/{run_id}/manifests/run.json"
        run_manifest = storage.get_json(bucket, run_manifest_key)
        segment_manifest_keys = list_segment_manifest_keys(storage, bucket, prefix_root, run_id)
        segment_manifests = [storage.get_json(bucket, manifest_key) for manifest_key in segment_manifest_keys]
        sync_segment_work_items(connection, run_id, segment_manifests)
        lock_owner = acquire_runtime_lock(connection, run_id)
        upsert_loader_run(connection, run_id, bucket, prefix_root, "loading")
        connection.commit()

        summary_segments: list[dict[str, Any]] = []
        for segment_manifest in segment_manifests:
            if should_skip_segment(connection, run_id, segment_manifest, force_replay):
                mark_segment_skipped(connection, run_id, segment_manifest)
                connection.commit()
                summary_segments.append({"segment_id": segment_manifest["segment_id"], "status": "skipped"})
                continue

            claim_token = f"{segment_manifest['segment_id']}:{int(time.time() * 1000)}"
            mark_segment_claimed(connection, run_id, segment_manifest, claim_token)
            mark_segment_loading(connection, run_id, segment_manifest)
            connection.commit()

            metrics = SegmentMetrics()
            merge_counts = {"events_inserted": 0, "legs_inserted": 0}
            target.reset_stage_tables()
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    local_segment_path = Path(temp_dir) / Path(segment_manifest["s3_key"]).name
                    started = time.perf_counter()
                    storage.download_to_file(bucket, segment_manifest["s3_key"], local_segment_path)
                    metrics.s3_read_ms = elapsed_ms(started)
                    metrics.bytes_read = local_segment_path.stat().st_size

                    raw_segment_sha256 = sha256_file(local_segment_path)
                    if raw_segment_sha256 != segment_manifest["sha256"]:
                        raise RuntimeError(
                            f"segment sha256 mismatch for {segment_manifest['segment_id']}: "
                            f"expected {segment_manifest['sha256']} got {raw_segment_sha256}"
                        )

                    manifest_size = int(segment_manifest.get("file_size_bytes") or 0)
                    if manifest_size and manifest_size != metrics.bytes_read:
                        raise RuntimeError(
                            f"segment size mismatch for {segment_manifest['segment_id']}: "
                            f"expected {manifest_size} got {metrics.bytes_read}"
                        )

                    seen_event_ids: set[str] = set()
                    seen_leg_ids: set[str] = set()
                    for batch_records in iter_record_batches(local_segment_path, str(segment_manifest["codec"]), LOADER_RECORD_BATCH_SIZE):
                        metrics.record_count += len(batch_records)
                        normalize_started = time.perf_counter()
                        event_rows, leg_rows = normalize_records(batch_records, segment_manifest, run_id)
                        event_rows = dedupe_rows(event_rows, "event_id", seen_event_ids)
                        leg_rows = dedupe_rows(leg_rows, "leg_id", seen_leg_ids)
                        metrics.normalize_ms += elapsed_ms(normalize_started)
                        metrics.event_rows_expected += len(event_rows)
                        metrics.leg_rows_expected += len(leg_rows)
                        stage_started = time.perf_counter()
                        target.append_stage_rows(event_rows, leg_rows)
                        metrics.stage_ms += elapsed_ms(stage_started)

                    if metrics.record_count != int(segment_manifest["record_count"]):
                        raise RuntimeError(
                            f"segment record count mismatch for {segment_manifest['segment_id']}: "
                            f"expected {segment_manifest['record_count']} got {metrics.record_count}"
                        )

                    merge_started = time.perf_counter()
                    merge_counts = target.merge_segment(run_id, segment_manifest["segment_id"])
                    metrics.merge_ms = elapsed_ms(merge_started)

                    audit_started = time.perf_counter()
                    target.insert_load_audit(build_audit_rows(run_id, segment_manifest, merge_counts, metrics))
                    metrics.audit_ms = elapsed_ms(audit_started)

                    mark_segment_after_merge(connection, run_id, segment_manifest, metrics)

                    validation_started = time.perf_counter()
                    segment_status, error_text = validate_segment_counts(target, run_id, segment_manifest, metrics)
                    metrics.validation_ms = elapsed_ms(validation_started)
                    mark_segment_validated(connection, run_id, segment_manifest, segment_status, metrics, error_text)
                    connection.commit()

                    if segment_status == "quarantined":
                        upsert_loader_run(connection, run_id, bucket, prefix_root, "quarantined", error_text or "")
                        connection.commit()
                        raise RuntimeError(error_text or f"segment {segment_manifest['segment_id']} quarantined")

                    summary_segments.append(
                        {
                            "segment_id": segment_manifest["segment_id"],
                            "status": segment_status,
                            "events_inserted": merge_counts["events_inserted"],
                            "legs_inserted": merge_counts["legs_inserted"],
                            "metrics": metrics.as_dict(),
                        }
                    )
            except Exception as exc:
                try:
                    target.reset_stage_tables()
                except Exception:
                    pass
                mark_segment_failed(connection, run_id, segment_manifest, "failed", str(exc))
                upsert_loader_run(connection, run_id, bucket, prefix_root, "failed", str(exc))
                connection.commit()
                raise

        upsert_loader_run(connection, run_id, bucket, prefix_root, "loading")
        connection.execute("UPDATE loader_runs SET finished_at = ? WHERE run_id = ?", (utc_now_iso(), run_id))
        connection.commit()
        return {"run_id": run_id, "run_manifest": run_manifest, "segments": summary_segments}
    finally:
        if lock_owner is not None:
            release_runtime_lock(connection, lock_owner)
            connection.commit()
        connection.close()


def validate_loaded_run(run_id: str, loader_db_path: Path, loader_schema_path: Path, load_target: Any | None = None) -> dict[str, Any]:
    target = load_target or default_clickhouse_target_from_env()
    connection = sqlite3.connect(loader_db_path)
    try:
        ensure_loader_state_schema(connection, loader_schema_path)
        events = target.count_rows(EVENTS_TABLE, f"load_run_id = {sql_quote(run_id)}")
        legs = target.count_rows(LEGS_TABLE, f"load_run_id = {sql_quote(run_id)}")
        status_counts = {
            row[0]: row[1]
            for row in connection.execute(
                "SELECT status, count(*) FROM loaded_segments WHERE run_id = ? GROUP BY status",
                (run_id,),
            ).fetchall()
        }
        failing_segments = sum(status_counts.get(status, 0) for status in ("failed", "quarantined"))
        status = "ok" if legs == events * 2 and failing_segments == 0 else "fail"
        connection.execute(
            "UPDATE loader_runs SET status = ?, finished_at = ? WHERE run_id = ?",
            ("validated" if status == "ok" else "failed", utc_now_iso(), run_id),
        )
        connection.commit()
        return {
            "run_id": run_id,
            "events": events,
            "legs": legs,
            "expected_legs": events * 2,
            "segment_status_counts": status_counts,
            "status": status,
        }
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a synthetic or real TRON USDT run from S3 into staging/canonical tables.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix-root", required=True)
    parser.add_argument("--loader-db-path", required=True, type=Path)
    parser.add_argument("--loader-schema-path", required=True, type=Path)
    parser.add_argument("--region")
    parser.add_argument("--force-replay", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = load_run_from_s3(
        run_id=args.run_id,
        bucket=args.bucket,
        prefix_root=args.prefix_root,
        loader_db_path=args.loader_db_path,
        loader_schema_path=args.loader_schema_path,
        region=args.region,
        force_replay=args.force_replay,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
