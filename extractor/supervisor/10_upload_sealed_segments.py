#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STATUS_SEALED = "sealed"
STATUS_UPLOADED = "uploaded"
CHECKSUMS_RELATIVE_PATH = Path("checksums") / "SHA256SUMS"
RUN_MANIFEST_RELATIVE_PATH = Path("manifests") / "run.json"
RUNTIME_MANIFEST_RELATIVE_PATH = Path("manifests") / "runtime.json"
CHECKPOINT_RELATIVE_PATH = Path("checkpoints") / "extraction.json"


@dataclass
class SegmentRow:
    segment_id: str
    run_id: str
    segment_seq: int
    file_path: Path
    first_block: int | None
    last_block: int | None
    first_tx_hash: str | None
    last_tx_hash: str | None
    byte_count: int | None
    sha256: str | None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_prefix(prefix_root: str) -> str:
    return prefix_root.rstrip("/")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    temp_path.replace(path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_upload_state_schema(connection: sqlite3.Connection, schema_path: Path) -> None:
    connection.executescript(schema_path.read_text(encoding="utf-8"))


def segment_row_from_tuple(row: tuple[Any, ...]) -> SegmentRow:
    return SegmentRow(
        segment_id=row[0],
        run_id=row[1],
        segment_seq=row[2],
        file_path=Path(row[3]),
        first_block=row[4],
        last_block=row[5],
        first_tx_hash=row[6],
        last_tx_hash=row[7],
        byte_count=row[8],
        sha256=row[9],
    )


def run_root_for_segment(segment_path: Path) -> Path:
    return segment_path.parent.parent


def workspace_root_for_db(db_path: Path) -> Path:
    return db_path.resolve().parent.parent


def raw_runs_root_for_db(db_path: Path) -> Path:
    return workspace_root_for_db(db_path) / "raw" / "runs"


def find_manifest_for_segment(segment_path: Path) -> Path:
    manifests_dir = run_root_for_segment(segment_path) / "manifests"
    for candidate in sorted(manifests_dir.glob("*.manifest.json")):
        payload = load_json(candidate)
        if payload.get("local_path") == str(segment_path):
            return candidate
    raise FileNotFoundError(f"no manifest found for segment {segment_path}")


def list_segment_manifests(run_root: Path) -> list[tuple[Path, dict[str, Any]]]:
    manifests: list[tuple[Path, dict[str, Any]]] = []
    manifests_dir = run_root / "manifests"
    for path in sorted(manifests_dir.glob("*.manifest.json")):
        manifests.append((path, load_json(path)))
    return sorted(manifests, key=lambda item: int(item[1]["segment_seq"]))


def discover_run_roots(db_path: Path, run_id: str | None = None) -> list[Path]:
    runs_root = raw_runs_root_for_db(db_path)
    if run_id:
        candidate = runs_root / run_id
        return [candidate] if candidate.exists() else []
    if not runs_root.exists():
        return []
    return sorted(path for path in runs_root.iterdir() if path.is_dir())


def build_run_prefix(prefix_root: str, run_id: str) -> str:
    return f"{normalize_prefix(prefix_root)}/runs/{run_id}"


def build_remote_keys(prefix_root: str, run_id: str, segment_path: Path, manifest_path: Path) -> dict[str, str]:
    run_prefix = build_run_prefix(prefix_root, run_id)
    return {
        "run_prefix": run_prefix,
        "segment_key": f"{run_prefix}/segments/{segment_path.name}",
        "segment_manifest_key": f"{run_prefix}/manifests/segments/{manifest_path.name}",
        "run_manifest_key": f"{run_prefix}/{RUN_MANIFEST_RELATIVE_PATH.as_posix()}",
        "runtime_manifest_key": f"{run_prefix}/{RUNTIME_MANIFEST_RELATIVE_PATH.as_posix()}",
        "checkpoint_key": f"{run_prefix}/{CHECKPOINT_RELATIVE_PATH.as_posix()}",
        "checksums_key": f"{run_prefix}/{CHECKSUMS_RELATIVE_PATH.as_posix()}",
    }


def resolve_required_manifest_value(explicit_value: str | None, existing_value: Any, field_name: str) -> str:
    value = explicit_value if explicit_value not in (None, "") else existing_value
    normalized = None if value is None else str(value).strip()
    if normalized in (None, "", "__UNSET__", "__FILL__", "__FILL_AFTER_RENDER__", "__FILL_AFTER_BUILD__"):
        raise RuntimeError(f"{field_name} is required for runtime/run manifest generation")
    return normalized


def write_runtime_manifest(
    run_root: Path,
    run_id: str,
    bucket: str,
    prefix_root: str,
    region: str | None,
    java_tron_version: str | None,
    config_sha256: str | None,
    plugin_build_id: str | None,
) -> Path:
    runtime_manifest_path = run_root / RUNTIME_MANIFEST_RELATIVE_PATH
    existing_payload = load_json(runtime_manifest_path) if runtime_manifest_path.exists() else {}
    payload = {
        "manifest_version": 1,
        "kind": "runtime_manifest",
        "run_id": run_id,
        "java_tron_version": resolve_required_manifest_value(
            java_tron_version, existing_payload.get("java_tron_version"), "java_tron_version"
        ),
        "event_framework_version": "v2",
        "config_sha256": resolve_required_manifest_value(
            config_sha256, existing_payload.get("config_sha256"), "config_sha256"
        ),
        "plugin_type": "custom-file-sink",
        "plugin_build_id": resolve_required_manifest_value(
            plugin_build_id, existing_payload.get("plugin_build_id"), "plugin_build_id"
        ),
        "sink_codec": existing_payload.get("sink_codec", "ndjson.gz"),
        "segment_target_bytes": int(existing_payload.get("segment_target_bytes", 268435456)),
        "s3_bucket": bucket,
        "s3_prefix_root": normalize_prefix(prefix_root),
        "extractor_region": resolve_required_manifest_value(
            region, existing_payload.get("extractor_region"), "extractor_region"
        ),
        "created_at_utc": existing_payload.get("created_at_utc", utc_now_iso()),
    }
    write_json(runtime_manifest_path, payload)
    return runtime_manifest_path


def write_run_manifest(run_root: Path, run_id: str, bucket: str, prefix_root: str, resolved_end_block: int) -> Path:
    segment_manifests = list_segment_manifests(run_root)
    if not segment_manifests:
        raise RuntimeError(f"no segment manifests found under {run_root}")
    first_manifest = segment_manifests[0][1]
    last_manifest = segment_manifests[-1][1]
    if resolved_end_block < int(last_manifest["block_to"]):
        raise RuntimeError("resolved_end_block must be >= the last uploaded segment boundary")
    run_manifest_path = run_root / RUN_MANIFEST_RELATIVE_PATH
    created_at = load_json(run_manifest_path).get("created_at_utc", utc_now_iso()) if run_manifest_path.exists() else utc_now_iso()
    run_prefix = build_run_prefix(prefix_root, run_id)
    payload = {
        "manifest_version": 1,
        "kind": "run_manifest",
        "run_id": run_id,
        "stream_name": first_manifest["stream_name"],
        "contract_address": first_manifest["contract_address"],
        "topic0": first_manifest["topic0"],
        "start_block": first_manifest["block_from"],
        "end_policy": "bounded_at_run_start",
        "resolved_end_block": resolved_end_block,
        "segment_count": len(segment_manifests),
        "segments_prefix": f"{run_prefix}/segments/",
        "runtime_manifest_s3_key": f"{run_prefix}/{RUNTIME_MANIFEST_RELATIVE_PATH.as_posix()}",
        "created_at_utc": created_at,
        "status": STATUS_UPLOADED if all(item[1]["status"] == STATUS_UPLOADED for item in segment_manifests) else "open",
    }
    write_json(run_manifest_path, payload)
    return run_manifest_path


def write_checksums(run_root: Path) -> Path:
    checksum_path = run_root / CHECKSUMS_RELATIVE_PATH
    checksum_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for relative_path in sorted(
        [
            path.relative_to(run_root)
            for path in run_root.rglob("*")
            if path.is_file()
            and ".partial" not in path.name
            and ".orphaned." not in path.name
            and path.relative_to(run_root).as_posix() != CHECKSUMS_RELATIVE_PATH.as_posix()
        ]
    ):
        lines.append(f"{sha256_file(run_root / relative_path)}  {relative_path.as_posix()}")
    checksum_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return checksum_path


def write_extraction_checkpoint(connection: sqlite3.Connection, run_root: Path, run_id: str) -> Path:
    row = connection.execute(
        """
        SELECT segment_id, last_block
        FROM segments
        WHERE run_id = ? AND status IN ('uploaded', 'validated', 'loaded')
        ORDER BY segment_seq DESC
        LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    checkpoint_path = run_root / CHECKPOINT_RELATIVE_PATH
    existing_payload = load_json(checkpoint_path) if checkpoint_path.exists() else {}
    payload = {
        "run_id": run_id,
        "last_uploaded_segment_id": row[0] if row else None,
        "last_uploaded_block_number": row[1] if row else None,
        "next_start_block_number": (row[1] + 1) if row and row[1] is not None else None,
    }
    if (
        existing_payload.get("run_id") == payload["run_id"]
        and existing_payload.get("last_uploaded_segment_id") == payload["last_uploaded_segment_id"]
        and existing_payload.get("last_uploaded_block_number") == payload["last_uploaded_block_number"]
        and existing_payload.get("next_start_block_number") == payload["next_start_block_number"]
    ):
        payload["updated_at"] = existing_payload.get("updated_at", utc_now_iso())
    else:
        payload["updated_at"] = utc_now_iso()
    write_json(checkpoint_path, payload)
    return checkpoint_path


class Boto3S3Client:
    def __init__(self, region_name: str | None = None) -> None:
        import boto3
        from botocore.exceptions import ClientError
        self._client = boto3.client("s3", region_name=region_name)
        self._client_error = ClientError

    def head_object(self, bucket: str, key: str) -> dict[str, Any]:
        try:
            return self._client.head_object(Bucket=bucket, Key=key)
        except self._client_error as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(f"s3://{bucket}/{key}") from exc
            raise

    def put_object(
        self,
        bucket: str,
        key: str,
        local_path: Path,
        metadata: dict[str, str],
        sse_mode: str | None = None,
        kms_key_arn: str | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": local_path.read_bytes(), "Metadata": metadata}
        if sse_mode:
            kwargs["ServerSideEncryption"] = sse_mode
        if kms_key_arn:
            kwargs["SSEKMSKeyId"] = kms_key_arn
        if content_type:
            kwargs["ContentType"] = content_type
        self._client.put_object(**kwargs)
        return self.head_object(bucket, key)


def head_matches_local(head: dict[str, Any], local_size: int, sha256_hex: str) -> bool:
    metadata = {str(key).lower(): str(value) for key, value in head.get("Metadata", {}).items()}
    return int(head.get("ContentLength", -1)) == local_size and metadata.get("sha256") == sha256_hex


def strip_etag(etag: Any) -> str | None:
    return None if etag is None else str(etag).strip('"')


def put_object_idempotent(
    s3_client: Any,
    bucket: str,
    key: str,
    local_path: Path,
    sha256_hex: str,
    *,
    allow_overwrite_on_mismatch: bool = False,
    sse_mode: str | None = None,
    kms_key_arn: str | None = None,
    content_type: str | None = None,
) -> tuple[str | None, bool]:
    local_size = local_path.stat().st_size
    try:
        head = s3_client.head_object(bucket, key)
        if not head_matches_local(head, local_size, sha256_hex):
            if allow_overwrite_on_mismatch:
                head = s3_client.put_object(
                    bucket=bucket,
                    key=key,
                    local_path=local_path,
                    metadata={"sha256": sha256_hex, "file_size_bytes": str(local_size)},
                    sse_mode=sse_mode,
                    kms_key_arn=kms_key_arn,
                    content_type=content_type,
                )
                return strip_etag(head.get("ETag")), True
            raise RuntimeError(f"remote object mismatch for s3://{bucket}/{key}")
        return strip_etag(head.get("ETag")), False
    except FileNotFoundError:
        head = s3_client.put_object(
            bucket=bucket,
            key=key,
            local_path=local_path,
            metadata={"sha256": sha256_hex, "file_size_bytes": str(local_size)},
            sse_mode=sse_mode,
            kms_key_arn=kms_key_arn,
            content_type=content_type,
        )
        return strip_etag(head.get("ETag")), True


def update_upload_state_attempt(connection: sqlite3.Connection, segment_id: str, bucket: str, s3_key: str, manifest_s3_key: str) -> None:
    connection.execute(
        """
        INSERT INTO segment_upload_state (segment_id, s3_bucket, s3_key, manifest_s3_key, upload_attempts, last_upload_error)
        VALUES (?, ?, ?, ?, 1, NULL)
        ON CONFLICT(segment_id) DO UPDATE SET
            s3_bucket = excluded.s3_bucket,
            s3_key = excluded.s3_key,
            manifest_s3_key = excluded.manifest_s3_key,
            upload_attempts = segment_upload_state.upload_attempts + 1,
            last_upload_error = NULL
        """,
        (segment_id, bucket, s3_key, manifest_s3_key),
    )


def update_upload_state_success(connection: sqlite3.Connection, segment_id: str, etag: str | None) -> None:
    connection.execute(
        "UPDATE segment_upload_state SET uploaded_at = ?, etag = ?, last_upload_error = NULL WHERE segment_id = ?",
        (utc_now_iso(), etag, segment_id),
    )


def update_upload_state_error(connection: sqlite3.Connection, segment_id: str, error_text: str) -> None:
    connection.execute("UPDATE segment_upload_state SET last_upload_error = ? WHERE segment_id = ?", (error_text, segment_id))


def mark_segment_uploaded(connection: sqlite3.Connection, segment_id: str) -> None:
    connection.execute("UPDATE segments SET status = ? WHERE segment_id = ?", (STATUS_UPLOADED, segment_id))


def select_sealed_segments(connection: sqlite3.Connection, run_id: str | None = None) -> list[SegmentRow]:
    query = """
        SELECT segment_id, run_id, segment_seq, file_path, first_block, last_block, first_tx_hash, last_tx_hash, byte_count, sha256
        FROM segments
        WHERE status = ?
    """
    params: list[Any] = [STATUS_SEALED]
    if run_id:
        query += " AND run_id = ?"
        params.append(run_id)
    query += " ORDER BY run_id, segment_seq"
    return [segment_row_from_tuple(row) for row in connection.execute(query, params).fetchall()]


def reconcile_local_manifests(connection: sqlite3.Connection, db_path: Path, run_id: str | None = None) -> None:
    for run_root in discover_run_roots(db_path, run_id=run_id):
        manifest_entries = list_segment_manifests(run_root)
        if not manifest_entries:
            continue

        current_run_id = str(manifest_entries[0][1]["run_id"])
        start_block = manifest_entries[0][1].get("block_from")
        end_block = manifest_entries[-1][1].get("block_to")

        connection.execute(
            """
            INSERT INTO runs (run_id, run_type, status, start_block, end_block, extractor_host)
            VALUES (?, 'extract', 'running', ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                start_block = COALESCE(runs.start_block, excluded.start_block),
                end_block = COALESCE(runs.end_block, excluded.end_block),
                extractor_host = COALESCE(runs.extractor_host, excluded.extractor_host),
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                current_run_id,
                start_block,
                end_block,
                manifest_entries[0][1].get("extractor_instance_id"),
            ),
        )

        for _, manifest in manifest_entries:
            segment_status = str(manifest.get("status") or STATUS_SEALED)
            connection.execute(
                """
                INSERT INTO segments (
                    segment_id,
                    run_id,
                    segment_seq,
                    file_path,
                    codec,
                    status,
                    first_block,
                    last_block,
                    first_tx_hash,
                    last_tx_hash,
                    first_event_hint,
                    last_event_hint,
                    row_count,
                    byte_count,
                    sha256,
                    opened_at,
                    closed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(segment_id) DO UPDATE SET
                    file_path = excluded.file_path,
                    codec = excluded.codec,
                    status = CASE
                        WHEN segments.status IN ('validated', 'loaded', 'failed', 'quarantined') THEN segments.status
                        ELSE excluded.status
                    END,
                    first_block = excluded.first_block,
                    last_block = excluded.last_block,
                    first_tx_hash = excluded.first_tx_hash,
                    last_tx_hash = excluded.last_tx_hash,
                    first_event_hint = excluded.first_event_hint,
                    last_event_hint = excluded.last_event_hint,
                    row_count = excluded.row_count,
                    byte_count = excluded.byte_count,
                    sha256 = excluded.sha256,
                    opened_at = excluded.opened_at,
                    closed_at = excluded.closed_at
                """,
                (
                    manifest["segment_id"],
                    current_run_id,
                    int(manifest["segment_seq"]),
                    manifest["local_path"],
                    manifest.get("codec", "ndjson.gz"),
                    segment_status,
                    manifest.get("block_from"),
                    manifest.get("block_to"),
                    manifest.get("first_tx_hash"),
                    manifest.get("last_tx_hash"),
                    manifest.get("first_event_key"),
                    manifest.get("last_event_key"),
                    int(manifest.get("record_count", 0)),
                    manifest.get("file_size_bytes"),
                    manifest.get("sha256"),
                    manifest.get("created_at_utc"),
                    manifest.get("closed_at_utc"),
                ),
            )


def upload_sealed_segments(
    db_path: Path,
    schema_path: Path,
    bucket: str,
    prefix_root: str,
    run_id: str | None = None,
    s3_client: Any | None = None,
    sse_mode: str | None = None,
    kms_key_arn: str | None = None,
    region: str | None = None,
    java_tron_version: str | None = None,
    config_sha256: str | None = None,
    plugin_build_id: str | None = None,
    resolved_end_block: int | None = None,
) -> list[dict[str, Any]]:
    client = s3_client or Boto3S3Client(region_name=region)
    connection = sqlite3.connect(db_path)
    try:
        ensure_upload_state_schema(connection, schema_path)
        reconcile_local_manifests(connection, db_path, run_id=run_id)
        connection.commit()
        segments = select_sealed_segments(connection, run_id=run_id)
        uploaded: list[dict[str, Any]] = []
        touched_runs: dict[str, Path] = {}
        for segment in segments:
            segment_path = segment.file_path
            manifest_path = find_manifest_for_segment(segment_path)
            manifest = load_json(manifest_path)
            keys = build_remote_keys(prefix_root, segment.run_id, segment_path, manifest_path)
            touched_runs[segment.run_id] = run_root_for_segment(segment_path)
            segment_sha256 = manifest.get("sha256") or segment.sha256 or sha256_file(segment_path)
            manifest["sha256"] = segment_sha256
            manifest["file_size_bytes"] = segment_path.stat().st_size
            manifest["s3_bucket"] = bucket
            manifest["s3_key"] = keys["segment_key"]
            update_upload_state_attempt(connection, segment.segment_id, bucket, keys["segment_key"], keys["segment_manifest_key"])
            connection.commit()
            try:
                etag, _ = put_object_idempotent(
                    s3_client=client,
                    bucket=bucket,
                    key=keys["segment_key"],
                    local_path=segment_path,
                    sha256_hex=segment_sha256,
                    sse_mode=sse_mode,
                    kms_key_arn=kms_key_arn,
                    content_type="application/gzip" if segment_path.suffix == ".gz" else "application/x-ndjson",
                )
                manifest["status"] = STATUS_UPLOADED
                write_json(manifest_path, manifest)
                put_object_idempotent(
                    s3_client=client,
                    bucket=bucket,
                    key=keys["segment_manifest_key"],
                    local_path=manifest_path,
                    sha256_hex=sha256_file(manifest_path),
                    sse_mode=sse_mode,
                    kms_key_arn=kms_key_arn,
                    content_type="application/json",
                )
                update_upload_state_success(connection, segment.segment_id, etag)
                mark_segment_uploaded(connection, segment.segment_id)
                connection.commit()
            except Exception as exc:
                update_upload_state_error(connection, segment.segment_id, str(exc))
                connection.commit()
                raise
            uploaded.append(
                {
                    "segment_id": segment.segment_id,
                    "run_id": segment.run_id,
                    "segment_s3_key": keys["segment_key"],
                    "manifest_s3_key": keys["segment_manifest_key"],
                    "etag": etag,
                }
            )
        for current_run_id, run_root in touched_runs.items():
            if resolved_end_block is None:
                raise RuntimeError("resolved_end_block is required for uploader run manifest generation")
            runtime_manifest_path = write_runtime_manifest(
                run_root,
                current_run_id,
                bucket,
                prefix_root,
                region,
                java_tron_version=java_tron_version,
                config_sha256=config_sha256,
                plugin_build_id=plugin_build_id,
            )
            run_manifest_path = write_run_manifest(
                run_root,
                current_run_id,
                bucket,
                prefix_root,
                resolved_end_block=resolved_end_block,
            )
            checkpoint_path = write_extraction_checkpoint(connection, run_root, current_run_id)
            checksums_path = write_checksums(run_root)
            first_segment = next(iter(sorted((run_root / "segments").glob("*"))))
            first_manifest = next(iter(sorted((run_root / "manifests").glob("*.manifest.json"))))
            sidecar_keys = build_remote_keys(prefix_root, current_run_id, first_segment, first_manifest)
            for local_path, remote_key, content_type in (
                (runtime_manifest_path, sidecar_keys["runtime_manifest_key"], "application/json"),
                (run_manifest_path, sidecar_keys["run_manifest_key"], "application/json"),
                (checkpoint_path, sidecar_keys["checkpoint_key"], "application/json"),
                (checksums_path, sidecar_keys["checksums_key"], "text/plain"),
            ):
                put_object_idempotent(
                    s3_client=client,
                    bucket=bucket,
                    key=remote_key,
                    local_path=local_path,
                    sha256_hex=sha256_file(local_path),
                    allow_overwrite_on_mismatch=True,
                    sse_mode=sse_mode,
                    kms_key_arn=kms_key_arn,
                    content_type=content_type,
                )
        return uploaded
    finally:
        connection.close()


def verify_uploaded_segments(
    db_path: Path,
    schema_path: Path,
    bucket: str,
    prefix_root: str,
    run_id: str | None = None,
    s3_client: Any | None = None,
    region: str | None = None,
) -> dict[str, Any]:
    client = s3_client or Boto3S3Client(region_name=region)
    connection = sqlite3.connect(db_path)
    try:
        ensure_upload_state_schema(connection, schema_path)
        rows = connection.execute(
            """
            SELECT s.segment_id, s.run_id, s.file_path, u.s3_key, u.manifest_s3_key
            FROM segments s
            JOIN segment_upload_state u ON u.segment_id = s.segment_id
            WHERE s.status = 'uploaded'
            """
            + (" AND s.run_id = ?" if run_id else "")
            + " ORDER BY s.run_id, s.segment_seq",
            ((run_id,) if run_id else ()),
        ).fetchall()
        verified_runs: dict[str, Path] = {}
        for segment_id, current_run_id, file_path, s3_key, manifest_s3_key in rows:
            segment_path = Path(file_path)
            manifest_path = find_manifest_for_segment(segment_path)
            if not head_matches_local(client.head_object(bucket, s3_key), segment_path.stat().st_size, sha256_file(segment_path)):
                raise RuntimeError(f"segment mismatch for {segment_id}")
            if not head_matches_local(client.head_object(bucket, manifest_s3_key), manifest_path.stat().st_size, sha256_file(manifest_path)):
                raise RuntimeError(f"manifest mismatch for {segment_id}")
            verified_runs[current_run_id] = run_root_for_segment(segment_path)
            connection.execute("UPDATE segment_upload_state SET last_verified_at = ? WHERE segment_id = ?", (utc_now_iso(), segment_id))
        for current_run_id, run_root in verified_runs.items():
            first_segment = next(iter(sorted((run_root / "segments").glob("*"))))
            first_manifest = next(iter(sorted((run_root / "manifests").glob("*.manifest.json"))))
            sidecar_keys = build_remote_keys(prefix_root, current_run_id, first_segment, first_manifest)
            client.head_object(bucket, sidecar_keys["run_manifest_key"])
            client.head_object(bucket, sidecar_keys["runtime_manifest_key"])
            client.head_object(bucket, sidecar_keys["checkpoint_key"])
            client.head_object(bucket, sidecar_keys["checksums_key"])
        connection.commit()
        return {"uploaded_segments": len(rows), "verified_runs": sorted(verified_runs.keys())}
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload sealed local segments into the frozen S3 buffer prefix.")
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--schema-path", required=True, type=Path)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix-root", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--region")
    parser.add_argument("--sse-mode")
    parser.add_argument("--kms-key-arn")
    parser.add_argument("--java-tron-version")
    parser.add_argument("--config-sha256")
    parser.add_argument("--plugin-build-id")
    parser.add_argument("--resolved-end-block", type=int)
    parser.add_argument("--verify", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result: dict[str, Any] = {
        "uploaded_segments": upload_sealed_segments(
            db_path=args.db_path,
            schema_path=args.schema_path,
            bucket=args.bucket,
            prefix_root=args.prefix_root,
            run_id=args.run_id,
            sse_mode=args.sse_mode,
            kms_key_arn=args.kms_key_arn,
            region=args.region,
            java_tron_version=args.java_tron_version,
            config_sha256=args.config_sha256,
            plugin_build_id=args.plugin_build_id,
            resolved_end_block=args.resolved_end_block,
        )
    }
    if args.verify:
        result["verification"] = verify_uploaded_segments(
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
