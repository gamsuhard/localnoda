#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


TOPIC0 = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
CONTRACT_ADDRESS = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
DEFAULT_BUCKET = "goldusdt-v2-stage-913378704801-raw"
DEFAULT_PREFIX_ROOT = "providers/tron-usdt-backfill/usdt-transfer-oneoff"
DEFAULT_EXTRACTOR_INSTANCE_ID = "i-demo-generator"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pad_topic(hex_body: str) -> str:
    body = hex_body.lower().replace("0x", "")
    return "0x" + body.rjust(64, "0")


def deterministic_tron_address(seed: int) -> str:
    return format(seed % (1 << 160), "040x")


def build_demo_records(
    record_count: int = 3,
    start_offset: int = 0,
    base_block: int = 54300001,
    base_timestamp_ms: int = 1693526400123,
    amount_base: int = 1_250_000,
    amount_step: int = 1_250_000,
) -> list[dict[str, object]]:
    if record_count <= 0:
        raise ValueError("record_count must be > 0")
    records: list[dict[str, object]] = []
    for offset in range(record_count):
        global_offset = start_offset + offset
        block_number = base_block + offset
        from_seed = 0xA1A1A1 + global_offset
        to_seed = 0xB2B2B2 + global_offset
        records.append(
            {
                "triggerName": "solidityLogTrigger",
                "transactionId": format(global_offset + 1, "064x"),
                "blockNumber": block_number,
                "timeStamp": base_timestamp_ms + (offset * 5000),
                "uniqueId": f"{block_number}-0-0",
                "address": CONTRACT_ADDRESS,
                "topics": [
                    TOPIC0,
                    pad_topic("41" + deterministic_tron_address(from_seed)),
                    pad_topic("41" + deterministic_tron_address(to_seed)),
                ],
                "data": "0x" + format(amount_base + (offset * amount_step), "064x"),
            }
        )
    return records


def normalize_corrupt_segment_sequences(values: Iterable[int] | None) -> set[int]:
    if values is None:
        return set()
    normalized = {int(value) for value in values}
    if any(value <= 0 for value in normalized):
        raise ValueError("corrupt segment sequences must be positive integers")
    return normalized


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_run_row(connection: sqlite3.Connection, run_id: str) -> None:
    existing = connection.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    if existing:
        return
    connection.execute(
        """
        INSERT INTO runs (run_id, run_type, status, started_at)
        VALUES (?, 'extract', 'running', CURRENT_TIMESTAMP)
        """,
        (run_id,),
    )


def write_demo_segment(
    *,
    run_root: Path,
    run_id: str,
    segment_seq: int,
    records: list[dict[str, object]],
    bucket: str,
    prefix_root: str,
    extractor_instance_id: str,
    db_path: Path | None,
    corrupt_after_manifest: bool,
) -> dict[str, object]:
    segments_dir = run_root / "segments"
    manifests_dir = run_root / "manifests"
    segments_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)

    segment_name = f"usdt_transfer_{segment_seq:06d}.ndjson.gz"
    manifest_name = f"usdt_transfer_{segment_seq:06d}.manifest.json"
    segment_path = segments_dir / segment_name
    manifest_path = manifests_dir / manifest_name

    with gzip.open(segment_path, "wt", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")

    segment_sha256 = sha256_file(segment_path)
    manifest_payload = {
        "manifest_version": 1,
        "kind": "segment_manifest",
        "segment_id": f"{run_id}-seg-{segment_seq:06d}",
        "run_id": run_id,
        "segment_seq": segment_seq,
        "stream_name": "usdt_transfer",
        "trigger_name": "solidityLogTrigger",
        "topic0": TOPIC0,
        "contract_address": CONTRACT_ADDRESS,
        "block_from": records[0]["blockNumber"],
        "block_to": records[-1]["blockNumber"],
        "first_event_key": records[0]["uniqueId"],
        "last_event_key": records[-1]["uniqueId"],
        "first_tx_hash": records[0]["transactionId"],
        "last_tx_hash": records[-1]["transactionId"],
        "record_count": len(records),
        "file_size_bytes": segment_path.stat().st_size,
        "sha256": segment_sha256,
        "codec": "ndjson.gz",
        "local_path": str(segment_path),
        "relative_path": f"segments/{segment_name}",
        "s3_bucket": bucket,
        "s3_key": f"{prefix_root.rstrip('/')}/runs/{run_id}/segments/{segment_name}",
        "extractor_instance_id": extractor_instance_id,
        "created_at_utc": utc_now_iso(),
        "closed_at_utc": utc_now_iso(),
        "status": "sealed",
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2) + "\n", encoding="utf-8")

    if db_path is not None:
        connection = sqlite3.connect(db_path)
        try:
            ensure_run_row(connection, run_id)
            existing = connection.execute(
                "SELECT 1 FROM segments WHERE segment_id = ?",
                (manifest_payload["segment_id"],),
            ).fetchone()
            if not existing:
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
                    ) VALUES (?, ?, ?, ?, 'sealed', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        manifest_payload["segment_id"],
                        run_id,
                        segment_seq,
                        str(segment_path),
                        manifest_payload["block_from"],
                        manifest_payload["block_to"],
                        manifest_payload["first_tx_hash"],
                        manifest_payload["last_tx_hash"],
                        manifest_payload["record_count"],
                        manifest_payload["file_size_bytes"],
                        manifest_payload["sha256"],
                    ),
                )
            connection.commit()
        finally:
            connection.close()

    if corrupt_after_manifest:
        with segment_path.open("ab") as handle:
            handle.write(b"\ncorruption")

    return {
        "segment_seq": segment_seq,
        "segment_path": str(segment_path),
        "manifest_path": str(manifest_path),
        "segment_id": manifest_payload["segment_id"],
        "record_count": len(records),
        "block_from": records[0]["blockNumber"],
        "block_to": records[-1]["blockNumber"],
        "corrupted_after_manifest": corrupt_after_manifest,
    }


def generate_demo_run(
    run_root: Path,
    run_id: str,
    db_path: Path | None = None,
    segment_count: int = 1,
    records_per_segment: int = 3,
    base_block: int = 54300001,
    base_timestamp_ms: int = 1693526400123,
    bucket: str = DEFAULT_BUCKET,
    prefix_root: str = DEFAULT_PREFIX_ROOT,
    extractor_instance_id: str = DEFAULT_EXTRACTOR_INSTANCE_ID,
    corrupt_segment_sequences: Iterable[int] | None = None,
) -> dict[str, object]:
    if segment_count <= 0:
        raise ValueError("segment_count must be > 0")
    if records_per_segment <= 0:
        raise ValueError("records_per_segment must be > 0")

    corrupted = normalize_corrupt_segment_sequences(corrupt_segment_sequences)
    segment_summaries: list[dict[str, object]] = []
    global_offset = 0
    for segment_seq in range(1, segment_count + 1):
        records = build_demo_records(
            record_count=records_per_segment,
            start_offset=global_offset,
            base_block=base_block + global_offset,
            base_timestamp_ms=base_timestamp_ms + (global_offset * 5000),
        )
        segment_summary = write_demo_segment(
            run_root=run_root,
            run_id=run_id,
            segment_seq=segment_seq,
            records=records,
            bucket=bucket,
            prefix_root=prefix_root,
            extractor_instance_id=extractor_instance_id,
            db_path=db_path,
            corrupt_after_manifest=segment_seq in corrupted,
        )
        segment_summaries.append(segment_summary)
        global_offset += records_per_segment

    return {
        "run_root": str(run_root),
        "run_id": run_id,
        "segment_count": segment_count,
        "records_per_segment": records_per_segment,
        "total_record_count": segment_count * records_per_segment,
        "block_from": segment_summaries[0]["block_from"],
        "block_to": segment_summaries[-1]["block_to"],
        "segments": segment_summaries,
    }


def generate_demo_segment(
    run_root: Path,
    run_id: str,
    db_path: Path | None = None,
    record_count: int = 3,
    base_block: int = 54300001,
    base_timestamp_ms: int = 1693526400123,
    bucket: str = DEFAULT_BUCKET,
    prefix_root: str = DEFAULT_PREFIX_ROOT,
    extractor_instance_id: str = DEFAULT_EXTRACTOR_INSTANCE_ID,
) -> dict[str, str]:
    run_result = generate_demo_run(
        run_root=run_root,
        run_id=run_id,
        db_path=db_path,
        segment_count=1,
        records_per_segment=record_count,
        base_block=base_block,
        base_timestamp_ms=base_timestamp_ms,
        bucket=bucket,
        prefix_root=prefix_root,
        extractor_instance_id=extractor_instance_id,
    )
    first_segment = run_result["segments"][0]
    return {
        "segment_path": str(first_segment["segment_path"]),
        "manifest_path": str(first_segment["manifest_path"]),
        "run_root": str(run_result["run_root"]),
        "run_id": run_id,
        "record_count": str(run_result["total_record_count"]),
        "block_from": str(run_result["block_from"]),
        "block_to": str(run_result["block_to"]),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a synthetic sealed TRON USDT segment for demo/testing.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--record-count", type=int, default=3)
    parser.add_argument("--segment-count", type=int, default=1)
    parser.add_argument("--records-per-segment", type=int)
    parser.add_argument("--base-block", type=int, default=54300001)
    parser.add_argument("--base-timestamp-ms", type=int, default=1693526400123)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix-root", default=DEFAULT_PREFIX_ROOT)
    parser.add_argument("--extractor-instance-id", default=DEFAULT_EXTRACTOR_INSTANCE_ID)
    parser.add_argument("--corrupt-segment-seq", action="append", type=int, default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    records_per_segment = args.records_per_segment or args.record_count
    result = generate_demo_run(
        run_root=args.run_root,
        run_id=args.run_id,
        db_path=args.db_path,
        segment_count=args.segment_count,
        records_per_segment=records_per_segment,
        base_block=args.base_block,
        base_timestamp_ms=args.base_timestamp_ms,
        bucket=args.bucket,
        prefix_root=args.prefix_root,
        extractor_instance_id=args.extractor_instance_id,
        corrupt_segment_sequences=args.corrupt_segment_seq,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
