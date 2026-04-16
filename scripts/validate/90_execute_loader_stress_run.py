#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def load_loader_module():
    module_path = PROJECT_ROOT / "loader" / "run" / "10_load_run_from_s3.py"
    spec = importlib.util.spec_from_file_location("run_loader_stress", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value)


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def redacted_url(url: str) -> str:
    return url.split("?", 1)[0]


class ClickHouseProbe:
    def __init__(self) -> None:
        self.args = [
            "clickhouse-client",
            "--host",
            os.environ["CLICKHOUSE_HOST"],
            "--port",
            os.environ.get("CLICKHOUSE_PORT", "9440"),
            "--user",
            os.environ["CLICKHOUSE_USER"],
        ]
        password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        if password:
            self.args.extend(["--password", password])
        if os.environ.get("CLICKHOUSE_SECURE", "1") == "1":
            self.args.append("--secure")

    def rows(self, query: str) -> tuple[list[dict[str, Any]], int]:
        started = time.perf_counter()
        completed = subprocess.run(self.args + ["--query", query], capture_output=True, text=True, check=True)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        rows = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
        return rows, elapsed_ms

    def value(self, query: str) -> str:
        completed = subprocess.run(self.args + ["--query", query], capture_output=True, text=True, check=True)
        return completed.stdout.strip()


def aggregate_segment_metrics(segments: list[dict[str, Any]]) -> dict[str, Any]:
    aggregate = {
        "segment_count": len(segments),
        "bytes_read": 0,
        "record_count": 0,
        "event_rows_expected": 0,
        "leg_rows_expected": 0,
        "s3_read_ms": 0,
        "normalize_ms": 0,
        "stage_ms": 0,
        "merge_ms": 0,
        "audit_ms": 0,
        "validation_ms": 0,
        "clickhouse_query_count": 0,
        "clickhouse_client_process_count": 0,
        "clickhouse_query_wall_ms": 0,
        "loader_peak_rss_kb_max": 0,
    }
    for segment in segments:
        metrics = segment.get("metrics", {})
        aggregate["bytes_read"] += int(metrics.get("bytes_read", 0))
        aggregate["record_count"] += int(metrics.get("record_count", 0))
        aggregate["event_rows_expected"] += int(metrics.get("event_rows_expected", 0))
        aggregate["leg_rows_expected"] += int(metrics.get("leg_rows_expected", 0))
        aggregate["s3_read_ms"] += int(metrics.get("s3_read_ms", 0))
        aggregate["normalize_ms"] += int(metrics.get("normalize_ms", 0))
        aggregate["stage_ms"] += int(metrics.get("stage_ms", 0))
        aggregate["merge_ms"] += int(metrics.get("merge_ms", 0))
        aggregate["audit_ms"] += int(metrics.get("audit_ms", 0))
        aggregate["validation_ms"] += int(metrics.get("validation_ms", 0))
        aggregate["clickhouse_query_count"] += int(metrics.get("clickhouse_query_count", 0))
        aggregate["clickhouse_client_process_count"] += int(metrics.get("clickhouse_client_process_count", 0))
        aggregate["clickhouse_query_wall_ms"] += int(metrics.get("clickhouse_query_wall_ms", 0))
        aggregate["loader_peak_rss_kb_max"] = max(
            aggregate["loader_peak_rss_kb_max"],
            int(metrics.get("loader_peak_rss_kb", 0)),
        )
    return aggregate


def throughput_from_aggregate(aggregate: dict[str, Any], wall_ms: int) -> dict[str, float]:
    seconds = max(wall_ms / 1000.0, 0.001)
    segment_count = max(int(aggregate["segment_count"]), 1)
    return {
        "rows_per_second": round(int(aggregate["record_count"]) / seconds, 2),
        "bytes_per_second": round(int(aggregate["bytes_read"]) / seconds, 2),
        "segments_per_second": round(int(aggregate["segment_count"]) / seconds, 4),
        "average_seconds_per_segment": round(seconds / segment_count, 4),
    }


def dominant_bottleneck(aggregate: dict[str, Any]) -> str:
    candidates = {
        "s3_read": int(aggregate["s3_read_ms"]),
        "normalize": int(aggregate["normalize_ms"]),
        "stage_insert": int(aggregate["stage_ms"]),
        "merge": int(aggregate["merge_ms"]),
        "audit": int(aggregate["audit_ms"]),
        "validation": int(aggregate["validation_ms"]),
    }
    return max(candidates, key=candidates.get)


def sqlite_export(loader_db_path: Path, run_id: str) -> dict[str, Any]:
    connection = sqlite3.connect(loader_db_path)
    connection.row_factory = sqlite3.Row
    try:
        loader_runs = [dict(row) for row in connection.execute("SELECT * FROM loader_runs WHERE run_id = ?", (run_id,)).fetchall()]
        loaded_segments = [dict(row) for row in connection.execute("SELECT * FROM loaded_segments WHERE run_id = ? ORDER BY segment_id", (run_id,)).fetchall()]
    finally:
        connection.close()
    return {
        "loader_runs": loader_runs,
        "loaded_segments": loaded_segments,
        "segment_status_summary": dict(Counter(row["status"] for row in loaded_segments)),
    }


def delete_loaded_segment_row(loader_db_path: Path, run_id: str, segment_id: str) -> None:
    connection = sqlite3.connect(loader_db_path)
    try:
        connection.execute("DELETE FROM loaded_segments WHERE run_id = ? AND segment_id = ?", (run_id, segment_id))
        connection.commit()
    finally:
        connection.close()


def clickhouse_counts(probe: ClickHouseProbe, database: str, run_id: str) -> dict[str, Any]:
    canonical_rows, _ = probe.rows(
        f"SELECT count() AS events, uniqExact(event_id) AS unique_events FROM {database}.trc20_transfer_events WHERE load_run_id = '{run_id}' FORMAT JSONEachRow"
    )
    leg_rows, _ = probe.rows(
        f"SELECT count() AS legs, uniqExact(leg_id) AS unique_legs FROM {database}.address_transfer_legs WHERE load_run_id = '{run_id}' FORMAT JSONEachRow"
    )
    audit_rows, _ = probe.rows(
        f"SELECT count() AS audit_rows FROM {database}.load_audit WHERE run_id = '{run_id}' FORMAT JSONEachRow"
    )
    audit_detail_rows, _ = probe.rows(
        f"""
        SELECT target_table, inserted_row_count, source_row_count, started_at, finished_at, note
        FROM {database}.load_audit
        WHERE run_id = '{run_id}'
        ORDER BY started_at ASC, target_table ASC
        FORMAT JSONEachRow
        """
    )
    return {
        "canonical": canonical_rows[0] if canonical_rows else {"events": 0, "unique_events": 0},
        "legs": leg_rows[0] if leg_rows else {"legs": 0, "unique_legs": 0},
        "audit": audit_rows[0] if audit_rows else {"audit_rows": 0},
        "audit_rows": audit_detail_rows,
    }


def storage_measurement(probe: ClickHouseProbe, database: str) -> dict[str, Any]:
    rows, _ = probe.rows(
        f"""
        SELECT
          table,
          sum(rows) AS rows,
          sum(bytes_on_disk) AS bytes_on_disk,
          sum(data_compressed_bytes) AS data_compressed_bytes,
          sum(data_uncompressed_bytes) AS data_uncompressed_bytes
        FROM system.parts
        WHERE database = '{database}' AND active
        GROUP BY table
        ORDER BY table ASC
        FORMAT JSONEachRow
        """
    )
    return {
        "table_stats": rows,
        "rows": sum(int(row["rows"]) for row in rows),
        "bytes_on_disk": sum(int(row["bytes_on_disk"]) for row in rows),
        "data_compressed_bytes": sum(int(row["data_compressed_bytes"]) for row in rows),
        "data_uncompressed_bytes": sum(int(row["data_uncompressed_bytes"]) for row in rows),
    }


def query_surface(
    probe: ClickHouseProbe,
    database: str,
    run_id: str,
    slice_start_utc: str | None,
    slice_end_utc: str | None,
    full_period_start_utc: str | None,
    full_period_end_utc: str | None,
) -> dict[str, Any]:
    sample_address_rows, _ = probe.rows(
        f"""
        SELECT address, count() AS row_count
        FROM {database}.address_transfer_legs
        WHERE load_run_id = '{run_id}'
        GROUP BY address
        ORDER BY row_count DESC, address ASC
        LIMIT 1
        FORMAT JSONEachRow
        """
    )
    if not sample_address_rows:
        return {}
    sample_address = sample_address_rows[0]["address"]
    sample_counterparty_rows, _ = probe.rows(
        f"""
        SELECT counterparty_address, count() AS row_count
        FROM {database}.address_transfer_legs
        WHERE load_run_id = '{run_id}' AND address = '{sample_address}'
        GROUP BY counterparty_address
        ORDER BY row_count DESC, counterparty_address ASC
        LIMIT 1
        FORMAT JSONEachRow
        """
    )
    sample_counterparty = sample_counterparty_rows[0]["counterparty_address"]
    sample_tx_rows, _ = probe.rows(
        f"""
        SELECT tx_hash, event_id, from_address, to_address, amount_decimal, block_number, log_index
        FROM {database}.trc20_transfer_events
        WHERE load_run_id = '{run_id}'
        ORDER BY block_number ASC, tx_hash ASC, log_index ASC
        LIMIT 1
        FORMAT JSONEachRow
        """
    )
    sample_tx = sample_tx_rows[0]["tx_hash"]
    sample_event_id = sample_tx_rows[0]["event_id"]

    time_filter = ""
    if slice_start_utc and slice_end_utc:
        start_text = slice_start_utc.replace("T", " ").replace("Z", "")
        end_text = slice_end_utc.replace("T", " ").replace("Z", "")
        time_filter = (
            " AND block_timestamp BETWEEN "
            f"toDateTime64('{start_text}', 3, 'UTC') "
            f"AND toDateTime64('{end_text}', 3, 'UTC')"
        )

    address_time_rows, address_time_ms = probe.rows(
        f"""
        SELECT address, toString(direction) AS direction, counterparty_address, tx_hash, block_number, amount_decimal, block_timestamp
        FROM {database}.address_transfer_legs
        WHERE load_run_id = '{run_id}' AND address = '{sample_address}'{time_filter}
        ORDER BY block_timestamp ASC, tx_hash ASC, log_index ASC, direction ASC
        LIMIT 5
        FORMAT JSONEachRow
        """
    )
    address_counterparty_rows, address_counterparty_ms = probe.rows(
        f"""
        SELECT address, toString(direction) AS direction, counterparty_address, tx_hash, block_number, amount_decimal, block_timestamp
        FROM {database}.address_transfer_legs
        WHERE load_run_id = '{run_id}' AND address = '{sample_address}' AND counterparty_address = '{sample_counterparty}'
        ORDER BY block_timestamp ASC, tx_hash ASC, log_index ASC, direction ASC
        LIMIT 5
        FORMAT JSONEachRow
        """
    )
    tx_lookup_rows, tx_lookup_ms = probe.rows(
        f"""
        SELECT event_id, tx_hash, from_address, to_address, amount_decimal, block_number, log_index
        FROM {database}.trc20_transfer_events
        WHERE load_run_id = '{run_id}' AND tx_hash = '{sample_tx}'
        ORDER BY log_index ASC
        FORMAT JSONEachRow
        """
    )
    max_amount = Decimal(probe.value(f"SELECT toString(max(amount_decimal)) FROM {database}.trc20_transfer_events WHERE load_run_id = '{run_id}'") or "0")
    threshold = (max_amount / Decimal("2")).quantize(Decimal("0.000001")) if max_amount > 0 else Decimal("0")
    minimum_amount_rows, minimum_amount_ms = probe.rows(
        f"""
        SELECT tx_hash, from_address, to_address, amount_decimal, block_number
        FROM {database}.trc20_transfer_events
        WHERE load_run_id = '{run_id}' AND amount_decimal >= toDecimal64('{threshold}', 6)
        ORDER BY amount_decimal DESC, tx_hash ASC
        LIMIT 5
        FORMAT JSONEachRow
        """
    )
    forward_page_1, forward_page_1_ms = probe.rows(
        f"""
        SELECT block_timestamp, tx_hash, log_index, toString(direction) AS direction, counterparty_address, amount_decimal
        FROM {database}.address_transfer_legs
        WHERE load_run_id = '{run_id}' AND address = '{sample_address}'
        ORDER BY block_timestamp ASC, tx_hash ASC, log_index ASC, direction ASC
        LIMIT 5 OFFSET 0
        FORMAT JSONEachRow
        """
    )
    forward_page_2, forward_page_2_ms = probe.rows(
        f"""
        SELECT block_timestamp, tx_hash, log_index, toString(direction) AS direction, counterparty_address, amount_decimal
        FROM {database}.address_transfer_legs
        WHERE load_run_id = '{run_id}' AND address = '{sample_address}'
        ORDER BY block_timestamp ASC, tx_hash ASC, log_index ASC, direction ASC
        LIMIT 5 OFFSET 5
        FORMAT JSONEachRow
        """
    )
    backward_page_1, backward_page_1_ms = probe.rows(
        f"""
        SELECT block_timestamp, tx_hash, log_index, toString(direction) AS direction, counterparty_address, amount_decimal
        FROM {database}.address_transfer_legs
        WHERE load_run_id = '{run_id}' AND address = '{sample_address}'
        ORDER BY block_timestamp DESC, tx_hash DESC, log_index DESC, direction DESC
        LIMIT 5 OFFSET 0
        FORMAT JSONEachRow
        """
    )
    backward_page_2, backward_page_2_ms = probe.rows(
        f"""
        SELECT block_timestamp, tx_hash, log_index, toString(direction) AS direction, counterparty_address, amount_decimal
        FROM {database}.address_transfer_legs
        WHERE load_run_id = '{run_id}' AND address = '{sample_address}'
        ORDER BY block_timestamp DESC, tx_hash DESC, log_index DESC, direction DESC
        LIMIT 5 OFFSET 5
        FORMAT JSONEachRow
        """
    )
    traceability_rows, traceability_ms = probe.rows(
        f"""
        SELECT event_id, leg_id, address, toString(direction) AS direction, counterparty_address, tx_hash, amount_decimal
        FROM {database}.address_transfer_legs
        WHERE load_run_id = '{run_id}' AND event_id = '{sample_event_id}'
        ORDER BY direction ASC, address ASC
        FORMAT JSONEachRow
        """
    )

    rows_per_day_estimate = 0
    rows_per_month_30d_estimate = 0
    if slice_start_utc and slice_end_utc:
        start_dt = datetime.fromisoformat(slice_start_utc.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(slice_end_utc.replace("Z", "+00:00"))
        duration_seconds = max((end_dt - start_dt).total_seconds(), 1.0)
        event_rows = int(probe.value(f"SELECT count() FROM {database}.trc20_transfer_events WHERE load_run_id = '{run_id}'") or "0")
        rows_per_day_estimate = int(event_rows * (86400 / duration_seconds))
        rows_per_month_30d_estimate = rows_per_day_estimate * 30
    projected_days = 0
    if full_period_start_utc and full_period_end_utc:
        start_dt = datetime.fromisoformat(full_period_start_utc.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(full_period_end_utc.replace("Z", "+00:00"))
        projected_days = max((end_dt - start_dt).days, 1)

    return {
        "sample_address": sample_address,
        "sample_counterparty": sample_counterparty,
        "sample_tx_hash": sample_tx,
        "address_time_range": {"latency_ms": address_time_ms, "rows": address_time_rows},
        "address_counterparty": {"latency_ms": address_counterparty_ms, "rows": address_counterparty_rows},
        "tx_hash_lookup": {"latency_ms": tx_lookup_ms, "rows": tx_lookup_rows},
        "minimum_amount_filter": {"latency_ms": minimum_amount_ms, "threshold": str(threshold), "rows": minimum_amount_rows},
        "forward_pagination": {
            "page_1_latency_ms": forward_page_1_ms,
            "page_1_rows": forward_page_1,
            "page_2_latency_ms": forward_page_2_ms,
            "page_2_rows": forward_page_2,
        },
        "backward_pagination": {
            "page_1_latency_ms": backward_page_1_ms,
            "page_1_rows": backward_page_1,
            "page_2_latency_ms": backward_page_2_ms,
            "page_2_rows": backward_page_2,
        },
        "traceability": {"latency_ms": traceability_ms, "event_id": sample_event_id, "leg_rows": traceability_rows},
        "rows_per_day_estimate": rows_per_day_estimate,
        "rows_per_month_30d_estimate": rows_per_month_30d_estimate,
        "projected_full_period_days": projected_days,
    }


def safe_runtime_env_snapshot() -> dict[str, str]:
    keys = (
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_PORT",
        "CLICKHOUSE_DATABASE",
        "CLICKHOUSE_SECURE",
        "LOADER_PYTHON_BIN",
        "LOADER_CONCURRENCY",
        "LOADER_RECORD_BATCH_SIZE",
        "AWS_REGION",
        "S3_BUFFER_BUCKET",
        "S3_BUFFER_PREFIX_ROOT",
    )
    return {key: os.environ[key] for key in keys if key in os.environ}


def compact_query_surface(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    return {
        "sample_address": payload.get("sample_address"),
        "sample_counterparty": payload.get("sample_counterparty"),
        "sample_tx_hash": payload.get("sample_tx_hash"),
        "address_time_range_latency_ms": payload.get("address_time_range", {}).get("latency_ms"),
        "address_counterparty_latency_ms": payload.get("address_counterparty", {}).get("latency_ms"),
        "tx_hash_lookup_latency_ms": payload.get("tx_hash_lookup", {}).get("latency_ms"),
        "minimum_amount_filter_latency_ms": payload.get("minimum_amount_filter", {}).get("latency_ms"),
        "forward_page_1_latency_ms": payload.get("forward_pagination", {}).get("page_1_latency_ms"),
        "forward_page_2_latency_ms": payload.get("forward_pagination", {}).get("page_2_latency_ms"),
        "backward_page_1_latency_ms": payload.get("backward_pagination", {}).get("page_1_latency_ms"),
        "backward_page_2_latency_ms": payload.get("backward_pagination", {}).get("page_2_latency_ms"),
        "traceability_latency_ms": payload.get("traceability", {}).get("latency_ms"),
        "rows_per_day_estimate": payload.get("rows_per_day_estimate"),
        "rows_per_month_30d_estimate": payload.get("rows_per_month_30d_estimate"),
        "projected_full_period_days": payload.get("projected_full_period_days"),
    }


def compact_report(report: dict[str, Any], output_path: Path | None) -> dict[str, Any]:
    compact: dict[str, Any] = {
        "run_id": report.get("run_id"),
        "label": report.get("label"),
        "status": report.get("status"),
        "clickhouse_database": report.get("clickhouse_database"),
        "workspace_artifact_sha256": report.get("workspace_artifact_sha256"),
        "workspace_artifact_source": report.get("workspace_artifact_source"),
        "loader_runtime_env_safe": report.get("loader_runtime_env_safe"),
        "started_at_utc": report.get("started_at_utc"),
        "finished_at_utc": report.get("finished_at_utc"),
        "expected_failure": report.get("expected_failure", False),
        "replay_delta_counts": report.get("replay_delta_counts", {}),
        "loader_state": {
            "segment_status_summary": report.get("loader_state", {}).get("segment_status_summary", {}),
            "loader_runs_count": len(report.get("loader_state", {}).get("loader_runs", [])),
            "loaded_segments_count": len(report.get("loader_state", {}).get("loaded_segments", [])),
        },
        "clickhouse_counts": {
            "canonical": report.get("clickhouse_counts", {}).get("canonical", {}),
            "legs": report.get("clickhouse_counts", {}).get("legs", {}),
            "audit": report.get("clickhouse_counts", {}).get("audit", {}),
        },
    }
    if output_path is not None:
        compact["host_report_path"] = str(output_path)
    if "initial_load" in report:
        compact["initial_load"] = {
            "wall_ms": report["initial_load"].get("wall_ms", 0),
            "aggregate_metrics": report["initial_load"].get("aggregate_metrics", {}),
            "throughput": report["initial_load"].get("throughput", {}),
            "dominant_bottleneck": report["initial_load"].get("dominant_bottleneck", ""),
        }
    if "replay" in report:
        compact["replay"] = {
            "wall_ms": report["replay"].get("wall_ms", 0),
            "aggregate_metrics": report["replay"].get("aggregate_metrics", {}),
            "throughput": report["replay"].get("throughput", {}),
            "dominant_bottleneck": report["replay"].get("dominant_bottleneck", ""),
        }
    if "storage_measurement" in report:
        storage = report["storage_measurement"]
        compact["storage_measurement"] = {
            "rows": storage.get("rows", 0),
            "bytes_on_disk": storage.get("bytes_on_disk", 0),
            "data_compressed_bytes": storage.get("data_compressed_bytes", 0),
            "data_uncompressed_bytes": storage.get("data_uncompressed_bytes", 0),
            "table_count": len(storage.get("table_stats", [])),
        }
    if "query_surface" in report:
        compact["query_surface"] = compact_query_surface(report["query_surface"])
    if "error" in report:
        compact["error"] = report["error"]
    return compact


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute one loader stress run on the remote loader host.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix-root", required=True)
    parser.add_argument("--loader-db-path", required=True, type=Path)
    parser.add_argument("--loader-schema-path", required=True, type=Path)
    parser.add_argument("--region", required=True)
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--skip-initial-load", action="store_true")
    parser.add_argument("--delete-loaded-segment-id")
    parser.add_argument("--expect-failure", action="store_true")
    parser.add_argument("--collect-storage", action="store_true")
    parser.add_argument("--collect-query-surface", action="store_true")
    parser.add_argument("--slice-start-utc")
    parser.add_argument("--slice-end-utc")
    parser.add_argument("--full-period-start-utc")
    parser.add_argument("--full-period-end-utc")
    parser.add_argument("--label", default="")
    parser.add_argument("--loader-env-file", type=Path, default=PROJECT_ROOT / "configs" / "loader" / "clickhouse.env")
    parser.add_argument("--workspace-artifact-sha256", default="")
    parser.add_argument("--workspace-artifact-source", default="")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--stdout-summary-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_file(args.loader_env_file)
    loader = load_loader_module()
    probe = ClickHouseProbe()
    database = os.environ.get("CLICKHOUSE_DATABASE", "tron_usdt_local")

    report: dict[str, Any] = {
        "run_id": args.run_id,
        "label": args.label,
        "clickhouse_database": database,
        "workspace_artifact_sha256": args.workspace_artifact_sha256,
        "workspace_artifact_source": args.workspace_artifact_source,
        "loader_runtime_env_safe": safe_runtime_env_snapshot(),
        "started_at_utc": utc_now_iso(),
        "expected_failure": args.expect_failure,
    }

    try:
        if args.delete_loaded_segment_id:
            delete_loaded_segment_row(args.loader_db_path, args.run_id, args.delete_loaded_segment_id)
            report["deleted_loaded_segment_id"] = args.delete_loaded_segment_id

        if not args.skip_initial_load:
            load_started = time.perf_counter()
            load_result = loader.load_run_from_s3(
                run_id=args.run_id,
                bucket=args.bucket,
                prefix_root=args.prefix_root,
                loader_db_path=args.loader_db_path,
                loader_schema_path=args.loader_schema_path,
                region=args.region,
            )
            load_wall_ms = int((time.perf_counter() - load_started) * 1000)
            report["initial_load"] = {
                "wall_ms": load_wall_ms,
                "result": load_result,
                "aggregate_metrics": aggregate_segment_metrics(load_result["segments"]),
                "throughput": throughput_from_aggregate(aggregate_segment_metrics(load_result["segments"]), load_wall_ms),
            }
            report["initial_load"]["dominant_bottleneck"] = dominant_bottleneck(report["initial_load"]["aggregate_metrics"])
            report["validation_after_load"] = loader.validate_loaded_run(
                run_id=args.run_id,
                loader_db_path=args.loader_db_path,
                loader_schema_path=args.loader_schema_path,
            )

        if args.replay:
            replay_started = time.perf_counter()
            replay_result = loader.load_run_from_s3(
                run_id=args.run_id,
                bucket=args.bucket,
                prefix_root=args.prefix_root,
                loader_db_path=args.loader_db_path,
                loader_schema_path=args.loader_schema_path,
                region=args.region,
                force_replay=True,
            )
            replay_wall_ms = int((time.perf_counter() - replay_started) * 1000)
            report["replay"] = {
                "wall_ms": replay_wall_ms,
                "result": replay_result,
                "aggregate_metrics": aggregate_segment_metrics(replay_result["segments"]),
                "throughput": throughput_from_aggregate(aggregate_segment_metrics(replay_result["segments"]), replay_wall_ms),
            }
            report["replay"]["dominant_bottleneck"] = dominant_bottleneck(report["replay"]["aggregate_metrics"])
            report["validation_after_replay"] = loader.validate_loaded_run(
                run_id=args.run_id,
                loader_db_path=args.loader_db_path,
                loader_schema_path=args.loader_schema_path,
            )
    except Exception as exc:  # noqa: BLE001
        report["error"] = {"type": exc.__class__.__name__, "message": str(exc)}

    report["loader_state"] = sqlite_export(args.loader_db_path, args.run_id)
    report["clickhouse_counts"] = clickhouse_counts(probe, database, args.run_id)
    if args.collect_storage:
        report["storage_measurement"] = storage_measurement(probe, database)
    if args.collect_query_surface:
        report["query_surface"] = query_surface(
            probe,
            database,
            args.run_id,
            args.slice_start_utc,
            args.slice_end_utc,
            args.full_period_start_utc,
            args.full_period_end_utc,
        )

    status_summary = report["loader_state"]["segment_status_summary"]
    replay_rows = report.get("replay", {}).get("result", {}).get("segments", [])
    replay_new_events = sum(int(segment.get("events_inserted", 0)) for segment in replay_rows)
    replay_new_legs = sum(int(segment.get("legs_inserted", 0)) for segment in replay_rows)
    report["replay_delta_counts"] = {
        "new_canonical_event_rows": replay_new_events,
        "new_leg_rows": replay_new_legs,
    }
    report["finished_at_utc"] = utc_now_iso()

    if args.expect_failure:
        report["status"] = "passed" if "error" in report else "failed_expected_failure_missing"
    elif "error" in report:
        report["status"] = "failed"
    else:
        counts = report["clickhouse_counts"]
        events = int(counts["canonical"]["events"])
        legs = int(counts["legs"]["legs"])
        failed_segments = int(status_summary.get("failed", 0)) + int(status_summary.get("quarantined", 0))
        report["status"] = "passed" if events >= 0 and legs >= 0 and failed_segments == 0 else "failed"

    rendered = json.dumps(report, indent=2)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    if args.stdout_summary_only:
        print(json.dumps(compact_report(report, args.output), indent=2))
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
