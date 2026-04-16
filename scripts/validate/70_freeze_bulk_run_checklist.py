#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_env_file(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Freeze the operator-visible pre-bulk gate checklist from validation and rehearsal reports.")
    parser.add_argument("--validation-report", required=True, type=Path)
    parser.add_argument("--rehearsal-report", required=True, type=Path)
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument("--output-markdown", required=True, type=Path)
    parser.add_argument("--loader-env-file", type=Path)
    parser.add_argument("--loader-instance-id", required=True)
    parser.add_argument("--workspace-artifact-sha256", required=True)
    parser.add_argument("--workspace-artifact-source", required=True)
    parser.add_argument("--target-schema", required=True)
    parser.add_argument("--loader-concurrency")
    parser.add_argument("--loader-record-batch-size")
    parser.add_argument("--retry-policy", default="segment-level rerun only; same run_id; same ordering; LOADER_CONCURRENCY=1")
    parser.add_argument(
        "--quarantine-policy",
        default="set segment status to quarantined, stop the run, investigate the manifest/segment pair, then rerun explicitly",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    validation = json.loads(args.validation_report.read_text(encoding="utf-8"))
    rehearsal = json.loads(args.rehearsal_report.read_text(encoding="utf-8"))
    env_values = load_env_file(args.loader_env_file)

    checklist = {
        "generated_at_utc": utc_now_iso(),
        "approval_status": "MANUAL_APPROVAL_REQUIRED",
        "workspace_artifact_sha256": args.workspace_artifact_sha256,
        "workspace_artifact_source": args.workspace_artifact_source,
        "loader_instance_id": args.loader_instance_id,
        "target_schema": args.target_schema,
        "loader_constraints": {
            "LOADER_CONCURRENCY": args.loader_concurrency or env_values.get("LOADER_CONCURRENCY", "1"),
            "LOADER_RECORD_BATCH_SIZE": args.loader_record_batch_size or env_values.get("LOADER_RECORD_BATCH_SIZE", "1000"),
            "retry_policy": args.retry_policy,
            "quarantine_policy": args.quarantine_policy,
            "staging_model": "global staging tables; single-worker only",
        },
        "fresh_disposable_validation": {
            "status": validation["status"],
            "run_id": validation["run_id"],
            "clickhouse_database": validation["clickhouse_database"],
            "events": validation["sql_verification"]["events"],
            "legs": validation["sql_verification"]["legs"],
            "audit_rows": validation["sql_verification"]["audit_rows"],
            "replay_status": validation["demo_report"]["replay_result"]["segments"][0]["status"],
        },
        "medium_rehearsal": {
            "status": rehearsal["status"],
            "run_id": rehearsal["run_id"],
            "clickhouse_database": rehearsal["clickhouse_database"],
            "throughput": rehearsal["throughput"],
            "aggregate_metrics": rehearsal["aggregate_metrics"],
        },
        "operator_checks": [
            {
                "name": "loader_status_counts",
                "kind": "sqlite",
                "query": "SELECT status, count() FROM loaded_segments GROUP BY status ORDER BY status;",
            },
            {
                "name": "loader_last_failures",
                "kind": "sqlite",
                "query": "SELECT run_id, segment_id, status, last_error FROM loaded_segments WHERE status IN ('failed','quarantined') ORDER BY updated_at DESC LIMIT 20;",
            },
            {
                "name": "canonical_event_count",
                "kind": "clickhouse",
                "query": f"SELECT count() FROM {args.target_schema}.trc20_transfer_events WHERE load_run_id = '<RUN_ID>';",
            },
            {
                "name": "canonical_leg_count",
                "kind": "clickhouse",
                "query": f"SELECT count() FROM {args.target_schema}.address_transfer_legs WHERE load_run_id = '<RUN_ID>';",
            },
            {
                "name": "load_audit_recent",
                "kind": "clickhouse",
                "query": f"SELECT run_id, segment_id, target_table, inserted_row_count, load_finished_at FROM {args.target_schema}.load_audit ORDER BY load_finished_at DESC LIMIT 20;",
            },
        ],
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_markdown.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(checklist, indent=2) + "\n", encoding="utf-8")

    markdown_lines = [
        "# Pre-Bulk Gate Checklist",
        "",
        f"- Generated at: `{checklist['generated_at_utc']}`",
        f"- Approval status: `{checklist['approval_status']}`",
        f"- Loader instance: `{args.loader_instance_id}`",
        f"- Workspace artifact sha256: `{args.workspace_artifact_sha256}`",
        f"- Workspace artifact source: `{args.workspace_artifact_source}`",
        f"- Target schema: `{args.target_schema}`",
        "",
        "## Frozen loader constraints",
        "",
        f"- `LOADER_CONCURRENCY={checklist['loader_constraints']['LOADER_CONCURRENCY']}`",
        f"- `LOADER_RECORD_BATCH_SIZE={checklist['loader_constraints']['LOADER_RECORD_BATCH_SIZE']}`",
        f"- Retry policy: {args.retry_policy}",
        f"- Quarantine policy: {args.quarantine_policy}",
        "",
        "## Fresh disposable validation",
        "",
        f"- Run id: `{validation['run_id']}`",
        f"- Status: `{validation['status']}`",
        f"- Events: `{validation['sql_verification']['events']}`",
        f"- Legs: `{validation['sql_verification']['legs']}`",
        f"- Audit rows: `{validation['sql_verification']['audit_rows']}`",
        f"- Replay status: `{validation['demo_report']['replay_result']['segments'][0]['status']}`",
        "",
        "## Medium rehearsal",
        "",
        f"- Run id: `{rehearsal['run_id']}`",
        f"- Status: `{rehearsal['status']}`",
        f"- Rows/sec: `{rehearsal['throughput']['rows_per_second']}`",
        f"- Bytes/sec: `{rehearsal['throughput']['bytes_per_second']}`",
        f"- Segments/sec: `{rehearsal['throughput']['segments_per_second']}`",
        f"- Avg seconds/segment: `{rehearsal['throughput']['average_seconds_per_segment']}`",
        "",
        "## Operator checks",
        "",
    ]
    for check in checklist["operator_checks"]:
        markdown_lines.append(f"- `{check['name']}` ({check['kind']}): `{check['query']}`")
    markdown_lines.extend(
        [
            "",
            "## Gate boundary",
            "",
            "- This checklist does not itself approve bulk historical load.",
            "- Bulk load stays blocked until an operator reviews this exact evidence set and explicitly approves the run.",
        ]
    )
    args.output_markdown.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    print(json.dumps(checklist, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
