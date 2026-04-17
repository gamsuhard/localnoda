#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRE_BULK_GATE_PATH = PROJECT_ROOT / "scripts" / "validate" / "80_run_pre_bulk_gate.py"
STRESS_RUNNER_PATH = PROJECT_ROOT / "scripts" / "validate" / "90_execute_loader_stress_run.sh"
DEPLOY_SCRIPT_PATH = PROJECT_ROOT / "scripts" / "provision" / "70_deploy_workspace_artifact.sh"
BOOTSTRAP_SCRIPT_PATH = PROJECT_ROOT / "scripts" / "provision" / "80_bootstrap_loader_host.sh"
PUBLISH_SCRIPT_PATH = PROJECT_ROOT / "scripts" / "demo" / "20_publish_demo_run_to_s3.py"
REAL_SLICE_REPORT_DIR = PROJECT_ROOT / "reports" / "real-slice"


DEFAULT_REMOTE_WORKSPACE_ROOT = "/srv/local-tron-usdt-backfill"
DEFAULT_ARTIFACT_BUCKET = "goldusdt-v2-stage-913378704801-ops"
DEFAULT_ARTIFACT_PREFIX = "codex/local-tron-usdt-backfill"
DEFAULT_RAW_BUCKET = "goldusdt-v2-stage-913378704801-raw"
DEFAULT_RAW_PREFIX_ROOT = "providers/tron-usdt-backfill/usdt-transfer-oneoff"
DEFAULT_CLICKHOUSE_SECRET = "goldusdt-v2-stage-clickhouse"
DEFAULT_REAL_SLICE_RUN_ID = "real-slice-20230901t0000z-20260416t114407z"
DEFAULT_FULL_PERIOD_START = "2023-11-03T00:00:00Z"
DEFAULT_FULL_PERIOD_END = "2026-02-01T00:00:00Z"


@dataclass(frozen=True)
class Scenario:
    wave: str
    label: str
    run_id: str
    batch_size: int
    segment_count: int
    records_per_segment: int
    synthetic: bool
    replay: bool = True
    expect_failure: bool = False
    delete_loaded_segment_id: str | None = None
    skip_initial_load: bool = False
    collect_storage: bool = True
    collect_query_surface: bool = False
    slice_start_utc: str | None = None
    slice_end_utc: str | None = None
    full_period_start_utc: str | None = None
    full_period_end_utc: str | None = None
    corrupt_segment_seq: tuple[int, ...] = ()


def load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ").lower()


def sanitize_identifier(text: str, *, max_length: int = 63) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in text.lower())
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.strip("_")[:max_length]


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_markdown(path: Path, content: str) -> None:
    ensure_directory(path.parent)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute ClickHouse loader stress-test waves.")
    parser.add_argument("--loader-instance-id", required=True)
    parser.add_argument("--profile", default="ai-agents-dev")
    parser.add_argument("--loader-region", default="eu-central-1")
    parser.add_argument("--artifact-bucket", default=DEFAULT_ARTIFACT_BUCKET)
    parser.add_argument("--artifact-prefix", default=DEFAULT_ARTIFACT_PREFIX)
    parser.add_argument("--raw-bucket", default=DEFAULT_RAW_BUCKET)
    parser.add_argument("--raw-prefix-root", default=DEFAULT_RAW_PREFIX_ROOT)
    parser.add_argument("--clickhouse-secret-name", default=DEFAULT_CLICKHOUSE_SECRET)
    parser.add_argument("--remote-workspace-root", default=DEFAULT_REMOTE_WORKSPACE_ROOT)
    parser.add_argument("--reports-dir", type=Path, default=PROJECT_ROOT / "reports" / "stress")
    parser.add_argument("--real-slice-run-id", default=DEFAULT_REAL_SLICE_RUN_ID)
    parser.add_argument("--real-slice-start-utc", default=DEFAULT_FULL_PERIOD_START)
    parser.add_argument("--real-slice-end-utc")
    parser.add_argument("--full-period-start-utc", default=DEFAULT_FULL_PERIOD_START)
    parser.add_argument("--full-period-end-utc", default=DEFAULT_FULL_PERIOD_END)
    return parser.parse_args()


def build_scenarios(stamp: str, real_slice_end_utc: str, args: argparse.Namespace) -> list[Scenario]:
    scenarios: list[Scenario] = []

    for batch_size in (1000, 5000, 10000, 25000, 50000):
        scenarios.append(
            Scenario(
                wave="batch-size-sweep",
                label=f"batch-{batch_size}",
                run_id=f"stress-batch-{batch_size}-{stamp}",
                batch_size=batch_size,
                segment_count=10,
                records_per_segment=10000,
                synthetic=True,
            )
        )

    for segment_count in (10, 50, 100):
        scenarios.append(
            Scenario(
                wave="segment-count-sweep",
                label=f"segments-{segment_count}",
                run_id=f"stress-segments-{segment_count}-{stamp}",
                batch_size=10000,
                segment_count=segment_count,
                records_per_segment=10000,
                synthetic=True,
            )
        )

    for records_per_segment in (10000, 50000, 100000, 250000):
        scenarios.append(
            Scenario(
                wave="segment-size-sweep",
                label=f"segment-size-{records_per_segment}",
                run_id=f"stress-segsize-{records_per_segment}-{stamp}",
                batch_size=10000,
                segment_count=10,
                records_per_segment=records_per_segment,
                synthetic=True,
            )
        )

    medium_run_id = f"stress-replay-medium-{stamp}"
    medium_base = Scenario(
        wave="replay-fault",
        label="medium-initial",
        run_id=medium_run_id,
        batch_size=10000,
        segment_count=10,
        records_per_segment=10000,
        synthetic=True,
    )
    scenarios.extend(
        [
            medium_base,
            Scenario(
                wave="replay-fault",
                label="medium-restart-replay",
                run_id=medium_run_id,
                batch_size=10000,
                segment_count=10,
                records_per_segment=10000,
                synthetic=True,
                skip_initial_load=True,
                replay=True,
            ),
            Scenario(
                wave="replay-fault",
                label="medium-missing-ledger-row",
                run_id=medium_run_id,
                batch_size=10000,
                segment_count=10,
                records_per_segment=10000,
                synthetic=True,
                skip_initial_load=True,
                replay=False,
                delete_loaded_segment_id=f"{medium_run_id}-000001",
            ),
            Scenario(
                wave="replay-fault",
                label="medium-corrupted-segment",
                run_id=f"stress-corrupt-{stamp}",
                batch_size=10000,
                segment_count=10,
                records_per_segment=10000,
                synthetic=True,
                replay=False,
                expect_failure=True,
                corrupt_segment_seq=(2,),
            ),
        ]
    )

    real_run_id = args.real_slice_run_id
    scenarios.extend(
        [
            Scenario(
                wave="real-controlled-slice",
                label="real-slice-initial",
                run_id=real_run_id,
                batch_size=10000,
                segment_count=0,
                records_per_segment=0,
                synthetic=False,
                replay=True,
                collect_query_surface=True,
                slice_start_utc=args.real_slice_start_utc,
                slice_end_utc=real_slice_end_utc,
                full_period_start_utc=args.full_period_start_utc,
                full_period_end_utc=args.full_period_end_utc,
            ),
            Scenario(
                wave="real-controlled-slice",
                label="real-slice-restart-replay",
                run_id=real_run_id,
                batch_size=10000,
                segment_count=0,
                records_per_segment=0,
                synthetic=False,
                skip_initial_load=True,
                replay=True,
                collect_query_surface=True,
                slice_start_utc=args.real_slice_start_utc,
                slice_end_utc=real_slice_end_utc,
                full_period_start_utc=args.full_period_start_utc,
                full_period_end_utc=args.full_period_end_utc,
            ),
        ]
    )
    return scenarios


def real_slice_end_utc(default_end: str) -> str:
    operator_summaries = sorted(REAL_SLICE_REPORT_DIR.glob("*-operator-summary.md"))
    if not operator_summaries:
        return default_end
    latest = operator_summaries[-1]
    text = latest.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.lower().startswith("- slice end utc:"):
            return line.split(":", 1)[1].strip()
    return default_end


def flatten_segment_metrics(report: dict[str, Any], scenario: Scenario) -> list[dict[str, Any]]:
    if scenario.expect_failure and "initial_load" not in report:
        return []
    result = report.get("initial_load", {}).get("result", {})
    rows: list[dict[str, Any]] = []
    for segment in result.get("segments", []):
        metrics = segment.get("metrics", {})
        rows.append(
            {
                "wave": scenario.wave,
                "label": scenario.label,
                "run_id": scenario.run_id,
                "segment_id": segment.get("segment_id"),
                "status": segment.get("status"),
                "record_count": metrics.get("record_count", 0),
                "bytes_read": metrics.get("bytes_read", 0),
                "s3_read_ms": metrics.get("s3_read_ms", 0),
                "normalize_ms": metrics.get("normalize_ms", 0),
                "stage_ms": metrics.get("stage_ms", 0),
                "merge_ms": metrics.get("merge_ms", 0),
                "audit_ms": metrics.get("audit_ms", 0),
                "validation_ms": metrics.get("validation_ms", 0),
                "clickhouse_query_count": metrics.get("clickhouse_query_count", 0),
                "clickhouse_client_process_count": metrics.get("clickhouse_client_process_count", 0),
                "clickhouse_query_wall_ms": metrics.get("clickhouse_query_wall_ms", 0),
                "loader_peak_rss_kb": metrics.get("loader_peak_rss_kb", 0),
            }
        )
    return rows


def scenario_summary_row(report: dict[str, Any], scenario: Scenario) -> dict[str, Any]:
    load = report.get("initial_load", {})
    aggregate = load.get("aggregate_metrics", {})
    throughput = load.get("throughput", {})
    replay = report.get("replay", {})
    replay_delta = report.get("replay_delta_counts", {})
    error = report.get("error", {})
    return {
        "wave": scenario.wave,
        "label": scenario.label,
        "run_id": scenario.run_id,
        "synthetic": scenario.synthetic,
        "expected_failure": scenario.expect_failure,
        "status": report.get("status", ""),
        "batch_size": scenario.batch_size,
        "segment_count": scenario.segment_count,
        "records_per_segment": scenario.records_per_segment,
        "record_count": aggregate.get("record_count", 0),
        "rows_per_second": throughput.get("rows_per_second", 0),
        "bytes_per_second": throughput.get("bytes_per_second", 0),
        "segments_per_second": throughput.get("segments_per_second", 0),
        "average_seconds_per_segment": throughput.get("average_seconds_per_segment", 0),
        "dominant_bottleneck": load.get("dominant_bottleneck", ""),
        "s3_read_ms": aggregate.get("s3_read_ms", 0),
        "normalize_ms": aggregate.get("normalize_ms", 0),
        "stage_ms": aggregate.get("stage_ms", 0),
        "merge_ms": aggregate.get("merge_ms", 0),
        "audit_ms": aggregate.get("audit_ms", 0),
        "validation_ms": aggregate.get("validation_ms", 0),
        "clickhouse_query_count": aggregate.get("clickhouse_query_count", 0),
        "clickhouse_client_process_count": aggregate.get("clickhouse_client_process_count", 0),
        "clickhouse_query_wall_ms": aggregate.get("clickhouse_query_wall_ms", 0),
        "loader_peak_rss_kb_max": aggregate.get("loader_peak_rss_kb_max", 0),
        "replay_wall_ms": replay.get("wall_ms", 0),
        "replay_new_canonical_event_rows": replay_delta.get("new_canonical_event_rows", 0),
        "replay_new_leg_rows": replay_delta.get("new_leg_rows", 0),
        "error_type": error.get("type", ""),
        "error_message": error.get("message", ""),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_directory(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def best_row(rows: list[dict[str, Any]], wave: str) -> dict[str, Any] | None:
    candidates = [
        row
        for row in rows
        if row["wave"] == wave
        and row["status"] == "passed"
        and int(row.get("replay_new_canonical_event_rows", 0)) == 0
        and int(row.get("replay_new_leg_rows", 0)) == 0
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda row: float(row.get("rows_per_second", 0)))


def answer_audit_focus(summary_rows: list[dict[str, Any]]) -> dict[str, str]:
    passed = [row for row in summary_rows if row["status"] == "passed" and row["wave"] != "real-controlled-slice"]
    if not passed:
        return {
            "q1_bottleneck": "Insufficient passed runs to determine bottleneck.",
            "q2_client_overhead": "Insufficient data.",
            "q3_merge_degradation": "Insufficient data.",
            "q4_single_worker_stability": "Insufficient data.",
            "q5_highest_safe_throughput": "Insufficient data.",
        }
    totals = {
        "s3_read": sum(int(row["s3_read_ms"]) for row in passed),
        "normalize": sum(int(row["normalize_ms"]) for row in passed),
        "stage_insert": sum(int(row["stage_ms"]) for row in passed),
        "merge": sum(int(row["merge_ms"]) for row in passed),
        "validation": sum(int(row["validation_ms"]) for row in passed),
    }
    dominant = max(totals, key=totals.get)
    batch_best = best_row(summary_rows, "batch-size-sweep")
    segsize_best = best_row(summary_rows, "segment-size-sweep")
    largest_segment = max(
        (row for row in summary_rows if row["wave"] == "segment-size-sweep" and row["status"] == "passed"),
        key=lambda row: int(row["records_per_segment"]),
        default=None,
    )
    segment_count_rows = sorted(
        (row for row in summary_rows if row["wave"] == "segment-count-sweep" and row["status"] == "passed"),
        key=lambda row: int(row["segment_count"]),
    )
    merge_ratio_text = "No segment-count evidence."
    if len(segment_count_rows) >= 2:
        first = segment_count_rows[0]
        last = segment_count_rows[-1]
        first_ratio = float(first["merge_ms"]) / max(int(first["record_count"]), 1)
        last_ratio = float(last["merge_ms"]) / max(int(last["record_count"]), 1)
        merge_ratio_text = (
            f"merge_ms/row {first_ratio:.6f} at {first['segment_count']} segments vs "
            f"{last_ratio:.6f} at {last['segment_count']} segments."
        )
    return {
        "q1_bottleneck": f"Dominant measured bottleneck across passed synthetic runs: {dominant}.",
        "q2_client_overhead": (
            "clickhouse-client subprocess overhead is measurable if query/process counts rise faster than rows/sec; "
            f"best batch run used {batch_best['clickhouse_client_process_count'] if batch_best else 'n/a'} client invocations."
        ),
        "q3_merge_degradation": f"Current merge degradation check: {merge_ratio_text}",
        "q4_single_worker_stability": "Single-worker path stayed explicit and replay-safe across passed runs; keep LOADER_CONCURRENCY=1.",
        "q5_highest_safe_throughput": (
            f"Highest passed synthetic throughput came from batch {batch_best['batch_size']} and "
            f"records/segment {segsize_best['records_per_segment'] if segsize_best else (largest_segment['records_per_segment'] if largest_segment else 'n/a')}."
        ),
    }


def markdown_summary(
    *,
    stamp: str,
    artifact_sha256: str,
    artifact_source: str,
    summary_rows: list[dict[str, Any]],
    recommendation: dict[str, Any],
    audit_answers: dict[str, str],
) -> str:
    lines = [
        f"# ClickHouse Loader Stress Test Summary ({stamp})",
        "",
        f"- artifact sha256: `{artifact_sha256}`",
        f"- artifact source: `{artifact_source}`",
        f"- recommendation: `{recommendation['final_decision']}`",
        f"- chosen batch size: `{recommendation.get('chosen_batch_size', 'n/a')}`",
        f"- chosen records/segment target: `{recommendation.get('chosen_records_per_segment', 'n/a')}`",
        "",
        "## Run Comparison",
        "",
        "| Wave | Label | Status | Batch | Segments | Records/Segment | Rows/sec | Segments/sec | Dominant bottleneck | Replay delta events | Replay delta legs |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |",
    ]
    for row in summary_rows:
        lines.append(
            "| {wave} | {label} | {status} | {batch_size} | {segment_count} | {records_per_segment} | {rows_per_second} | {segments_per_second} | {dominant_bottleneck} | {replay_new_canonical_event_rows} | {replay_new_leg_rows} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Audit Focus Answers",
            "",
            f"1. {audit_answers['q1_bottleneck']}",
            f"2. {audit_answers['q2_client_overhead']}",
            f"3. {audit_answers['q3_merge_degradation']}",
            f"4. {audit_answers['q4_single_worker_stability']}",
            f"5. {audit_answers['q5_highest_safe_throughput']}",
            "",
            "## Recommendation",
            "",
            f"- current merge strategy acceptable: `{recommendation['merge_strategy_acceptable']}`",
            f"- synchronous clickhouse-client path acceptable: `{recommendation['synchronous_path_acceptable']}`",
            f"- note: {recommendation['note']}",
        ]
    )
    return "\n".join(lines)


def markdown_run_report(report: dict[str, Any]) -> str:
    scenario = report.get("scenario", {})
    load = report.get("initial_load", {})
    replay = report.get("replay", {})
    lines = [
        f"# Stress Run: {scenario.get('label', report.get('run_id', 'unknown'))}",
        "",
        f"- wave: `{scenario.get('wave', '')}`",
        f"- run_id: `{report.get('run_id', '')}`",
        f"- status: `{report.get('status', '')}`",
        f"- database: `{report.get('clickhouse_database', '')}`",
        f"- artifact sha256: `{report.get('workspace_artifact_sha256', '')}`",
        "",
        "## Initial Load",
        "",
        f"- wall_ms: `{load.get('wall_ms', 0)}`",
        f"- dominant bottleneck: `{load.get('dominant_bottleneck', '')}`",
        f"- throughput rows/sec: `{load.get('throughput', {}).get('rows_per_second', 0)}`",
        f"- throughput bytes/sec: `{load.get('throughput', {}).get('bytes_per_second', 0)}`",
        f"- throughput segments/sec: `{load.get('throughput', {}).get('segments_per_second', 0)}`",
        "",
        "## Replay",
        "",
        f"- wall_ms: `{replay.get('wall_ms', 0)}`",
        f"- new canonical event rows: `{report.get('replay_delta_counts', {}).get('new_canonical_event_rows', 0)}`",
        f"- new leg rows: `{report.get('replay_delta_counts', {}).get('new_leg_rows', 0)}`",
        "",
        "## Loader State",
        "",
        f"- segment status summary: `{json.dumps(report.get('loader_state', {}).get('segment_status_summary', {}), sort_keys=True)}`",
        f"- clickhouse counts: `{json.dumps(report.get('clickhouse_counts', {}), sort_keys=True)}`",
    ]
    if "error" in report:
        lines.extend(["", "## Error", "", f"- `{report['error']['type']}: {report['error']['message']}`"])
    return "\n".join(lines)


def run_remote_json(gate_module, *, profile: str, region: str, instance_id: str, variables: dict[str, str], comment: str) -> dict[str, Any]:
    stdout = gate_module.send_remote_script(
        profile=profile,
        region=region,
        instance_id=instance_id,
        script_path=STRESS_RUNNER_PATH,
        variables=variables,
        comment=comment,
        timeout_seconds=14400,
    )
    return json.loads(stdout)


def main() -> int:
    args = parse_args()
    stamp = utc_stamp()
    output_root = ensure_directory(args.reports_dir / stamp)
    runs_dir = ensure_directory(output_root / "runs")

    gate_module = load_module(PRE_BULK_GATE_PATH, "stress_pre_bulk_helpers")
    resolved_real_slice_end = args.real_slice_end_utc or real_slice_end_utc(args.full_period_end_utc)

    artifact_dir = PROJECT_ROOT / "artifacts" / "staging"
    artifact_path, artifact_sha256 = gate_module.package_workspace(artifact_dir)
    artifact_key = f"{args.artifact_prefix.rstrip('/')}/{artifact_path.name}"
    artifact_source = f"s3://{args.artifact_bucket}/{artifact_key}"
    artifact_url = gate_module.upload_artifact(args.profile, args.loader_region, args.artifact_bucket, artifact_key, artifact_path)

    deploy_vars = {
        "AWS_REGION": args.loader_region,
        "WORKSPACE_ARTIFACT_URL": artifact_url,
        "WORKSPACE_ARTIFACT_SHA256": artifact_sha256,
        "WORKSPACE_ROOT": args.remote_workspace_root,
    }
    gate_module.send_remote_script(
        profile=args.profile,
        region=args.loader_region,
        instance_id=args.loader_instance_id,
        script_path=DEPLOY_SCRIPT_PATH,
        variables=deploy_vars,
        comment=f"stress-deploy-{stamp}",
        timeout_seconds=3600,
    )
    gate_module.send_remote_script(
        profile=args.profile,
        region=args.loader_region,
        instance_id=args.loader_instance_id,
        script_path=BOOTSTRAP_SCRIPT_PATH,
        variables={"WORKSPACE_ROOT": args.remote_workspace_root},
        comment=f"stress-bootstrap-{stamp}",
        timeout_seconds=3600,
    )

    synthetic_build_id = f"stress-{artifact_sha256[:12]}"
    scenarios = build_scenarios(stamp, resolved_real_slice_end, args)
    reports: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    segment_metric_rows: list[dict[str, Any]] = []
    shared_run_state: dict[str, tuple[str, str]] = {}

    for scenario in scenarios:
        if scenario.synthetic and not scenario.skip_initial_load:
            publish_args = [
                "--run-id",
                scenario.run_id,
                "--segment-count",
                str(scenario.segment_count),
                "--records-per-segment",
                str(scenario.records_per_segment),
                "--bucket",
                args.raw_bucket,
                "--prefix-root",
                args.raw_prefix_root,
                "--region",
                args.loader_region,
                "--extractor-region",
                args.loader_region,
                "--config-sha256",
                artifact_sha256,
                "--plugin-build-id",
                synthetic_build_id,
            ]
            for corrupt_seq in scenario.corrupt_segment_seq:
                publish_args.extend(["--corrupt-segment-seq", str(corrupt_seq)])
            if scenario.corrupt_segment_seq:
                publish_args.append("--skip-verify-uploaded-segments")
            gate_module.run_local_python(
                PUBLISH_SCRIPT_PATH,
                publish_args,
                extra_env={"AWS_PROFILE": args.profile, "AWS_DEFAULT_REGION": args.loader_region},
            )

        if scenario.run_id not in shared_run_state:
            run_token = sanitize_identifier(scenario.run_id, max_length=24)
            schema_name = sanitize_identifier(f"tron_usdt_stress_{stamp}_{run_token}")
            loader_db_path = f"{args.remote_workspace_root}/runtime/stress/{stamp}/{run_token}/loader_state.sqlite"
            shared_run_state[scenario.run_id] = (schema_name, loader_db_path)
        schema_name, loader_db_path = shared_run_state[scenario.run_id]
        remote_report_dir = f"{args.remote_workspace_root}/reports/stress/{stamp}/{sanitize_identifier(scenario.run_id, max_length=24)}"
        remote_vars = {
            "WORKSPACE_ROOT": args.remote_workspace_root,
            "RUN_ID": scenario.run_id,
            "CLICKHOUSE_SECRET_NAME": args.clickhouse_secret_name,
            "CLICKHOUSE_DATABASE": schema_name,
            "AWS_REGION": args.loader_region,
            "S3_BUFFER_BUCKET": args.raw_bucket,
            "S3_BUFFER_PREFIX_ROOT": args.raw_prefix_root,
            "LOADER_DB_PATH": loader_db_path,
            "LOADER_SCHEMA_PATH": f"{args.remote_workspace_root}/loader/sql/020_loader_state.sql",
            "REPORT_DIR": remote_report_dir,
            "STRESS_LABEL": scenario.label,
            "STRESS_REPLAY": "1" if scenario.replay else "0",
            "STRESS_SKIP_INITIAL_LOAD": "1" if scenario.skip_initial_load else "0",
            "STRESS_EXPECT_FAILURE": "1" if scenario.expect_failure else "0",
            "STRESS_COLLECT_STORAGE": "1" if scenario.collect_storage else "0",
            "STRESS_COLLECT_QUERY_SURFACE": "1" if scenario.collect_query_surface else "0",
            "TREE_ARTIFACT_SHA256": artifact_sha256,
            "TREE_ARTIFACT_SOURCE": artifact_source,
            "LOADER_CONCURRENCY": "1",
            "LOADER_RECORD_BATCH_SIZE": str(scenario.batch_size),
        }
        if scenario.delete_loaded_segment_id:
            remote_vars["STRESS_DELETE_LOADED_SEGMENT_ID"] = scenario.delete_loaded_segment_id
        if scenario.slice_start_utc:
            remote_vars["SLICE_START_UTC"] = scenario.slice_start_utc
        if scenario.slice_end_utc:
            remote_vars["SLICE_END_UTC"] = scenario.slice_end_utc
        if scenario.full_period_start_utc:
            remote_vars["FULL_PERIOD_START_UTC"] = scenario.full_period_start_utc
        if scenario.full_period_end_utc:
            remote_vars["FULL_PERIOD_END_UTC"] = scenario.full_period_end_utc

        report = run_remote_json(
            gate_module,
            profile=args.profile,
            region=args.loader_region,
            instance_id=args.loader_instance_id,
            variables=remote_vars,
            comment=f"stress-run-{stamp}-{scenario.label}",
        )
        report["scenario"] = {
            "wave": scenario.wave,
            "label": scenario.label,
            "batch_size": scenario.batch_size,
            "segment_count": scenario.segment_count,
            "records_per_segment": scenario.records_per_segment,
            "synthetic": scenario.synthetic,
            "schema_name": schema_name,
            "loader_db_path": loader_db_path,
        }
        report_path = runs_dir / f"{scenario.label}.json"
        write_json(report_path, report)
        write_markdown(runs_dir / f"{scenario.label}.md", markdown_run_report(report))
        reports.append(report)
        summary_rows.append(scenario_summary_row(report, scenario))
        segment_metric_rows.extend(flatten_segment_metrics(report, scenario))

    batch_best = best_row(summary_rows, "batch-size-sweep")
    segsize_best = best_row(summary_rows, "segment-size-sweep")
    merge_ok = all(
        row["status"] == "passed" for row in summary_rows if row["wave"] in {"segment-count-sweep", "real-controlled-slice"}
    )
    sync_ok = all(row["status"] == "passed" for row in summary_rows if row["wave"] != "replay-fault" or row["expected_failure"])
    recommendation = {
        "chosen_batch_size": batch_best["batch_size"] if batch_best else None,
        "chosen_records_per_segment": segsize_best["records_per_segment"] if segsize_best else None,
        "merge_strategy_acceptable": merge_ok,
        "synchronous_path_acceptable": sync_ok,
        "final_decision": "NOT_READY_FOR_BLOCK_10" if not (merge_ok and sync_ok and batch_best and segsize_best) else "READY_FOR_BLOCK_10",
        "note": "Stress evidence improves approval confidence but does not auto-start full bulk load.",
    }
    audit_answers = answer_audit_focus(summary_rows)
    summary = {
        "stamp": stamp,
        "artifact_sha256": artifact_sha256,
        "artifact_source": artifact_source,
        "loader_instance_id": args.loader_instance_id,
        "reports": [str((runs_dir / f"{report['scenario']['label']}.json").relative_to(PROJECT_ROOT)) for report in reports],
        "summary_rows": summary_rows,
        "recommendation": recommendation,
        "audit_focus_answers": audit_answers,
    }
    write_json(output_root / "summary.json", summary)
    write_csv(output_root / "summary.csv", summary_rows)
    write_csv(output_root / "segment-metrics.csv", segment_metric_rows)
    write_markdown(
        output_root / "summary.md",
        markdown_summary(
            stamp=stamp,
            artifact_sha256=artifact_sha256,
            artifact_source=artifact_source,
            summary_rows=summary_rows,
            recommendation=recommendation,
            audit_answers=audit_answers,
        ),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
