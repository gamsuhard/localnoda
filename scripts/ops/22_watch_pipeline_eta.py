#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import subprocess
import sys
import time
from datetime import datetime, timezone


SG_REGION = "ap-southeast-1"
SG_INSTANCE = "i-0b9c2749efd34e796"
FR_REGION = "eu-central-1"
FR_INSTANCE = "i-0835e4602efaacf59"
DEFAULT_PROFILE = "ai-agents-dev"
DEFAULT_BUCKET = "goldusdt-v2-stage-913378704801-raw"
DEFAULT_PREFIX_ROOT = "providers/tron-usdt-backfill/usdt-transfer-oneoff"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: list[str], env: dict[str, str]) -> str:
    result = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
    return result.stdout.strip()


def aws(env: dict[str, str], args: list[str]) -> str:
    return run(["aws", *args], env)


def ssm_run(env: dict[str, str], region: str, instance_id: str, commands: list[str], timeout_seconds: int = 180) -> str:
    payload = json.dumps({"commands": commands})
    command_id = aws(
        env,
        [
            "ssm",
            "send-command",
            "--region",
            region,
            "--instance-ids",
            instance_id,
            "--document-name",
            "AWS-RunShellScript",
            "--parameters",
            payload,
            "--query",
            "Command.CommandId",
            "--output",
            "text",
        ],
    ).strip()

    deadline = time.time() + timeout_seconds
    last = None
    while time.time() < deadline:
        try:
            raw = aws(
                env,
                [
                    "ssm",
                    "get-command-invocation",
                    "--region",
                    region,
                    "--command-id",
                    command_id,
                    "--instance-id",
                    instance_id,
                    "--output",
                    "json",
                ],
            )
        except subprocess.CalledProcessError:
            time.sleep(2)
            continue
        last = json.loads(raw)
        if last["Status"] in {"Success", "Failed", "TimedOut", "Cancelled", "Cancelling"}:
            break
        time.sleep(2)

    if last is None:
        raise RuntimeError(f"SSM returned no result for {instance_id}")
    if last["Status"] != "Success":
        raise RuntimeError(
            f"SSM failed for {instance_id} status={last['Status']} "
            f"stdout={last.get('StandardOutputContent', '')} "
            f"stderr={last.get('StandardErrorContent', '')}"
        )
    return last.get("StandardOutputContent", "").strip()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_hours(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def build_sg_commands() -> list[str]:
    return [
        "set -euo pipefail",
        "WS=/srv/local-tron-usdt-backfill",
        "curl -sf --max-time 10 -H 'Content-Type: application/json' -d '{}' http://127.0.0.1:8090/wallet/getnodeinfo >/tmp/tron-nodeinfo.json 2>/dev/null || true",
        "RUN_ROOT=$(ls -1dt \"$WS\"/raw/runs/* 2>/dev/null | head -n1 || true)",
        "export RUN_ROOT WS",
        """python3 - <<'PY'
import json, os
from pathlib import Path
from datetime import datetime, timezone

ws = Path(os.environ["WS"])
run_root = Path(os.environ["RUN_ROOT"]) if os.environ.get("RUN_ROOT") else None
payload = {"timestamp_utc": datetime.now(timezone.utc).isoformat()}
if not run_root or not run_root.exists():
    print(json.dumps({"error": "run_root_missing", **payload}))
    raise SystemExit(0)

run_id = run_root.name
checkpoint_path = run_root / "checkpoints" / "extraction.json"
run_manifest_path = run_root / "manifests" / "run.json"
segments_dir = run_root / "segments"
checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8")) if checkpoint_path.exists() else {}
run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8")) if run_manifest_path.exists() else {}
sealed_count = len(list(segments_dir.glob("*.ndjson.gz")))
partial_count = len(list(segments_dir.glob("*.partial")))
orphaned_partial_count = len(list(segments_dir.glob("*.partial.orphaned.*")))
latest_manifest = None
manifest_paths = sorted((run_root / "manifests").glob("*.manifest.json")) if run_root else []
if manifest_paths:
    latest_manifest = json.loads(manifest_paths[-1].read_text(encoding="utf-8"))

nodeinfo = None
nodeinfo_path = Path("/tmp/tron-nodeinfo.json")
if nodeinfo_path.exists():
    try:
        nodeinfo = json.loads(nodeinfo_path.read_text(encoding="utf-8"))
    except Exception:
        nodeinfo = None

payload.update(
    {
        "run_id": run_id,
        "sealed_segments": sealed_count,
        "partial_segments": partial_count,
        "orphaned_partial_segments": orphaned_partial_count,
        "start_block": run_manifest.get("start_block"),
        "resolved_end_block": run_manifest.get("resolved_end_block"),
        "run_manifest_segment_count": run_manifest.get("segment_count"),
        "run_status": run_manifest.get("status"),
        "latest_sealed_segment_id": latest_manifest.get("segment_id") if latest_manifest else None,
        "latest_sealed_block_to": latest_manifest.get("block_to") if latest_manifest else None,
        "latest_sealed_record_count": latest_manifest.get("record_count") if latest_manifest else None,
        "latest_sealed_file_size_bytes": latest_manifest.get("file_size_bytes") if latest_manifest else None,
        "last_uploaded_segment_id": checkpoint.get("last_uploaded_segment_id"),
        "last_uploaded_block_number": checkpoint.get("last_uploaded_block_number"),
        "next_start_block_number": checkpoint.get("next_start_block_number"),
        "checkpoint_updated_at": checkpoint.get("updated_at"),
    }
)
if nodeinfo:
    payload.update(
        {
            "node_block": nodeinfo.get("block"),
            "begin_sync_num": nodeinfo.get("beginSyncNum"),
            "active_connect_count": nodeinfo.get("activeConnectCount"),
        }
    )
print(json.dumps(payload))
PY""",
    ]


def build_fr_commands() -> list[str]:
    return [
        "set -euo pipefail",
        "WS=/srv/local-tron-usdt-backfill",
        "export WORKSPACE_ROOT=$WS",
        "export AWS_REGION=eu-central-1",
        "export CLICKHOUSE_SECRET_NAME=goldusdt-v2-stage-clickhouse",
        "export CLICKHOUSE_DATABASE=tron_usdt_reprmonth_20231103_20231203_20260417t221647z",
        "bash \"$WS/scripts/run/35_prepare_loader_runtime.sh\" >/dev/null",
        "set -a",
        "source \"$WS/configs/loader/clickhouse.env\"",
        "set +a",
        "CH_JSON=$(clickhouse-client --host \"$CLICKHOUSE_HOST\" --port \"$CLICKHOUSE_PORT\" --secure --user \"$CLICKHOUSE_USER\" --password \"$CLICKHOUSE_PASSWORD\" --database \"$CLICKHOUSE_DATABASE\" --query \"SELECT toUInt64(count()) AS events, toUInt64(uniqExact(source_segment_id)) AS segs, toUInt64(count())*2 AS legs FROM trc20_transfer_events\" --format JSONEachRow)",
        "export CH_JSON",
        """python3 - <<'PY'
import json, os, sqlite3, subprocess
from datetime import datetime, timezone

ws = os.environ.get("WORKSPACE_ROOT", "/srv/local-tron-usdt-backfill")
db_path = os.path.join(ws, "runtime", "loader_state.sqlite")
payload = {"timestamp_utc": datetime.now(timezone.utc).isoformat()}

conn = sqlite3.connect(db_path)
status_counts = {row[0]: row[1] for row in conn.execute("SELECT status, count(*) FROM loaded_segments GROUP BY status")}
recent = conn.execute(
    "SELECT segment_id, load_finished_at FROM loaded_segments WHERE status='validated' ORDER BY load_finished_at DESC LIMIT 1"
).fetchone()
conn.close()

worker_services = {}
for slot in (1, 2):
    service_output = subprocess.check_output(
        [
            "systemctl",
            "show",
            f"local-tron-incremental-loader@{slot}.service",
            "-p",
            "ActiveState",
            "-p",
            "SubState",
            "-p",
            "MainPID",
            "-p",
            "ExecMainStartTimestamp",
        ],
        text=True,
    )
    service_map = {}
    for line in service_output.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            service_map[key] = value
    worker_services[str(slot)] = {
        "active": service_map.get("ActiveState"),
        "substate": service_map.get("SubState"),
        "main_pid": service_map.get("MainPID"),
        "started_at": service_map.get("ExecMainStartTimestamp"),
    }

ch = json.loads(os.environ["CH_JSON"])
payload.update(
    {
        "loader_status_counts": status_counts,
        "latest_validated_segment_id": recent[0] if recent else None,
        "latest_validated_at": recent[1] if recent else None,
        "clickhouse_events": ch.get("events"),
        "clickhouse_segments": ch.get("segs"),
        "clickhouse_legs": ch.get("legs"),
        "loader_workers": worker_services,
    }
)
print(json.dumps(payload))
PY""",
    ]


def get_s3_summary(env: dict[str, str], bucket: str, prefix_root: str, run_id: str) -> dict[str, int]:
    prefix = f"s3://{bucket}/{prefix_root}/runs/{run_id}/segments/"
    output = run(
        ["aws", "s3", "ls", prefix, "--recursive", "--summarize", "--region", FR_REGION],
        env,
    )
    total_objects = 0
    total_size = 0
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("Total Objects:"):
            total_objects = int(stripped.split(":", 1)[1].strip())
        if stripped.startswith("Total Size:"):
            total_size = int(stripped.split(":", 1)[1].strip())
    return {"s3_segments": total_objects, "s3_segment_bytes": total_size}


def capture(env: dict[str, str], bucket: str, prefix_root: str) -> dict:
    singapore = json.loads(ssm_run(env, SG_REGION, SG_INSTANCE, build_sg_commands()))
    run_id = singapore.get("run_id")
    if not run_id:
        raise RuntimeError(f"run_id missing from Singapore sample: {singapore}")
    s3_summary = get_s3_summary(env, bucket, prefix_root, run_id)
    frankfurt = json.loads(ssm_run(env, FR_REGION, FR_INSTANCE, build_fr_commands()))
    return {
        "captured_at_utc": utc_now_iso(),
        "singapore": singapore,
        "s3": s3_summary,
        "frankfurt": frankfurt,
    }


def derive(prev_sample: dict, current_sample: dict) -> dict:
    singapore_prev = prev_sample["singapore"]
    singapore_cur = current_sample["singapore"]
    s3_prev = prev_sample["s3"]
    s3_cur = current_sample["s3"]
    frankfurt_prev = prev_sample["frankfurt"]
    frankfurt_cur = current_sample["frankfurt"]

    sg_prev_ts = parse_iso(singapore_prev.get("timestamp_utc")) or parse_iso(prev_sample["captured_at_utc"])
    sg_cur_ts = parse_iso(singapore_cur.get("timestamp_utc")) or parse_iso(current_sample["captured_at_utc"])
    fr_prev_ts = parse_iso(frankfurt_prev.get("timestamp_utc")) or parse_iso(prev_sample["captured_at_utc"])
    fr_cur_ts = parse_iso(frankfurt_cur.get("timestamp_utc")) or parse_iso(current_sample["captured_at_utc"])

    sg_hours = max((sg_cur_ts - sg_prev_ts).total_seconds() / 3600.0, 1e-9)
    fr_hours = max((fr_cur_ts - fr_prev_ts).total_seconds() / 3600.0, 1e-9)

    sealed_delta = (singapore_cur.get("sealed_segments") or 0) - (singapore_prev.get("sealed_segments") or 0)
    s3_delta = (s3_cur.get("s3_segments") or 0) - (s3_prev.get("s3_segments") or 0)
    ch_seg_delta = (frankfurt_cur.get("clickhouse_segments") or 0) - (frankfurt_prev.get("clickhouse_segments") or 0)
    ch_events_delta = (frankfurt_cur.get("clickhouse_events") or 0) - (frankfurt_prev.get("clickhouse_events") or 0)
    sealed_block_delta = (singapore_cur.get("latest_sealed_block_to") or 0) - (
        singapore_prev.get("latest_sealed_block_to") or 0
    )
    uploaded_block_delta = (singapore_cur.get("last_uploaded_block_number") or 0) - (
        singapore_prev.get("last_uploaded_block_number") or 0
    )

    sealed_rate_h = sealed_delta / sg_hours
    s3_rate_h = s3_delta / sg_hours
    ch_rate_h = ch_seg_delta / fr_hours
    ch_events_rate_h = ch_events_delta / fr_hours
    sealed_block_rate_h = sealed_block_delta / sg_hours
    uploaded_block_rate_h = uploaded_block_delta / sg_hours

    start_block = singapore_cur.get("start_block")
    resolved_end_block = singapore_cur.get("resolved_end_block")
    sealed_block = singapore_cur.get("latest_sealed_block_to")
    backlog_raw_to_s3 = max(0, (singapore_cur.get("sealed_segments") or 0) - (s3_cur.get("s3_segments") or 0))
    backlog_s3_to_loader = max(0, (s3_cur.get("s3_segments") or 0) - (frankfurt_cur.get("clickhouse_segments") or 0))

    extract_eta_hours = None
    if (
        start_block is not None
        and resolved_end_block is not None
        and sealed_block is not None
        and sealed_block_rate_h > 0
    ):
        remaining_blocks = max(0, resolved_end_block - sealed_block)
        extract_eta_hours = remaining_blocks / sealed_block_rate_h

    pipeline_eta_hours = None
    if extract_eta_hours is not None and ch_rate_h > 0:
        backlog_at_extract_finish = backlog_s3_to_loader + max(0.0, sealed_rate_h - ch_rate_h) * extract_eta_hours
        pipeline_eta_hours = extract_eta_hours + (backlog_at_extract_finish / ch_rate_h)

    return {
        "window_start_utc": prev_sample["captured_at_utc"],
        "window_end_utc": current_sample["captured_at_utc"],
        "sg_window_hours": sg_hours,
        "fr_window_hours": fr_hours,
        "sealed_delta_segments": sealed_delta,
        "s3_delta_segments": s3_delta,
        "clickhouse_delta_segments": ch_seg_delta,
        "clickhouse_delta_events": ch_events_delta,
        "sealed_block_delta": sealed_block_delta,
        "uploaded_block_delta": uploaded_block_delta,
        "sealed_rate_segments_per_hour": sealed_rate_h,
        "s3_rate_segments_per_hour": s3_rate_h,
        "clickhouse_rate_segments_per_hour": ch_rate_h,
        "clickhouse_rate_events_per_hour": ch_events_rate_h,
        "sealed_block_rate_per_hour": sealed_block_rate_h,
        "uploaded_block_rate_per_hour": uploaded_block_rate_h,
        "backlog_raw_to_s3_segments": backlog_raw_to_s3,
        "backlog_s3_to_clickhouse_segments": backlog_s3_to_loader,
        "extract_eta_hours": extract_eta_hours,
        "pipeline_eta_hours": pipeline_eta_hours,
    }


def write_log_line(path: pathlib.Path, line: str) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(line + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix-root", default=DEFAULT_PREFIX_ROOT)
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--max-iterations", type=int, default=0)
    parser.add_argument(
        "--log-path",
        default=str(pathlib.Path(__file__).resolve().parents[2] / "logs" / "pipeline-watch.utf8.log"),
    )
    args = parser.parse_args()

    env = os.environ.copy()
    env["AWS_PROFILE"] = args.profile

    log_path = pathlib.Path(args.log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    previous = None
    iteration = 0
    while True:
        iteration += 1
        try:
            current = capture(env, args.bucket, args.prefix_root)
            headline = (
                f"===== {current['captured_at_utc']} iteration={iteration} "
                f"run_id={current['singapore'].get('run_id')} ====="
            )
            print(headline)
            write_log_line(log_path, headline)
            write_log_line(log_path, json.dumps(current, ensure_ascii=True))

            if previous is not None:
                derived = derive(previous, current)
                summary = (
                    "summary "
                    f"sealed_rate_h={derived['sealed_rate_segments_per_hour']:.2f} "
                    f"s3_rate_h={derived['s3_rate_segments_per_hour']:.2f} "
                    f"loader_rate_h={derived['clickhouse_rate_segments_per_hour']:.2f} "
                    f"backlog_raw_s3={derived['backlog_raw_to_s3_segments']} "
                    f"backlog_s3_ch={derived['backlog_s3_to_clickhouse_segments']} "
                    f"extract_eta_h={format_hours(derived['extract_eta_hours'])} "
                    f"pipeline_eta_h={format_hours(derived['pipeline_eta_hours'])}"
                )
                print(summary)
                write_log_line(log_path, json.dumps({"derived": derived}, ensure_ascii=True))
                write_log_line(log_path, summary)

            previous = current
        except Exception as exc:
            message = f"ALERT {utc_now_iso()} pipeline_watch_failed={exc}"
            print(message, file=sys.stderr)
            write_log_line(log_path, message)

        if args.max_iterations > 0 and iteration >= args.max_iterations:
            break
        time.sleep(args.interval_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
