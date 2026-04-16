#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REMOTE_WORKSPACE_ROOT = "/srv/local-tron-usdt-backfill"
DEFAULT_ARTIFACT_BUCKET = "goldusdt-v2-stage-913378704801-ops"
DEFAULT_ARTIFACT_PREFIX = "codex/local-tron-usdt-backfill"
DEFAULT_RAW_BUCKET = "goldusdt-v2-stage-913378704801-raw"
DEFAULT_RAW_PREFIX_ROOT = "providers/tron-usdt-backfill/usdt-transfer-oneoff"
DEFAULT_CLICKHOUSE_SECRET = "goldusdt-v2-stage-clickhouse"


def utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ").lower()


def sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def should_exclude(relative_path: str) -> bool:
    normalized = relative_path.replace("\\", "/")
    prefixes = (
        ".git/",
        ".idea/",
        ".vscode/",
        "raw/",
        "logs/",
        "reports/",
        "runtime/",
        "artifacts/deploy/",
        "artifacts/staging/",
        "artifacts/plugins/",
        "build/",
        ".gradle/",
    )
    if normalized in {"local-tron-usdt-backfill", "."}:
        return False
    if any(part == "__pycache__" for part in normalized.split("/")):
        return True
    return normalized.startswith(prefixes)


def package_workspace(output_dir: Path) -> tuple[Path, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = output_dir / f"workspace-{utc_compact()}.tar.gz"
    with tarfile.open(artifact_path, "w:gz") as archive:
        for path in sorted(PROJECT_ROOT.rglob("*")):
            if path == artifact_path:
                continue
            relative = path.relative_to(PROJECT_ROOT)
            if should_exclude(relative.as_posix()):
                continue
            if path.is_dir():
                continue
            archive.add(path, arcname=Path(PROJECT_ROOT.name) / relative)
    return artifact_path, sha256_file(artifact_path)


def aws(profile: str, region: str, args: list[str], *, input_json: dict | None = None) -> str:
    command = ["aws", "--profile", profile, "--region", region, *args]
    if input_json is None:
        completed = subprocess.run(command, capture_output=True, text=True, check=True)
    else:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as handle:
            json.dump(input_json, handle)
            temp_json = handle.name
        try:
            completed = subprocess.run(command + ["--cli-input-json", f"file://{temp_json}"], capture_output=True, text=True, check=True)
        finally:
            Path(temp_json).unlink(missing_ok=True)
    return completed.stdout.strip()


def upload_artifact(profile: str, region: str, bucket: str, key: str, local_path: Path) -> str:
    subprocess.run(
        ["aws", "--profile", profile, "--region", region, "s3", "cp", str(local_path), f"s3://{bucket}/{key}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return aws(profile, region, ["s3", "presign", f"s3://{bucket}/{key}", "--expires-in", "3600"])


def run_local_python(script_path: Path, args: list[str], extra_env: dict[str, str] | None = None) -> dict:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    completed = subprocess.run([sys.executable, str(script_path), *args], capture_output=True, text=True, check=True, env=env)
    return json.loads(completed.stdout)


def send_remote_script(
    *,
    profile: str,
    region: str,
    instance_id: str,
    script_path: Path,
    variables: dict[str, str],
    comment: str,
    timeout_seconds: int = 3600,
) -> str:
    script_body = script_path.read_text(encoding="utf-8")
    remote_path = f"/tmp/{script_path.name}"
    exports = [f"export {key}={json.dumps(value)}" for key, value in variables.items()]
    commands = [
        "set -euo pipefail",
        *exports,
        f"cat > {remote_path} <<'EOF_SCRIPT'",
        *script_body.splitlines(),
        "EOF_SCRIPT",
        f"chmod +x {remote_path}",
        f"bash {remote_path}",
    ]
    payload = {
        "DocumentName": "AWS-RunShellScript",
        "InstanceIds": [instance_id],
        "Comment": comment,
        "Parameters": {"commands": commands},
    }
    command_id = aws(profile, region, ["ssm", "send-command", "--query", "Command.CommandId", "--output", "text"], input_json=payload)
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            status = aws(
                profile,
                region,
                [
                    "ssm",
                    "get-command-invocation",
                    "--command-id",
                    command_id,
                    "--instance-id",
                    instance_id,
                    "--query",
                    "Status",
                    "--output",
                    "text",
                ],
            )
        except subprocess.CalledProcessError:
            time.sleep(5)
            continue
        if status in {"Pending", "InProgress", "Delayed"}:
            time.sleep(5)
            continue
        stdout = aws(
            profile,
            region,
            [
                "ssm",
                "get-command-invocation",
                "--command-id",
                command_id,
                "--instance-id",
                instance_id,
                "--query",
                "StandardOutputContent",
                "--output",
                "text",
            ],
        )
        stderr = aws(
            profile,
            region,
            [
                "ssm",
                "get-command-invocation",
                "--command-id",
                command_id,
                "--instance-id",
                instance_id,
                "--query",
                "StandardErrorContent",
                "--output",
                "text",
            ],
        )
        if status != "Success":
            raise RuntimeError(f"{script_path.name} failed with status {status}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
        if stderr.strip():
            print(stderr.strip(), file=sys.stderr)
        return stdout.strip()
    raise TimeoutError(f"Timed out waiting for remote script {script_path.name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the three-part pre-bulk gate against the loader host.")
    parser.add_argument("--loader-instance-id", required=True)
    parser.add_argument("--profile", default="ai-agents-dev")
    parser.add_argument("--loader-region", default="eu-central-1")
    parser.add_argument("--artifact-bucket", default=DEFAULT_ARTIFACT_BUCKET)
    parser.add_argument("--artifact-prefix", default=DEFAULT_ARTIFACT_PREFIX)
    parser.add_argument("--raw-bucket", default=DEFAULT_RAW_BUCKET)
    parser.add_argument("--raw-prefix-root", default=DEFAULT_RAW_PREFIX_ROOT)
    parser.add_argument("--clickhouse-secret-name", default=DEFAULT_CLICKHOUSE_SECRET)
    parser.add_argument("--remote-workspace-root", default=DEFAULT_REMOTE_WORKSPACE_ROOT)
    parser.add_argument("--medium-record-count", type=int, default=10000)
    parser.add_argument("--loader-record-batch-size", type=int, default=1000)
    parser.add_argument("--reports-dir", type=Path, default=PROJECT_ROOT / "reports" / "gates")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    gate_stamp = utc_compact()
    tiny_run_id = f"prebulk-demo-{gate_stamp}"
    medium_run_id = f"prebulk-medium-{gate_stamp}"
    demo_schema = f"tron_usdt_gate_demo_{gate_stamp.replace('-', '_')}"[:60]
    medium_schema = f"tron_usdt_gate_medium_{gate_stamp.replace('-', '_')}"[:60]

    artifact_dir = PROJECT_ROOT / "artifacts" / "staging"
    artifact_path, artifact_sha256 = package_workspace(artifact_dir)
    artifact_key = f"{args.artifact_prefix.rstrip('/')}/{artifact_path.name}"
    artifact_source = f"s3://{args.artifact_bucket}/{artifact_key}"
    artifact_url = upload_artifact(args.profile, args.loader_region, args.artifact_bucket, artifact_key, artifact_path)

    publish_script = PROJECT_ROOT / "scripts" / "demo" / "20_publish_demo_run_to_s3.py"
    synthetic_build_id = f"prebulk-{artifact_sha256[:12]}"
    common_publish_args = [
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
    tiny_publish = run_local_python(
        publish_script,
        ["--run-id", tiny_run_id, "--record-count", "3", *common_publish_args],
        extra_env={"AWS_PROFILE": args.profile, "AWS_DEFAULT_REGION": args.loader_region},
    )
    medium_publish = run_local_python(
        publish_script,
        [
            "--run-id",
            medium_run_id,
            "--record-count",
            str(args.medium_record_count),
            "--base-block",
            "54400001",
            *common_publish_args,
        ],
        extra_env={"AWS_PROFILE": args.profile, "AWS_DEFAULT_REGION": args.loader_region},
    )

    bootstrap_script = PROJECT_ROOT / "scripts" / "provision" / "80_bootstrap_loader_host.sh"
    deploy_script = PROJECT_ROOT / "scripts" / "provision" / "70_deploy_workspace_artifact.sh"
    validation_script = PROJECT_ROOT / "scripts" / "validate" / "50_server_disposable_validation.sh"
    rehearsal_script = PROJECT_ROOT / "scripts" / "validate" / "60_medium_rehearsal.sh"

    send_remote_script(
        profile=args.profile,
        region=args.loader_region,
        instance_id=args.loader_instance_id,
        script_path=deploy_script,
        variables={
            "AWS_REGION": args.loader_region,
            "WORKSPACE_ROOT": args.remote_workspace_root,
            "WORKSPACE_ARTIFACT_URL": artifact_url,
            "WORKSPACE_ARTIFACT_SHA256": artifact_sha256,
        },
        comment="pre-bulk-gate-deploy-workspace",
        timeout_seconds=1800,
    )
    send_remote_script(
        profile=args.profile,
        region=args.loader_region,
        instance_id=args.loader_instance_id,
        script_path=bootstrap_script,
        variables={"WORKSPACE_ROOT": args.remote_workspace_root},
        comment="pre-bulk-gate-bootstrap-loader",
        timeout_seconds=1800,
    )
    validation_stdout = send_remote_script(
        profile=args.profile,
        region=args.loader_region,
        instance_id=args.loader_instance_id,
        script_path=validation_script,
        variables={
            "AWS_REGION": args.loader_region,
            "WORKSPACE_ROOT": args.remote_workspace_root,
            "RUN_ID": tiny_run_id,
            "CLICKHOUSE_SECRET_NAME": args.clickhouse_secret_name,
            "CLICKHOUSE_DATABASE": demo_schema,
            "TREE_ARTIFACT_SHA256": artifact_sha256,
            "WORKSPACE_ARTIFACT_SOURCE": artifact_source,
            "LOADER_RECORD_BATCH_SIZE": str(args.loader_record_batch_size),
            "LOADER_CONCURRENCY": "1",
        },
        comment="pre-bulk-gate-disposable-validation",
        timeout_seconds=3600,
    )
    rehearsal_stdout = send_remote_script(
        profile=args.profile,
        region=args.loader_region,
        instance_id=args.loader_instance_id,
        script_path=rehearsal_script,
        variables={
            "AWS_REGION": args.loader_region,
            "WORKSPACE_ROOT": args.remote_workspace_root,
            "RUN_ID": medium_run_id,
            "CLICKHOUSE_SECRET_NAME": args.clickhouse_secret_name,
            "CLICKHOUSE_DATABASE": medium_schema,
            "LOADER_RECORD_BATCH_SIZE": str(args.loader_record_batch_size),
            "LOADER_CONCURRENCY": "1",
        },
        comment="pre-bulk-gate-medium-rehearsal",
        timeout_seconds=3600,
    )

    args.reports_dir.mkdir(parents=True, exist_ok=True)
    validation_report_path = args.reports_dir / f"{tiny_run_id}-server-disposable-validation.json"
    rehearsal_report_path = args.reports_dir / f"{medium_run_id}-medium-rehearsal.json"
    validation_report_path.write_text(validation_stdout + ("\n" if not validation_stdout.endswith("\n") else ""), encoding="utf-8")
    rehearsal_report_path.write_text(rehearsal_stdout + ("\n" if not rehearsal_stdout.endswith("\n") else ""), encoding="utf-8")

    checklist_script = PROJECT_ROOT / "scripts" / "validate" / "70_freeze_bulk_run_checklist.py"
    local_loader_env = PROJECT_ROOT / "configs" / "loader" / "clickhouse.env.example"
    checklist_json_path = args.reports_dir / f"pre-bulk-gate-{gate_stamp}.json"
    checklist_md_path = args.reports_dir / f"pre-bulk-gate-{gate_stamp}.md"
    subprocess.run(
        [
            sys.executable,
            str(checklist_script),
            "--validation-report",
            str(validation_report_path),
            "--rehearsal-report",
            str(rehearsal_report_path),
            "--output-json",
            str(checklist_json_path),
            "--output-markdown",
            str(checklist_md_path),
            "--loader-env-file",
            str(local_loader_env),
            "--loader-instance-id",
            args.loader_instance_id,
            "--workspace-artifact-sha256",
            artifact_sha256,
            "--workspace-artifact-source",
            artifact_source,
            "--target-schema",
            medium_schema,
            "--loader-concurrency",
            "1",
            "--loader-record-batch-size",
            str(args.loader_record_batch_size),
        ],
        check=True,
    )

    summary = {
        "artifact": {
            "path": str(artifact_path),
            "sha256": artifact_sha256,
            "source": artifact_source,
        },
        "tiny_run_publish": tiny_publish,
        "medium_run_publish": medium_publish,
        "validation_report": str(validation_report_path),
        "rehearsal_report": str(rehearsal_report_path),
        "checklist_json": str(checklist_json_path),
        "checklist_markdown": str(checklist_md_path),
        "schemas": {
            "demo": demo_schema,
            "medium": medium_schema,
        },
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
