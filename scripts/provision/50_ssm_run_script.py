#!/usr/bin/env python3
import argparse
import json
import pathlib
import shlex
import subprocess
import sys
import tempfile
import time


def aws(profile: str, region: str, args: list[str]) -> str:
    cmd = ["aws", "--profile", profile, "--region", region, *args]
    res = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return res.stdout.strip()


def shell_quote(value: str) -> str:
    return shlex.quote(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--script", required=True)
    parser.add_argument("--profile", default="ai-agents-dev")
    parser.add_argument("--region", default="ap-southeast-1")
    parser.add_argument("--comment", default="codex-block-02")
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--set", action="append", default=[], metavar="KEY=VALUE")
    args = parser.parse_args()

    script_path = pathlib.Path(args.script)
    script_body = script_path.read_text(encoding="utf-8")
    remote_path = f"/tmp/{script_path.name}"

    exports = []
    for item in args.set:
        key, sep, value = item.partition("=")
        if not sep:
            raise SystemExit(f"Invalid --set value: {item}")
        exports.append(f"export {key}={shell_quote(value)}")

    commands = [
        "set -euo pipefail",
        *exports,
        f"cat > {shell_quote(remote_path)} <<'EOF_SCRIPT'",
        *script_body.splitlines(),
        "EOF_SCRIPT",
        f"chmod +x {shell_quote(remote_path)}",
        f"bash {shell_quote(remote_path)}",
    ]

    payload = {
        "DocumentName": "AWS-RunShellScript",
        "InstanceIds": [args.instance_id],
        "Comment": args.comment,
        "Parameters": {"commands": commands},
    }

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(payload, tmp)
        tmp_path = tmp.name

    command_id = aws(
        args.profile,
        args.region,
        ["ssm", "send-command", "--cli-input-json", f"file://{tmp_path}", "--query", "Command.CommandId", "--output", "text"],
    )

    deadline = time.time() + args.timeout_seconds
    while time.time() < deadline:
        try:
            status = aws(
                args.profile,
                args.region,
                [
                    "ssm",
                    "get-command-invocation",
                    "--command-id",
                    command_id,
                    "--instance-id",
                    args.instance_id,
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
            args.profile,
            args.region,
            [
                "ssm",
                "get-command-invocation",
                "--command-id",
                command_id,
                "--instance-id",
                args.instance_id,
                "--query",
                "StandardOutputContent",
                "--output",
                "text",
            ],
        )
        stderr = aws(
            args.profile,
            args.region,
            [
                "ssm",
                "get-command-invocation",
                "--command-id",
                command_id,
                "--instance-id",
                args.instance_id,
                "--query",
                "StandardErrorContent",
                "--output",
                "text",
            ],
        )
        if stdout:
            print(stdout)
        if stderr:
            print(stderr, file=sys.stderr)
        if status != "Success":
            print(f"Command {command_id} failed with status {status}", file=sys.stderr)
            return 1
        print(f"COMMAND_ID={command_id}")
        return 0

    print(f"Timed out waiting for command {command_id}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
