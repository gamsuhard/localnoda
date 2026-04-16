#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import time


def aws(profile: str, region: str, args: list[str]) -> str:
    cmd = ["aws", "--profile", profile, "--region", region, *args]
    res = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return res.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--profile", default="ai-agents-dev")
    parser.add_argument("--region", default="ap-southeast-1")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()

    deadline = time.time() + args.timeout_seconds
    seen_running = False

    while time.time() < deadline:
        state = aws(
            args.profile,
            args.region,
            [
                "ec2",
                "describe-instances",
                "--instance-ids",
                args.instance_id,
                "--query",
                "Reservations[0].Instances[0].State.Name",
                "--output",
                "text",
            ],
        )
        if state == "running":
            seen_running = True

        info_raw = aws(
            args.profile,
            args.region,
            [
                "ssm",
                "describe-instance-information",
                "--filters",
                f"Key=InstanceIds,Values={args.instance_id}",
                "--output",
                "json",
            ],
        )
        info = json.loads(info_raw)
        if info.get("InstanceInformationList"):
            ping = info["InstanceInformationList"][0].get("PingStatus")
            if seen_running and ping == "Online":
                print(f"INSTANCE_ID={args.instance_id}")
                print("SSM_STATUS=Online")
                return 0

        time.sleep(10)

    print(f"Timed out waiting for SSM on {args.instance_id}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
