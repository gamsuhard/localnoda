#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import time


QUOTA_CODE = "L-1216C47A"
SERVICE_CODE = "ec2"


def aws(profile: str, region: str, args: list[str]) -> dict:
    cmd = ["aws", "--profile", profile, "--region", region, *args]
    res = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(res.stdout)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="ai-agents-dev")
    parser.add_argument("--region", default="ap-southeast-1")
    parser.add_argument("--target", type=float, default=32.0)
    parser.add_argument("--interval-seconds", type=int, default=30)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    args = parser.parse_args()

    deadline = time.time() + args.timeout_seconds
    last_status = None
    last_value = None

    while time.time() < deadline:
        quota = aws(
            args.profile,
            args.region,
            ["service-quotas", "get-service-quota", "--service-code", SERVICE_CODE, "--quota-code", QUOTA_CODE],
        )["Quota"]
        value = float(quota["Value"])

        history = aws(
            args.profile,
            args.region,
            [
                "service-quotas",
                "list-requested-service-quota-change-history-by-quota",
                "--service-code",
                SERVICE_CODE,
                "--quota-code",
                QUOTA_CODE,
                "--quota-requested-at-level",
                "ACCOUNT",
            ],
        ).get("RequestedQuotas", [])

        status = history[0]["Status"] if history else "NO_REQUEST"

        if status != last_status or value != last_value:
            print(
                json.dumps(
                    {
                        "quota_value": value,
                        "latest_request_status": status,
                        "latest_request_id": history[0]["Id"] if history else None,
                        "latest_case_id": history[0].get("CaseId") if history else None,
                    },
                    ensure_ascii=True,
                ),
                flush=True,
            )
            last_status = status
            last_value = value

        if value >= args.target:
            return 0

        time.sleep(args.interval_seconds)

    print("Timed out waiting for quota increase", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
