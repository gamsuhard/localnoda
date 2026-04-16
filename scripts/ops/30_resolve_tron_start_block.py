#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import random
import sys
import time
import urllib.error
import urllib.request


DEFAULT_API_BASE = "https://api.trongrid.io"


def tron_post(api_base: str, path: str, payload: dict, retries: int = 8) -> dict:
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "local-tron-usdt-backfill/1.0",
    }
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(api_base + path, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code == 429 and attempt < retries - 1:
                time.sleep(min(10.0, 1.0 * (2 ** attempt)) + random.random() * 0.25)
                continue
            raise
        except Exception as exc:  # pragma: no cover - best effort network retry
            last_error = exc
            if attempt < retries - 1:
                time.sleep(min(5.0, 0.5 * (attempt + 1)))
                continue
            raise
    assert last_error is not None
    raise last_error


def to_utc_iso(timestamp_ms: int) -> str:
    return dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_target_timestamp(value: str) -> int:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(parsed.timestamp() * 1000)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-timestamp", default="2023-09-01T00:00:00Z")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    target_ms = parse_target_timestamp(args.target_timestamp)
    target_iso = to_utc_iso(target_ms)
    cache: dict[int, dict] = {}

    def get_block(num: int) -> dict:
        if num not in cache:
            cache[num] = tron_post(args.api_base, "/wallet/getblockbynum", {"num": num})
        return cache[num]

    def get_block_ts(num: int) -> tuple[int, str]:
        block = get_block(num)
        return block["block_header"]["raw_data"]["timestamp"], block["blockID"]

    latest = tron_post(args.api_base, "/wallet/getnowblock", {})["block_header"]["raw_data"]["number"]
    low, high = 0, latest
    while low < high:
        mid = (low + high) // 2
        ts_ms, _ = get_block_ts(mid)
        if ts_ms < target_ms:
            low = mid + 1
        else:
            high = mid

    start_block = low
    start_ts_ms, start_block_id = get_block_ts(start_block)
    previous_ts_ms, previous_block_id = get_block_ts(start_block - 1)

    result = {
        "target_timestamp_utc": target_iso,
        "boundary_rule": "first block with block_timestamp >= target_timestamp_utc",
        "start_block_number": start_block,
        "start_block_timestamp_utc": to_utc_iso(start_ts_ms),
        "start_block_id": start_block_id,
        "previous_block_number": start_block - 1,
        "previous_block_timestamp_utc": to_utc_iso(previous_ts_ms),
        "previous_block_id": previous_block_id,
        "api_base": args.api_base,
    }

    output_text = json.dumps(result, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(output_text + "\n")
    print(output_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
