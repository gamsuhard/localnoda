#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3


DEFAULT_PROVIDER_SECRET_ARN = (
    "arn:aws:secretsmanager:eu-central-1:913378704801:"
    "secret:goldusdt-v2-stage-provider-api-keys-nzOViH"
)
DEFAULT_TRONGRID_BASE_URL = "https://api.trongrid.io"
DEFAULT_TRONGRID_API_KEY_HEADER = "TRON-PRO-API-KEY"
DEFAULT_CHAINBASE_API_BASE_URL = "https://api.chainbase.online"
DEFAULT_CHAINBASE_RAW_BASE_URL = "https://api.chainbase.com/api/v1"


@dataclass(frozen=True)
class ProviderSecrets:
    secret_arn: str
    trongrid_api_key: str
    chainbase_api_key: str
    arkham_api_key: str = ""


def boto3_session(*, profile: str = "", region: str = "eu-central-1") -> boto3.session.Session:
    if profile:
        return boto3.session.Session(profile_name=profile, region_name=region)
    return boto3.session.Session(region_name=region)


def fetch_provider_secrets(
    *,
    secret_arn: str = DEFAULT_PROVIDER_SECRET_ARN,
    profile: str = "",
    region: str = "eu-central-1",
) -> ProviderSecrets:
    session = boto3_session(profile=profile, region=region)
    client = session.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_arn)
    payload = json.loads(response["SecretString"])
    return ProviderSecrets(
        secret_arn=secret_arn,
        trongrid_api_key=str(payload.get("trongridApiKey", "") or ""),
        chainbase_api_key=str(payload.get("chainbaseApiKey", "") or ""),
        arkham_api_key=str(payload.get("arkhamApiKey", "") or ""),
    )


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def write_env_file(path: Path, values: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{key}={value}" for key, value in values.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def sync_provider_runtime_env(
    *,
    output_path: Path,
    secret_arn: str = DEFAULT_PROVIDER_SECRET_ARN,
    profile: str = "",
    region: str = "eu-central-1",
) -> dict[str, Any]:
    secrets = fetch_provider_secrets(secret_arn=secret_arn, profile=profile, region=region)
    values = {
        "AWS_REGION": region,
        "PROVIDER_SECRET_ARN": secrets.secret_arn,
        "TRONGRID_BASE_URL": DEFAULT_TRONGRID_BASE_URL,
        "TRONGRID_API_KEY_HEADER": DEFAULT_TRONGRID_API_KEY_HEADER,
        "TRONGRID_API_KEY": secrets.trongrid_api_key,
        "CHAINBASE_API_BASE_URL": DEFAULT_CHAINBASE_API_BASE_URL,
        "CHAINBASE_RAW_BASE_URL": DEFAULT_CHAINBASE_RAW_BASE_URL,
        "CHAINBASE_API_KEY": secrets.chainbase_api_key,
    }
    write_env_file(output_path, values)
    return {
        "status": "synced",
        "output_path": str(output_path),
        "secret_arn": secrets.secret_arn,
        "trongrid_present": bool(secrets.trongrid_api_key),
        "chainbase_present": bool(secrets.chainbase_api_key),
        "trongrid_length": len(secrets.trongrid_api_key),
        "chainbase_length": len(secrets.chainbase_api_key),
    }


class JsonHttpClient:
    def __init__(self, *, timeout_seconds: float = 30.0) -> None:
        self.timeout_seconds = timeout_seconds

    def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> dict[str, Any]:
        final_url = url
        if params:
            query = urllib.parse.urlencode(
                {key: value for key, value in params.items() if value is not None},
                doseq=True,
            )
            final_url = f"{url}?{query}"
        body = None
        request_headers = dict(headers or {})
        if json_body is not None:
            body = json.dumps(json_body).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")
        request = urllib.request.Request(
            final_url,
            data=body,
            headers=request_headers,
            method=method.upper(),
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw_body = response.read().decode("utf-8", errors="replace")
                return {
                    "status_code": response.status,
                    "headers": dict(response.headers),
                    "payload": json.loads(raw_body) if raw_body else {},
                    "raw_text": raw_body,
                }
        except urllib.error.HTTPError as exc:
            raw_body = exc.read().decode("utf-8", errors="replace")
            payload: Any
            try:
                payload = json.loads(raw_body) if raw_body else {}
            except json.JSONDecodeError:
                payload = {"raw_text": raw_body}
            return {
                "status_code": exc.code,
                "headers": dict(exc.headers),
                "payload": payload,
                "raw_text": raw_body,
            }


class TronGridClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        api_key_header: str = DEFAULT_TRONGRID_API_KEY_HEADER,
        transport: JsonHttpClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.api_key_header = api_key_header
        self.transport = transport or JsonHttpClient()

    def trc20_transactions(
        self,
        *,
        address: str,
        limit: int = 25,
        only_confirmed: bool = True,
    ) -> dict[str, Any]:
        headers = {"accept": "application/json", self.api_key_header: self.api_key}
        return self.transport.request(
            method="GET",
            url=f"{self.base_url}/v1/accounts/{address}/transactions/trc20",
            headers=headers,
            params={"limit": limit, "only_confirmed": str(only_confirmed).lower()},
        )


class ChainbaseRawClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        transport: JsonHttpClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.transport = transport or JsonHttpClient(timeout_seconds=60.0)

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-KEY": self.api_key,
        }

    def execute_sql(self, *, sql: str) -> dict[str, Any]:
        return self.transport.request(
            method="POST",
            url=f"{self.base_url}/query/execute",
            headers=self._headers(),
            json_body={"sql": sql},
        )

    def execution_status(self, *, execution_id: str) -> dict[str, Any]:
        return self.transport.request(
            method="GET",
            url=f"{self.base_url}/execution/{execution_id}/status",
            headers=self._headers(),
        )

    def execution_results(self, *, execution_id: str) -> dict[str, Any]:
        return self.transport.request(
            method="GET",
            url=f"{self.base_url}/execution/{execution_id}/results",
            headers=self._headers(),
        )

    def run_sql(
        self,
        *,
        sql: str,
        poll_interval_seconds: float = 2.0,
        timeout_seconds: float = 180.0,
    ) -> dict[str, Any]:
        execute = self.execute_sql(sql=sql)
        rows = execute.get("payload", {}).get("data", [])
        if not isinstance(rows, list) or not rows:
            return {
                "execute": execute,
                "status": None,
                "results": None,
                "execution_id": "",
                "final_status": "MISSING_EXECUTION_METADATA",
            }
        execution = rows[0] if isinstance(rows[0], dict) else {}
        execution_id = str(execution.get("executionId", "") or "")
        final_status = str(execution.get("status", "") or "").upper()
        if not execution_id:
            return {
                "execute": execute,
                "status": None,
                "results": None,
                "execution_id": "",
                "final_status": "MISSING_EXECUTION_ID",
            }
        deadline = time.time() + timeout_seconds
        status = None
        while time.time() < deadline:
            status = self.execution_status(execution_id=execution_id)
            status_rows = status.get("payload", {}).get("data", [])
            if isinstance(status_rows, list) and status_rows:
                row = status_rows[0] if isinstance(status_rows[0], dict) else {}
                final_status = str(row.get("status", "") or "").upper()
                if final_status not in {"PENDING", "RUNNING"}:
                    break
            time.sleep(max(0.1, poll_interval_seconds))
        results = self.execution_results(execution_id=execution_id)
        return {
            "execute": execute,
            "status": status,
            "results": results,
            "execution_id": execution_id,
            "final_status": final_status,
        }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync = subparsers.add_parser("sync-env")
    sync.add_argument("--output", required=True)
    sync.add_argument("--secret-arn", default=DEFAULT_PROVIDER_SECRET_ARN)
    sync.add_argument("--profile", default="")
    sync.add_argument("--region", default="eu-central-1")

    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    if args.command == "sync-env":
        result = sync_provider_runtime_env(
            output_path=Path(args.output),
            secret_arn=args.secret_arn,
            profile=args.profile,
            region=args.region,
        )
        print(json.dumps(result, indent=2))
        return 0
    parser.error(f"Unknown command: {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

