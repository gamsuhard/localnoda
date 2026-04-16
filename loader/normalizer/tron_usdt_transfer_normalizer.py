from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


USDT_DECIMALS = Decimal("1000000")
TRANSFER_TOPIC0 = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def sha256_hex(parts: list[str]) -> str:
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def format_timestamp_ms(timestamp_ms: int | None) -> str:
    if timestamp_ms is None:
        return "1970-01-01 00:00:00.000"
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def normalize_hex(value: str | None) -> str:
    if not value:
        return ""
    return value.lower().replace("0x", "")


def normalize_contract_address(value: str | None) -> str:
    if value is None:
        return ""
    normalized = value.strip()
    return normalized.lower() if normalized.startswith("0x") else normalized


def decode_topic_address(topic_value: str | None) -> str:
    normalized = normalize_hex(topic_value)
    if not normalized:
        return ""
    tail = normalized[-42:]
    if len(tail) == 42 and tail.startswith("41"):
        return tail
    if len(normalized) >= 40:
        return "41" + normalized[-40:]
    return normalized


def decode_amount_raw(data_value: str | None) -> int:
    normalized = normalize_hex(data_value)
    if not normalized:
        return 0
    return int(normalized, 16)


def decode_log_index(payload: dict[str, Any]) -> int:
    for field_name in ("logIndex", "eventIndex"):
        value = payload.get(field_name)
        if value is None:
            continue
        return int(value)
    unique_id = str(payload.get("uniqueId", ""))
    parts = unique_id.split("-")
    if len(parts) >= 2 and parts[-1].isdigit():
        return int(parts[-1])
    return 0


def normalize_event(payload: dict[str, Any], segment_id: str, load_run_id: str) -> dict[str, Any]:
    topics = payload.get("topics") or []
    topic0 = normalize_hex(topics[0] if len(topics) > 0 else "")
    topic1 = normalize_hex(topics[1] if len(topics) > 1 else "")
    topic2 = normalize_hex(topics[2] if len(topics) > 2 else "")
    tx_hash = str(payload.get("transactionId") or "")
    contract_address = normalize_contract_address(str(payload.get("address") or ""))
    log_index = decode_log_index(payload)
    event_id = sha256_hex([tx_hash, str(log_index), contract_address.lower(), topic0])
    amount_raw = decode_amount_raw(str(payload.get("data") or ""))
    amount_decimal = (Decimal(amount_raw) / USDT_DECIMALS).quantize(Decimal("0.000001"))
    return {
        "chain": "tron",
        "token_symbol": "USDT",
        "event_id": event_id,
        "contract_address": contract_address,
        "block_number": int(payload.get("blockNumber") or 0),
        "block_timestamp": format_timestamp_ms(payload.get("timeStamp")),
        "block_hash": str(payload.get("blockHash") or ""),
        "tx_hash": tx_hash,
        "log_index": log_index,
        "from_address": decode_topic_address(topic1),
        "to_address": decode_topic_address(topic2),
        "amount_raw": amount_raw,
        "amount_decimal": str(amount_decimal),
        "raw_topic0": topic0,
        "raw_topic1": topic1,
        "raw_topic2": topic2,
        "raw_data": str(payload.get("data") or ""),
        "source_segment_id": segment_id,
        "load_run_id": load_run_id,
    }


def build_legs(event_row: dict[str, Any]) -> list[dict[str, Any]]:
    outbound_id = sha256_hex([event_row["event_id"], "outbound", event_row["from_address"], event_row["to_address"]])
    inbound_id = sha256_hex([event_row["event_id"], "inbound", event_row["to_address"], event_row["from_address"]])
    common = {
        "event_id": event_row["event_id"],
        "contract_address": event_row["contract_address"],
        "token_symbol": "USDT",
        "block_number": event_row["block_number"],
        "block_timestamp": event_row["block_timestamp"],
        "tx_hash": event_row["tx_hash"],
        "log_index": event_row["log_index"],
        "amount_raw": event_row["amount_raw"],
        "amount_decimal": event_row["amount_decimal"],
        "source_segment_id": event_row["source_segment_id"],
        "load_run_id": event_row["load_run_id"],
    }
    return [
        {
            "leg_id": outbound_id,
            "address": event_row["from_address"],
            "direction": "outbound",
            "counterparty_address": event_row["to_address"],
            **common,
        },
        {
            "leg_id": inbound_id,
            "address": event_row["to_address"],
            "direction": "inbound",
            "counterparty_address": event_row["from_address"],
            **common,
        },
    ]


def normalize_records(records: list[dict[str, Any]], segment_manifest: dict[str, Any], load_run_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    legs: list[dict[str, Any]] = []
    for payload in records:
        event_row = normalize_event(payload, segment_manifest["segment_id"], load_run_id)
        if event_row["raw_topic0"] != TRANSFER_TOPIC0:
            continue
        events.append(event_row)
        legs.extend(build_legs(event_row))
    return events, legs
