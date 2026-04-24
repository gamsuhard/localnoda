from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


TRX_DECIMALS = Decimal("1000000")
TRANSFER_CONTRACT = "TransferContract"


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


def looks_like_hex_address(value: str) -> bool:
    normalized = normalize_hex(value)
    if len(normalized) == 42 and normalized.startswith("41"):
        return True
    return len(normalized) == 40


def normalize_tron_address(value: str | None) -> str:
    if value is None:
        return ""
    normalized = value.strip()
    if not normalized:
        return ""
    if looks_like_hex_address(normalized):
        hex_value = normalize_hex(normalized)
        if len(hex_value) == 40:
            return "41" + hex_value
        return hex_value
    return normalized


def extract_first_contract(payload: dict[str, Any]) -> dict[str, Any]:
    for container_name in ("rawData", "raw_data"):
        container = payload.get(container_name)
        if not isinstance(container, dict):
            continue
        contracts = container.get("contract")
        if isinstance(contracts, list) and contracts:
            first_contract = contracts[0]
            if isinstance(first_contract, dict):
                return first_contract
    transaction = payload.get("transaction")
    if isinstance(transaction, dict):
        raw_data = transaction.get("raw_data") or transaction.get("rawData")
        if isinstance(raw_data, dict):
            contracts = raw_data.get("contract")
            if isinstance(contracts, list) and contracts:
                first_contract = contracts[0]
                if isinstance(first_contract, dict):
                    return first_contract
    return {}


def extract_contract_type(payload: dict[str, Any]) -> str:
    value = payload.get("contractType")
    if value:
        return str(value)
    first_contract = extract_first_contract(payload)
    contract_type = first_contract.get("type")
    return "" if contract_type is None else str(contract_type)


def extract_parameter_value(payload: dict[str, Any]) -> dict[str, Any]:
    first_contract = extract_first_contract(payload)
    parameter = first_contract.get("parameter")
    if isinstance(parameter, dict):
        value = parameter.get("value")
        if isinstance(value, dict):
            return value
    return {}


def extract_amount_raw(payload: dict[str, Any]) -> int:
    parameter_value = extract_parameter_value(payload)
    amount_value = parameter_value.get("amount")
    if amount_value is None:
        amount_value = payload.get("amount")
    if amount_value is None:
        amount_value = payload.get("assetAmount")
    if amount_value in (None, ""):
        return 0
    return int(amount_value)


def extract_tx_hash(payload: dict[str, Any]) -> str:
    return str(payload.get("transactionId") or payload.get("txID") or payload.get("hash") or "")


def extract_block_hash(payload: dict[str, Any]) -> str:
    return str(payload.get("blockHash") or "")


def extract_owner_address(payload: dict[str, Any]) -> str:
    parameter_value = extract_parameter_value(payload)
    return normalize_tron_address(
        str(
            parameter_value.get("owner_address")
            or parameter_value.get("ownerAddress")
            or payload.get("owner_address")
            or payload.get("ownerAddress")
            or payload.get("from_address")
            or payload.get("fromAddress")
            or ""
        )
    )


def extract_to_address(payload: dict[str, Any]) -> str:
    parameter_value = extract_parameter_value(payload)
    return normalize_tron_address(
        str(
            parameter_value.get("to_address")
            or parameter_value.get("toAddress")
            or payload.get("to_address")
            or payload.get("toAddress")
            or ""
        )
    )


def extract_contract_index(payload: dict[str, Any]) -> int:
    for field_name in ("contractIndex", "eventIndex", "logIndex"):
        value = payload.get(field_name)
        if value is None:
            continue
        return int(value)
    return 0


def normalize_event(payload: dict[str, Any], segment_id: str, load_run_id: str) -> dict[str, Any]:
    contract_type = extract_contract_type(payload)
    tx_hash = extract_tx_hash(payload)
    from_address = extract_owner_address(payload)
    to_address = extract_to_address(payload)
    amount_raw = extract_amount_raw(payload)
    contract_index = extract_contract_index(payload)
    event_id = sha256_hex([tx_hash, str(contract_index), from_address, to_address, str(amount_raw)])
    amount_decimal = (Decimal(amount_raw) / TRX_DECIMALS).quantize(Decimal("0.000001"))
    return {
        "chain": "tron",
        "token_symbol": "TRX",
        "event_id": event_id,
        "block_number": int(payload.get("blockNumber") or 0),
        "block_timestamp": format_timestamp_ms(payload.get("timeStamp")),
        "block_hash": extract_block_hash(payload),
        "tx_hash": tx_hash,
        "from_address": from_address,
        "to_address": to_address,
        "amount_raw": amount_raw,
        "amount_decimal": str(amount_decimal),
        "contract_type": contract_type,
        "raw_payload": payload,
        "source_segment_id": segment_id,
        "load_run_id": load_run_id,
    }


def normalize_records(
    records: list[dict[str, Any]],
    segment_manifest: dict[str, Any],
    load_run_id: str,
    include_legs: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    del include_legs
    events: list[dict[str, Any]] = []
    for payload in records:
        if extract_contract_type(payload) != TRANSFER_CONTRACT:
            continue
        event_row = normalize_event(payload, segment_manifest["segment_id"], load_run_id)
        if not event_row["to_address"]:
            continue
        events.append(event_row)
    return events, []
