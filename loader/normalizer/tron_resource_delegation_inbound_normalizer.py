from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


TRX_DECIMALS = Decimal("1000000")
DELEGATE_RESOURCE_CONTRACT = "DelegateResourceContract"
UNDELEGATE_RESOURCE_CONTRACT = "UnDelegateResourceContract"
SUPPORTED_CONTRACT_TYPES = {DELEGATE_RESOURCE_CONTRACT, UNDELEGATE_RESOURCE_CONTRACT}
RESOURCE_CODE_MAP = {
    0: "BANDWIDTH",
    1: "ENERGY",
    "0": "BANDWIDTH",
    "1": "ENERGY",
    "BANDWIDTH": "BANDWIDTH",
    "ENERGY": "ENERGY",
}


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


def extract_tx_hash(payload: dict[str, Any]) -> str:
    return str(payload.get("transactionId") or payload.get("txID") or payload.get("hash") or "")


def extract_block_hash(payload: dict[str, Any]) -> str:
    return str(payload.get("blockHash") or "")


def extract_contract_index(payload: dict[str, Any]) -> int:
    for field_name in ("contractIndex", "eventIndex", "logIndex"):
        value = payload.get(field_name)
        if value is None:
            continue
        return int(value)
    return 0


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


def extract_receiver_address(payload: dict[str, Any]) -> str:
    parameter_value = extract_parameter_value(payload)
    return normalize_tron_address(
        str(
            parameter_value.get("receiver_address")
            or parameter_value.get("receiverAddress")
            or payload.get("receiver_address")
            or payload.get("receiverAddress")
            or payload.get("to_address")
            or payload.get("toAddress")
            or ""
        )
    )


def extract_balance_raw(payload: dict[str, Any]) -> int:
    parameter_value = extract_parameter_value(payload)
    balance_value = parameter_value.get("balance")
    if balance_value is None:
        balance_value = payload.get("balance")
    if balance_value is None:
        balance_value = payload.get("assetAmount")
    if balance_value in (None, ""):
        return 0
    return int(balance_value)


def extract_resource_type(payload: dict[str, Any]) -> str:
    parameter_value = extract_parameter_value(payload)
    resource_value = parameter_value.get("resource")
    if resource_value is None:
        resource_value = payload.get("resource")
    if resource_value is None:
        resource_value = payload.get("resourceType")
    return RESOURCE_CODE_MAP.get(resource_value, str(resource_value or "UNKNOWN"))


def extract_lock(payload: dict[str, Any]) -> int:
    parameter_value = extract_parameter_value(payload)
    lock_value = parameter_value.get("lock")
    if lock_value is None:
        lock_value = payload.get("lock")
    return 1 if bool(lock_value) else 0


def extract_lock_period(payload: dict[str, Any]) -> int:
    parameter_value = extract_parameter_value(payload)
    lock_period = parameter_value.get("lock_period")
    if lock_period is None:
        lock_period = parameter_value.get("lockPeriod")
    if lock_period is None:
        lock_period = payload.get("lock_period")
    if lock_period is None:
        lock_period = payload.get("lockPeriod")
    if lock_period in (None, ""):
        return 0
    return int(lock_period)


def action_for_contract_type(contract_type: str) -> str:
    if contract_type == DELEGATE_RESOURCE_CONTRACT:
        return "delegate"
    if contract_type == UNDELEGATE_RESOURCE_CONTRACT:
        return "undelegate"
    return "unknown"


def normalize_event(payload: dict[str, Any], segment_id: str, load_run_id: str) -> dict[str, Any]:
    contract_type = extract_contract_type(payload)
    tx_hash = extract_tx_hash(payload)
    delegator_address = extract_owner_address(payload)
    delegatee_address = extract_receiver_address(payload)
    balance_raw = extract_balance_raw(payload)
    resource_type = extract_resource_type(payload)
    contract_index = extract_contract_index(payload)
    observation_id = sha256_hex(
        [
            tx_hash,
            str(contract_index),
            contract_type,
            delegator_address,
            delegatee_address,
            resource_type,
            str(balance_raw),
        ]
    )
    balance_decimal = (Decimal(balance_raw) / TRX_DECIMALS).quantize(Decimal("0.000001"))
    return {
        "chain": "tron",
        "token_symbol": "TRX",
        "observation_id": observation_id,
        "block_number": int(payload.get("blockNumber") or 0),
        "block_timestamp": format_timestamp_ms(payload.get("timeStamp")),
        "block_hash": extract_block_hash(payload),
        "tx_hash": tx_hash,
        "delegator_address": delegator_address,
        "delegatee_address": delegatee_address,
        "resource_type": resource_type,
        "contract_type": contract_type,
        "direction": "inbound",
        "action": action_for_contract_type(contract_type),
        "balance_raw": balance_raw,
        "balance_decimal": str(balance_decimal),
        "lock": extract_lock(payload),
        "lock_period": extract_lock_period(payload),
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
        contract_type = extract_contract_type(payload)
        if contract_type not in SUPPORTED_CONTRACT_TYPES:
            continue
        event_row = normalize_event(payload, segment_manifest["segment_id"], load_run_id)
        if not event_row["delegatee_address"]:
            continue
        events.append(event_row)
    return events, []
