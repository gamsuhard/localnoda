CREATE DATABASE IF NOT EXISTS tron_usdt_local;

CREATE TABLE IF NOT EXISTS tron_usdt_local.trc20_transfer_events_staging
(
    chain LowCardinality(String) DEFAULT 'tron',
    token_symbol LowCardinality(String) DEFAULT 'USDT',
    event_id FixedString(64),
    contract_address String,
    block_number UInt64,
    block_timestamp DateTime64(3, 'UTC'),
    block_hash String,
    tx_hash String,
    log_index UInt32,
    from_address String,
    to_address String,
    amount_raw UInt256,
    amount_decimal Decimal(38, 6),
    raw_topic0 String,
    raw_topic1 String,
    raw_topic2 String,
    raw_data String,
    source_segment_id String,
    load_run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (load_run_id, source_segment_id, event_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS tron_usdt_local.address_transfer_legs_staging
(
    leg_id FixedString(64),
    event_id FixedString(64),
    address String,
    direction Enum8('outbound' = 1, 'inbound' = 2),
    counterparty_address String,
    contract_address String,
    token_symbol LowCardinality(String) DEFAULT 'USDT',
    block_number UInt64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_hash String,
    log_index UInt32,
    amount_raw UInt256,
    amount_decimal Decimal(38, 6),
    source_segment_id String,
    load_run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (load_run_id, source_segment_id, leg_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS tron_usdt_local.trc20_transfer_events
(
    chain LowCardinality(String) DEFAULT 'tron',
    token_symbol LowCardinality(String) DEFAULT 'USDT',
    event_id FixedString(64),
    contract_address String,
    block_number UInt64,
    block_timestamp DateTime64(3, 'UTC'),
    block_hash String,
    tx_hash String,
    log_index UInt32,
    from_address String,
    to_address String,
    amount_raw UInt256,
    amount_decimal Decimal(38, 6),
    raw_topic0 String,
    raw_topic1 String,
    raw_topic2 String,
    raw_data String,
    source_segment_id String,
    load_run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (contract_address, block_number, tx_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS tron_usdt_local.address_transfer_legs
(
    leg_id FixedString(64),
    event_id FixedString(64),
    address String,
    direction Enum8('outbound' = 1, 'inbound' = 2),
    counterparty_address String,
    contract_address String,
    token_symbol LowCardinality(String) DEFAULT 'USDT',
    block_number UInt64,
    block_timestamp DateTime64(3, 'UTC'),
    tx_hash String,
    log_index UInt32,
    amount_raw UInt256,
    amount_decimal Decimal(38, 6),
    source_segment_id String,
    load_run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (address, block_timestamp, tx_hash, log_index, direction)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS tron_usdt_local.load_audit
(
    load_batch_id String,
    target_table LowCardinality(String),
    run_id String,
    segment_id String,
    source_file String,
    source_sha256 String,
    source_row_count UInt64,
    inserted_row_count UInt64,
    status LowCardinality(String),
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),
    note String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (target_table, run_id, segment_id, started_at)
SETTINGS index_granularity = 8192;
