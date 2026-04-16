PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS loader_runs (
    run_id TEXT PRIMARY KEY,
    s3_bucket TEXT NOT NULL,
    s3_prefix_root TEXT NOT NULL,
    clickhouse_database TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('planned', 'discovering', 'loading', 'validated', 'failed', 'quarantined')),
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    note TEXT
);

CREATE TABLE IF NOT EXISTS loader_runtime_lock (
    lock_name TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    clickhouse_database TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    released_at TEXT
);

CREATE TABLE IF NOT EXISTS loaded_segments (
    run_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    source_s3_key TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'claimed', 'loading', 'merged', 'validated', 'failed', 'quarantined', 'skipped')),
    claim_token TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    bytes_read INTEGER NOT NULL DEFAULT 0,
    record_count INTEGER NOT NULL DEFAULT 0,
    event_rows INTEGER NOT NULL DEFAULT 0,
    leg_rows INTEGER NOT NULL DEFAULT 0,
    s3_read_ms INTEGER NOT NULL DEFAULT 0,
    normalize_ms INTEGER NOT NULL DEFAULT 0,
    stage_ms INTEGER NOT NULL DEFAULT 0,
    merge_ms INTEGER NOT NULL DEFAULT 0,
    audit_ms INTEGER NOT NULL DEFAULT 0,
    validation_ms INTEGER NOT NULL DEFAULT 0,
    claimed_at TEXT,
    load_started_at TEXT,
    merged_at TEXT,
    load_finished_at TEXT,
    last_error TEXT,
    PRIMARY KEY (run_id, segment_id)
);

CREATE INDEX IF NOT EXISTS idx_loaded_segments_status ON loaded_segments(status);
