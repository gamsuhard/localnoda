PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    run_type TEXT NOT NULL CHECK (run_type IN ('extract', 'load', 'validate', 'extract_load')),
    status TEXT NOT NULL CHECK (status IN ('planned', 'running', 'paused', 'completed', 'failed', 'aborted')),
    start_block INTEGER,
    end_block INTEGER,
    resolved_tip_block INTEGER,
    config_fingerprint TEXT,
    extractor_host TEXT,
    operator_note TEXT,
    restart_of_run_id TEXT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS segments (
    segment_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    segment_seq INTEGER NOT NULL,
    file_path TEXT NOT NULL UNIQUE,
    codec TEXT NOT NULL DEFAULT 'gzip',
    status TEXT NOT NULL CHECK (status IN ('open', 'sealed', 'uploaded', 'validated', 'loaded', 'failed', 'quarantined')),
    first_block INTEGER,
    last_block INTEGER,
    first_tx_hash TEXT,
    last_tx_hash TEXT,
    first_event_hint TEXT,
    last_event_hint TEXT,
    row_count INTEGER NOT NULL DEFAULT 0,
    byte_count INTEGER,
    sha256 TEXT,
    opened_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at TEXT,
    loaded_at TEXT,
    note TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_segments_run_seq ON segments(run_id, segment_seq);
CREATE INDEX IF NOT EXISTS idx_segments_status ON segments(status);
CREATE INDEX IF NOT EXISTS idx_segments_block_range ON segments(first_block, last_block);

CREATE TABLE IF NOT EXISTS extraction_checkpoints (
    run_id TEXT NOT NULL,
    stream_name TEXT NOT NULL DEFAULT 'usdt_transfer',
    checkpoint_kind TEXT NOT NULL CHECK (checkpoint_kind IN ('segment_flush', 'segment_close', 'manual', 'resume')),
    last_block_number INTEGER,
    last_tx_hash TEXT,
    last_log_index INTEGER,
    current_segment_id TEXT,
    emitted_rows_total INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, stream_name),
    FOREIGN KEY (run_id) REFERENCES runs(run_id),
    FOREIGN KEY (current_segment_id) REFERENCES segments(segment_id)
);

CREATE TABLE IF NOT EXISTS load_checkpoints (
    target_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    last_segment_id TEXT,
    last_source_path TEXT,
    last_loaded_row_num INTEGER,
    loaded_rows_total INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (target_name, run_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id),
    FOREIGN KEY (last_segment_id) REFERENCES segments(segment_id)
);

CREATE TABLE IF NOT EXISTS validation_checks (
    validation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    check_name TEXT NOT NULL,
    scope_type TEXT NOT NULL,
    scope_ref TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'ok', 'warn', 'fail')),
    details_json TEXT,
    checked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_validation_status ON validation_checks(status);
CREATE INDEX IF NOT EXISTS idx_validation_check_name ON validation_checks(check_name);
