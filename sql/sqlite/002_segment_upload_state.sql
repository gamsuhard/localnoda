PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS segment_upload_state (
    segment_id TEXT PRIMARY KEY,
    s3_bucket TEXT NOT NULL,
    s3_key TEXT NOT NULL,
    manifest_s3_key TEXT NOT NULL,
    uploaded_at TEXT,
    etag TEXT,
    upload_attempts INTEGER NOT NULL DEFAULT 0,
    last_upload_error TEXT,
    last_verified_at TEXT,
    FOREIGN KEY (segment_id) REFERENCES segments(segment_id)
);

CREATE INDEX IF NOT EXISTS idx_segment_upload_state_bucket_key ON segment_upload_state(s3_bucket, s3_key);
CREATE INDEX IF NOT EXISTS idx_segment_upload_state_uploaded_at ON segment_upload_state(uploaded_at);
