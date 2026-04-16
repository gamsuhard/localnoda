#!/usr/bin/env bash
set -euo pipefail

: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"
: "${EXTRACTOR_ENV_FILE:=$WORKSPACE_ROOT/configs/extractor/extractor.env}"

if [ -f "$EXTRACTOR_ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$EXTRACTOR_ENV_FILE"
  set +a
fi

: "${RUN_STATE_DB:=$WORKSPACE_ROOT/runtime/run_state.sqlite}"
: "${S3_BUFFER_BUCKET:?S3_BUFFER_BUCKET is required}"
: "${S3_BUFFER_PREFIX_ROOT:?S3_BUFFER_PREFIX_ROOT is required}"
: "${S3_BUFFER_UPLOAD_REGION:=eu-central-1}"
: "${S3_BUFFER_SSE_MODE:=}"
: "${S3_BUFFER_KMS_KEY_ARN:=}"
: "${TRON_FILE_SINK_RUN_ID:=}"
: "${RUNTIME_MANIFEST_JAVA_TRON_VERSION:?RUNTIME_MANIFEST_JAVA_TRON_VERSION is required}"
: "${RUNTIME_MANIFEST_CONFIG_SHA256:?RUNTIME_MANIFEST_CONFIG_SHA256 is required}"
: "${RUNTIME_MANIFEST_PLUGIN_BUILD_ID:?RUNTIME_MANIFEST_PLUGIN_BUILD_ID is required}"
: "${RUN_RESOLVED_END_BLOCK:?RUN_RESOLVED_END_BLOCK is required}"

ARGS=(
  --db-path "$RUN_STATE_DB"
  --schema-path "$WORKSPACE_ROOT/sql/sqlite/002_segment_upload_state.sql"
  --bucket "$S3_BUFFER_BUCKET"
  --prefix-root "$S3_BUFFER_PREFIX_ROOT"
  --region "$S3_BUFFER_UPLOAD_REGION"
  --java-tron-version "$RUNTIME_MANIFEST_JAVA_TRON_VERSION"
  --config-sha256 "$RUNTIME_MANIFEST_CONFIG_SHA256"
  --plugin-build-id "$RUNTIME_MANIFEST_PLUGIN_BUILD_ID"
  --resolved-end-block "$RUN_RESOLVED_END_BLOCK"
)

if [ -n "$TRON_FILE_SINK_RUN_ID" ]; then
  ARGS+=(--run-id "$TRON_FILE_SINK_RUN_ID")
fi

if [ -n "$S3_BUFFER_SSE_MODE" ]; then
  ARGS+=(--sse-mode "$S3_BUFFER_SSE_MODE")
fi

if [ -n "$S3_BUFFER_KMS_KEY_ARN" ]; then
  ARGS+=(--kms-key-arn "$S3_BUFFER_KMS_KEY_ARN")
fi

python3 "$WORKSPACE_ROOT/extractor/supervisor/10_upload_sealed_segments.py" "${ARGS[@]}"

VERIFY_ARGS=(
  --db-path "$RUN_STATE_DB"
  --schema-path "$WORKSPACE_ROOT/sql/sqlite/002_segment_upload_state.sql"
  --bucket "$S3_BUFFER_BUCKET"
  --prefix-root "$S3_BUFFER_PREFIX_ROOT"
  --region "$S3_BUFFER_UPLOAD_REGION"
)

if [ -n "$TRON_FILE_SINK_RUN_ID" ]; then
  VERIFY_ARGS+=(--run-id "$TRON_FILE_SINK_RUN_ID")
fi

python3 "$WORKSPACE_ROOT/extractor/supervisor/40_verify_uploaded_segments.py" "${VERIFY_ARGS[@]}"
