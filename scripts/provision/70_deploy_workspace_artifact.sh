#!/usr/bin/env bash
set -euo pipefail

: "${AWS_REGION:=eu-central-1}"
: "${WORKSPACE_ARTIFACT_URL:=}"
: "${WORKSPACE_ARTIFACT_SHA256:?WORKSPACE_ARTIFACT_SHA256 is required}"
: "${WORKSPACE_ROOT:=/srv/local-tron-usdt-backfill}"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

PARENT_DIR="$(dirname "$WORKSPACE_ROOT")"
ARTIFACT_PATH="$TMP_DIR/workspace.tar.gz"
EXTRACT_DIR="$TMP_DIR/extract"
DEPLOY_METADATA_DIR=".deploy-metadata"

mkdir -p "$PARENT_DIR"
if [ -n "$WORKSPACE_ARTIFACT_URL" ]; then
  curl -fL "$WORKSPACE_ARTIFACT_URL" -o "$ARTIFACT_PATH"
else
  : "${WORKSPACE_BUCKET:?WORKSPACE_BUCKET is required when WORKSPACE_ARTIFACT_URL is not set}"
  : "${WORKSPACE_ARTIFACT_KEY:?WORKSPACE_ARTIFACT_KEY is required when WORKSPACE_ARTIFACT_URL is not set}"
  aws s3 cp "s3://$WORKSPACE_BUCKET/$WORKSPACE_ARTIFACT_KEY" "$ARTIFACT_PATH" --region "$AWS_REGION"
fi

ACTUAL_SHA256="$(sha256sum "$ARTIFACT_PATH" | awk '{print $1}')"
if [ "$ACTUAL_SHA256" != "$WORKSPACE_ARTIFACT_SHA256" ]; then
  echo "Workspace artifact SHA mismatch: expected $WORKSPACE_ARTIFACT_SHA256 got $ACTUAL_SHA256" >&2
  exit 1
fi

mkdir -p "$EXTRACT_DIR"
tar -xzf "$ARTIFACT_PATH" -C "$EXTRACT_DIR"

EXTRACTED_ROOT="$(find "$EXTRACT_DIR" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
if [ -z "$EXTRACTED_ROOT" ]; then
  echo "No extracted workspace root found in artifact" >&2
  exit 1
fi

rm -rf "$WORKSPACE_ROOT"
mv "$EXTRACTED_ROOT" "$WORKSPACE_ROOT"
mkdir -p "$WORKSPACE_ROOT/$DEPLOY_METADATA_DIR"
printf '%s\n' "$WORKSPACE_ARTIFACT_SHA256" > "$WORKSPACE_ROOT/$DEPLOY_METADATA_DIR/workspace_artifact.sha256"
if [ -n "$WORKSPACE_ARTIFACT_URL" ]; then
  printf '%s\n' "$WORKSPACE_ARTIFACT_URL" > "$WORKSPACE_ROOT/$DEPLOY_METADATA_DIR/workspace_artifact.source"
else
  printf 's3://%s/%s\n' "$WORKSPACE_BUCKET" "$WORKSPACE_ARTIFACT_KEY" > "$WORKSPACE_ROOT/$DEPLOY_METADATA_DIR/workspace_artifact.source"
fi

bash "$WORKSPACE_ROOT/scripts/01_prepare_workspace.sh"

echo "Deployed workspace artifact to: $WORKSPACE_ROOT"
echo "Artifact SHA256: $WORKSPACE_ARTIFACT_SHA256"
if [ -n "$WORKSPACE_ARTIFACT_URL" ]; then
  echo "Artifact source: $WORKSPACE_ARTIFACT_URL"
else
  echo "Artifact source: s3://$WORKSPACE_BUCKET/$WORKSPACE_ARTIFACT_KEY"
fi
