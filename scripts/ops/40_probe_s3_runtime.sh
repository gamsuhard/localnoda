#!/usr/bin/env bash
set -euo pipefail

: "${S3_BUFFER_BUCKET:=goldusdt-v2-stage-913378704801-raw}"
: "${S3_BUFFER_KEY:=providers/tron-usdt-backfill/usdt-transfer-oneoff/_probe/hello.txt}"
: "${S3_BUFFER_KMS_KEY_ARN:=arn:aws:kms:eu-central-1:913378704801:key/81eb65d1-90dc-4df1-a8ca-6dde2bb109d3}"

aws sts get-caller-identity
printf '%s\n' "$(hostname)" >/tmp/hello.txt
aws s3api put-object \
  --bucket "$S3_BUFFER_BUCKET" \
  --key "$S3_BUFFER_KEY" \
  --body /tmp/hello.txt \
  --server-side-encryption aws:kms \
  --ssekms-key-id "$S3_BUFFER_KMS_KEY_ARN"
aws s3api get-object \
  --bucket "$S3_BUFFER_BUCKET" \
  --key "$S3_BUFFER_KEY" \
  /tmp/hello.out >/tmp/get-object.json
cat /tmp/hello.out
cat /tmp/get-object.json
