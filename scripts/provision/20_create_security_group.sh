#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=ai-agents-dev}"
: "${AWS_REGION:=ap-southeast-1}"
: "${SECURITY_GROUP_NAME:=local-tron-usdt-backfill-noingress}"
: "${PROJECT_TAG:=local-tron-usdt-backfill}"
: "${VPC_ID:=}"

if [ -z "$VPC_ID" ]; then
  VPC_ID="$(aws ec2 describe-vpcs \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --filters Name=is-default,Values=true \
    --query 'Vpcs[0].VpcId' \
    --output text)"
fi

SG_ID="$(aws ec2 describe-security-groups \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --filters Name=group-name,Values="$SECURITY_GROUP_NAME" Name=vpc-id,Values="$VPC_ID" \
  --query 'SecurityGroups[0].GroupId' \
  --output text)"

if [ "$SG_ID" = "None" ] || [ -z "$SG_ID" ]; then
  SG_ID="$(aws ec2 create-security-group \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --group-name "$SECURITY_GROUP_NAME" \
    --description "No-ingress SG for $PROJECT_TAG extractor host" \
    --vpc-id "$VPC_ID" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=$SECURITY_GROUP_NAME},{Key=Project,Value=$PROJECT_TAG}]" \
    --query 'GroupId' \
    --output text)"
fi

INGRESS_COUNT="$(aws ec2 describe-security-groups \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --group-ids "$SG_ID" \
  --query 'length(SecurityGroups[0].IpPermissions)' \
  --output text)"

if [ "$INGRESS_COUNT" != "0" ]; then
  echo "Security group $SG_ID already has ingress rules; refusing to modify shared state." >&2
  exit 1
fi

echo "VPC_ID=$VPC_ID"
echo "SECURITY_GROUP_ID=$SG_ID"
