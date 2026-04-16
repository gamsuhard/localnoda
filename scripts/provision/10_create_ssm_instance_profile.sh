#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=ai-agents-dev}"
: "${AWS_REGION:=ap-southeast-1}"
: "${INSTANCE_ROLE_NAME:=local-tron-usdt-backfill-ssm-role}"
: "${INSTANCE_PROFILE_NAME:=local-tron-usdt-backfill-ssm-profile}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/runtime/aws"
TRUST_POLICY_PATH="$RUNTIME_DIR/ec2-trust-policy.json"

mkdir -p "$RUNTIME_DIR"

cat >"$TRUST_POLICY_PATH" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON

TRUST_POLICY_URI="file://$TRUST_POLICY_PATH"
if command -v cygpath >/dev/null 2>&1; then
  TRUST_POLICY_URI="file://$(cygpath -w "$TRUST_POLICY_PATH")"
fi

if ! aws iam get-role --role-name "$INSTANCE_ROLE_NAME" --profile "$AWS_PROFILE" >/dev/null 2>&1; then
  aws iam create-role \
    --role-name "$INSTANCE_ROLE_NAME" \
    --assume-role-policy-document "$TRUST_POLICY_URI" \
    --profile "$AWS_PROFILE" \
    >/dev/null
fi

aws iam attach-role-policy \
  --role-name "$INSTANCE_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore \
  --profile "$AWS_PROFILE" \
  >/dev/null

if ! aws iam get-instance-profile --instance-profile-name "$INSTANCE_PROFILE_NAME" --profile "$AWS_PROFILE" >/dev/null 2>&1; then
  aws iam create-instance-profile \
    --instance-profile-name "$INSTANCE_PROFILE_NAME" \
    --profile "$AWS_PROFILE" \
    >/dev/null
fi

if ! aws iam get-instance-profile \
  --instance-profile-name "$INSTANCE_PROFILE_NAME" \
  --profile "$AWS_PROFILE" \
  --query "InstanceProfile.Roles[?RoleName=='$INSTANCE_ROLE_NAME'] | length(@)" \
  --output text | grep -q '^1$'; then
  aws iam add-role-to-instance-profile \
    --instance-profile-name "$INSTANCE_PROFILE_NAME" \
    --role-name "$INSTANCE_ROLE_NAME" \
    --profile "$AWS_PROFILE" \
    >/dev/null || true
fi

echo "INSTANCE_ROLE_NAME=$INSTANCE_ROLE_NAME"
echo "INSTANCE_PROFILE_NAME=$INSTANCE_PROFILE_NAME"
