#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=ai-agents-dev}"
: "${AWS_REGION:=ap-southeast-1}"
: "${PROJECT_TAG:=local-tron-usdt-backfill}"
: "${INSTANCE_NAME:=local-tron-usdt-backfill-block02}"
: "${INSTANCE_TYPE:=i4i.4xlarge}"
: "${ROOT_VOLUME_SIZE_GB:=120}"
: "${AMI_SSM_PARAMETER:=/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64}"
: "${PREFERRED_AZ:=ap-southeast-1a}"
: "${SUBNET_ID:=}"
: "${SECURITY_GROUP_ID:=}"
: "${INSTANCE_PROFILE_NAME:=local-tron-usdt-backfill-ssm-profile}"

if [ -z "$SUBNET_ID" ]; then
  SUBNET_ID="$(aws ec2 describe-subnets \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --filters Name=default-for-az,Values=true Name=availability-zone,Values="$PREFERRED_AZ" \
    --query 'Subnets[0].SubnetId' \
    --output text)"
fi

if [ -z "$SECURITY_GROUP_ID" ]; then
  echo "SECURITY_GROUP_ID is required" >&2
  exit 1
fi

AMI_ID="$(MSYS_NO_PATHCONV=1 aws ssm get-parameter \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --name "$AMI_SSM_PARAMETER" \
  --query 'Parameter.Value' \
  --output text)"

INSTANCE_ID="$(aws ec2 run-instances \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --iam-instance-profile Name="$INSTANCE_PROFILE_NAME" \
  --subnet-id "$SUBNET_ID" \
  --security-group-ids "$SECURITY_GROUP_ID" \
  --associate-public-ip-address \
  --metadata-options "HttpTokens=required,HttpEndpoint=enabled" \
  --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":$ROOT_VOLUME_SIZE_GB,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME},{Key=Project,Value=$PROJECT_TAG},{Key=Block,Value=02},{Key=Access,Value=SSM-only}]" \
  --query 'Instances[0].InstanceId' \
  --output text)"

echo "AMI_ID=$AMI_ID"
echo "SUBNET_ID=$SUBNET_ID"
echo "INSTANCE_ID=$INSTANCE_ID"
