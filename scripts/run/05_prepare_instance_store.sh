#!/usr/bin/env bash
set -euo pipefail

: "${TRON_DATA_DIR:=/tron-data/java-tron}"
: "${TRON_USER:=tron}"

MOUNT_ROOT="$(dirname "$TRON_DATA_DIR")"

mkdir -p "$MOUNT_ROOT"

if findmnt "$MOUNT_ROOT" >/dev/null 2>&1; then
  mkdir -p "$TRON_DATA_DIR"
  if id "$TRON_USER" >/dev/null 2>&1; then
    chown -R "$TRON_USER:$TRON_USER" "$MOUNT_ROOT"
  fi
  echo "Instance store already mounted at $MOUNT_ROOT"
  exit 0
fi

ROOT_PARENT="$(lsblk -no PKNAME "$(findmnt -no SOURCE /)" | head -n1 || true)"
INSTANCE_DISK="$(lsblk -dpno NAME,TYPE,MOUNTPOINT | awk '$2=="disk" && $3=="" {print $1}' | grep -v "/dev/${ROOT_PARENT}$" | head -n1)"

if [ -z "$INSTANCE_DISK" ]; then
  echo "Could not resolve an unmounted instance-store disk" >&2
  exit 1
fi

mkfs.xfs -f "$INSTANCE_DISK"
grep -q " $MOUNT_ROOT " /etc/fstab || echo "$INSTANCE_DISK $MOUNT_ROOT xfs defaults,nofail 0 2" >> /etc/fstab
mount "$MOUNT_ROOT"
mkdir -p "$TRON_DATA_DIR"
if id "$TRON_USER" >/dev/null 2>&1; then
  chown -R "$TRON_USER:$TRON_USER" "$MOUNT_ROOT"
fi

echo "Mounted $INSTANCE_DISK at $MOUNT_ROOT"
