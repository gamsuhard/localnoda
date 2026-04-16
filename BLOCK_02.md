# BLOCK_02 - Temporary extractor host bootstrap

Date: 2026-04-15
Status: ready for execution
Scope: one-off USDT-on-TRON historical backfill

---

## 1. Goal

Provision and bootstrap the temporary Linux extractor host used for the bounded historical extraction.

Block 02 includes:

- isolated EC2 provisioning for this project only
- SSM-only host access
- package install and runtime directory prep
- `java-tron` download
- streaming restore of the official FullNode RocksDB snapshot
- FullNode startup and health checks

Block 02 does not implement the custom file sink yet.
That remains Block 04.

---

## 2. Frozen execution inputs

- AWS profile: `ai-agents-dev`
- AWS region: `ap-southeast-1`
- access method: SSM only
- no secrets in repo
- no Terraform unless explicitly requested later
- no reuse of unrelated project resources

---

## 3. Temporary host shape

Preferred extractor profile:

- instance type: `i4i.4xlarge`
- OS: Amazon Linux 2023 x86_64
- root volume: modest gp3 for OS and tools
- data path: local NVMe instance store mounted on the host

The instance store is used for snapshot restore and `java-tron` data.

---

## 4. Provisioning contour

Project-owned AWS resources for this block:

- one IAM role for EC2 + SSM
- one instance profile
- one security group with no inbound rules
- one tagged EC2 instance

Expected launch order:

1. `bash scripts/provision/10_create_ssm_instance_profile.sh`
2. `bash scripts/provision/20_create_security_group.sh`
3. `bash scripts/provision/30_launch_extractor_host.sh`
4. `python scripts/provision/40_wait_for_ssm.py`

Host bootstrap order:

1. `scripts/run/05_prepare_instance_store.sh`
2. `scripts/run/10_install_java_tron.sh`
3. `scripts/snapshot/10_stream_restore_snapshot.sh`
4. `scripts/run/20_start_fullnode.sh`
5. `scripts/validate/10_healthcheck_fullnode.sh`
6. `scripts/validate/20_print_sync_status.sh`

---

## 5. Notes on event service posture

The event-service overlay is already staged in `configs/fullnode/config.conf.overlay.template`.

For Block 02:

- the node is bootstrapped and started first
- event-service activation with `--es` is deferred until the custom plugin exists
- the chosen release and config path must still be compatible with later Event Service V2 work

This preserves the Block 04 boundary and avoids pretending the custom sink already exists.

---

## 6. Snapshot restore posture

The restore script must:

- discover the latest official backup directory unless explicitly overridden
- stream `FullNode_output-directory.tgz` directly into `tar`
- extract into the mounted NVMe-backed data path
- avoid writing a second full archive copy to disk

Optional checksum verification can be added when it does not require a second archive copy.

---

## 7. Expected deliverables for Block 02

- server provision notes
- isolated AWS launch scripts
- package install script
- `java-tron` acquisition script
- streaming snapshot restore script
- FullNode startup script
- health-check and sync-status scripts

---

## 8. Acceptance checklist

Block 02 is accepted only if all of the following are true:

- temporary extractor instance exists and is tagged for this project
- instance is reachable through SSM
- local NVMe is mounted for chain data
- JDK and `java-tron` are installed
- snapshot restore uses a streaming contour
- FullNode process starts and writes logs locally
- sync progress is visible from local commands or HTTP API

---

## 9. Operator notes

- keep all AWS resources tagged with `Project=local-tron-usdt-backfill`
- do not open SSH
- do not store copied `.env` files with secrets in git
- do not introduce Kafka or MongoDB in this block
