# Pre-Bulk Gate

## Goal

Freeze the last operational gate before any real historical bulk load:

1. fresh server-side disposable-schema validation from this exact tree
2. medium-size performance rehearsal on real `S3 -> private ClickHouse`
3. bulk-run checklist freeze with operator-visible constraints

This gate does **not** approve bulk load by itself. It only produces the evidence set that must be reviewed before the real run.

## Scripts

- `scripts/demo/20_publish_demo_run_to_s3.py`
  - generates a synthetic run
  - uploads it into the frozen S3 buffer prefix
  - verifies uploaded segment/manifests/checkpoint/checksums
- `scripts/validate/50_server_disposable_validation.sh`
  - prepares loader runtime
  - applies disposable ClickHouse schema
  - probes private ClickHouse connectivity
  - runs `load -> validate -> replay`
  - records SQL row counts and audit counts
- `scripts/validate/60_medium_rehearsal.sh`
  - prepares loader runtime for a second disposable schema
  - loads a medium synthetic run
  - validates it
  - emits timing and throughput metrics
- `scripts/validate/70_freeze_bulk_run_checklist.py`
  - freezes the operator-visible checklist
  - writes JSON + Markdown evidence
- `scripts/validate/80_run_pre_bulk_gate.py`
  - packages this exact tree
  - uploads the workspace artifact
  - publishes tiny + medium S3-backed synthetic runs
  - deploys the exact tree to the loader host
  - runs the first two gate phases remotely
  - writes the checklist freeze locally

## Expected evidence

After a successful run:

- `reports/gates/*-server-disposable-validation.json`
- `reports/gates/*-medium-rehearsal.json`
- `reports/gates/pre-bulk-gate-*.json`
- `reports/gates/pre-bulk-gate-*.md`

## Frozen boundaries

- loader concurrency stays `1`
- global staging tables remain single-worker only
- bulk run remains blocked until this exact-tree evidence is reviewed
- disposable validation schema must be separate from any future bulk target schema
- medium rehearsal is for throughput and bounded-memory confidence, not for analytical completeness
