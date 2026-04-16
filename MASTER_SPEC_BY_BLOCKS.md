# LOCAL_TRON_NODE_MASTER_SPEC_BY_BLOCKS_v2_ONE_OFF_USDT_BACKFILL

Date: 2026-04-15  
Status: master spec by blocks v2  
Supersedes: v1 permanent historical source plan

---

## 0. Master objective

Deliver a **bounded one-off local historical USDT-on-TRON backfill** with the smallest stack that still makes engineering sense:

- temporary FullNode
- historical replay from `2023-09-01`
- only USDT `Transfer`
- raw file sink
- ClickHouse canonical index
- address-history queries
- node removed after completion

---

## 1. Global project constraints

The project must obey all of these constraints:

1. no permanent FullNode in phase 1
2. no Kafka cluster
3. no MongoDB Event Query Service
4. no GitHub remote required
5. local git only
6. Codex edits files directly in the local/server workspace
7. only USDT contract in phase 1
8. only `Transfer(address,address,uint256)` in phase 1
9. no live tail / no forever-sync in phase 1
10. ClickHouse is the final persistent analytical store

---

## 2. Block breakdown

## Block 00 — freeze architecture and runbook baseline

### Goal
Freeze the bounded one-off architecture and remove assumptions from the earlier “permanent local source” direction.

### Deliverables
- final architecture markdown
- final master spec by blocks markdown
- decision log
- risk log

### Acceptance
- architecture explicitly says temporary FullNode
- architecture explicitly excludes Kafka/Mongo/Event Query Service
- architecture explicitly excludes GitHub remote
- architecture explicitly freezes USDT-only phase 1

---

## Block 01 — create minimal local workspace and repo skeleton

### Goal
Prepare the smallest workable code/config layout.

### Deliverables
- local directory layout
- local git repo initialization
- `.gitignore`
- README with run order
- config templates
- script folders
- SQLite schema draft for run state

### Required structure
```text
local-tron-usdt-backfill/
  README.md
  ARCHITECTURE.md
  MASTER_SPEC_BY_BLOCKS.md
  configs/
  extractor/
  loader/
  sql/
  scripts/
  raw/
  logs/
  reports/
```

### Required local git policy
- local commits only
- no GitHub remote required
- one commit per accepted block or sub-block
- tags only for accepted milestones

### Acceptance
- local repo exists
- tree is created
- no GitHub remote is required to proceed
- all paths are local and executable on the chosen server/workspace

---

## Block 02 — provision temporary extractor host and bootstrap FullNode

### Goal
Bring up the temporary Linux extractor and boot `java-tron` from the official snapshot.

### Deliverables
- server provision notes
- install scripts
- JDK install
- `java-tron` acquisition/build or release download
- snapshot streaming script
- FullNode startup script
- health-check script

### Requirements
- Linux only
- storage-optimized host
- local NVMe volume
- snapshot is restored using streamed download/extract
- node starts with event subscription capability available

### Acceptance
- node is running
- block sync progress is visible
- health checks pass
- logs are written to local files
- snapshot bootstrap does not require a second full disk copy

---

## Block 03 — resolve extraction boundaries and config freeze

### Goal
Resolve the exact bounded extraction window and freeze runtime parameters.

### Deliverables
- exact start block for `2023-09-01 UTC`
- optional exact end block if bounded by date instead of “to current approved tip”
- frozen runtime config manifest
- contract and topic filter manifest

### Required frozen values
- `event.subscribe.version = 1`
- `startSyncBlockNum = <resolved block>`
- `contractAddress = [TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t]`
- `contractTopic = [USDT Transfer topic hash]`

### Acceptance
- block boundary is recorded in a manifest
- runtime config template is frozen
- no broad “all TRC20” subscription remains

---

## Block 04 — implement minimal custom event sink plugin

### Goal
Replace Kafka/Mongo with the smallest custom sink that writes local raw files.

### Deliverables
- minimal plugin implementation
- plugin packaging/build instructions
- plugin config wiring
- raw file writer
- file rotation logic
- manifest writer
- failure logging

### Design requirements
- sink writes **NDJSON** segment files
- each segment has deterministic metadata
- each segment is marked with status
- plugin failures are visible in logs
- writes are append-safe
- gzip rotation is allowed

### Acceptance
- FullNode starts with `--es`
- plugin loads successfully
- only USDT Transfer-related events appear in raw files
- raw files are rotated and registered in manifests

---

## Block 05 — add extraction supervisor and restart safety

### Goal
Make the bounded extraction restartable without guesswork.

### Deliverables
- supervisor script/service
- run manifest
- `run_state.sqlite`
- restart logic
- segment reconciliation logic

### Required SQLite objects
At minimum:
- `runs`
- `segments`
- `extraction_checkpoints`
- `load_checkpoints`
- `validation_checks`

### Acceptance
- interrupted run can be resumed
- already closed raw segments are not lost
- segment/manifests stay consistent after restart
- operator can see current run status from local state

---

## Block 06 — implement normalization pipeline

### Goal
Convert raw event payloads into canonical USDT transfer rows.

### Deliverables
- raw reader
- parser
- canonical row builder
- deterministic `event_id` builder
- reject log for malformed rows
- unit tests for decoding

### Canonical event fields
- `event_id`
- `block_number`
- `block_timestamp`
- `block_hash`
- `tx_hash`
- `log_index`
- `contract_address`
- `from_address`
- `to_address`
- `amount_raw`
- `amount_decimal`
- `raw_topic0`
- `raw_topic1`
- `raw_topic2`
- `raw_data`
- `source_segment_id`
- `ingested_at`

### Acceptance
- sample raw file is decoded correctly
- duplicate replay does not create different canonical ids
- malformed rows are isolated and logged

---

## Block 07 — create ClickHouse schema and bulk loader

### Goal
Create the final persistent analytical store.

### Deliverables
- ClickHouse DDL for canonical table
- ClickHouse DDL for address-leg table
- load audit DDL
- bulk loader
- idempotent batch load posture
- loader metrics/report

### Required tables
- `usdt_transfer_events`
- `usdt_address_legs`
- `load_audit`

### Acceptance
- sample month can be loaded successfully
- counts match source manifests
- duplicate loads are handled safely
- address query latency is acceptable for local investigative use

---

## Block 08 — build wallet-centric projection

### Goal
Materialize the query surface actually needed by the investigation workflow.

### Deliverables
- address-leg transformation
- direction assignment
- counterparty derivation
- query examples
- validation queries

### Required query support
- address + time range
- address + counterparty
- tx hash lookup
- minimum amount filter
- forward/backward pagination

### Acceptance
- known sample addresses return correct inbound/outbound history
- counterparties can be listed efficiently
- projection rows are traceable back to canonical events

---

## Block 09 — run representative sample and measure storage

### Goal
Avoid guessing final ClickHouse size.

### Deliverables
- one representative month extract and load
- measurement report
- compressed bytes / row estimate
- total storage estimate for full period
- final disk recommendation for ClickHouse

### Required measurement outputs
- rows loaded
- `bytes_on_disk`
- `data_compressed_bytes`
- `data_uncompressed_bytes`
- rows per month
- estimated rows for target full period

### Acceptance
- sizing report exists
- final ClickHouse disk purchase can be justified from actual measured data
- phase 1 avoids premature oversizing

---

## Block 10 — execute full bounded extraction

### Goal
Run the full approved historical period.

### Deliverables
- extraction run
- manifests for all segments
- ClickHouse loaded data
- daily or per-run progress report
- final reconciliation report

### Acceptance
- all approved blocks are covered
- manifests show no unexplained gaps
- ClickHouse counts reconcile with manifests
- sample rows validate against external spot checks

---

## Block 11 — validation, audit, and handoff

### Goal
Prove that the extracted dataset is usable and explain its limits.

### Deliverables
- validation report
- random sample tx checks
- gap report
- duplicate report
- known limitations section
- operator handoff notes

### Required validation themes
- completeness over block span
- duplicate detection
- event identity consistency
- address-history correctness
- amount parsing correctness

### Acceptance
- validation report signed off
- major inconsistencies resolved or explicitly documented

---

## Block 12 — decommission temporary node infrastructure

### Goal
End recurring node cost once extraction is complete.

### Deliverables
- FullNode shutdown runbook
- archive of configs/code/manifests/reports
- optional raw-file retention decision
- server teardown checklist

### Acceptance
- temporary FullNode is stopped
- temporary node data is removed or archived by decision
- ClickHouse remains as the long-term local index
- no unnecessary recurring FullNode spend remains

---

## Block 13 — optional Gold BC adapter block

### Goal
Expose the resulting local dataset to Gold BC later, if approved.

### Deliverables
- adapter contract draft
- mapping from local canonical rows to Gold BC expected historical transfer shape
- integration notes
- non-goals statement preserving Gold BC business rules

### Acceptance
- adapter path is documented
- no Gold BC business policy is rewritten

This block is optional for the bounded one-off extractor delivery.
It should not delay the extractor project itself.

---

## 3. Minimal technical choices frozen for v2

### FullNode
- `java-tron` FullNode
- temporary only
- snapshot-bootstrapped

### Event framework
- Event Service Framework **V2**
- `version = 1`

### Filter posture
- one contract
- one event family
- filter at subscription layer

### Sink posture
- custom minimal local file sink
- NDJSON first
- Parquet optional later

### Metadata posture
- SQLite, not PostgreSQL

### Final query store
- ClickHouse

### Repo posture
- local git only

---

## 4. Required artifacts by project completion

1. architecture doc  
2. master spec by blocks  
3. local repo/workspace  
4. provision scripts  
5. snapshot bootstrap scripts  
6. custom sink plugin  
7. supervisor/checkpoint logic  
8. normalization pipeline  
9. ClickHouse DDL  
10. bulk loader  
11. measurement report  
12. validation report  
13. teardown runbook

---

## 5. Hard no list

The project must not drift into:

- permanent node ops in phase 1
- Kafka-first design
- MongoDB-first design
- generalized multi-contract explorer scope
- mandatory GitHub workflow
- UI-first development before extraction correctness
- speculative ClickHouse oversizing without sample measurement
