# LOCAL_TRON_NODE_ARCHITECTURE_v2_ONE_OFF_USDT_BACKFILL

Date: 2026-04-15  
Status: architecture freeze v2  
Supersedes: v1 perpetual/local-source draft

---

## 1. Revised project intent

This project is **not** a permanent TRON historical platform in phase 1.

This project is now frozen as a **bounded one-off historical extraction contour**:

- temporary TRON FullNode from official snapshot
- historical replay for **USDT on TRON only**
- bounded backfill starting from **2023-09-01**
- export into our own index
- serve queries from our own index
- **stop and remove the FullNode after successful extraction**

The steady-state product is the **local index in ClickHouse**, not a forever-running TRON node.

---

## 2. Scope freeze

## In scope

- temporary `java-tron` FullNode on Linux
- bootstrap from official FullNode RocksDB snapshot
- historical event replay using TRON Event Service Framework V2
- only one target contract in phase 1:
  - `USDT (TRC20)` on TRON
  - contract: `TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t`
- only one target event family in phase 1:
  - `Transfer(address,address,uint256)`
- custom minimal sink to local files
- bulk load into ClickHouse
- queryable local historical index
- local git only
- Codex edits files directly on the server/workspace

## Out of scope

- permanent FullNode service
- live tail / forever-sync in phase 1
- Kafka cluster
- MongoDB Event Query Service
- broad TRON explorer platform
- indexing all contracts
- GitHub remote / public repo / CI pipeline

---

## 3. Core architecture decision

The architecture is now:

**temporary extractor node -> raw local event files -> S3 buffer -> Frankfurt loader -> ClickHouse query index**

Not:

**permanent full historical stack -> forever-running node -> always-on event service**

This is the main v2 correction.

---

## 4. Important technical nuance

TRON officially recommends the **local event plugin subscription** contour for durable event handling and historical replay, and officially ships Kafka and MongoDB plugin examples.

For this project, we will **not** deploy Kafka or MongoDB.

Instead, we will implement a **minimal custom plugin sink** using the event-plugin interface model and write events to local files.

### Consequence

“Write directly to local files / NDJSON / Parquet” is treated as a **custom engineering target**, not as an out-of-the-box stock destination.

That is acceptable for this project because the scope is narrow and bounded.

---

## 5. Phase-1 frozen topology

## A. Temporary extractor host

Runs only for bootstrap + historical extraction.

Components:

- Linux host
- `java-tron` FullNode
- TRON event subscription enabled with `--es`
- Event Service Framework V2
- custom minimal file-sink plugin
- local operational state:
  - `run_state.sqlite`
  - manifests / checkpoints
  - rotating raw event segment files

This host is disposable after completion.

## B. Persistent analytics host

Runs the final query domain.

Components:

- S3 raw buffer in `eu-central-1`
- Frankfurt loader/writer inside stage VPC
- ClickHouse
- canonical transfer-event table
- wallet-centric address-leg table
- optional load audit tables

This host remains after extraction is complete.

## C. Operator workspace

Minimal development contour:

- one local directory on server or mounted workspace
- local git repository only
- Codex works directly against files
- no GitHub remote required

---

## 5.1 Inter-region bridge freeze

The temporary extractor host in `ap-southeast-1` must **not** write directly to ClickHouse.

The frozen bridge is:

- extractor host writes raw segments and manifests to S3
- S3 bucket remains in `eu-central-1`
- loader runs inside the Frankfurt stage VPC
- loader reads from S3 through the existing gateway endpoint
- loader writes to ClickHouse through the existing private endpoint

This avoids:

- opening ClickHouse to the public internet
- building cross-region private networking only for a one-off job
- coupling the disposable extractor host to the long-lived analytics network path

---

## 6. Extraction method freeze

## 6.1 Node bootstrap

The FullNode must be initialized from the official **RocksDB FullNode snapshot** using the streaming extract method to avoid needing a second large temporary disk copy.

## 6.2 Historical replay

Use Event Service Framework V2 with:

- `event.subscribe.version = 1`
- `startSyncBlockNum = <resolved block height for 2023-09-01 UTC>`

The exact block number is **not** hardcoded in architecture.
It must be resolved and recorded during Block 01/02.

## 6.3 Event filtering

The extractor must filter as early as possible:

- `contractAddress = [TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t]`
- `contractTopic = [Transfer(address,address,uint256) topic hash]`

The project must not subscribe to broad TRC20 traffic and trim later.
Filtering must happen at the event subscription layer.

## 6.4 Chosen raw trigger posture

Phase 1 canonical extract will use the **contract log/event subscription contour** for the USDT Transfer stream only.

The downstream normalizer is responsible for converting raw event payloads into canonical transfer rows.

---

## 7. Raw sink freeze

## 7.1 Primary raw format

Primary raw capture format:

- **NDJSON segment files**

Reason:

- append-friendly
- simplest failure recovery
- easy gzip rotation
- easy replay into loader
- no heavy buffering logic inside plugin

## 7.2 Optional intermediate compaction

Optional later step:

- compact NDJSON segments into Parquet before ClickHouse load

This step is optional.
It should not block initial delivery.

## 7.3 Segment rotation

Raw files must be rotated by bounded size and/or bounded event count, for example:

- `raw/usdt_transfer_000001.ndjson.gz`
- `raw/usdt_transfer_000002.ndjson.gz`

Each segment must have a matching manifest record with:

- segment id
- first block
- last block
- first event key
- last event key
- row count
- checksum
- created_at
- status

---

## 8. Minimal persistent state freeze

To keep the stack minimal, phase 1 does **not** introduce a separate PostgreSQL control plane.

Use:

- local SQLite for operational state on extractor side
- ClickHouse for final analytical/query storage

### SQLite responsibilities

- run metadata
- segment manifests
- extraction checkpoints
- loader checkpoints
- validation status

This keeps the bounded one-off project much smaller.

---

## 9. ClickHouse data model freeze

## 9.1 Canonical table

Table: `usdt_transfer_events`

One row = one canonical USDT TRC20 transfer event.

Required fields:

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
- `ingested_at`
- `source_segment_id`

### Canonical identity rule

`event_id` is deterministic and derived from:

- `tx_hash`
- `log_index`
- `contract_address`

## 9.2 Wallet-centric projection

Table: `usdt_address_legs`

One transfer creates two rows:

- outbound leg for `from_address`
- inbound leg for `to_address`

Required fields:

- `leg_id`
- `event_id`
- `address`
- `direction`
- `counterparty_address`
- `block_number`
- `block_timestamp`
- `tx_hash`
- `log_index`
- `amount_raw`
- `amount_decimal`
- `contract_address`

This is the primary fast query surface for address history.

## 9.3 Load audit

Table: `load_audit`

Required fields:

- `batch_id`
- `source_segment_id`
- `rows_read`
- `rows_loaded`
- `rows_rejected`
- `first_block`
- `last_block`
- `status`
- `error_note`
- `started_at`
- `finished_at`

---

## 10. Query contract freeze

The final system must answer at least:

1. all USDT transfers for one address in a time range
2. all counterparties for one address in a time range
3. all transfers above a minimum amount
4. all transfers for one tx hash
5. paginated address history
6. block-range scans for validation

Phase 1 may expose this as:

- SQL first
- minimal API second

A polished UI is not required.

---

## 11. Resource envelope freeze

## 11.1 Temporary extractor host

Preferred practical temporary shape:

- storage-optimized instance
- **16 vCPU**
- **128 GiB RAM**
- **~3.75 TB local NVMe**
- unrestricted or strong network throughput
- Linux

Reference shape:
- `i4i.4xlarge` in `ap-southeast-1` is acceptable as a temporary extractor profile

### Practical note

This is viable only if we truly use:

- streaming snapshot extract
- one bounded raw-sink contour
- no second full local snapshot copy
- no MongoDB/Kafka sidecar storage

This is a **minimum practical temporary volume**, not a luxurious one.

## 11.2 ClickHouse host

Phase-1 cost posture:

- start with **500 GB to 1 TB NVMe**
- no replication in phase 1
- measure one representative month first
- then extrapolate actual storage need from measured compressed bytes per row

This avoids buying final storage based on guesswork.

---

## 12. Operational lifecycle freeze

## Before extraction

- provision temporary extractor
- initialize local git repo
- place code/config/scripts locally
- bootstrap FullNode from snapshot
- verify node health
- resolve exact start block for `2023-09-01 UTC`

## During extraction

- historical replay only for USDT Transfer
- write raw segments
- maintain manifests/checkpoints
- normalize and bulk-load to ClickHouse
- build address-leg projection
- validate totals and sample rows

## After extraction

- finalize reconciliation reports
- confirm ClickHouse query completeness
- archive code/config/manifests
- stop FullNode
- remove temporary node data and temporary server

The FullNode is **not retained** unless a later phase explicitly changes scope.

---

## 13. Validation posture freeze

The project is accepted only if all are true:

1. bounded historical extraction starts from the approved start block
2. only USDT Transfer events are extracted in phase 1
3. deterministic event identity prevents duplicate canonical rows
4. ClickHouse contains the canonical transfer table
5. ClickHouse contains the address-leg projection
6. address-history queries work over the extracted period
7. manifests/checkpoints allow restart and replay
8. the temporary FullNode can be removed after completion
9. no Kafka, no MongoDB, and no GitHub remote were required

---

## 14. Hard prohibitions

Phase 1 must not:

- silently broaden scope to all TRC20 contracts
- keep a permanent node “just in case”
- introduce Kafka unless explicitly approved later
- introduce MongoDB Event Query Service unless explicitly approved later
- require a public GitHub repo
- rebuild Gold BC business logic
- block delivery waiting for a perfect generalized platform

---

## 15. Recommended minimal directory layout

```text
local-tron-usdt-backfill/
  README.md
  ARCHITECTURE.md
  MASTER_SPEC_BY_BLOCKS.md

  configs/
    fullnode/
      config.conf.template
    extractor/
      runtime.env.example

  extractor/
    plugin/
    supervisor/
    state/
      run_state.sqlite

  loader/
    normalize/
    clickhouse/

  sql/
    clickhouse/
    sqlite/

  scripts/
    provision/
    snapshot/
    run/
    validate/
    teardown/

  raw/
  logs/
  reports/
```

Local git is initialized in this directory.
No GitHub remote is assumed.

---

## 16. Source validation basis used for freeze

This v2 freeze was aligned against:

- TRON Event Subscription / Event Service Framework V2
- TRON Kafka/Mongo event plugin deployment docs
- official TRON snapshot docs
- official java-tron deployment sizing docs
- official Tether supported protocols page
- AWS i4i instance specs
- ClickHouse `system.parts` docs for measured sizing
