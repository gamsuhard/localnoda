# BLOCK_01 — Minimal local workspace and repo skeleton

Date: 2026-04-15  
Status: ready for execution  
Scope: one-off USDT-on-TRON historical backfill

---

## 1. Goal

Prepare the smallest workable local workspace for the project:

- local git only
- no GitHub remote
- versioned docs and config templates
- filesystem layout for extractor, loader, raw files, logs and reports
- SQLite schema draft for extractor-side operational state
- ClickHouse DDL draft for final analytical storage

Block 01 does **not** implement the custom plugin yet and does **not** provision the temporary FullNode host yet.

---

## 2. Frozen decisions for Block 01

1. The workspace is intentionally **local-only**. `git init` is required, `git remote add origin ...` is not.
2. `java-tron` host runtime is expected to be **Linux** in later blocks.
3. The extractor-side control plane stays minimal: `run_state.sqlite` instead of PostgreSQL.
4. The final analytical store is ClickHouse.
5. The raw capture format is NDJSON segment files with gzip allowed.
6. The phase-1 scope is still: one contract, one event family, one bounded backfill.

---

## 3. Deliverables in this package

- `README.md`
- `ARCHITECTURE.md`
- `MASTER_SPEC_BY_BLOCKS.md`
- `BLOCK_01.md`
- `.gitignore`
- `configs/` templates
- `sql/sqlite/001_run_state.sql`
- `sql/clickhouse/010_core_schema.sql`
- helper scripts in `scripts/`
- empty runtime/raw/log/report directories

---

## 4. Repo tree frozen for Block 01

```text
local-tron-usdt-backfill/
  README.md
  ARCHITECTURE.md
  MASTER_SPEC_BY_BLOCKS.md
  BLOCK_01.md
  REFERENCES.md
  .gitignore
  configs/
    fullnode/
      README.md
      config.conf.overlay.template
    extractor/
      extractor.env.example
    loader/
      clickhouse.env.example
  extractor/
    README.md
    plugin/
      README.md
    supervisor/
      README.md
  loader/
    README.md
    normalizer/
      README.md
    queries/
      README.md
  sql/
    clickhouse/
      010_core_schema.sql
      900_sample_queries.sql
    sqlite/
      001_run_state.sql
  scripts/
    00_init_local_git.sh
    01_prepare_workspace.sh
    02_init_run_state_sqlite.sh
    03_print_tree.sh
    04_apply_clickhouse_schema.sh
  artifacts/
    plugins/
  raw/
  logs/
  reports/
  runtime/
```

---

## 5. Local git policy frozen for this project

- local commits only
- no GitHub remote required
- one commit per accepted block or sub-block
- milestone tags only after explicit acceptance

Recommended commit naming pattern:

- `block-01 skeleton + configs + ddl`
- `block-02 extractor host bootstrap`
- `block-03 boundary + config freeze`

Recommended lightweight acceptance tag pattern:

- `accepted/block-01`
- `accepted/block-02`

---

## 6. Workspace conventions

### 6.1 Paths

Recommended root path on the working server:

- `/srv/local-tron-usdt-backfill`

Recommended runtime DB path:

- `/srv/local-tron-usdt-backfill/runtime/run_state.sqlite`

Recommended raw segment path:

- `/srv/local-tron-usdt-backfill/raw/`

### 6.2 What is versioned vs not versioned

**Commit to git:**

- markdown docs
- templates
- SQL
- scripts
- small code files

**Do not commit:**

- snapshots
- FullNode database
- plugin zip build output
- raw NDJSON segments
- logs
- runtime SQLite db
- secrets and `.env` files copied from examples

---

## 7. Config freeze notes

### 7.1 Why there is an overlay template instead of a full `config.conf`

`java-tron` ships a large version-specific config file. For Block 01 we freeze only the event-subscription overlay that must later be merged into the chosen release config.

This avoids carrying a stale full config before Block 02 chooses the exact FullNode release/package.

### 7.2 Current event-subscription intent

Default intent for the overlay template:

- V2 event service (`event.subscribe.version = 1`)
- historical replay from `startSyncBlockNum`
- plugin mode, not native ZeroMQ queue
- `soliditylog` enabled by default for solidified logs only
- filter by USDT contract address
- filter by `Transfer(address,address,uint256)` topic0

The exact start block and exact topic0 literal stay placeholder values until Block 03 freeze.

---

## 8. SQLite run-state freeze for Block 01

The SQLite schema is intentionally small and operational.

### Required tables

#### `runs`
One row per extraction/load run envelope.

#### `segments`
One row per raw NDJSON segment file.

#### `extraction_checkpoints`
Current extractor position and counters for restart safety.

#### `load_checkpoints`
Current loader position by target table and source segment.

#### `validation_checks`
Recorded validation results and anomalies.

### Design principles

- restart must not depend on parsing logs
- segment lifecycle must be visible from SQLite
- loader must be restartable independently of extractor
- validation results must be queryable without scanning text files

---

## 9. ClickHouse schema freeze for Block 01

The DDL in this package creates three core objects:

1. `tron_usdt_local.trc20_transfer_events`
2. `tron_usdt_local.address_transfer_legs`
3. `tron_usdt_local.load_audit`

### Design intent

- canonical event table is the first persistent truth layer
- address-leg table is the main wallet-history query layer
- `MergeTree` is used directly; no dependence on background dedup for correctness
- monthly partitioning by event time is sufficient for the bounded one-off extract

Materialized views are intentionally deferred. Initial load can write canonical rows first and populate address legs in a separate step.

---

## 10. Acceptance checklist

Block 01 is accepted only if all of the following are true:

- repo skeleton exists locally
- `git init` works and no remote is required
- `.gitignore` protects runtime and data directories
- config templates exist
- SQLite schema file exists
- ClickHouse DDL file exists
- helper scripts exist
- repo is understandable without external tribal knowledge

---

## 11. Handoff to Block 02

After Block 01 acceptance, the next block should do only these tasks:

1. provision temporary Linux extractor host
2. install JDK and `java-tron`
3. stream-restore snapshot
4. prove node health and sync visibility
5. do **not** start custom plugin work before host bootstrap is real

---

## 12. Notes for Codex execution

Codex should work against these files directly in the local workspace.

For this project:

- do not assume GitHub
- do not create CI/CD files unless explicitly requested
- do not introduce Docker unless explicitly approved for a specific subtask
- prefer plain shell + SQL + small focused source files

---

## 13. Operator quick start

```bash
cd /srv/local-tron-usdt-backfill
bash scripts/00_init_local_git.sh
bash scripts/01_prepare_workspace.sh
bash scripts/02_init_run_state_sqlite.sh
```

For ClickHouse schema bootstrap later:

```bash
export CLICKHOUSE_HOST=127.0.0.1
export CLICKHOUSE_PORT=9000
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=''
bash scripts/04_apply_clickhouse_schema.sh
```
