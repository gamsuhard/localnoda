# CLICKHOUSE_INTEGRATION_MEMORY

Date: 2026-04-15
Purpose: preserve ClickHouse compatibility rules before Block 07 / canonical integration work
Sources:
- `LOCAL_TRON_HISTORICAL_INDEXER_CONTEXT_V2.md`
- `LOCAL_TRON_HISTORICAL_INDEXER_MASTER_SPEC_V2.md`

---

## 1. Why this note exists

The current workspace still builds the bounded TRON USDT extractor contour.

However, later ClickHouse work must no longer assume a standalone local analytical schema.
When the project reaches canonical ClickHouse integration, it must integrate into the existing analytical substrate carefully and non-destructively.

This note is a hard reminder for future Block 07+ work.

---

## 2. Hard schema-discovery gate

Before any ClickHouse implementation or schema change, Codex must perform a compatibility discovery pass.

Mandatory discovery targets:

- current canonical table contracts
- current writer path
- current substrate reader path
- live ClickHouse schema, if access exists

At minimum inspect these compatibility boundaries first:

- `docs/contracts/clickhouse-analytics-contracts.md`
- `services/control-plane/migrations/phase2/0002_clickhouse_analytics_contracts.sql`
- `services/control-plane/src/goldusdt_control_plane/analytics_store.py`
- `services/control-plane/src/goldusdt_control_plane/phase4_substrate_adapter.py`

Do not start implementation from memory or from old migration assumptions only.

---

## 3. Mandatory first deliverable

Before staging DDL, importer code, merge code, or routing code, Codex must produce a compatibility report.

The report must answer:

- what the current repo-declared canonical schema is
- what current readers expect
- what current writers produce
- what the live schema is, if ClickHouse access exists
- whether direct canonical integration is already compatible
- whether only additive staging/merge work is needed
- whether any additive canonical migration is required

Preferred output path:

- `docs/validation/local-tron-indexer-schema-compatibility.md`

---

## 4. Protected canonical tables

The following analytical tables are protected compatibility boundaries:

- `normalized_transfers`
- `flow_facts`
- `balance_snapshots`

Hard rules:

- no destructive changes
- no drop
- no rename
- no silent incompatible schema rewrite

If canonical schema changes are ever required, they must be:

- additive only
- isolated in a separate migration
- explicitly documented
- proven compatible with existing readers

---

## 5. Staging-first integration rule

Default integration order:

1. staging tables first
2. canonical merge second

Expected local-indexer-owned staging objects include examples such as:

- `tron_local_usdt_transfers_staging`
- optional `tron_local_usdt_transfer_legs_staging`
- `tron_local_import_batches`
- optional coverage/import registry tables

Do not write node-derived rows directly into canonical analytical tables on first hop.

---

## 6. One analytical truth surface only

Algorithms must not read the node database directly.

Correct architecture:

local node -> staging tables -> canonical ClickHouse substrate -> algorithms

Wrong architecture:

- algorithms read node DB directly
- algorithms read mixed ad hoc stores depending on source
- algorithms branch between unrelated analytical truth surfaces

Future logic must continue to read one unified ClickHouse substrate.

---

## 7. Canonical target posture

The local TRON source is an additive source contour, not a new downstream substrate.

Ultimate canonical merge targets remain:

- `normalized_transfers`
- `flow_facts`

Current project posture already treats ClickHouse as the heavy analytics domain, and the current substrate reader already consumes `normalized_transfers` as persisted analytical substrate rather than only provider-inline data.

This means the integration target is:

- careful staging/import
- compatibility-preserving canonical merge
- no second analytical truth surface for downstream algorithms

---

## 8. Dedupe and lineage constraints

Every imported row must keep explicit provenance, including:

- `source_kind = local_tron_index`
- import batch id
- covered time window
- source contract address
- bounded backfill posture

Stable event identity must prevent duplicates across:

- local node imports
- existing provider-derived imports

Preferred event identity:

- network
- contract_address
- tx_hash
- log_index

---

## 9. Practical implementation reminder for this workspace

The current local DDL draft in this workspace is only a bounded extractor baseline.
It must not be treated as permission to bypass compatibility discovery when the project reaches real ClickHouse integration.

Before Block 07 or any canonical-write step:

- re-open this note
- produce the compatibility report first
- verify current repo/live schema
- keep canonical changes additive
- keep downstream algorithms on unified ClickHouse substrate only
