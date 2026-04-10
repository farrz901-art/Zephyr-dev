# AGENTS.md

This file adds `it-stream` package-local guardrails on top of the repository root `AGENTS.md`.
Use it for `it-stream` recovery, checkpoint, and source/runtime changes.

## Package role
- `it-stream` owns structured-flow parsing and local checkpoint/resume semantics for `it` inputs.
- It must keep flow-local recovery/runtime behavior in-package and must not push first-generation
  recovery mechanics into shared orchestration surfaces casually.

## Recovery governance boundary
Treat the current P4 recovery model as an explicit architecture boundary, not an implementation
accident.

Keep these concepts distinct:
- task identity: identifies the requested work
- checkpoint identity: identifies a specific checkpoint entry for that work
- lineage: `parent_checkpoint_identity_key` links only the immediate prior checkpoint
- progress family: `progress_kind` is explicit and remains separate from raw `progress` payload data
- resume selection: selected checkpoint facts stay explicit and inspectable
- resume provenance: resumed execution facts stay explicit and inspectable
- recovery failure class: `malformed`, `incompatible`, `unsupported`, and `blocked` remain distinct

Do not collapse these concepts into one another through convenience helpers, implicit inference, or
source-specific shortcuts.

## Current supported recovery model
- Compatibility/schema checks happen before resume selection and remain separate from runtime resume
  decisions.
- Resume selection is explicit and currently supports the proven cursor-based subset.
- Recognized progress families may still be unsupported for resume; recognition is not the same as
  resumability.
- Blocked recovery within a supported family is distinct from malformed and incompatible
  checkpoints.
- Real-path HTTP cursor source recovery is currently bounded by the existing execution model:
  cursor-state continuation is supported, while record-level continuation on that path remains
  blocked unless resumable record timestamps exist.

## Current second-round `it-stream` source breadth
The current second-round non-enterprise `it-stream` source breadth is explicitly supported as this
bounded set:
- `postgresql_incremental_v1`
- `clickhouse_incremental_v1`
- `kafka_partition_offset_v1`
- `mongodb_incremental_v1`

These sources are supported only in the narrow subsets already proven by code and anti-drift tests:
- `postgresql_incremental_v1`: ordered incremental table reads over one ascending cursor column,
  explicit selected columns, optional starting cursor, bounded batch size, and shared
  `cursor_v1` checkpoint/resume behavior
- `clickhouse_incremental_v1`: ordered incremental warehouse table-query reads over one ascending
  cursor column, explicit selected columns, optional starting cursor, bounded batch size, and
  shared `cursor_v1` checkpoint/resume behavior
- `kafka_partition_offset_v1`: explicit topic plus one partition, ascending offset progression,
  optional starting offset, bounded batch size, JSON-object payload subset, and shared `cursor_v1`
  checkpoint/resume behavior
- `mongodb_incremental_v1`: ordered collection reads over one ascending cursor field, explicit
  projected fields, optional starting cursor, bounded batch size, and shared `cursor_v1`
  checkpoint/resume behavior

Across that full second-round breadth, these semantics are currently shared and should be treated
as the stable batch boundary:
- task identity is derived from stable source selector plus intended read slice; secrets, transient
  sessions, and inspect-only toggles must not affect identity
- progress family stays explicit as `cursor_v1`
- checkpoint identity stays distinct from task identity
- lineage remains checkpoint-local: `parent_checkpoint_identity_key` links only the immediate prior
  checkpoint
- resume selection and resumed provenance stay explicit and inspectable
- resumed runs keep the current shared provenance shape: `run_origin="resume"`,
  `delivery_origin="primary"`, explicit resumed-from checkpoint identity, and explicit cursor
  continuation

The following semantics remain intentionally source-local and should not be normalized further
right now:
- SQL/query shaping, ordering-column selection, and row/document projection details
- Kafka broker/client construction, partition assignment, JSON decoding, and offset fetch behavior
- Mongo query/projection shaping, BSON mapping, and local cursor encoding
- ClickHouse batching/window sizing, query shaping, and engine-specific read tuning
- PostgreSQL read consistency hints, SQL dialect shaping, and local cursor encoding
- source-local progress payload fields such as `schema`, `query_mode`, `topic`, `partition`,
  `last_offset`, `collection`, `cursor_field`, `row_count`, and `document_count`

The following categories or deeper capabilities remain deferred beyond the current P4 support
boundary:
- non-cursor progress families
- CDC, changefeed, oplog-token, or log-native recovery models
- multi-partition or consumer-group-managed stream reads
- broader warehouse export/job workflows or staged-query recovery
- snapshot/session/transaction-owned database recovery semantics
- additional `it` source families outside the currently listed four-source breadth
- enterprise-managed source connectors

## Change discipline
When changing `it-stream` checkpoint/resume behavior:
- update this file in the same change if you alter the recovery boundary or the supported recovery
  subset
- keep tests focused on boundary semantics, not broad invalid-input matrices
- prefer strengthening existing checkpoint/provenance/recovery semantics over creating a parallel
  recovery path

If a future P4/P5 change intentionally expands recovery support, make the boundary change explicit
here and in focused anti-drift tests in the same patch.
