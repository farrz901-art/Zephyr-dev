# Observability Invariants (Non-negotiable)

This document defines Zephyr's **operational contracts** for observability.
These are treated as engineering invariants: do not change them casually.

When a change is required:
- update docs (`OPS_READY.md`, `CONFIG.md`) and tests
- consider schema/version bumps where appropriate

---

## 1) Stable log event names (contracts)

### 1.1 zephyr-ingest events (SSOT)

SSOT emitter:
- `packages/zephyr-ingest/src/zephyr_ingest/obs/events.py`
- events are emitted by:
  - `packages/zephyr-ingest/src/zephyr_ingest/runner.py`
  - `packages/zephyr-ingest/src/zephyr_ingest/replay_delivery.py`

Stable event names:
- `run_start`, `run_done`
- `doc_start`, `doc_done`
- `delivery_start`, `delivery_done`, `delivery_failed`
- `dlq_written`
- `replay_start`, `replay_attempt`, `replay_result`, `replay_done`

Invariant:
- Renaming an event is treated as a breaking ops change.
- Additive changes (new events / new fields) are allowed.

### 1.2 Minimum fields by event (must be present when meaningful)

| Event | Minimum fields | Notes |
|---|---|---|
| `run_start` | `run_id`, `pipeline_version`, `out_root` | |
| `run_done` | `run_id`, `pipeline_version` | include doc/delivery totals when available |
| `doc_start` | `run_id`, `pipeline_version`, `sha256` | uri/extension recommended |
| `doc_done` | `run_id`, `pipeline_version`, `sha256`, `outcome` | stage timing fields recommended |
| `delivery_start` | `run_id`, `pipeline_version`, `sha256`, `destination` | |
| `delivery_done` | `run_id`, `pipeline_version`, `sha256`, `destination`, `ok` | receipt-derived |
| `delivery_failed` | `run_id`, `pipeline_version`, `sha256`, `destination` | retryable/error_code recommended |
| `dlq_written` | `run_id`, `pipeline_version`, `sha256`, `destination`, `dlq_path` | |
| `replay_start` | `out_root`, `destination`, `total` | |
| `replay_attempt` | `destination`, `dlq_file`, `sha256`, `run_id` | |
| `replay_result` | `destination`, `dlq_file` | ok/retryable/invalid_record recommended |
| `replay_done` | `out_root`, `destination`, `attempted`, `succeeded`, `failed` | |

### 1.3 Stage timing fields

When emitted, the following fields are treated as stable names:
- `hash_ms`
- `partition_ms`
- `delivery_ms`

They are expected to appear in `doc_done` events when the doc was processed.

---

## 2) uns-stream partition event contracts

SSOT emitter:
- `packages/uns-stream/src/uns_stream/service.py`

Stable event names:
- `partition_start`, `partition_done`
- `partition_error` (ZephyrError)
- `partition_failed` (unexpected exception wrapped)

Minimum fields:
- Always: `file`, `kind`, `strategy`, engine info (`engine`, `backend`)
- Must be present when provided by caller (zephyr-ingest runner):
  - `run_id`, `pipeline_version`, `sha256`

Invariant:
- `zephyr-ingest` should pass `run_id/pipeline_version/sha256/size_bytes` into `uns-stream` to ensure log correlation.

---

## 3) Prometheus export invariants

SSOT exporter:
- `packages/zephyr-ingest/src/zephyr_ingest/obs/prom_export.py`

### 3.1 Text exposition validity
Invariants:
- The exporter must produce valid Prometheus text exposition format.
- A given metric name must not emit multiple conflicting `# HELP/# TYPE` blocks.

### 3.2 Unit conventions
Invariants:
- Durations must be exported in **seconds** (suffix `_seconds`).
- Per-run counts may be exported as gauges (batch job semantics).

### 3.3 Label cardinality
Invariants:
- Labels must be low-cardinality.
- Do not include `sha256`, file paths, or raw error strings as labels.

---

## 4) config_snapshot.sources naming invariants

SSOT:
- `packages/zephyr-core/src/zephyr_core/contracts/v2/config_snapshot.py`

Invariants:
- `config_snapshot.sources` uses flat dotted keys (stable strings).
- Do not rename keys without explicit migration guidance.

Examples:
- `runner.strategy`
- `backend.kind`
- `backend.api_key`
- `destinations.webhook.url`
- `destinations.kafka.brokers`

---

## 5) Testing invariants (guardrails)

Invariants:
- Tests must assert presence of key lifecycle events (`run_start/run_done`, `replay_start/replay_done`, etc.)
- Spec/CLI anti-drift tests must remain green:
  - `test_spec_consistency.py` is a gate
```
