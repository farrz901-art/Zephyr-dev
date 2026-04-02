# Ops-Ready Contract (P2-M6-05)

This document is an operational index of Zephyr's **stable, parseable contracts** for production use:

- log event contracts
- batch_report.json contract
- DLQ (replay + prune) contract
- Prometheus export contract
- bench contract

The SSOT for shapes lives in `zephyr_core/contracts/v2/*` (platform typed contracts) and selected
runtime emitters in `zephyr_ingest/obs/*` and `uns_stream/service.py`.

---

## 1) Log event contracts

### 1.1 zephyr-ingest events (SSOT)

SSOT: `packages/zephyr-ingest/src/zephyr_ingest/obs/events.py`

Stable event names (prefix):
- `run_start`, `run_done`
- `doc_start`, `doc_done`
- `delivery_start`, `delivery_done`, `delivery_failed`
- `dlq_written`
- `replay_start`, `replay_attempt`, `replay_result`, `replay_done`

Field contract (minimum):
- `run_id`, `pipeline_version`
- doc/delivery scope: `sha256`
- delivery scope: `destination`, `ok` (or receipt-derived)
+### 1.1.1 Event name + minimum fields (operational contract)
+
| Event | Minimum fields (must exist when meaningful) | Recommended fields |
|---|---|---|
| `run_start` | `run_id`, `pipeline_version`, `out_root` | `workers`, `executor` |
| `run_done` | `run_id`, `pipeline_version`, `docs_total`, `delivery_ok`, `delivery_failed` | `dlq_written_total`, `run_wall_ms` |
| `doc_start` | `run_id`, `pipeline_version`, `sha256` | `uri`, `extension` |
| `doc_done` | `run_id`, `pipeline_version`, `sha256`, `outcome` | `hash_ms`, `partition_ms`, `delivery_ms`, `attempts` |
| `delivery_start` | `run_id`, `pipeline_version`, `sha256`, `destination` | `outcome` |
| `delivery_done` | `run_id`, `pipeline_version`, `sha256`, `destination`, `ok` | `attempts` |
| `delivery_failed` | `run_id`, `pipeline_version`, `sha256`, `destination` | `retryable`, `error_code` |
| `dlq_written` | `run_id`, `pipeline_version`, `sha256`, `destination`, `dlq_path` |  |
| `replay_start` | `out_root`, `destination`, `total` | `limit`, `dry_run`, `move_done` |
| `replay_attempt` | `destination`, `dlq_file`, `sha256`, `run_id` |  |
| `replay_result` | `destination`, `dlq_file` | `ok`, `retryable`, `invalid_record` |
| `replay_done` | `out_root`, `destination`, `attempted`, `succeeded`, `failed` | `moved_to_done` |


Note:
- `doc_done` is expected to include stage timings when available:
- `hash_ms`, `partition_ms`, `delivery_ms`

### 1.1.2 Example log lines (illustrative)

These are example shapes; exact values vary:

```
run_start out_root=/abs/out pipeline_version=p1 run_id=<uuid> workers=4
doc_done delivery_ms=12 hash_ms=1 outcome=success partition_ms=203 pipeline_version=p1 run_id=<uuid> sha256=<sha>
delivery_failed destination=webhook pipeline_version=p1 retryable=true run_id=<uuid> sha256=<sha>
dlq_written destination=webhook dlq_path=/abs/out/_dlq/delivery/<file>.json pipeline_version=p1 run_id=<uuid> sha256=<sha>
```

### 1.2 uns-stream partition events (SSOT)

SSOT: `packages/uns-stream/src/uns_stream/service.py`

Stable event names:
- `partition_start`, `partition_done`
- `partition_error` (ZephyrError)
- `partition_failed` (unexpected exception wrapped)

Field contract (minimum, when provided by caller):
- `run_id`, `pipeline_version`, `sha256`
- `kind`, `strategy`, backend engine info

### 1.2.1 Event name + minimum fields (operational contract)

| Event | Minimum fields | Recommended fields |
|---|---|---|
| `partition_start` | `file`, `kind`, `strategy`, `engine`, `backend`, `sha256` | `run_id`, `pipeline_version`, `bytes`, `kwargs` |
| `partition_done` | `file`, `kind`, `strategy`, `sha256`, `ms` | `run_id`, `pipeline_version`, `elements`, `text_len` |
| `partition_error` | `file`, `kind`, `strategy`, `sha256`, `code`, `retryable` | `run_id`, `pipeline_version`, `ms` |
| `partition_failed` | `file`, `kind`, `strategy`, `sha256`, `exc_type`, `retryable` | `run_id`, `pipeline_version`, `ms` |

Note:
- `run_id` / `pipeline_version` are included when passed by the caller (zephyr-ingest runner).

---

## 2) batch_report.json contract

SSOT (typed contract): `packages/zephyr-core/src/zephyr_core/contracts/v2/batch_report.py`

File location:
- `<out_root>/batch_report.json`

Key points:
- `schema_version` is required.
- `metrics` is required for ops baselines (wall time, docs/min, totals).
- `stage_durations_ms` may be present (hash/partition/delivery min/max/avg/p95).
- `counts_by_error_code` includes both partition error codes and delivery error codes (from receipts).

### 2.1 Metric naming expectations (pragmatic)

- Batch jobs emit **per-run gauges** (not long-lived counters).
- Durations are exported in Prometheus as **seconds** (see prom exporter).

---

## 3) DeliveryPayloadV1 contract

SSOT: `packages/zephyr-core/src/zephyr_core/contracts/v2/delivery_payload.py`

Delivery payload must contain:
- `schema_version = 1`
- `sha256`
- `run_meta` (dict from `RunMetaV1.to_dict()`)
- `artifacts` absolute paths:
- `run_meta_path`, `elements_path`, `normalized_path`

This is the payload shape used by:
- Webhook destination
- Kafka destination
- replay-delivery sinks

---

## 4) DLQ contract

See `docs/DLQ.md` for directory layout and retention.

Key commands:
- `zephyr-ingest replay-delivery --dest webhook|kafka|weaviate|all ...`
- `zephyr-ingest dlq prune ...`

---

## 5) Prometheus export contract

SSOT emitter: `packages/zephyr-ingest/src/zephyr_ingest/obs/prom_export.py`

Command:
```bash
zephyr-ingest metrics export-prom --out <out_root> [--textfile <path.prom>]
```

Important contract constraints:
- The exporter must emit valid Prometheus text exposition format.
- Metric names should be treated as an operational contract.
- Label keys are intended to be low-cardinality (e.g. pipeline_version/strategy/executor/backend_kind).

### 5.1 Key exported metric families (non-exhaustive)

| Metric | Meaning |
|---|---|
| `zephyr_ingest_run_info{...} 1` | Run identity labels (pipeline_version/strategy/executor/backend_kind) |
| `zephyr_ingest_run_wall_seconds{...}` | Wall-clock runtime (seconds) |
| `zephyr_ingest_run_docs_per_minute{...}` | Derived throughput |
| `zephyr_ingest_run_delivery_failed_total{destination="..."} ` | Delivery failed by destination |
| `zephyr_ingest_run_stage_duration_seconds{stage="partition",stat="p95"}` | Stage duration stats |

---

## 6) Bench contract

See `docs/BENCH.md`.

Bench consumes:
- `<out_root>/batch_report.json.metrics`

and prints a JSON summary with per-iteration results.

---

## 7) Sources key naming convention (config_snapshot.sources)

SSOT: `packages/zephyr-core/src/zephyr_core/contracts/v2/config_snapshot.py`

`sources` uses flat dotted keys. Examples:
- `runner.strategy`
- `backend.kind`
- `backend.api_key`
- `destinations.webhook.url`
- `destinations.kafka.brokers`

This is an operational contract: avoid renaming keys unless you bump schema / provide migration guidance.
