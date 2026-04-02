# 3.契约清单

# Contracts Index (Zephyr)

This document lists all contract-like outputs and their sources of truth.

---

## Contract versioning note (v1 vs v2 SSOT)

- `zephyr_core/contracts/v1/*` remains the SSOT for **per-document** contracts (PartitionResult, RunMetaV1, etc).
- `zephyr_core/contracts/v2/*` is the SSOT for **platform-level typed contracts** used across packages:
- Delivery payload
- Batch report
- Config snapshot
- Connector spec

Moving SSOT to `contracts/v2` does **not** necessarily change on-disk `schema_version` immediately.

## 1) Partition contract (uns-stream output)
**Source of truth:** `zephyr-core/contracts/v1/models.py`

- `PartitionResult`
  - `document: DocumentMetadata`
  - `engine: EngineInfo`
  - `elements: list[ZephyrElement]`
  - `normalized_text: str`
  - `warnings: list[str]`

- `ZephyrElement`
  - `element_id: str`
  - `type: str`
  - `text: str`
  - `metadata: dict[str, Any]` (normalized)

**Metadata normalization (must remain consistent across backends):**
**Source:** `uns_stream/_internal/normalize.py`
- remove `file_directory`
- cast `"true"/"false"` to bool (e.g., `is_extracted`)
- derive `bbox` from `coordinates.points`

---

## 2) Run meta contract (run_meta.json)
**Source of truth:** `zephyr-core/contracts/v1/run_meta.py`

- `RunMetaV1`
  - `schema_version: int` (from `zephyr_core/versioning.py`)
  - `run_id: str`
  - `pipeline_version: str`
  - `timestamp_utc: str`
  - `outcome: RunOutcome | None` ("success"/"failed"/"skipped_unsupported"/"skipped_existing")
  - `document/engine` optional
  - `metrics: MetricsV1` (includes `attempts`)
  - `warnings`
  - `error: ErrorInfoV1 | None`
    - guarantee `details["retryable"]` exists on failures (injected in runner)

**Schema evolution rule:**
- Any shape change MUST bump `RUN_META_SCHEMA_VERSION`.

---

## 3) Delivery payload contract (sent to destinations)
Source of truth (SSOT): `zephyr-core/contracts/v2/delivery_payload.py`

Compat wrapper (builder + stable import path):
* `zephyr_ingest/_internal/delivery_payload.py` (must re-export v2 types and build v1 payloads)

- `DeliveryPayloadV1`
  - `schema_version`
  - `sha256`
  - `run_meta` (dict from `RunMetaV1.to_dict()`)
  - `artifacts` paths (out_dir/run_meta_path/elements_path/normalized_path)

All destinations must use the builder; do not hand-roll payload dicts.

---

## 4) Artifact layout contract (filesystem destination)
**Source of truth:** behavior of:
- `uns_stream/_internal/artifacts.py` (partition dump writer)
- `zephyr_ingest/destinations/filesystem.py` (delivery write)
- `zephyr_ingest/runner.py` (delivery_receipt, lockfile)

Per document:
- `<out_root>/<sha256>/run_meta.json` (always)
- `<out_root>/<sha256>/elements.json` (success only)
- `<out_root>/<sha256>/normalized.txt` (success only)
- `<out_root>/<sha256>/delivery_receipt.json` (if destination invoked)
- `<out_root>/<sha256>/.zephyr.lock` (transient; should not remain)

Batch:
- `<out_root>/batch_report.json`

---

## 5.5) Batch report contract (batch_report.json)

Source of truth (SSOT): `zephyr-core/contracts/v2/batch_report.py`

Compat wrapper:
* `zephyr_ingest/obs/batch_report_v1.py` (must re-export v2 types)

Key points:
* `schema_version` is required
* `metrics` and `stage_durations_ms` are part of the ops-ready contract

* * *
## 5.6) Config snapshot contract (config_snapshot)

Source of truth (SSOT): `zephyr-core/contracts/v2/config_snapshot.py`

Compat wrapper:
* `zephyr_ingest/config/snapshot_v1.py` (must re-export v2 types)

Key points:
* `config_snapshot.sources` uses flat dotted keys, e.g.:
* `runner.strategy`
* `backend.kind`
* `destinations.webhook.url`

* * *
## 5.7) Connector spec contract (spec)

Source of truth (SSOT): `zephyr-core/contracts/v2/spec.py`

Implementation registry:
* `zephyr_ingest/spec/registry.py`

* * *

## 5) Destination contract (plugins)
**Source of truth:** `zephyr_ingest/destinations/base.py`

- `Destination` is a keyword-only Protocol:
  - read-only `name` property
  - `__call__(*, out_root, sha256, meta, result=None) -> DeliveryReceipt`

- `DeliveryReceipt`
  - `destination: str`
  - `ok: bool`
  - `details: dict[str, Any] | None`

Fanout:
- `FanoutDestination` returns receipt.destination="fanout"
- `receipt.details["receipts"]` contains per-child receipts for stats

---

## 6) Delivery DLQ contract (delivery failures)
**Source of truth:** `zephyr_ingest/_internal/delivery_dlq.py`

Location:
- `<out_root>/_dlq/delivery/*.json`
Success replay moves to:
- `<out_root>/_dlq/delivery_done/*.json`

Record MUST contain:
- sha256, run_id, destination_receipt, run_meta, written_at_utc

---

## 7) Replay contract (replay-delivery)
**Source of truth:** `zephyr_ingest/replay_delivery.py`
- Reads delivery DLQ records
- Re-sends to one or more destinations (webhook/kafka/weaviate) via replay sinks
- Moves to delivery_done when ok and move_done=True
