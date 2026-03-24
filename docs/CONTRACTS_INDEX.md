# 3.契约清单

# Contracts Index (Zephyr)

This document lists all contract-like outputs and their sources of truth.

---

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
**Source of truth:** `zephyr-ingest/_internal/delivery_payload.py`

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
- Re-sends to webhook (using WebhookDestination.post_payload + retry)
- Moves to delivery_done when ok and move_done=True
