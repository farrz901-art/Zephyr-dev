# 2.仓库地图

# Repo Map (Zephyr-dev)

This is a monorepo (uv workspace). Key packages live under `packages/`.

Latest stable reference SHA (update as needed): 49ffa9bec0c785c0dfc31c82ddf6319c0027f237

---

## Root-level
- `.github/workflows/ci.yml`
  - CI gates: ruff format/check, pyright strict, mypy, pytest, py_compile tools
  - Actions are pinned by SHA (supply-chain hardening)
- `Makefile`
  - `make tidy / make check / make test`
- `docker-compose.uns-api.yml`
  - starts `uns-api` (Unstructured API service) on localhost:8001
- `docs/`
  - project charter, invariants, repo map, contract index
- `tools/`
  - `partition_dump.py` (CLI dump of artifacts via zephyr-ingest/uns-stream)
  - `uns_api_smoke.py` (connectivity helper)

---

## packages/zephyr-core (Contracts + governance primitives)
Purpose: single source of truth for contracts and stable types.

Key files:
- `zephyr_core/contracts/v1/models.py`
  - `DocumentMetadata`, `EngineInfo`, `ZephyrElement`, `PartitionResult`
- `zephyr_core/contracts/v1/enums.py`
  - `PartitionStrategy`, `RunOutcome`
- `zephyr_core/contracts/v1/run_meta.py`
  - `RunMetaV1`, `EngineMetaV1`, `MetricsV1`, `ErrorInfoV1`
- `zephyr_core/contracts/v1/document_ref.py`
  - `DocumentRef` (ingest input reference)
- `zephyr_core/run_context.py`
  - `RunContext` (run_id/pipeline_version/timestamp/schema_version)
- `zephyr_core/versioning.py`
  - `PIPELINE_VERSION`, `RUN_META_SCHEMA_VERSION`
- `zephyr_core/errors/*`
  - `ErrorCode`, `ZephyrError` (typed attrs: code/message/details)

Platform-level typed contracts (SSOT):
  * `zephyr_core/contracts/v2/delivery_payload.py`
    * `DeliveryPayloadV1`, `ArtifactsPathsV1`
  * `zephyr_core/contracts/v2/batch_report.py`
    * `BatchReportV1` (+ metrics, stage durations)
  * `zephyr_core/contracts/v2/config_snapshot.py`
    * `ConfigSnapshotV1` (+ sources)
  * `zephyr_core/contracts/v2/spec.py`
    * `ConnectorSpecV1`, `SpecFieldV1`

---

## packages/uns-stream (Document partition engine adapter)
Purpose: unified interface over Unstructured (local python lib or uns-api over HTTP).
Callers do NOT depend on `unstructured.*`.

Key files:
- `uns_stream/service.py`
  - `partition_file(...)` orchestrates: compute sha/meta → backend.partition_elements → build `PartitionResult`
  - governance: logging + error wrapping + retryable injection
- `uns_stream/partition/*`
  - stable external API functions, e.g.
    - `uns_stream.partition.auto.partition`
    - `uns_stream.partition.pdf.partition_pdf`
    - `uns_stream.partition.docx.partition_docx`
- `uns_stream/backends/local_unstructured.py`
  - in-process backend calling `unstructured.partition.*`
- `uns_stream/backends/http_uns_api.py`
  - HTTP backend calling uns-api endpoint `/general/v0/general`
- `uns_stream/_internal/normalize.py`
  - metadata normalization: remove file_directory, bool cast, bbox derivation
- `uns_stream/_internal/serde.py`
  - element conversion to `ZephyrElement`
- `uns_stream/_internal/retry_policy.py`
  - `is_retryable_exception(...)` for partition failures

Tests:
- unit tests validate routing/shims, metadata normalization, error wrapping, logging
- integration tests are optional/skip unless extras+fixtures exist

---

## packages/zephyr-ingest (Pipeline runner + destinations)
Purpose: batch processing pipeline:
source → partition → artifacts → destinations → reports

Key files:
- `zephyr_ingest/cli.py`
  - `zephyr-ingest run ...`
  -`zephyr-ingest replay-delivery ...` (webhook/kafka/weaviate/all)
  -`zephyr-ingest dlq prune ...`
  -`zephyr-ingest config resolve/init ...`
  -`zephyr-ingest spec list/show ...`
  -`zephyr-ingest metrics export-prom ...`
  -`zephyr-ingest bench ...`
- `zephyr_ingest/runner.py`
  - `_process_one(...)` processes a single `DocumentRef`
  - `run_documents(...)` orchestrates serial/thread workers + report aggregation
  - lockfile `.zephyr.lock` prevents same-sha concurrent writes
- `zephyr_ingest/sources/local_file.py`
  - local file discovery (supports multiple paths + brace-like glob expansion)
- `zephyr_ingest/destinations/base.py`
  - `Destination` Protocol (keyword-only), `DeliveryReceipt`
- `zephyr_ingest/destinations/filesystem.py`
  - writes artifacts via `dump_partition_artifacts`
- `zephyr_ingest/destinations/webhook.py`
  - posts DeliveryPayloadV1 with retry
- `zephyr_ingest/destinations/fanout.py`
  - fan-out to (filesystem, webhook, ...)
- `zephyr_ingest/_internal/delivery_payload.py`
  -builder + compat wrapper (SSOT types live in `zephyr_core/contracts/v2/delivery_payload.py`)
- `zephyr_ingest/obs/batch_report_v1.py`
  -compat wrapper (SSOT types live in `zephyr_core/contracts/v2/batch_report.py`)
- `zephyr_ingest/config/snapshot_v1.py`
  -compat wrapper (SSOT types live in `zephyr_core/contracts/v2/config_snapshot.py`)
- `zephyr_ingest/spec/types.py`
  -compat wrapper (SSOT types live in `zephyr_core/contracts/v2/spec.py`)
- `zephyr_ingest/_internal/delivery_dlq.py`
  - write DLQ records on delivery failures
- `zephyr_ingest/replay_delivery.py`
  - replay DLQ delivery records to webhook/kafka/weaviate (fanout supported)

Outputs:
- per-document: `<out_root>/<sha256>/*`
- batch: `<out_root>/batch_report.json`
- DLQ: `<out_root>/_dlq/delivery/*.json`, replay moves to `delivery_done/`

---

## packages/zephyr-api (optional future control plane)
Currently not required for Phase2 pipeline, but reserved for:
- auth/rate limiting
- multi-tenant control plane
- dashboard/operations UI
