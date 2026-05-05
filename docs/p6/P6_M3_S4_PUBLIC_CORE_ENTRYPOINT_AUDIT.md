# P6-M3-S4 public-core entrypoint audit

This audit was performed against the current Zephyr-dev working tree at source SHA
`780690a21ddc60bb5a01529d3d35358f08903685`.

## Result

- real entrypoint found: yes
- recommended entrypoint:
  - `zephyr_ingest` local `run` path
  - concretely: `LocalFileSource -> run_documents -> UnsFlowProcessor -> uns_stream.service.partition_file -> FilesystemDestination`
- requires P4.5 substrate: no
- commercial logic required: no

## What was inspected

- `packages/zephyr-ingest/src/zephyr_ingest/cli.py`
- `packages/zephyr-ingest/src/zephyr_ingest/runner.py`
- `packages/zephyr-ingest/src/zephyr_ingest/flow_processor.py`
- `packages/zephyr-ingest/src/zephyr_ingest/sources/local_file.py`
- `packages/zephyr-ingest/src/zephyr_ingest/_internal/delivery_payload.py`
- `packages/zephyr-ingest/src/zephyr_ingest/usage_record.py`
- `packages/uns-stream/src/uns_stream/sources/__init__.py`
- `packages/uns-stream/src/uns_stream/sources/http_source.py`
- `packages/uns-stream/src/uns_stream/partition/auto.py`
- `packages/uns-stream/src/uns_stream/service.py`
- `docs/p6/public_core_export_manifest.json`
- `docs/p6/base_product_capability_manifest.json`
- `docs/p6/base_local_bridge_contract.json`

## Why this qualifies as a real public-core path

The selected path is not the S3 fixture runner. It is the existing Zephyr-dev local document
processing path already used by `zephyr_ingest run` for local files. It:

- enumerates local documents through `LocalFileSource`
- routes through the real `uns` flow processor
- partitions with `uns_stream.service.partition_file`
- writes canonical runtime artifacts through `FilesystemDestination`
- emits real `run_meta.json`, `delivery_receipt.json`, and `usage_record.json`

The path was locally exercised against a `.txt` file and produced:

- `normalized.txt`
- `elements.json`
- `run_meta.json`
- `delivery_receipt.json`
- `usage_record.json`

## Current first-slice limits

- direct local file support: yes
- direct local text support: no native text-only CLI argument exists
- allowed S4 bridge strategy for local text:
  - write a temporary local `.txt` file
  - feed that file into the same real local-file path
- first-slice supported input extensions:
  - `.txt`
  - `.md`

Unsupported extensions must return a safe Base error contract instead of silently falling back to
fixture mode.

## Mapping to Base bridge artifacts

The real Zephyr-dev path already emits enough stable technical truth to map into the Base bridge
contract:

- `normalized.txt` -> `normalized_text.txt`
- delivery payload content-evidence helper -> `content_evidence.json`
- `delivery_receipt.json` -> `receipt.json`
- `usage_record.json` -> `usage_fact.json`
- mapped aggregate view -> `run_result.json`

`usage_fact.billing_semantics` remains `false`.

## Truthful blockers

No blocker prevents a real local adapter first slice for `.txt` and `.md`.

Known bounded limitations:

- Base local text requires a temporary-file bridge into the real local-file path.
- This slice does not yet bundle public core into Zephyr-base runtime packaging.
- This slice does not yet implement the Tauri command bridge.
