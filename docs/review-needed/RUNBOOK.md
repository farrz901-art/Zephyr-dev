# 5.运行手册

# Zephyr Runbook (Operations & Dev)

This runbook shows how to run Zephyr end-to-end and how to troubleshoot.

---

## 0) Prerequisites
- Python >= 3.12
- uv installed
- Docker Desktop installed (only required for uns-api backend)
- Repo uses uv workspace: `uv.lock` is committed

---

## 1) Golden commands (CI gates)
Always use these before pushing:

```bash
make tidy
make check
make test
```

------

## 2) Start uns-api service (optional backend)

Zephyr supports two partition backends:

-   `local` (default): in-process `unstructured` python library
-   `uns-api`: HTTP calls to Unstructured API service

### 2.1 Start via docker-compose



```
docker compose -f docker-compose.uns-api.yml up -d
```

Stop:



```
docker compose -f docker-compose.uns-api.yml down
```

### 2.2 Quick connectivity check



```
uv run python tools/uns_api_smoke.py
```

Notes:

-   Seeing `404` on `/` is normal; we only need the POST endpoint to be reachable.
-   Partition endpoint (POST):
    -   `http://localhost:8001/general/v0/general`

### 2.3 Authentication (401 troubleshooting)

If you get `401 Unauthorized` when calling `/general/v0/general`:

-   uns-api likely has API key enabled via `UNSTRUCTURED_API_KEY`.
-   Pass the same key via CLI: `--uns-api-key <key>`
-   Or disable key in compose and restart.

------

## 3) Run ingest pipeline (filesystem artifacts + optional webhook)

Main command:



```
uv run --package zephyr-ingest zephyr-ingest run \
  --path <INPUT_DIR> \
  --glob "**/*" \
  --out .cache/out \
  --strategy auto
```

### 3.1 Typical input path

If you use local fixtures:



```
uv run --package zephyr-ingest zephyr-ingest run \
  --path packages/uns-stream/src/uns_stream/tests/fixtures/example-docs \
  --glob "**/*.txt" \
  --out .cache/out \
  --strategy auto
```

### 3.2 Backend switching

**Local backend (default):**



```
uv run --package zephyr-ingest zephyr-ingest run \
  --path <INPUT_DIR> --glob "**/*.txt" --out .cache/out \
  --backend local
```

**uns-api backend (service required):**



```
uv run --package zephyr-ingest zephyr-ingest run \
  --path <INPUT_DIR> --glob "**/*.txt" --out .cache/out \
  --backend uns-api \
  --uns-api-url http://localhost:8001/general/v0/general \
  --uns-api-key <YOUR_KEY>
```

### 3.3 Idempotency / rerun behavior

-   `--skip-existing` (default): if `<out>/<sha>/run_meta.json` exists, skip
-   `--force`: reprocess even if exists

Example:



```
uv run --package zephyr-ingest zephyr-ingest run \
  --path <INPUT_DIR> --glob "**/*.txt" --out .cache/out \
  --skip-existing

uv run --package zephyr-ingest zephyr-ingest run \
  --path <INPUT_DIR> --glob "**/*.txt" --out .cache/out \
  --force
```

### 3.4 Workers (thread pool)



```
uv run --package zephyr-ingest zephyr-ingest run \
  --path <INPUT_DIR> --glob "**/*.txt" --out .cache/out \
  --workers 4
```

### 3.5 Partition retry



```
uv run --package zephyr-ingest zephyr-ingest run \
  --path <INPUT_DIR> --glob "**/*.txt" --out .cache/out \
  --max-attempts 3 --base-backoff-ms 200 --max-backoff-ms 5000
```

Disable retry:



```
uv run --package zephyr-ingest zephyr-ingest run \
  --path <INPUT_DIR> --glob "**/*.txt" --out .cache/out \
  --no-retry
```

------

## 4) Enable webhook delivery (filesystem + webhook fanout)

Provide webhook URL:



```
uv run --package zephyr-ingest zephyr-ingest run \
  --path <INPUT_DIR> --glob "**/*.txt" --out .cache/out \
  --webhook-url http://localhost:9001/hook
```

### 4.1 Quick webhook echo server (optional)

Create `tools/webhook_echo.py` (see project docs) and run:



```
uv run python tools/webhook_echo.py
```

------

## 5) Outputs (what to expect on disk)

### 5.1 Per-document directory

Location:

-   `<out_root>/<sha256>/`

Files:

-   `run_meta.json` (always written)
-   `elements.json` (success only)
-   `normalized.txt` (success only)
-   `delivery_receipt.json` (if destination invoked)
-   `.zephyr.lock` (transient lock, should not persist)

### 5.2 Batch report

-   `<out_root>/batch_report.json`

Contains:

-   counts (success/failed/skipped_*)
-   retry stats
-   durations stats
-   delivery stats (by destination + fanout children)

------

## 6) Delivery DLQ & replay (webhook reliability)

### 6.1 DLQ location

-   `<out_root>/_dlq/delivery/*.json`
-   after successful replay moved to:
    -   `<out_root>/_dlq/delivery_done/*.json`

### 6.2 Replay delivery



```
uv run --package zephyr-ingest zephyr-ingest replay-delivery \
  --out .cache/out \
  --webhook-url http://localhost:9001/hook
```

Dry run:



```
uv run --package zephyr-ingest zephyr-ingest replay-delivery \
  --out .cache/out \
  --webhook-url http://localhost:9001/hook \
  --dry-run
```

Do not move to done:



```
uv run --package zephyr-ingest zephyr-ingest replay-delivery \
  --out .cache/out \
  --webhook-url http://localhost:9001/hook \
  --no-move-done
```

------

## 7) Common troubleshooting

### 7.1 401 Unauthorized from uns-api

-   Ensure `--uns-api-key` matches `UNSTRUCTURED_API_KEY` in compose.

-   Restart service after changing env:



    ```
    docker compose -f docker-compose.uns-api.yml down
    docker compose -f docker-compose.uns-api.yml up -d
    ```

### 7.2 "SKIPPED_EXISTING" unexpectedly

-   Caused by:
    -   `--skip-existing` and run_meta already exists
    -   `.zephyr.lock` exists (another worker or stale lock)
-   If stale lock is suspected:
    -   Check `<out>/<sha>/.zephyr.lock`
    -   Future enhancement: implement stale-lock cleanup by mtime threshold.

### 7.3 Tools scripts not running but CI is green

CI includes `py_compile tools/*.py`. If a tools script breaks, fix syntax/imports. Run locally:



```
python -m py_compile tools/*.py
```

------

## 8) Next milestones (Phase2)

-   P2-M6: destinations expansion
    -   Kafka destination (event bus)
    -   Weaviate destination (RAG-oriented)
-   Maintain delivery payload contract (`DeliveryPayloadV1`) for all destinations.
