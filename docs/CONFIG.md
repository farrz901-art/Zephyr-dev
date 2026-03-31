# Zephyr Ingest Configuration (TOML)

This doc describes how `zephyr-ingest` resolves configuration from:

**CLI explicit flags > ENV secrets > TOML file > defaults**

## 1) Config file schema version

Top-level `schema_version` is supported:

```toml
schema_version = 1
```

- If omitted, it defaults to `1`.
- `zephyr-ingest config resolve --strict` will **fail** if `schema_version` is omitted.
- Any `schema_version != 1` fails.

## 2) File layout (v1)

```toml
schema_version = 1

[run]
backend = "uns-api" # or "local"
strategy = "auto"   # auto/fast/hi_res/ocr_only
uns_api_url = "http://localhost:8001/general/v0/general"
uns_api_timeout_s = 60.0

[retry]
enabled = true
max_attempts = 3
base_backoff_ms = 200
max_backoff_ms = 5000

[destinations.webhook]
url = "http://localhost:9000/ingest"
timeout_s = 10.0

[destinations.kafka]
topic = "zephyr.delivery"
brokers = "localhost:9092"
flush_timeout_s = 10.0

[destinations.weaviate]
collection = "ZephyrDoc"
http_host = "localhost"
http_port = 8080
http_secure = false
grpc_host = "localhost"
grpc_port = 50051
grpc_secure = false
skip_init_checks = true
```

### Secrets
Prefer ENV injection for secrets:

- Unstructured API key:
- `ZEPHYR_UNS_API_KEY` (preferred), or `UNS_API_KEY`, or `UNSTRUCTURED_API_KEY`
- Weaviate API key:
- `ZEPHYR_WEAVIATE_API_KEY` (preferred), or `WEAVIATE_API_KEY`

## 3) Resolve / debug effective config

Use:

```bash
zephyr-ingest config resolve --config ./cfg.toml
```

This prints JSON that includes `config_snapshot` and `config_snapshot.sources`.

## 3.1) Generate a starter config

```bash
zephyr-ingest config init --out ./cfg.toml
```

This writes a valid v1 TOML file with optional destination blocks commented out.

## 3.2) DLQ operations (replay / prune)
See docs/DLQ.md for: +- replay (replay-delivery)
- retention / disk governance (dlq prune)

## 4) `config_snapshot.sources` key naming convention

`sources` uses **flat dotted keys**. Examples:

- `runner.strategy`
- `backend.kind`
- `backend.api_key`
- `destinations.webhook.url`
- `destinations.kafka.brokers`

This naming scheme is part of the operational contract: avoid renaming keys unless you bump schema / provide a migration path.
