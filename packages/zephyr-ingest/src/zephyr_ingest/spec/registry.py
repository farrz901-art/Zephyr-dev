from __future__ import annotations

from zephyr_ingest.spec.types import ConnectorSpecV1

_REGISTRY: dict[str, ConnectorSpecV1] = {
    "backend.uns_api.v1": {
        "id": "backend.uns_api.v1",
        "version": 1,
        "kind": "backend",
        "name": "Unstructured API backend",
        "description": "Send partition requests to an Unstructured API server.",
        "fields": [
            {
                "name": "backend.kind",
                "type": "string",
                "required": True,
                "default": "local",
                "cli_flags": ["--backend"],
                "help": "Backend kind: local or uns-api.",
                "examples": ["uns-api"],
            },
            {
                "name": "backend.url",
                "type": "string",
                "required": False,
                "default": "http://localhost:8001/general/v0/general",
                "cli_flags": ["--uns-api-url"],
                "help": "Unstructured API endpoint URL.",
            },
            {
                "name": "backend.timeout_s",
                "type": "float",
                "required": False,
                "default": 60.0,
                "cli_flags": ["--uns-api-timeout-s"],
                "help": "Unstructured API request timeout (seconds).",
            },
            {
                "name": "backend.api_key",
                "type": "string",
                "required": False,
                "secret": True,
                "cli_flags": ["--uns-api-key"],
                "env_names": ["ZEPHYR_UNS_API_KEY", "UNS_API_KEY", "UNSTRUCTURED_API_KEY"],
                "help": "Unstructured API key. Prefer ENV injection.",
            },
        ],
    },
    "destination.webhook.v1": {
        "id": "destination.webhook.v1",
        "version": 1,
        "kind": "destination",
        "name": "Webhook destination",
        "description": "POST DeliveryPayloadV1 to an HTTP endpoint.",
        "fields": [
            {
                "name": "destinations.webhook.url",
                "type": "string",
                "required": True,
                "cli_flags": ["--webhook-url"],
                "help": "Webhook URL to POST payload to.",
            },
            {
                "name": "destinations.webhook.timeout_s",
                "type": "float",
                "required": False,
                "default": 10.0,
                "cli_flags": ["--webhook-timeout-s"],
                "help": "Webhook request timeout (seconds).",
            },
        ],
    },
    "destination.kafka.v1": {
        "id": "destination.kafka.v1",
        "version": 1,
        "kind": "destination",
        "name": "Kafka destination",
        "description": "Publish DeliveryPayloadV1 to a Kafka topic.",
        "fields": [
            {
                "name": "destinations.kafka.topic",
                "type": "string",
                "required": True,
                "cli_flags": ["--kafka-topic"],
                "help": "Kafka topic name.",
            },
            {
                "name": "destinations.kafka.brokers",
                "type": "string",
                "required": True,
                "cli_flags": ["--kafka-brokers"],
                "help": "Kafka broker addresses (comma-separated).",
                "examples": ["localhost:9092"],
            },
            {
                "name": "destinations.kafka.flush_timeout_s",
                "type": "float",
                "required": False,
                "default": 10.0,
                "cli_flags": ["--kafka-flush-timeout-s"],
                "help": "Producer flush timeout (seconds).",
            },
        ],
    },
    "destination.weaviate.v1": {
        "id": "destination.weaviate.v1",
        "version": 1,
        "kind": "destination",
        "name": "Weaviate destination",
        "description": "Insert/replace 1 Zephyr document as 1 "
        "Weaviate object using a deterministic UUID.",
        "fields": [
            {
                "name": "destinations.weaviate.collection",
                "type": "string",
                "required": True,
                "cli_flags": ["--weaviate-collection"],
                "help": "Weaviate collection name.",
            },
            {
                "name": "destinations.weaviate.max_batch_errors",
                "type": "int",
                "required": False,
                "default": 0,
                "cli_flags": ["--weaviate-max-batch-errors"],
                "help": "Max tolerated batch errors before failing delivery.",
            },
            {
                "name": "destinations.weaviate.http_host",
                "type": "string",
                "required": False,
                "default": "localhost",
                "cli_flags": ["--weaviate-http-host"],
            },
            {
                "name": "destinations.weaviate.http_port",
                "type": "int",
                "required": False,
                "default": 8080,
                "cli_flags": ["--weaviate-http-port"],
            },
            {
                "name": "destinations.weaviate.http_secure",
                "type": "bool",
                "required": False,
                "default": False,
                "cli_flags": ["--weaviate-http-secure"],
            },
            {
                "name": "destinations.weaviate.grpc_host",
                "type": "string",
                "required": False,
                "default": "localhost",
                "cli_flags": ["--weaviate-grpc-host"],
            },
            {
                "name": "destinations.weaviate.grpc_port",
                "type": "int",
                "required": False,
                "default": 50051,
                "cli_flags": ["--weaviate-grpc-port"],
            },
            {
                "name": "destinations.weaviate.grpc_secure",
                "type": "bool",
                "required": False,
                "default": False,
                "cli_flags": ["--weaviate-grpc-secure"],
            },
            {
                "name": "destinations.weaviate.skip_init_checks",
                "type": "bool",
                "required": False,
                "default": False,
                "cli_flags": ["--weaviate-skip-init-checks"],
            },
            {
                "name": "destinations.weaviate.api_key",
                "type": "string",
                "required": False,
                "secret": True,
                "cli_flags": ["--weaviate-api-key"],
                "env_names": ["ZEPHYR_WEAVIATE_API_KEY", "WEAVIATE_API_KEY"],
                "help": "Weaviate API key. Prefer ENV injection.",
            },
        ],
    },
}


def list_spec_ids() -> list[str]:
    return sorted(_REGISTRY.keys())


def get_spec(*, spec_id: str) -> ConnectorSpecV1 | None:
    return _REGISTRY.get(spec_id)
