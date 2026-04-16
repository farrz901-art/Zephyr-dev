from __future__ import annotations

import hashlib
import importlib
import json
import socket
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol, cast
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit

from zephyr_ingest.testing.p45 import LoadedP45Env


def as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def read_json_dict(path: Path) -> dict[str, object]:
    raw: object = json.loads(path.read_text(encoding="utf-8"))
    return as_dict(raw)


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def reserve_unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def write_kafka_source_spec(
    path: Path,
    *,
    stream: str,
    connection_name: str,
    brokers: Sequence[str],
    topic: str,
    partition: int,
    offset_start: int | None,
    batch_size: int,
) -> None:
    payload: dict[str, object] = {
        "source": {
            "kind": "kafka_partition_offset_v1",
            "stream": stream,
            "connection_name": connection_name,
            "brokers": list(brokers),
            "topic": topic,
            "partition": partition,
            "offset_start": offset_start,
            "batch_size": batch_size,
        }
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_mongodb_source_spec(
    path: Path,
    *,
    stream: str,
    connection_name: str,
    uri: str,
    database: str,
    collection: str,
    fields: Sequence[str],
    cursor_field: str,
    cursor_start: str | None,
    batch_size: int,
) -> None:
    payload: dict[str, object] = {
        "source": {
            "kind": "mongodb_incremental_v1",
            "stream": stream,
            "connection_name": connection_name,
            "uri": uri,
            "database": database,
            "collection": collection,
            "fields": list(fields),
            "cursor_field": cursor_field,
            "cursor_start": cursor_start,
            "batch_size": batch_size,
        }
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def kafka_bootstrap_servers(env: LoadedP45Env) -> tuple[str, ...]:
    raw = env.require("ZEPHYR_P45_KAFKA_BROKERS")
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def make_kafka_topic_name(*, prefix: str, suffix: str) -> str:
    normalized_suffix = "".join(
        character if character.isalnum() else "-" for character in suffix.lower()
    ).strip("-")
    return f"{prefix}-{normalized_suffix}"


def _load_confluent_kafka_module() -> object:
    return importlib.import_module("confluent_kafka")


def create_kafka_topic(
    *,
    env: LoadedP45Env,
    topic: str,
    partitions: int = 1,
) -> None:
    kafka_module = _load_confluent_kafka_module()
    admin_module = importlib.import_module("confluent_kafka.admin")
    admin_client_cls = getattr(admin_module, "AdminClient")
    new_topic_cls = getattr(admin_module, "NewTopic")
    admin_client = admin_client_cls({"bootstrap.servers": ",".join(kafka_bootstrap_servers(env))})
    futures = admin_client.create_topics(
        [new_topic_cls(topic=topic, num_partitions=partitions, replication_factor=1)]
    )
    future = futures[topic]
    try:
        future.result(10.0)
    except Exception as err:
        message = str(err)
        if (
            "TopicExists" not in message
            and "already exists" not in message
            and "already been created" not in message
        ):
            raise
    del kafka_module


class _KafkaDeliveryReportProtocol(Protocol):
    def offset(self) -> int: ...


def produce_kafka_messages(
    *,
    env: LoadedP45Env,
    topic: str,
    partition: int,
    values: Sequence[bytes | str | dict[str, object]],
) -> list[int]:
    kafka_module = _load_confluent_kafka_module()
    producer_cls = getattr(kafka_module, "Producer")
    producer = producer_cls({"bootstrap.servers": ",".join(kafka_bootstrap_servers(env))})
    offsets: list[int] = []
    errors: list[BaseException] = []

    def _on_delivery(err: object, message: object) -> None:
        if err is not None:
            if isinstance(err, BaseException):
                errors.append(err)
            else:
                errors.append(RuntimeError(str(err)))
            return
        typed_message = cast(_KafkaDeliveryReportProtocol, message)
        offsets.append(typed_message.offset())

    for value in values:
        payload: bytes
        if isinstance(value, dict):
            payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        elif isinstance(value, str):
            payload = value.encode("utf-8")
        else:
            payload = value
        producer.produce(
            topic,
            value=payload,
            partition=partition,
            on_delivery=_on_delivery,
        )
        producer.poll(0.0)
    producer.flush(10.0)
    if errors:
        raise errors[0]
    return sorted(offsets)


def mongodb_uri(
    env: LoadedP45Env,
    *,
    password: str | None = None,
    port: int | None = None,
) -> str:
    raw_uri = env.require("ZEPHYR_P45_MONGODB_URI")
    split = urlsplit(raw_uri)
    assert split.hostname is not None
    username = split.username or env.require("ZEPHYR_P45_MONGODB_USERNAME")
    resolved_password = password or (split.password or env.require("ZEPHYR_P45_MONGODB_PASSWORD"))
    host = split.hostname
    resolved_port = split.port if port is None else port
    netloc = (
        f"{quote(username, safe='')}:{quote(resolved_password, safe='')}@{host}:{resolved_port}"
    )
    query_items = dict(parse_qsl(split.query, keep_blank_values=True))
    query_items["serverSelectionTimeoutMS"] = "1500"
    query_items["connectTimeoutMS"] = "1500"
    query_items["socketTimeoutMS"] = "1500"
    query = "&".join(f"{quote(name)}={quote(value)}" for name, value in sorted(query_items.items()))
    return urlunsplit((split.scheme, netloc, split.path, query, split.fragment))


class _MongoCollectionProtocol(Protocol):
    def drop(self) -> object: ...

    def insert_many(self, documents: Sequence[dict[str, object]]) -> object: ...


class _MongoDatabaseProtocol(Protocol):
    def __getitem__(self, name: str) -> _MongoCollectionProtocol: ...


class _MongoClientProtocol(Protocol):
    def __getitem__(self, name: str) -> _MongoDatabaseProtocol: ...

    def close(self) -> object: ...


def _connect_pymongo(uri: str) -> _MongoClientProtocol:
    pymongo_module = importlib.import_module("pymongo")
    mongo_client_cls = getattr(pymongo_module, "MongoClient")
    client = mongo_client_cls(uri)
    return cast(_MongoClientProtocol, client)


def reset_mongodb_collection(
    env: LoadedP45Env,
    *,
    database: str,
    collection: str,
    documents: Sequence[dict[str, object]],
) -> None:
    client = _connect_pymongo(mongodb_uri(env))
    try:
        collection_handle = client[database][collection]
        collection_handle.drop()
        if documents:
            collection_handle.insert_many(list(documents))
    finally:
        client.close()


def airbyte_fixture_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "p45_airbyte_message_json"
