from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass
from typing import Callable, Protocol, Sequence, cast

from zephyr_core import ErrorCode, ZephyrError

KAFKA_PARTITION_SOURCE_KIND = "kafka_partition_offset_v1"
_MAX_KAFKA_SOURCE_BATCHES = 1000
_OFFSET_CURSOR_WIDTH = 20
_DEFAULT_POLL_TIMEOUT_S = 1.0


@dataclass(frozen=True, slots=True)
class KafkaPartitionSourceConfigV1:
    stream: str
    connection_name: str
    brokers: tuple[str, ...]
    topic: str
    partition: int
    offset_start: int | None
    batch_size: int


@dataclass(frozen=True, slots=True)
class KafkaPartitionSourceRecordV1:
    data: dict[str, object]
    cursor: str


@dataclass(frozen=True, slots=True)
class KafkaPartitionSourceDocumentV1:
    stream: str
    records: list[KafkaPartitionSourceRecordV1]
    states: list[dict[str, object]]
    logs: list[tuple[str, str]]


@dataclass(frozen=True, slots=True)
class KafkaPartitionAssignmentV1:
    topic: str
    partition: int
    offset: int


class KafkaMessageProtocol(Protocol):
    def value(self) -> object: ...

    def offset(self) -> int: ...

    def error(self) -> object | None: ...


class KafkaConsumerProtocol(Protocol):
    def assign(self, partitions: Sequence[KafkaPartitionAssignmentV1]) -> object: ...

    def consume(self, num_messages: int, timeout: float) -> Sequence[KafkaMessageProtocol]: ...

    def close(self) -> object: ...


class _KafkaMetadataConsumerProtocol(KafkaConsumerProtocol, Protocol):
    def list_topics(self, topic: str | None = None, timeout: float = ...) -> object: ...


def is_kafka_partition_source_spec(raw: dict[str, object]) -> bool:
    source = raw.get("source")
    if not isinstance(source, dict):
        return False
    typed_source = cast("dict[str, object]", source)
    return typed_source.get("kind") == KAFKA_PARTITION_SOURCE_KIND


def _read_required_non_empty_string(
    *,
    source: dict[str, object],
    key: str,
    error_message: str,
) -> str:
    value = source.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(error_message)
    return value


def _validate_broker(broker: str) -> str:
    if broker != broker.strip():
        raise ValueError("it-stream Kafka source brokers must not contain leading/trailing spaces")
    if ":" not in broker:
        raise ValueError("it-stream Kafka source brokers must use host:port entries")
    host, port = broker.rsplit(":", 1)
    if not host or not port.isdigit():
        raise ValueError("it-stream Kafka source brokers must use host:port entries")
    return broker


def _normalize_brokers_for_identity(brokers: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted(brokers))


def load_kafka_partition_source_config(raw: dict[str, object]) -> KafkaPartitionSourceConfigV1:
    source = raw.get("source")
    if not isinstance(source, dict):
        raise ValueError("it-stream Kafka source field 'source' must be an object")
    typed_source = cast("dict[str, object]", source)

    stream = _read_required_non_empty_string(
        source=typed_source,
        key="stream",
        error_message="it-stream Kafka source field 'source.stream' must be a non-empty string",
    )
    connection_name = _read_required_non_empty_string(
        source=typed_source,
        key="connection_name",
        error_message=(
            "it-stream Kafka source field 'source.connection_name' must be a non-empty string"
        ),
    )
    topic = _read_required_non_empty_string(
        source=typed_source,
        key="topic",
        error_message="it-stream Kafka source field 'source.topic' must be a non-empty string",
    )
    if any(character.isspace() for character in topic):
        raise ValueError("it-stream Kafka source field 'source.topic' must not contain spaces")

    brokers_raw = typed_source.get("brokers")
    if not isinstance(brokers_raw, list) or not brokers_raw:
        raise ValueError("it-stream Kafka source field 'source.brokers' must be a non-empty list")

    brokers: list[str] = []
    seen_brokers: set[str] = set()
    for item in cast("list[object]", brokers_raw):
        if not isinstance(item, str) or not item:
            raise ValueError("it-stream Kafka source brokers must be non-empty strings")
        broker = _validate_broker(item)
        if broker in seen_brokers:
            raise ValueError("it-stream Kafka source brokers must not repeat values")
        seen_brokers.add(broker)
        brokers.append(broker)

    partition_raw = typed_source.get("partition")
    if not isinstance(partition_raw, int) or isinstance(partition_raw, bool) or partition_raw < 0:
        raise ValueError(
            "it-stream Kafka source field 'source.partition' must be a non-negative integer"
        )

    offset_start_raw = typed_source.get("offset_start")
    offset_start: int | None
    if offset_start_raw is None:
        offset_start = None
    elif (
        isinstance(offset_start_raw, int)
        and not isinstance(offset_start_raw, bool)
        and offset_start_raw >= 0
    ):
        offset_start = offset_start_raw
    else:
        raise ValueError(
            "it-stream Kafka source field 'source.offset_start' must be a non-negative integer "
            "or null"
        )

    batch_size_raw = typed_source.get("batch_size")
    if (
        not isinstance(batch_size_raw, int)
        or isinstance(batch_size_raw, bool)
        or batch_size_raw < 1
    ):
        raise ValueError(
            "it-stream Kafka source field 'source.batch_size' must be a positive integer"
        )

    return KafkaPartitionSourceConfigV1(
        stream=stream,
        connection_name=connection_name,
        brokers=tuple(brokers),
        topic=topic,
        partition=partition_raw,
        offset_start=offset_start,
        batch_size=batch_size_raw,
    )


def normalize_kafka_partition_source_identity_sha(
    *,
    config: KafkaPartitionSourceConfigV1,
) -> str:
    canonical = json.dumps(
        {
            "kind": KAFKA_PARTITION_SOURCE_KIND,
            "stream": config.stream,
            "connection_name": config.connection_name,
            "brokers": _normalize_brokers_for_identity(config.brokers),
            "topic": config.topic,
            "partition": config.partition,
            "offset_start": config.offset_start,
            "batch_size": config.batch_size,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _source_error(
    *,
    message: str,
    retryable: bool,
    details: dict[str, object] | None = None,
) -> ZephyrError:
    merged_details: dict[str, object] = {
        "retryable": retryable,
        "source_kind": KAFKA_PARTITION_SOURCE_KIND,
    }
    if details is not None:
        merged_details.update(details)
    return ZephyrError(
        code=ErrorCode.IO_READ_FAILED,
        message=message,
        details=merged_details,
    )


def _kafka_error_code(value: object) -> str | int | None:
    code_attr = getattr(value, "code", None)
    if callable(code_attr):
        try:
            code_value = code_attr()
        except Exception:
            code_value = None
    else:
        code_value = code_attr
    if isinstance(code_value, (str, int)) and not isinstance(code_value, bool):
        return code_value
    args = getattr(value, "args", None)
    typed_args = cast("tuple[object, ...] | None", args)
    if typed_args:
        return _kafka_error_code(typed_args[0])
    return None


def _is_retryable_kafka_error(err: object) -> bool:
    if isinstance(err, (OSError, TimeoutError)):
        return True
    args = getattr(err, "args", None)
    typed_args = cast("tuple[object, ...] | None", args)
    if typed_args and typed_args[0] is not err and _is_retryable_kafka_error(typed_args[0]):
        return True
    retriable = getattr(err, "retriable", None)
    if callable(retriable):
        try:
            result = retriable()
        except Exception:
            return False
        return isinstance(result, bool) and result
    if isinstance(retriable, bool):
        return retriable
    message = str(err).lower()
    return any(
        token in message
        for token in (
            "broker transport failure",
            "all brokers down",
            "_transport",
            "timed out",
        )
    )


class _ConfluentKafkaConsumerAdapter:
    def __init__(
        self,
        *,
        consumer: object,
        topic_partition_factory: Callable[[str, int, int], object],
    ) -> None:
        self._consumer = consumer
        self._topic_partition_factory = topic_partition_factory

    def assign(self, partitions: Sequence[KafkaPartitionAssignmentV1]) -> object:
        native_assignments = [
            self._topic_partition_factory(
                assignment.topic,
                assignment.partition,
                assignment.offset,
            )
            for assignment in partitions
        ]
        assign = getattr(self._consumer, "assign")
        return assign(native_assignments)

    def consume(self, num_messages: int, timeout: float) -> Sequence[KafkaMessageProtocol]:
        consume = getattr(self._consumer, "consume")
        messages = consume(num_messages=num_messages, timeout=timeout)
        return cast("Sequence[KafkaMessageProtocol]", messages)

    def close(self) -> object:
        close = getattr(self._consumer, "close", None)
        if callable(close):
            return close()
        return None


def _connect_kafka_partition_source(
    *,
    config: KafkaPartitionSourceConfigV1,
) -> KafkaConsumerProtocol:
    try:
        kafka_module = importlib.import_module("confluent_kafka")
    except ModuleNotFoundError as err:
        raise _source_error(
            message="it-stream Kafka source requires the optional 'confluent-kafka' dependency",
            retryable=False,
            details={"connection_name": config.connection_name},
        ) from err

    consumer_cls = getattr(kafka_module, "Consumer", None)
    topic_partition_cls = getattr(kafka_module, "TopicPartition", None)
    if not callable(consumer_cls) or not callable(topic_partition_cls):
        raise _source_error(
            message="it-stream Kafka source could not resolve confluent_kafka consumer classes",
            retryable=False,
            details={"connection_name": config.connection_name},
        )

    consumer_config: dict[str, object] = {
        "bootstrap.servers": ",".join(config.brokers),
        "group.id": f"zephyr-it-stream-{config.connection_name}",
        "enable.auto.commit": False,
        "enable.partition.eof": False,
        "auto.offset.reset": "error",
    }
    try:
        consumer = consumer_cls(consumer_config)
    except Exception as err:
        raise _source_error(
            message="it-stream Kafka source connection failed",
            retryable=_is_retryable_kafka_error(err),
            details={
                "connection_name": config.connection_name,
                "topic": config.topic,
                "partition": config.partition,
                "error_code": _kafka_error_code(err),
            },
        ) from err

    metadata_consumer = cast("_KafkaMetadataConsumerProtocol", consumer)
    try:
        metadata_consumer.list_topics(config.topic, timeout=_DEFAULT_POLL_TIMEOUT_S)
    except Exception as err:
        close = getattr(consumer, "close", None)
        if callable(close):
            close()
        raise _source_error(
            message="it-stream Kafka source metadata probe failed",
            retryable=_is_retryable_kafka_error(err),
            details={
                "connection_name": config.connection_name,
                "topic": config.topic,
                "partition": config.partition,
                "error_code": _kafka_error_code(err),
            },
        ) from err

    return cast(
        "KafkaConsumerProtocol",
        _ConfluentKafkaConsumerAdapter(
            consumer=consumer,
            topic_partition_factory=lambda topic, partition, offset: topic_partition_cls(
                topic,
                partition,
                offset,
            ),
        ),
    )


def _encode_offset_cursor(offset: int) -> str:
    if offset < 0:
        raise ValueError("Kafka offsets must be non-negative")
    return f"{offset:0{_OFFSET_CURSOR_WIDTH}d}"


def _decode_message_dict(*, value: object, details: dict[str, object]) -> dict[str, object]:
    if isinstance(value, dict):
        normalized: dict[str, object] = {}
        for key, item in cast("dict[object, object]", value).items():
            if not isinstance(key, str):
                raise _source_error(
                    message="it-stream Kafka source message objects must use string keys",
                    retryable=False,
                    details=details,
                )
            normalized[key] = item
        return normalized

    if isinstance(value, (bytes, bytearray)):
        try:
            decoded = bytes(value).decode("utf-8")
        except UnicodeDecodeError as err:
            raise _source_error(
                message="it-stream Kafka source message values must be valid UTF-8 JSON objects",
                retryable=False,
                details=details,
            ) from err
    elif isinstance(value, str):
        decoded = value
    else:
        raise _source_error(
            message="it-stream Kafka source message values must be JSON object bytes or strings",
            retryable=False,
            details=details,
        )

    try:
        raw = json.loads(decoded)
    except json.JSONDecodeError as err:
        raise _source_error(
            message="it-stream Kafka source message values must decode to JSON objects",
            retryable=False,
            details=details,
        ) from err
    if not isinstance(raw, dict):
        raise _source_error(
            message="it-stream Kafka source message values must decode to JSON objects",
            retryable=False,
            details=details,
        )
    return cast("dict[str, object]", raw)


def _consume_messages(
    *,
    config: KafkaPartitionSourceConfigV1,
    after_offset: int | None,
) -> list[KafkaMessageProtocol]:
    consumer = _connect_kafka_partition_source(config=config)
    next_offset = 0 if after_offset is None else after_offset + 1
    try:
        consumer.assign(
            [
                KafkaPartitionAssignmentV1(
                    topic=config.topic,
                    partition=config.partition,
                    offset=next_offset,
                )
            ]
        )
        messages = consumer.consume(
            num_messages=config.batch_size,
            timeout=_DEFAULT_POLL_TIMEOUT_S,
        )
    except Exception as err:
        raise _source_error(
            message="it-stream Kafka source consume failed",
            retryable=_is_retryable_kafka_error(err),
            details={
                "connection_name": config.connection_name,
                "topic": config.topic,
                "partition": config.partition,
                "after_offset": after_offset,
                "error_code": _kafka_error_code(err),
            },
        ) from err
    finally:
        consumer.close()

    return list(messages)


def fetch_kafka_partition_source(
    *,
    config: KafkaPartitionSourceConfigV1,
) -> KafkaPartitionSourceDocumentV1:
    records: list[KafkaPartitionSourceRecordV1] = []
    states: list[dict[str, object]] = []
    logs: list[tuple[str, str]] = []
    after_offset = config.offset_start

    for batch_number in range(1, _MAX_KAFKA_SOURCE_BATCHES + 1):
        messages = _consume_messages(config=config, after_offset=after_offset)
        if not messages:
            logs.append(("INFO", f"source exhausted after batch={batch_number}"))
            return KafkaPartitionSourceDocumentV1(
                stream=config.stream,
                records=records,
                states=states,
                logs=logs,
            )

        batch_records: list[KafkaPartitionSourceRecordV1] = []
        batch_offset: int | None = None
        for message in messages:
            message_error = message.error()
            if message_error is not None:
                raise _source_error(
                    message="it-stream Kafka source returned a broker message error",
                    retryable=_is_retryable_kafka_error(message_error),
                    details={
                        "connection_name": config.connection_name,
                        "topic": config.topic,
                        "partition": config.partition,
                        "after_offset": after_offset,
                        "error_code": _kafka_error_code(message_error),
                    },
                )

            message_offset = message.offset()
            if message_offset < 0:
                raise _source_error(
                    message="it-stream Kafka source offsets must be non-negative",
                    retryable=False,
                    details={
                        "connection_name": config.connection_name,
                        "topic": config.topic,
                        "partition": config.partition,
                        "offset": message_offset,
                    },
                )
            if after_offset is not None and message_offset <= after_offset:
                raise _source_error(
                    message="it-stream Kafka source offset progression must advance strictly",
                    retryable=False,
                    details={
                        "connection_name": config.connection_name,
                        "topic": config.topic,
                        "partition": config.partition,
                        "offset": message_offset,
                        "after_offset": after_offset,
                    },
                )
            if batch_offset is not None and message_offset <= batch_offset:
                raise _source_error(
                    message="it-stream Kafka source offset progression must advance strictly",
                    retryable=False,
                    details={
                        "connection_name": config.connection_name,
                        "topic": config.topic,
                        "partition": config.partition,
                        "offset": message_offset,
                        "previous_offset": batch_offset,
                    },
                )

            record_details: dict[str, object] = {
                "connection_name": config.connection_name,
                "topic": config.topic,
                "partition": config.partition,
                "offset": message_offset,
            }
            data = _decode_message_dict(value=message.value(), details=record_details)
            batch_offset = message_offset
            batch_records.append(
                KafkaPartitionSourceRecordV1(
                    data=data,
                    cursor=_encode_offset_cursor(message_offset),
                )
            )

        final_batch_offset = batch_offset
        if final_batch_offset is None:
            raise _source_error(
                message="it-stream Kafka source batch must contain at least one offset",
                retryable=False,
                details={
                    "connection_name": config.connection_name,
                    "topic": config.topic,
                    "partition": config.partition,
                },
            )

        records.extend(batch_records)
        states.append(
            {
                "cursor": _encode_offset_cursor(final_batch_offset),
                "connection_name": config.connection_name,
                "topic": config.topic,
                "partition": config.partition,
                "read_mode": "explicit_partition_offset",
                "read_direction": "asc",
                "record_count": len(batch_records),
                "last_offset": final_batch_offset,
            }
        )
        logs.append(
            (
                "INFO",
                f"fetched batch={batch_number} records={len(batch_records)} "
                f"offset={final_batch_offset}",
            )
        )
        after_offset = final_batch_offset

    raise _source_error(
        message="it-stream Kafka source exceeded the maximum supported batch count",
        retryable=False,
        details={
            "connection_name": config.connection_name,
            "topic": config.topic,
            "partition": config.partition,
            "max_batches": _MAX_KAFKA_SOURCE_BATCHES,
        },
    )
