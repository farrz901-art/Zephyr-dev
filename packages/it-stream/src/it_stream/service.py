from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
)


@dataclass(frozen=True, slots=True)
class ItRecordV1:
    data: dict[str, object]
    emitted_at: str | None = None


@dataclass(frozen=True, slots=True)
class ItStateV1:
    data: dict[str, object]


@dataclass(frozen=True, slots=True)
class ItLogV1:
    level: str
    message: str


@dataclass(frozen=True, slots=True)
class ItInputDocumentV1:
    stream: str
    records: list[ItRecordV1]


@dataclass(frozen=True, slots=True)
class ItNormalizedInputDocumentV1:
    stream: str | None
    records: list[ItRecordV1]
    states: list[ItStateV1]
    logs: list[ItLogV1]


def _record_text(*, record: ItRecordV1) -> str:
    return json.dumps(record.data, ensure_ascii=False, sort_keys=True)


def _read_object_dict(value: object, *, error_message: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(error_message)
    return cast("dict[str, object]", value)


def _load_legacy_input_document(raw: dict[str, object]) -> ItNormalizedInputDocumentV1:
    stream = raw.get("stream")
    records_raw = raw.get("records")
    if not isinstance(stream, str) or not stream:
        raise ValueError("it-stream input field 'stream' must be a non-empty string")
    if not isinstance(records_raw, list):
        raise ValueError("it-stream input field 'records' must be a list")
    typed_records_raw = cast("list[object]", records_raw)

    records: list[ItRecordV1] = []
    for item in typed_records_raw:
        typed_item = _read_object_dict(item, error_message="it-stream record must be a JSON object")
        data = _read_object_dict(
            typed_item.get("data"),
            error_message="it-stream record field 'data' must be a JSON object",
        )
        emitted_at = typed_item.get("emitted_at")
        if emitted_at is not None and not isinstance(emitted_at, str):
            raise ValueError("it-stream record field 'emitted_at' must be a string")
        records.append(ItRecordV1(data=data, emitted_at=emitted_at))

    return ItNormalizedInputDocumentV1(stream=stream, records=records, states=[], logs=[])


def _load_message_input_document(raw: dict[str, object]) -> ItNormalizedInputDocumentV1:
    messages_raw = raw.get("messages")
    if not isinstance(messages_raw, list):
        raise ValueError("it-stream input field 'messages' must be a list")
    typed_messages = cast("list[object]", messages_raw)

    stream: str | None = None
    records: list[ItRecordV1] = []
    states: list[ItStateV1] = []
    logs: list[ItLogV1] = []

    for item in typed_messages:
        typed_item = _read_object_dict(
            item,
            error_message="it-stream message must be a JSON object",
        )
        msg_type = typed_item.get("type")
        if msg_type == "RECORD":
            record_obj = _read_object_dict(
                typed_item.get("record"),
                error_message="it-stream RECORD message field 'record' must be an object",
            )
            record_stream = record_obj.get("stream")
            if not isinstance(record_stream, str) or not record_stream:
                raise ValueError(
                    "it-stream RECORD message field 'record.stream' must be a non-empty string"
                )
            data = _read_object_dict(
                record_obj.get("data"),
                error_message="it-stream RECORD message field 'record.data' must be an object",
            )
            emitted_at = record_obj.get("emitted_at")
            if emitted_at is not None and not isinstance(emitted_at, str):
                raise ValueError(
                    "it-stream RECORD message field 'record.emitted_at' must be a string"
                )
            if stream is None:
                stream = record_stream
            elif stream != record_stream:
                raise ValueError(
                    "it-stream message subset currently requires a single record stream"
                )
            records.append(ItRecordV1(data=data, emitted_at=emitted_at))
            continue

        if msg_type == "STATE":
            state_obj = _read_object_dict(
                typed_item.get("state"),
                error_message="it-stream STATE message field 'state' must be an object",
            )
            data = _read_object_dict(
                state_obj.get("data"),
                error_message="it-stream STATE message field 'state.data' must be an object",
            )
            states.append(ItStateV1(data=data))
            continue

        if msg_type == "LOG":
            log_obj = _read_object_dict(
                typed_item.get("log"),
                error_message="it-stream LOG message field 'log' must be an object",
            )
            level = log_obj.get("level")
            message = log_obj.get("message")
            if not isinstance(level, str) or not level:
                raise ValueError(
                    "it-stream LOG message field 'log.level' must be a non-empty string"
                )
            if not isinstance(message, str) or not message:
                raise ValueError(
                    "it-stream LOG message field 'log.message' must be a non-empty string"
                )
            logs.append(ItLogV1(level=level, message=message))
            continue

        raise ValueError(f"Unsupported it-stream message type: {msg_type!r}")

    return ItNormalizedInputDocumentV1(stream=stream, records=records, states=states, logs=logs)


def load_input_document(path: Path) -> ItNormalizedInputDocumentV1:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("it-stream input must be a JSON object")
    typed_raw = cast("dict[str, object]", raw)

    if "messages" in typed_raw:
        return _load_message_input_document(typed_raw)
    return _load_legacy_input_document(typed_raw)


def partition_input_document(
    *,
    doc: ItNormalizedInputDocumentV1,
    filename: str,
    strategy: PartitionStrategy,
    sha256: str,
    size_bytes: int,
) -> PartitionResult:
    elements: list[ZephyrElement] = []
    texts: list[str] = []
    for index, record in enumerate(doc.records):
        record_text = _record_text(record=record)
        texts.append(record_text)
        elements.append(
            ZephyrElement(
                element_id=f"{doc.stream or 'it'}:record:{index}",
                type="StructuredRecord",
                text=record_text,
                metadata={
                    "flow_kind": "it",
                    "artifact_kind": "record",
                    "stream": doc.stream,
                    "record_index": index,
                    "emitted_at": record.emitted_at,
                    "data": record.data,
                },
            )
        )

    for index, state in enumerate(doc.states):
        state_text = json.dumps(state.data, ensure_ascii=False, sort_keys=True)
        texts.append(state_text)
        elements.append(
            ZephyrElement(
                element_id=f"{doc.stream or 'it'}:state:{index}",
                type="StructuredState",
                text=state_text,
                metadata={
                    "flow_kind": "it",
                    "artifact_kind": "state",
                    "state_index": index,
                    "data": state.data,
                },
            )
        )

    for index, log in enumerate(doc.logs):
        texts.append(log.message)
        elements.append(
            ZephyrElement(
                element_id=f"{doc.stream or 'it'}:log:{index}",
                type="StructuredLog",
                text=log.message,
                metadata={
                    "flow_kind": "it",
                    "artifact_kind": "log",
                    "log_index": index,
                    "level": log.level,
                },
            )
        )

    return PartitionResult(
        document=DocumentMetadata(
            filename=filename,
            mime_type="application/json",
            sha256=sha256,
            size_bytes=size_bytes,
            created_at_utc=datetime.now(timezone.utc).isoformat(),
        ),
        engine=EngineInfo(
            name="it-stream",
            backend="airbyte-message-json",
            version="0.1.0",
            strategy=strategy,
        ),
        elements=elements,
        normalized_text="\n".join(texts),
        warnings=[],
    )


def process_file(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    sha256: str,
    size_bytes: int | None = None,
) -> PartitionResult:
    path = Path(filename)
    doc = load_input_document(path)
    size = size_bytes if size_bytes is not None else path.stat().st_size
    return partition_input_document(
        doc=doc,
        filename=path.name,
        strategy=strategy,
        sha256=sha256,
        size_bytes=size,
    )
