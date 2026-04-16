from __future__ import annotations

import base64
import json
import subprocess
import time
import uuid as uuid_module
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from zephyr_core import DocumentRef, RunContext
from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_ingest.destinations.base import Destination
from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.runner import RunnerConfig, build_task_execution_handler, run_documents
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)
from zephyr_ingest.testing.p45 import LoadedP45Env
from zephyr_ingest.tests._p45_destination_wave1_helpers import (
    BatchRunArtifacts,
    StaticTextProcessor,
    WorkerRunArtifacts,
    expected_uns_sha,
    make_text_document,
    read_json_dict,
)
from zephyr_ingest.worker_runtime import run_worker


@dataclass(frozen=True, slots=True)
class KafkaRecord:
    topic: str
    key: str | None
    value: dict[str, object]


def _default_weaviate_items() -> list[tuple[dict[str, Any], str | None]]:
    return []


def _default_failed_objects() -> list[dict[str, object]]:
    return []


def kafka_topic_name(*, prefix: str = "zephyr_p45_m3") -> str:
    return f"{prefix}_{uuid_module.uuid4().hex[:10]}"


def current_timestamp_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def consume_kafka_records(
    *,
    brokers: str,
    topic: str,
    expected_count: int,
    timeout_s: float = 8.0,
) -> list[KafkaRecord]:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"zephyr-p45-m3-{uuid_module.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    try:
        records: list[KafkaRecord] = []
        deadline = time.time() + timeout_s
        while time.time() < deadline and len(records) < expected_count:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            raw_value = message.value()
            assert raw_value is not None
            raw_obj: object = json.loads(raw_value.decode("utf-8"))
            assert isinstance(raw_obj, dict)
            value = cast(dict[str, object], raw_obj)
            raw_key = message.key()
            key = None if raw_key is None else raw_key.decode("utf-8")
            records.append(KafkaRecord(topic=topic, key=key, value=value))
        return records
    finally:
        consumer.close()


def weaviate_base_url(env: LoadedP45Env) -> str:
    host = env.require("ZEPHYR_P45_WEAVIATE_HTTP_HOST")
    port = env.require("ZEPHYR_P45_WEAVIATE_HTTP_PORT")
    return f"http://{host}:{port}".rstrip("/")


def weaviate_headers(*, api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"}


def weaviate_collection_name(*, prefix: str = "ZephyrP45M3") -> str:
    return f"{prefix}{uuid_module.uuid4().hex[:8]}"


def _weaviate_property(name: str, data_type: str) -> dict[str, object]:
    return {"name": name, "dataType": [data_type]}


def ensure_weaviate_collection(*, base_url: str, api_key: str, collection_name: str) -> None:
    schema = {
        "class": collection_name,
        "properties": [
            _weaviate_property("sha256", "text"),
            _weaviate_property("uuid", "text"),
            _weaviate_property("run_id", "text"),
            _weaviate_property("pipeline_version", "text"),
            _weaviate_property("timestamp_utc", "text"),
            _weaviate_property("outcome", "text"),
            _weaviate_property("filename", "text"),
            _weaviate_property("mime_type", "text"),
            _weaviate_property("size_bytes", "int"),
            _weaviate_property("created_at_utc", "text"),
            _weaviate_property("engine_name", "text"),
            _weaviate_property("engine_backend", "text"),
            _weaviate_property("engine_version", "text"),
            _weaviate_property("strategy", "text"),
            _weaviate_property("elements_count", "int"),
            _weaviate_property("normalized_text", "text"),
            _weaviate_property("delivery_origin", "text"),
            _weaviate_property("execution_mode", "text"),
        ],
    }
    with httpx.Client(
        timeout=10.0,
        trust_env=False,
        headers=weaviate_headers(api_key=api_key),
    ) as client:
        client.delete(f"{base_url}/v1/schema/{collection_name}")
        response = client.post(f"{base_url}/v1/schema", json=schema)
        response.raise_for_status()


def get_weaviate_object(
    *,
    base_url: str,
    api_key: str,
    collection_name: str,
    object_id: str,
) -> dict[str, object]:
    with httpx.Client(
        timeout=10.0,
        trust_env=False,
        headers=weaviate_headers(api_key=api_key),
    ) as client:
        response = client.get(
            f"{base_url}/v1/objects/{object_id}",
            params={"class": collection_name},
        )
        response.raise_for_status()
    raw_obj: object = response.json()
    assert isinstance(raw_obj, dict)
    return cast(dict[str, object], raw_obj)


def list_weaviate_objects(
    *,
    base_url: str,
    api_key: str,
    collection_name: str,
) -> list[dict[str, object]]:
    with httpx.Client(
        timeout=10.0,
        trust_env=False,
        headers=weaviate_headers(api_key=api_key),
    ) as client:
        response = client.get(
            f"{base_url}/v1/objects",
            params={"class": collection_name, "limit": "100"},
        )
        response.raise_for_status()
    raw_obj: object = response.json()
    assert isinstance(raw_obj, dict)
    payload = cast(dict[str, object], raw_obj)
    items_obj = payload.get("objects")
    assert isinstance(items_obj, list)
    items = cast(list[object], items_obj)
    return [cast(dict[str, object], item) for item in items if isinstance(item, dict)]


@dataclass(slots=True)
class _LiveWeaviateBatch:
    parent: "LiveWeaviateCollection"
    number_errors: int = 0
    _items: list[tuple[dict[str, Any], str | None]] = field(default_factory=_default_weaviate_items)

    def add_object(self, properties: dict[str, Any], uuid: str | None = None) -> str:
        self._items.append((properties, uuid))
        return uuid or str(uuid_module.uuid4())

    def flush(self) -> None:
        self.parent.batch.failed_objects = []
        self.number_errors = 0
        with httpx.Client(
            timeout=self.parent.timeout_s,
            trust_env=False,
            headers=self.parent.headers,
        ) as client:
            for properties, object_id in self._items:
                payload: dict[str, object] = {
                    "class": self.parent.collection_name,
                    "properties": properties,
                }
                if object_id is not None:
                    payload["id"] = object_id
                response = client.post(f"{self.parent.base_url}/v1/objects", json=payload)
                if 200 <= response.status_code < 300:
                    continue
                self.number_errors += 1
                self.parent.batch.failed_objects.append(
                    {
                        "status": response.status_code,
                        "message": response.text[:500],
                        "uuid": object_id,
                    }
                )


@dataclass(slots=True)
class _LiveWeaviateBatchManager:
    parent: "LiveWeaviateCollection"
    failed_objects: list[dict[str, object]] = field(default_factory=_default_failed_objects)

    @contextmanager
    def dynamic(self) -> Iterator[_LiveWeaviateBatch]:
        batch = _LiveWeaviateBatch(parent=self.parent)
        yield batch
        batch.flush()


@dataclass(slots=True)
class LiveWeaviateCollection:
    base_url: str
    collection_name: str
    api_key: str
    timeout_s: float = 10.0
    batch: _LiveWeaviateBatchManager = field(init=False)

    def __post_init__(self) -> None:
        self.batch = _LiveWeaviateBatchManager(parent=self)

    @property
    def headers(self) -> dict[str, str]:
        return weaviate_headers(api_key=self.api_key)


@dataclass(frozen=True, slots=True)
class MongoShellReplaceResult:
    acknowledged: bool
    matched_count: int
    modified_count: int
    upserted_id: object | None


class MongoShellError(RuntimeError):
    pass


def _json_obj_from_text(text: str) -> dict[str, object]:
    raw_obj: object = json.loads(text)
    assert isinstance(raw_obj, dict)
    return cast(dict[str, object], raw_obj)


def _rewrite_mongodb_runtime_uri(*, uri: str, env: LoadedP45Env) -> str:
    parts = urlsplit(uri)
    host = parts.hostname
    port = parts.port
    if host not in {"127.0.0.1", "localhost"} or port is None:
        return uri
    runtime_port = int(env.require("ZEPHYR_P45_MONGODB_PORT"))
    target_host = "127.0.0.1" if port == runtime_port else "host.docker.internal"
    target_port = 27017 if port == runtime_port else port
    username = parts.username or ""
    password = parts.password or ""
    auth_prefix = ""
    if username:
        auth_prefix = username
        if password:
            auth_prefix += f":{password}"
        auth_prefix += "@"
    query = urlencode(parse_qsl(parts.query, keep_blank_values=True))
    netloc = f"{auth_prefix}{target_host}:{target_port}"
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))


def _docker_compose_exec_args(env: LoadedP45Env) -> list[str]:
    runtime_paths = env.runtime_paths
    env_path = runtime_paths.env_dir / ".env.p45.local"
    return [
        "docker",
        "compose",
        "--env-file",
        str(env_path),
        "-f",
        str(runtime_paths.compose_path),
        "exec",
        "-T",
        "mongodb",
    ]


def _run_mongosh(*, env: LoadedP45Env, uri: str, js: str) -> str:
    command = _docker_compose_exec_args(env) + [
        "mongosh",
        "--quiet",
        _rewrite_mongodb_runtime_uri(uri=uri, env=env),
        "--eval",
        js,
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        text = completed.stderr.strip() or completed.stdout.strip()
        raise MongoShellError(text)
    return completed.stdout.strip()


def _b64_json(value: object) -> str:
    return base64.b64encode(json.dumps(value, ensure_ascii=False).encode("utf-8")).decode("ascii")


def _int_from_json(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"Expected int-like JSON value, got {type(value).__name__}")


@dataclass(slots=True)
class MongoShellCollection:
    env: LoadedP45Env
    uri: str
    database: str
    collection: str

    def replace_one(
        self,
        filter: Mapping[str, object],
        replacement: Mapping[str, object],
        *,
        upsert: bool,
    ) -> MongoShellReplaceResult:
        js = (
            "const filter = JSON.parse("
            f"Buffer.from('{_b64_json(dict(filter))}','base64').toString('utf8')"
            ");"
            "const replacement = JSON.parse("
            f"Buffer.from('{_b64_json(dict(replacement))}','base64').toString('utf8')"
            ");"
            f"const result = db.getSiblingDB('{self.database}').getCollection('{self.collection}')"
            f".replaceOne(filter, replacement, {{upsert:{str(upsert).lower()}}});"
            "print(JSON.stringify({"
            "acknowledged: result.acknowledged,"
            "matched_count: result.matchedCount,"
            "modified_count: result.modifiedCount,"
            "upserted_id: result.upsertedId ?? null"
            "}));"
        )
        output = _run_mongosh(env=self.env, uri=self.uri, js=js)
        result = _json_obj_from_text(output)
        return MongoShellReplaceResult(
            acknowledged=bool(result.get("acknowledged")),
            matched_count=_int_from_json(result.get("matched_count", 0)),
            modified_count=_int_from_json(result.get("modified_count", 0)),
            upserted_id=result.get("upserted_id"),
        )


def drop_mongodb_collection(
    *,
    env: LoadedP45Env,
    uri: str,
    database: str,
    collection: str,
) -> None:
    js = f"db.getSiblingDB('{database}').getCollection('{collection}').drop();"
    _run_mongosh(env=env, uri=uri, js=js)


def find_mongodb_document(
    *,
    env: LoadedP45Env,
    uri: str,
    database: str,
    collection: str,
    document_id: str,
) -> dict[str, object] | None:
    js = (
        f"const doc = db.getSiblingDB('{database}').getCollection('{collection}')"
        f".findOne({_json_query({'_id': document_id})});"
        "print(JSON.stringify(doc ?? null));"
    )
    output = _run_mongosh(env=env, uri=uri, js=js)
    if output == "null":
        return None
    return _json_obj_from_text(output)


def count_mongodb_documents(
    *,
    env: LoadedP45Env,
    uri: str,
    database: str,
    collection: str,
    document_id: str | None = None,
) -> int:
    filter_obj: dict[str, object] = {} if document_id is None else {"_id": document_id}
    js = (
        f"const count = db.getSiblingDB('{database}').getCollection('{collection}')"
        f".countDocuments({_json_query(filter_obj)});"
        "print(String(count));"
    )
    return int(_run_mongosh(env=env, uri=uri, js=js))


def _json_query(value: object) -> str:
    return json.dumps(value, ensure_ascii=False)


def loki_query_streams(
    *,
    url: str,
    tenant_id: str,
    selector: str,
    timestamp_utc: str,
    timeout_s: float = 10.0,
) -> list[dict[str, object]]:
    parsed = datetime.fromisoformat(timestamp_utc.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    center_ns = int(parsed.timestamp() * 1_000_000_000)
    start_ns = center_ns - (300 * 1_000_000_000)
    end_ns = center_ns + (300 * 1_000_000_000)
    with httpx.Client(timeout=timeout_s, trust_env=False) as client:
        response = client.get(
            f"{url.rstrip('/')}/loki/api/v1/query_range",
            headers={"X-Scope-OrgID": tenant_id},
            params={
                "query": selector,
                "limit": "100",
                "start": str(start_ns),
                "end": str(end_ns),
                "direction": "forward",
            },
        )
        response.raise_for_status()
    raw_obj: object = response.json()
    assert isinstance(raw_obj, dict)
    payload = cast(dict[str, object], raw_obj)
    data_obj = payload.get("data")
    assert isinstance(data_obj, dict)
    data = cast(dict[str, object], data_obj)
    result_obj = data.get("result")
    assert isinstance(result_obj, list)
    result = cast(list[object], result_obj)
    return [cast(dict[str, object], item) for item in result if isinstance(item, dict)]


def list_loki_series(
    *,
    url: str,
    tenant_id: str,
    selector: str,
    timeout_s: float = 10.0,
) -> list[dict[str, object]]:
    now_ns = int(time.time() * 1_000_000_000)
    with httpx.Client(timeout=timeout_s, trust_env=False) as client:
        response = client.get(
            f"{url.rstrip('/')}/loki/api/v1/series",
            headers={"X-Scope-OrgID": tenant_id},
            params={
                "match[]": selector,
                "start": str(now_ns - (600 * 1_000_000_000)),
                "end": str(now_ns + (600 * 1_000_000_000)),
            },
        )
        response.raise_for_status()
    raw_obj: object = response.json()
    assert isinstance(raw_obj, dict)
    payload = cast(dict[str, object], raw_obj)
    data_obj = payload.get("data")
    assert isinstance(data_obj, list)
    data = cast(list[object], data_obj)
    return [cast(dict[str, object], item) for item in data if isinstance(item, dict)]


def _run_ctx(
    *,
    run_id: str,
    timestamp_utc: str,
    pipeline_version: str = "p45-m3-wave2",
) -> RunContext:
    return RunContext.new(
        pipeline_version=pipeline_version,
        run_id=run_id,
        timestamp_utc=timestamp_utc,
    )


def run_batch_case_with_timestamp(
    *,
    tmp_root: Path,
    destination: Destination,
    run_id: str,
    text: str,
    timestamp_utc: str,
    force: bool = False,
) -> BatchRunArtifacts:
    doc = make_text_document(root=tmp_root / "docs", text=text)
    out_root = tmp_root / "out-batch"
    run_documents(
        docs=[doc],
        cfg=RunnerConfig(out_root=out_root, destination=destination, force=force),
        ctx=_run_ctx(run_id=run_id, timestamp_utc=timestamp_utc),
        flow_kind="uns",
        processor=StaticTextProcessor(),
    )
    return BatchRunArtifacts(
        doc=doc,
        out_root=out_root,
        sha256=expected_uns_sha(doc),
        batch_report=read_json_dict(out_root / "batch_report.json"),
    )


def run_worker_case_with_timestamp(
    *,
    tmp_root: Path,
    destination: Destination,
    run_id: str,
    doc: DocumentRef,
    timestamp_utc: str,
) -> WorkerRunArtifacts:
    out_root = tmp_root / "out-worker"
    sha256 = expected_uns_sha(doc)
    queue_backend = build_local_queue_backend(kind="sqlite", root=tmp_root / "queue-worker")
    queue_backend.enqueue(
        TaskV1(
            task_id=f"task-{run_id}",
            kind="uns",
            inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(doc)),
            execution=TaskExecutionV1(
                strategy=PartitionStrategy.AUTO,
                unique_element_ids=True,
            ),
            identity=TaskIdentityV1(
                pipeline_version="p45-m3-wave2",
                sha256=sha256,
            ),
        )
    )
    worker_ctx = _run_ctx(run_id=run_id, timestamp_utc=timestamp_utc)
    rc = run_worker(
        ctx=worker_ctx,
        poll_interval_ms=1,
        queue_backend=queue_backend,
        task_handler=build_task_execution_handler(
            cfg=RunnerConfig(out_root=out_root, destination=destination),
            ctx=worker_ctx,
            flow_kind="uns",
            processor=StaticTextProcessor(),
        ),
        drain_on_empty=True,
        sleep_fn=lambda _: None,
    )
    assert rc == 0
    return WorkerRunArtifacts(doc=doc, out_root=out_root, sha256=sha256)
