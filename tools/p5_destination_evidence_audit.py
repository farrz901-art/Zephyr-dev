from __future__ import annotations

import argparse
import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

import httpx

from it_stream.service import process_file as process_it_file
from zephyr_core import PartitionResult, RunContext
from zephyr_core.contracts.v1.enums import PartitionStrategy, RunOutcome
from zephyr_core.contracts.v1.run_meta import EngineMetaV1, MetricsV1, RunMetaV1, RunProvenanceV1
from zephyr_core.contracts.v2.delivery_payload import DELIVERY_PAYLOAD_SCHEMA_VERSION
from zephyr_ingest._internal.artifacts import dump_partition_artifacts
from zephyr_ingest._internal.delivery_payload import (
    build_artifacts_paths_for_run_meta_v1,
    build_delivery_content_evidence_v1,
)
from zephyr_ingest._internal.kafka_producer import make_kafka_producer
from zephyr_ingest._internal.utils import sha256_file
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
    normalize_weaviate_delivery_object_id,
)
from zephyr_ingest.destinations.base import DeliveryReceipt, Destination
from zephyr_ingest.destinations.clickhouse import ClickHouseDestination
from zephyr_ingest.destinations.fanout import FanoutDestination
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.destinations.kafka import KafkaDestination
from zephyr_ingest.destinations.loki import LokiDestination
from zephyr_ingest.destinations.mongodb import MongoDBDestination
from zephyr_ingest.destinations.opensearch import OpenSearchDestination
from zephyr_ingest.destinations.s3 import S3Destination, build_s3_delivery_object_key
from zephyr_ingest.destinations.sqlite import SqliteDestination
from zephyr_ingest.destinations.weaviate import WeaviateDestination
from zephyr_ingest.destinations.webhook import WebhookDestination
from zephyr_ingest.flow_processor import normalize_flow_input_identity_sha
from zephyr_ingest.testing.p45 import (
    DEFAULT_P45_ENV,
    P45_ENV_ALIASES,
    P45_HOME_ENV_NAME,
    LoadedP45Env,
    get_p45_opensearch_auth,
    get_p45_opensearch_url,
    get_p45_opensearch_verify_tls,
    parse_env_file,
    repo_root,
    resolve_p45_runtime_paths,
)
from zephyr_ingest.tests._p45_destination_wave1_helpers import (
    SigV4MinioClient,
    StaticTextProcessor,
    expected_uns_sha,
    make_text_document,
    webhook_events,
    webhook_reset,
)
from zephyr_ingest.tests._p45_destination_wave2_helpers import (
    LiveWeaviateCollection,
    MongoShellCollection,
    consume_kafka_records,
    current_timestamp_utc,
    drop_mongodb_collection,
    ensure_weaviate_collection,
    find_mongodb_document,
    get_weaviate_object,
    kafka_topic_name,
    loki_query_streams,
    weaviate_base_url,
)

FlowName = Literal["uns", "it"]
ModeName = Literal["direct", "fanout"]
ModeOption = Literal["direct", "fanout", "both", "remote-only"]
ValidationMode = Literal[
    "artifact_prepared_direct",
    "fanout_audit",
    "remote_only_direct",
]
TokenStrategy = Literal[
    "seeded_source_marker",
    "derived_from_source_result",
    "synthetic_local_marker",
]
DestinationId = Literal[
    "filesystem",
    "webhook",
    "kafka",
    "weaviate",
    "s3",
    "opensearch",
    "clickhouse",
    "mongodb",
    "loki",
    "sqlite",
]
Severity = Literal["none", "major"]

ALL_DESTINATIONS: tuple[DestinationId, ...] = (
    "filesystem",
    "webhook",
    "kafka",
    "weaviate",
    "s3",
    "opensearch",
    "clickhouse",
    "mongodb",
    "loki",
    "sqlite",
)
CONTENT_RICH_EVIDENCE_KINDS: frozenset[str] = frozenset(
    {"normalized_text_preview_v1", "normalized_text_and_records_preview_v1"}
)
DESTINATION_ALIASES: dict[str, DestinationId] = {
    "filesystem": "filesystem",
    "destination.webhook.v1": "webhook",
    "webhook": "webhook",
    "destination.kafka.v1": "kafka",
    "kafka": "kafka",
    "destination.weaviate.v1": "weaviate",
    "weaviate": "weaviate",
    "destination.s3.v1": "s3",
    "s3": "s3",
    "destination.opensearch.v1": "opensearch",
    "opensearch": "opensearch",
    "destination.clickhouse.v1": "clickhouse",
    "clickhouse": "clickhouse",
    "destination.mongodb.v1": "mongodb",
    "mongodb": "mongodb",
    "destination.loki.v1": "loki",
    "loki": "loki",
    "destination.sqlite.v1": "sqlite",
    "sqlite": "sqlite",
}


@dataclass(frozen=True, slots=True)
class PreparedCase:
    flow: FlowName
    mode: ModeName
    validation_mode: ValidationMode
    destination: DestinationId
    token: str
    sha256: str
    run_id: str
    out_root: Path
    meta: RunMetaV1
    result: PartitionResult
    artifacts_pre_dumped: bool
    token_strategy: TokenStrategy
    user_seeded_marker_proof: bool


@dataclass(frozen=True, slots=True)
class EndpointEvidence:
    endpoint_readback_ok: bool
    content_evidence_found: bool
    evidence_kind: str
    normalized_text_preview_found: bool
    records_preview_found: bool
    token_pass: bool
    note: str
    delivery_payload_visible: bool = False
    delivery_payload_core_metadata_check: bool = False
    delivery_locator_found: bool = False
    structured_state_log_status: str = "not_checked"


@dataclass(slots=True)
class AuditContext:
    env: LoadedP45Env
    out_root: Path
    suffix: str
    _clickhouse_table: str | None = None
    _mongodb_collection: str | None = None
    _weaviate_collection: str | None = None

    def clickhouse_table(self) -> str:
        if self._clickhouse_table is None:
            self._clickhouse_table = f"zephyr_p5_1_ev_{self.suffix}"
        return self._clickhouse_table

    def mongodb_collection(self) -> str:
        if self._mongodb_collection is None:
            self._mongodb_collection = f"zephyr_p5_1_ev_{self.suffix}"
        return self._mongodb_collection

    def weaviate_collection(self) -> str:
        if self._weaviate_collection is None:
            self._weaviate_collection = f"ZephyrP51Ev{self.suffix}"
        return self._weaviate_collection


def _normalize_env_value(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped != "" else None


def _load_env_values(env_file: Path) -> LoadedP45Env:
    raw_values = parse_env_file(env_file)
    merged: dict[str, str] = dict(DEFAULT_P45_ENV)
    sources: dict[str, str] = dict.fromkeys(merged, "default")
    for name, value in raw_values.items():
        normalized = _normalize_env_value(value)
        if normalized is None:
            continue
        merged[name] = normalized
        sources[name] = f"env-file:{env_file}"
    for canonical_name, alias_names in P45_ENV_ALIASES.items():
        if _normalize_env_value(merged.get(canonical_name)) is not None:
            continue
        for alias_name in alias_names:
            alias_value = _normalize_env_value(merged.get(alias_name))
            if alias_value is None:
                continue
            merged[canonical_name] = alias_value
            sources[canonical_name] = f"env-file:{env_file}|alias:{alias_name}"
            break
    runtime_paths = resolve_p45_runtime_paths(
        repo_root_path=repo_root(),
        environ={P45_HOME_ENV_NAME: str(env_file.parent.parent)},
    )
    return LoadedP45Env(values=merged, sources=sources, runtime_paths=runtime_paths)


def _repo_relative(path: Path) -> str:
    return path.relative_to(repo_root()).as_posix()


def _sanitize_issue(exc: Exception) -> str:
    return f"{type(exc).__name__}"


def _json_dict_from_text(text: str) -> dict[str, object]:
    raw_obj: object = json.loads(text)
    assert isinstance(raw_obj, dict)
    return cast(dict[str, object], raw_obj)


def _object_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _object_list(value: object) -> list[object]:
    assert isinstance(value, list)
    return cast(list[object], value)


def _search_token(value: object, token: str) -> bool:
    if isinstance(value, str):
        return token in value
    if isinstance(value, list):
        return any(_search_token(item, token) for item in value)
    if isinstance(value, dict):
        return any(_search_token(item, token) for item in value.values())
    return False


def _is_object_dict(value: object) -> bool:
    return isinstance(value, dict)


def _it_structured_state_log_status(*, payload: dict[str, object], flow: FlowName) -> str:
    if flow != "it":
        return "not_applicable"
    artifacts_obj = payload.get("artifacts")
    if not isinstance(artifacts_obj, dict):
        return "not_checked"
    artifacts = cast(dict[str, object], artifacts_obj)
    has_state = isinstance(artifacts.get("state_path"), str)
    has_logs = isinstance(artifacts.get("logs_path"), str)
    if has_state and has_logs:
        return "artifact_paths_only"
    return "not_available"


def _evaluate_content_evidence(
    *,
    content_evidence: dict[str, object] | None,
    flow: FlowName,
    token: str,
) -> EndpointEvidence:
    if content_evidence is None:
        return EndpointEvidence(
            endpoint_readback_ok=True,
            content_evidence_found=False,
            evidence_kind="missing",
            normalized_text_preview_found=False,
            records_preview_found=False,
            token_pass=False,
            note="content_evidence_missing",
        )
    evidence_kind_obj = content_evidence.get("evidence_kind")
    evidence_kind = evidence_kind_obj if isinstance(evidence_kind_obj, str) else "unknown"
    normalized_preview_obj = content_evidence.get("normalized_text_preview")
    normalized_found = isinstance(normalized_preview_obj, str) and token in normalized_preview_obj
    records_preview_obj = content_evidence.get("records_preview")
    records_found = _search_token(records_preview_obj, token)
    token_pass = normalized_found or (flow == "it" and records_found)
    return EndpointEvidence(
        endpoint_readback_ok=True,
        content_evidence_found=True,
        evidence_kind=evidence_kind,
        normalized_text_preview_found=normalized_found,
        records_preview_found=records_found,
        token_pass=token_pass,
        note="",
    )


def _evaluate_payload(
    *,
    payload: dict[str, object],
    flow: FlowName,
    token: str,
    expected_sha256: str,
    expected_run_id: str,
    delivery_locator_found: bool,
) -> EndpointEvidence:
    content_obj = payload.get("content_evidence")
    content_evidence = content_obj if isinstance(content_obj, dict) else None
    evidence = _evaluate_content_evidence(
        content_evidence=cast(dict[str, object] | None, content_evidence),
        flow=flow,
        token=token,
    )
    run_meta_obj = payload.get("run_meta")
    run_meta = run_meta_obj if isinstance(run_meta_obj, dict) else None
    engine_obj = None if run_meta is None else run_meta.get("engine")
    metrics_obj = None if run_meta is None else run_meta.get("metrics")
    provenance_obj = None if run_meta is None else run_meta.get("provenance")
    schema_ok = payload.get("schema_version") == DELIVERY_PAYLOAD_SCHEMA_VERSION
    sha_ok = payload.get("sha256") == expected_sha256
    run_id_ok = isinstance(run_meta, dict) and run_meta.get("run_id") == expected_run_id
    engine_ok = isinstance(engine_obj, dict) and isinstance(engine_obj.get("name"), str)
    metrics_ok = _is_object_dict(metrics_obj)
    provenance_ok = _is_object_dict(provenance_obj)
    content_ok = isinstance(content_evidence, dict)
    return EndpointEvidence(
        endpoint_readback_ok=evidence.endpoint_readback_ok,
        content_evidence_found=evidence.content_evidence_found,
        evidence_kind=evidence.evidence_kind,
        normalized_text_preview_found=evidence.normalized_text_preview_found,
        records_preview_found=evidence.records_preview_found,
        token_pass=evidence.token_pass,
        note=evidence.note,
        delivery_payload_visible=True,
        delivery_payload_core_metadata_check=(
            schema_ok
            and sha_ok
            and run_id_ok
            and engine_ok
            and metrics_ok
            and provenance_ok
            and content_ok
            and delivery_locator_found
        ),
        delivery_locator_found=delivery_locator_found,
        structured_state_log_status=_it_structured_state_log_status(payload=payload, flow=flow),
    )


def _evaluate_weaviate_properties(*, properties: dict[str, object], token: str) -> EndpointEvidence:
    normalized_obj = properties.get("normalized_text")
    normalized_found = isinstance(normalized_obj, str) and token in normalized_obj
    return EndpointEvidence(
        endpoint_readback_ok=True,
        content_evidence_found=isinstance(normalized_obj, str),
        evidence_kind="normalized_text_projection_v1",
        normalized_text_preview_found=normalized_found,
        records_preview_found=False,
        token_pass=normalized_found,
        note="weaviate_projects_normalized_text",
        delivery_payload_visible=False,
        delivery_payload_core_metadata_check=False,
        delivery_locator_found=True,
        structured_state_log_status="not_checked",
    )


def _ensure_clickhouse_table(env: LoadedP45Env, *, table: str) -> None:
    database = env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE")
    username = env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME")
    password = env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD")
    base_url = env.require("ZEPHYR_P45_CLICKHOUSE_URL").rstrip("/")
    create_query = (
        f"CREATE TABLE IF NOT EXISTS {database}.{table} ("
        "identity_key String, sha256 String, run_id String, schema_version UInt32, "
        "payload_json String, delivered_at_utc String"
        ") ENGINE = ReplacingMergeTree() ORDER BY identity_key"
    )
    with httpx.Client(timeout=10.0, trust_env=False) as client:
        client.post(
            base_url, params={"query": create_query}, auth=(username, password)
        ).raise_for_status()


def _prepare_weaviate_collection(env: LoadedP45Env, *, collection_name: str) -> None:
    api_key = env.require("ZEPHYR_P45_WEAVIATE_API_KEY")
    ensure_weaviate_collection(
        base_url=weaviate_base_url(env),
        api_key=api_key,
        collection_name=collection_name,
    )


def _build_run_meta(
    *,
    ctx: RunContext,
    result_document: object,
    result_engine: object,
    normalized_text_len: int,
    elements_count: int,
) -> RunMetaV1:
    from zephyr_core.contracts.v1.models import DocumentMetadata, EngineInfo

    assert isinstance(result_document, DocumentMetadata)
    assert isinstance(result_engine, EngineInfo)
    return RunMetaV1(
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        timestamp_utc=ctx.timestamp_utc,
        schema_version=ctx.run_meta_schema_version,
        outcome=RunOutcome.SUCCESS,
        document=result_document,
        engine=EngineMetaV1(
            name=result_engine.name,
            backend=result_engine.backend,
            version=result_engine.version,
            strategy=str(result_engine.strategy),
        ),
        metrics=MetricsV1(
            duration_ms=0,
            elements_count=elements_count,
            normalized_text_len=normalized_text_len,
            attempts=1,
        ),
        warnings=[],
        error=None,
        provenance=RunProvenanceV1(
            run_origin="intake",
            delivery_origin="primary",
            execution_mode="batch",
            task_id=f"task-{ctx.run_id}",
            task_identity_key=f"task-ident:{ctx.run_id}",
        ),
    )


def _build_uns_case(
    *,
    root: Path,
    destination: DestinationId,
    mode: ModeName,
    validation_mode: ValidationMode,
    artifacts_pre_dumped: bool,
) -> PreparedCase:
    token = f"zephyr_p5_1_uns_marker_{uuid.uuid4().hex}"
    run_id = f"p5ev_uns_{mode}_{destination}_{uuid.uuid4().hex[:8]}"
    doc = make_text_document(root=root / "input", name="uns_marker.txt", text=f"UNS {token}\n")
    sha256 = expected_uns_sha(doc)
    ctx = RunContext.new(
        pipeline_version="p5.1.destination_evidence_audit",
        run_id=run_id,
        timestamp_utc=current_timestamp_utc(),
    )
    result = StaticTextProcessor().process(
        doc=doc,
        strategy=PartitionStrategy.AUTO,
        unique_element_ids=True,
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        sha256=sha256,
    )
    meta = _build_run_meta(
        ctx=ctx,
        result_document=result.document,
        result_engine=result.engine,
        normalized_text_len=len(result.normalized_text),
        elements_count=len(result.elements),
    )
    out_root = root / "out"
    out_root.mkdir(parents=True, exist_ok=True)
    if artifacts_pre_dumped:
        dump_partition_artifacts(out_root=out_root, sha256=sha256, meta=meta, result=result)
    return PreparedCase(
        flow="uns",
        mode=mode,
        validation_mode=validation_mode,
        destination=destination,
        token=token,
        sha256=sha256,
        run_id=run_id,
        out_root=out_root,
        meta=meta,
        result=result,
        artifacts_pre_dumped=artifacts_pre_dumped,
        token_strategy="synthetic_local_marker",
        user_seeded_marker_proof=False,
    )


def _build_it_case(
    *,
    root: Path,
    destination: DestinationId,
    mode: ModeName,
    validation_mode: ValidationMode,
    artifacts_pre_dumped: bool,
) -> PreparedCase:
    token = f"zephyr_p5_1_it_marker_{uuid.uuid4().hex}"
    run_id = f"p5ev_it_{mode}_{destination}_{uuid.uuid4().hex[:8]}"
    input_root = root / "input"
    input_root.mkdir(parents=True, exist_ok=True)
    input_path = input_root / "it_records.json"
    ctx = RunContext.new(
        pipeline_version="p5.1.destination_evidence_audit",
        run_id=run_id,
        timestamp_utc=current_timestamp_utc(),
    )
    input_payload = {
        "stream": "zephyr-p5-1-audit",
        "records": [
            {
                "data": {"id": 1, "marker": token, "message": f"IT {token}"},
                "emitted_at": ctx.timestamp_utc,
            },
            {
                "data": {"id": 2, "kind": "secondary", "nested": {"marker": token}},
                "emitted_at": ctx.timestamp_utc,
            },
        ],
    }
    input_path.write_text(json.dumps(input_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    sha256 = normalize_flow_input_identity_sha(
        flow_kind="it",
        filename=str(input_path),
        default_sha=sha256_file(input_path),
    )
    result = process_it_file(
        filename=str(input_path),
        strategy=PartitionStrategy.AUTO,
        sha256=sha256,
        size_bytes=input_path.stat().st_size,
    )
    meta = _build_run_meta(
        ctx=ctx,
        result_document=result.document,
        result_engine=result.engine,
        normalized_text_len=len(result.normalized_text),
        elements_count=len(result.elements),
    )
    out_root = root / "out"
    out_root.mkdir(parents=True, exist_ok=True)
    if artifacts_pre_dumped:
        dump_partition_artifacts(out_root=out_root, sha256=sha256, meta=meta, result=result)
    return PreparedCase(
        flow="it",
        mode=mode,
        validation_mode=validation_mode,
        destination=destination,
        token=token,
        sha256=sha256,
        run_id=run_id,
        out_root=out_root,
        meta=meta,
        result=result,
        artifacts_pre_dumped=artifacts_pre_dumped,
        token_strategy="synthetic_local_marker",
        user_seeded_marker_proof=False,
    )


def _build_case(
    *,
    flow: FlowName,
    root: Path,
    destination: DestinationId,
    mode: ModeName,
    validation_mode: ValidationMode,
    artifacts_pre_dumped: bool,
) -> PreparedCase:
    if flow == "uns":
        return _build_uns_case(
            root=root,
            destination=destination,
            mode=mode,
            validation_mode=validation_mode,
            artifacts_pre_dumped=artifacts_pre_dumped,
        )
    return _build_it_case(
        root=root,
        destination=destination,
        mode=mode,
        validation_mode=validation_mode,
        artifacts_pre_dumped=artifacts_pre_dumped,
    )


def _delivery_identity(case: PreparedCase) -> str:
    return normalize_delivery_idempotency_key(
        identity=DeliveryIdentityV1(sha256=case.sha256, run_id=case.run_id)
    )


def _build_primary_destination(
    *,
    destination_id: DestinationId,
    ctx: AuditContext,
    case: PreparedCase,
) -> Destination:
    env = ctx.env
    if destination_id == "filesystem":
        return FilesystemDestination()
    if destination_id == "webhook":
        return WebhookDestination(url=env.require("ZEPHYR_P45_WEBHOOK_ECHO_URL"))
    if destination_id == "kafka":
        topic = kafka_topic_name(prefix=f"zephyr_p5_1_ev_{case.flow}_{case.mode}")
        return KafkaDestination(
            topic=topic,
            producer=make_kafka_producer(brokers=env.require("ZEPHYR_P45_KAFKA_BROKERS")),
        )
    if destination_id == "weaviate":
        collection_name = ctx.weaviate_collection()
        _prepare_weaviate_collection(env, collection_name=collection_name)
        return WeaviateDestination(
            collection_name=collection_name,
            collection=LiveWeaviateCollection(
                base_url=weaviate_base_url(env),
                collection_name=collection_name,
                api_key=env.require("ZEPHYR_P45_WEAVIATE_API_KEY"),
            ),
        )
    if destination_id == "s3":
        return S3Destination(
            bucket=env.require("ZEPHYR_P45_S3_BUCKET"),
            region=env.require("ZEPHYR_P45_S3_REGION"),
            access_key=env.require("ZEPHYR_P45_S3_ACCESS_KEY"),
            secret_key=env.require("ZEPHYR_P45_S3_SECRET_KEY"),
            endpoint_url=env.require("ZEPHYR_P45_S3_ENDPOINT"),
            session_token=env.get("ZEPHYR_P45_S3_SESSION_TOKEN"),
            prefix=f"p5_1_destination_evidence/{case.flow}/{case.mode}/{destination_id}/{case.run_id}",
        )
    if destination_id == "opensearch":
        auth = get_p45_opensearch_auth(env)
        username = None if auth is None else auth[0]
        password = None if auth is None else auth[1]
        return OpenSearchDestination(
            url=get_p45_opensearch_url(env),
            index=env.require("ZEPHYR_P45_OPENSEARCH_INDEX"),
            verify_tls=get_p45_opensearch_verify_tls(env),
            username=username,
            password=password,
        )
    if destination_id == "clickhouse":
        table = ctx.clickhouse_table()
        _ensure_clickhouse_table(env, table=table)
        return ClickHouseDestination(
            url=env.require("ZEPHYR_P45_CLICKHOUSE_URL").rstrip("/"),
            database=env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
            table=table,
            username=env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
            password=env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD"),
        )
    if destination_id == "mongodb":
        collection = ctx.mongodb_collection()
        uri = env.require("ZEPHYR_P45_MONGODB_URI")
        database = env.require("ZEPHYR_P45_MONGODB_DATABASE")
        drop_mongodb_collection(env=env, uri=uri, database=database, collection=collection)
        return MongoDBDestination(
            uri=uri,
            database=database,
            collection=collection,
            collection_obj=MongoShellCollection(
                env=env,
                uri=uri,
                database=database,
                collection=collection,
            ),
        )
    if destination_id == "loki":
        return LokiDestination(
            url=env.require("ZEPHYR_P45_LOKI_URL"),
            stream=env.require("ZEPHYR_P45_LOKI_STREAM"),
            tenant_id=env.require("ZEPHYR_P45_LOKI_TENANT_ID"),
        )
    if destination_id == "sqlite":
        return SqliteDestination(
            db_path=case.out_root / "sqlite" / "delivery.db",
            table_name="delivery_rows",
        )
    raise ValueError(f"Unsupported destination id: {destination_id}")


def _build_audit_destination(*, case: PreparedCase) -> tuple[Destination, str]:
    if case.destination == "sqlite":
        return FilesystemDestination(), "filesystem"
    return (
        SqliteDestination(
            db_path=case.out_root / "fanout_audit" / "audit.sqlite",
            table_name="delivery_rows",
        ),
        "sqlite",
    )


def _find_child_receipt(
    receipt: DeliveryReceipt,
    *,
    destination_name: str,
) -> dict[str, object] | None:
    details = receipt.details
    if details is None:
        return None
    children_obj = details.get("children")
    if not isinstance(children_obj, list):
        return None
    for item in children_obj:
        if not isinstance(item, dict):
            continue
        child = cast(dict[str, object], item)
        child_name = child.get("destination")
        if child_name == destination_name:
            return child
    return None


def _audit_sqlite_evidence(
    *,
    db_path: Path,
    identity_key: str,
    flow: FlowName,
    token: str,
    expected_sha256: str,
    expected_run_id: str,
) -> EndpointEvidence:
    if not db_path.exists():
        return EndpointEvidence(False, False, "missing", False, False, False, "sqlite_db_missing")
    connection = sqlite3.connect(db_path)
    try:
        row = connection.execute(
            "SELECT payload_json FROM delivery_rows WHERE identity_key = ?",
            (identity_key,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return EndpointEvidence(False, False, "missing", False, False, False, "sqlite_row_missing")
    payload = _json_dict_from_text(cast(str, row[0]))
    run_meta_obj = payload.get("run_meta")
    assert isinstance(run_meta_obj, dict)
    return _evaluate_payload(
        payload=payload,
        flow=flow,
        token=token,
        expected_sha256=expected_sha256,
        expected_run_id=expected_run_id,
        delivery_locator_found=True,
    )


def _audit_filesystem_evidence(case: PreparedCase) -> EndpointEvidence:
    artifacts = build_artifacts_paths_for_run_meta_v1(
        out_root=case.out_root,
        sha256=case.sha256,
        run_meta=case.meta.to_dict(),
    )
    content = build_delivery_content_evidence_v1(
        artifacts=artifacts,
    )
    evidence = _evaluate_content_evidence(
        content_evidence=cast(dict[str, object], content),
        flow=case.flow,
        token=case.token,
    )
    normalized_path = case.out_root / case.sha256 / "normalized.txt"
    return EndpointEvidence(
        endpoint_readback_ok=normalized_path.exists(),
        content_evidence_found=evidence.content_evidence_found,
        evidence_kind=evidence.evidence_kind,
        normalized_text_preview_found=evidence.normalized_text_preview_found,
        records_preview_found=evidence.records_preview_found,
        token_pass=evidence.token_pass,
        note=evidence.note,
    )


def _audit_webhook_evidence(env: LoadedP45Env, case: PreparedCase) -> EndpointEvidence:
    base_url = env.require("ZEPHYR_P45_WEBHOOK_ECHO_URL").removesuffix("/ingest")
    items = webhook_events(base_url=base_url)
    for item in items:
        payload_obj = item.get("payload")
        if not isinstance(payload_obj, dict):
            continue
        payload = cast(dict[str, object], payload_obj)
        run_meta_obj = payload.get("run_meta")
        if not isinstance(run_meta_obj, dict):
            continue
        run_meta = cast(dict[str, object], run_meta_obj)
        if run_meta.get("run_id") != case.run_id:
            continue
        return _evaluate_payload(
            payload=payload,
            flow=case.flow,
            token=case.token,
            expected_sha256=case.sha256,
            expected_run_id=case.run_id,
            delivery_locator_found=True,
        )
    return EndpointEvidence(False, False, "missing", False, False, False, "webhook_event_missing")


def _audit_kafka_evidence(
    *,
    brokers: str,
    destination: Destination,
    case: PreparedCase,
) -> EndpointEvidence:
    assert isinstance(destination, KafkaDestination)
    records = consume_kafka_records(
        brokers=brokers,
        topic=destination.topic,
        expected_count=1,
    )
    if not records:
        return EndpointEvidence(
            False, False, "missing", False, False, False, "kafka_record_missing"
        )
    payload = records[0].value
    return _evaluate_payload(
        payload=payload,
        flow=case.flow,
        token=case.token,
        expected_sha256=case.sha256,
        expected_run_id=case.run_id,
        delivery_locator_found=records[0].key == _delivery_identity(case),
    )


def _audit_s3_evidence(
    env: LoadedP45Env, destination: Destination, case: PreparedCase
) -> EndpointEvidence:
    assert isinstance(destination, S3Destination)
    client = SigV4MinioClient(
        endpoint_url=env.require("ZEPHYR_P45_S3_ENDPOINT"),
        access_key=env.require("ZEPHYR_P45_S3_ACCESS_KEY"),
        secret_key=env.require("ZEPHYR_P45_S3_SECRET_KEY"),
        region=env.require("ZEPHYR_P45_S3_REGION"),
    )
    object_key = build_s3_delivery_object_key(
        prefix=destination.prefix,
        idempotency_key=_delivery_identity(case),
    )
    if object_key not in client.list_keys(
        bucket=env.require("ZEPHYR_P45_S3_BUCKET"),
        prefix=destination.prefix,
    ):
        return EndpointEvidence(False, False, "missing", False, False, False, "s3_object_missing")
    payload = _json_dict_from_text(
        client.get_object_text(bucket=env.require("ZEPHYR_P45_S3_BUCKET"), key=object_key)
    )
    return _evaluate_payload(
        payload=payload,
        flow=case.flow,
        token=case.token,
        expected_sha256=case.sha256,
        expected_run_id=case.run_id,
        delivery_locator_found=True,
    )


def _audit_opensearch_evidence(env: LoadedP45Env, case: PreparedCase) -> EndpointEvidence:
    doc_id = _delivery_identity(case)
    auth = get_p45_opensearch_auth(env)
    request_kwargs: dict[str, object] = {
        "timeout": 10.0,
        "verify": get_p45_opensearch_verify_tls(env),
        "trust_env": False,
    }
    if auth is not None:
        request_kwargs["auth"] = auth
    with httpx.Client(**request_kwargs) as client:
        response = client.get(
            f"{get_p45_opensearch_url(env).rstrip('/')}/{env.require('ZEPHYR_P45_OPENSEARCH_INDEX')}/_doc/{doc_id}"
        )
    if response.status_code == 404:
        return EndpointEvidence(
            False, False, "missing", False, False, False, "opensearch_doc_missing"
        )
    response.raise_for_status()
    body = _json_dict_from_text(response.text)
    source_obj = body.get("_source")
    if not isinstance(source_obj, dict):
        return EndpointEvidence(
            False, False, "missing", False, False, False, "opensearch_source_missing"
        )
    source = cast(dict[str, object], source_obj)
    payload_obj = source.get("payload")
    if not isinstance(payload_obj, dict):
        return EndpointEvidence(
            False, False, "missing", False, False, False, "opensearch_payload_missing"
        )
    payload = cast(dict[str, object], payload_obj)
    return _evaluate_payload(
        payload=payload,
        flow=case.flow,
        token=case.token,
        expected_sha256=case.sha256,
        expected_run_id=case.run_id,
        delivery_locator_found=source.get("delivery_identity") == doc_id,
    )


def _audit_clickhouse_evidence(
    env: LoadedP45Env, ctx: AuditContext, case: PreparedCase
) -> EndpointEvidence:
    identity_key = _delivery_identity(case)
    database = env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE")
    table = ctx.clickhouse_table()
    query = (
        f"SELECT payload_json FROM {database}.{table} "
        f"WHERE identity_key = '{identity_key}' "
        "ORDER BY delivered_at_utc DESC LIMIT 1 FORMAT JSONEachRow"
    )
    with httpx.Client(timeout=10.0, trust_env=False) as client:
        response = client.get(
            env.require("ZEPHYR_P45_CLICKHOUSE_URL").rstrip("/"),
            params={"query": query},
            auth=(
                env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
                env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD"),
            ),
        )
    response.raise_for_status()
    raw_text = response.text.strip()
    if raw_text == "":
        return EndpointEvidence(
            False, False, "missing", False, False, False, "clickhouse_row_missing"
        )
    row = _json_dict_from_text(raw_text)
    payload_json = row.get("payload_json")
    if not isinstance(payload_json, str):
        return EndpointEvidence(
            False, False, "missing", False, False, False, "clickhouse_payload_missing"
        )
    payload = _json_dict_from_text(payload_json)
    return _evaluate_payload(
        payload=payload,
        flow=case.flow,
        token=case.token,
        expected_sha256=case.sha256,
        expected_run_id=case.run_id,
        delivery_locator_found=True,
    )


def _audit_mongodb_evidence(
    env: LoadedP45Env, ctx: AuditContext, case: PreparedCase
) -> EndpointEvidence:
    document = find_mongodb_document(
        env=env,
        uri=env.require("ZEPHYR_P45_MONGODB_URI"),
        database=env.require("ZEPHYR_P45_MONGODB_DATABASE"),
        collection=ctx.mongodb_collection(),
        document_id=_delivery_identity(case),
    )
    if document is None:
        return EndpointEvidence(
            False, False, "missing", False, False, False, "mongodb_document_missing"
        )
    payload_obj = document.get("payload")
    if not isinstance(payload_obj, dict):
        return EndpointEvidence(
            False, False, "missing", False, False, False, "mongodb_payload_missing"
        )
    payload = cast(dict[str, object], payload_obj)
    return _evaluate_payload(
        payload=payload,
        flow=case.flow,
        token=case.token,
        expected_sha256=case.sha256,
        expected_run_id=case.run_id,
        delivery_locator_found=True,
    )


def _audit_loki_evidence(env: LoadedP45Env, case: PreparedCase) -> EndpointEvidence:
    selector = f'{{zephyr_delivery_identity="{_delivery_identity(case)}"}}'
    streams = loki_query_streams(
        url=env.require("ZEPHYR_P45_LOKI_URL"),
        tenant_id=env.require("ZEPHYR_P45_LOKI_TENANT_ID"),
        selector=selector,
        timestamp_utc=case.meta.timestamp_utc,
    )
    if not streams:
        return EndpointEvidence(False, False, "missing", False, False, False, "loki_stream_missing")
    values_obj = streams[0].get("values")
    if not isinstance(values_obj, list) or not values_obj:
        return EndpointEvidence(False, False, "missing", False, False, False, "loki_values_missing")
    first_entry = values_obj[0]
    if (
        not isinstance(first_entry, list)
        or len(first_entry) != 2
        or not isinstance(first_entry[1], str)
    ):
        return EndpointEvidence(
            False, False, "missing", False, False, False, "loki_payload_missing"
        )
    payload = _json_dict_from_text(first_entry[1])
    return _evaluate_payload(
        payload=payload,
        flow=case.flow,
        token=case.token,
        expected_sha256=case.sha256,
        expected_run_id=case.run_id,
        delivery_locator_found=True,
    )


def _audit_weaviate_evidence(
    env: LoadedP45Env, ctx: AuditContext, case: PreparedCase
) -> EndpointEvidence:
    object_data = get_weaviate_object(
        base_url=weaviate_base_url(env),
        api_key=env.require("ZEPHYR_P45_WEAVIATE_API_KEY"),
        collection_name=ctx.weaviate_collection(),
        object_id=normalize_weaviate_delivery_object_id(sha256=case.sha256),
    )
    properties_obj = object_data.get("properties")
    if not isinstance(properties_obj, dict):
        return EndpointEvidence(
            False, False, "missing", False, False, False, "weaviate_properties_missing"
        )
    properties = cast(dict[str, object], properties_obj)
    return _evaluate_weaviate_properties(properties=properties, token=case.token)


def _readback_target_evidence(
    *,
    destination_id: DestinationId,
    destination: Destination,
    ctx: AuditContext,
    case: PreparedCase,
) -> EndpointEvidence:
    if destination_id == "filesystem":
        return _audit_filesystem_evidence(case)
    if destination_id == "webhook":
        return _audit_webhook_evidence(ctx.env, case)
    if destination_id == "kafka":
        return _audit_kafka_evidence(
            brokers=ctx.env.require("ZEPHYR_P45_KAFKA_BROKERS"),
            destination=destination,
            case=case,
        )
    if destination_id == "weaviate":
        return _audit_weaviate_evidence(ctx.env, ctx, case)
    if destination_id == "s3":
        return _audit_s3_evidence(ctx.env, destination, case)
    if destination_id == "opensearch":
        return _audit_opensearch_evidence(ctx.env, case)
    if destination_id == "clickhouse":
        return _audit_clickhouse_evidence(ctx.env, ctx, case)
    if destination_id == "mongodb":
        return _audit_mongodb_evidence(ctx.env, ctx, case)
    if destination_id == "loki":
        return _audit_loki_evidence(ctx.env, case)
    if destination_id == "sqlite":
        db_path = case.out_root / "sqlite" / "delivery.db"
        return _audit_sqlite_evidence(
            db_path=db_path,
            identity_key=_delivery_identity(case),
            flow=case.flow,
            token=case.token,
            expected_sha256=case.sha256,
            expected_run_id=case.run_id,
        )
    raise ValueError(f"Unsupported destination id: {destination_id}")


def _readback_audit_child_evidence(
    *,
    audit_destination_name: str,
    case: PreparedCase,
) -> EndpointEvidence:
    if audit_destination_name == "filesystem":
        return _audit_filesystem_evidence(case)
    audit_db_path = case.out_root / "fanout_audit" / "audit.sqlite"
    return _audit_sqlite_evidence(
        db_path=audit_db_path,
        identity_key=_delivery_identity(case),
        flow=case.flow,
        token=case.token,
        expected_sha256=case.sha256,
        expected_run_id=case.run_id,
    )


def _child_ok(child: dict[str, object] | None) -> bool:
    return isinstance(child, dict) and child.get("ok") is True


def _severity_and_issue(
    *,
    executed: bool,
    delivery_ok: bool,
    endpoint_readback_ok: bool,
    content_evidence_found: bool,
    token_pass: bool,
    validation_mode: ValidationMode,
    delivery_payload_core_metadata_check: bool,
) -> tuple[str, Severity]:
    if not executed:
        return ("execution_failed", "major")
    if not delivery_ok:
        return ("delivery_failed", "major")
    if not endpoint_readback_ok:
        return ("endpoint_readback_failed", "major")
    if not content_evidence_found:
        return ("content_evidence_missing", "major")
    if not token_pass:
        return ("token_missing_from_evidence", "major")
    if validation_mode == "remote_only_direct" and not delivery_payload_core_metadata_check:
        return ("delivery_payload_core_metadata_missing", "major")
    return ("", "none")


def _validation_mode_for_mode(mode: ModeName) -> ValidationMode:
    return "artifact_prepared_direct" if mode == "direct" else "fanout_audit"


def _product_direct_claim_safe(
    *,
    validation_mode: ValidationMode,
    issue: str,
    delivery_payload_core_metadata_check: bool,
) -> bool:
    return (
        validation_mode == "remote_only_direct"
        and issue == ""
        and delivery_payload_core_metadata_check
    )


def _audit_one(
    *,
    ctx: AuditContext,
    destination_id: DestinationId,
    flow: FlowName,
    mode: ModeName,
) -> dict[str, object]:
    case_root = ctx.out_root / mode / flow / destination_id / uuid.uuid4().hex[:8]
    case = _build_case(
        flow=flow,
        root=case_root,
        destination=destination_id,
        mode=mode,
        validation_mode=_validation_mode_for_mode(mode),
        artifacts_pre_dumped=True,
    )
    primary_destination = _build_primary_destination(
        destination_id=destination_id, ctx=ctx, case=case
    )
    primary_destination_name = primary_destination.name

    fanout_top_level_ok: bool | None = None
    audit_destination_name: str | None = None
    audit_evidence: EndpointEvidence | None = None
    target_child_ok: bool | None = None
    target_receipt_ok = False
    executed = True

    if destination_id == "webhook":
        webhook_reset(
            base_url=ctx.env.require("ZEPHYR_P45_WEBHOOK_ECHO_URL").removesuffix("/ingest")
        )

    if mode == "direct":
        receipt = primary_destination(
            out_root=case.out_root,
            sha256=case.sha256,
            meta=case.meta,
            result=case.result,
        )
        target_receipt_ok = receipt.ok
    else:
        audit_destination, audit_destination_name = _build_audit_destination(case=case)
        fanout = FanoutDestination(destinations=(primary_destination, audit_destination))
        receipt = fanout(
            out_root=case.out_root, sha256=case.sha256, meta=case.meta, result=case.result
        )
        fanout_top_level_ok = receipt.ok
        primary_child = _find_child_receipt(receipt, destination_name=primary_destination_name)
        audit_child = _find_child_receipt(receipt, destination_name=audit_destination_name)
        target_child_ok = _child_ok(primary_child)
        target_receipt_ok = target_child_ok is True
        if audit_destination_name is not None and _child_ok(audit_child):
            audit_evidence = _readback_audit_child_evidence(
                audit_destination_name=audit_destination_name,
                case=case,
            )

    target_evidence = _readback_target_evidence(
        destination_id=destination_id,
        destination=primary_destination,
        ctx=ctx,
        case=case,
    )
    issue, severity = _severity_and_issue(
        executed=executed,
        delivery_ok=target_receipt_ok,
        endpoint_readback_ok=target_evidence.endpoint_readback_ok,
        content_evidence_found=target_evidence.content_evidence_found,
        token_pass=target_evidence.token_pass,
        validation_mode=case.validation_mode,
        delivery_payload_core_metadata_check=target_evidence.delivery_payload_core_metadata_check,
    )

    return {
        "destination": destination_id,
        "flow": flow,
        "mode": mode,
        "validation_mode": case.validation_mode,
        "artifacts_pre_dumped": case.artifacts_pre_dumped,
        "product_direct_claim_safe": _product_direct_claim_safe(
            validation_mode=case.validation_mode,
            issue=issue,
            delivery_payload_core_metadata_check=target_evidence.delivery_payload_core_metadata_check,
        ),
        "user_seeded_marker_proof": case.user_seeded_marker_proof,
        "token_strategy": case.token_strategy,
        "run_id": case.run_id,
        "sha256": case.sha256,
        "delivery_identity": _delivery_identity(case),
        "executed": executed,
        "delivery_ok": target_receipt_ok,
        "fanout_top_level_ok": fanout_top_level_ok,
        "fanout_child_ok": target_child_ok,
        "endpoint_readback_ok": target_evidence.endpoint_readback_ok,
        "content_evidence_found": target_evidence.content_evidence_found,
        "evidence_kind": target_evidence.evidence_kind,
        "delivery_payload_visible": target_evidence.delivery_payload_visible,
        "delivery_payload_core_metadata_check": (
            target_evidence.delivery_payload_core_metadata_check
        ),
        "delivery_locator_found": target_evidence.delivery_locator_found,
        "structured_state_log_status": target_evidence.structured_state_log_status,
        "normalized_text_preview_found": target_evidence.normalized_text_preview_found,
        "records_preview_found": target_evidence.records_preview_found,
        "token_pass": target_evidence.token_pass,
        "issue": issue,
        "severity": severity,
        "note": target_evidence.note,
        "audit_child_destination": audit_destination_name,
        "audit_endpoint_readback_ok": None
        if audit_evidence is None
        else audit_evidence.endpoint_readback_ok,
        "audit_content_evidence_found": None
        if audit_evidence is None
        else audit_evidence.content_evidence_found,
        "audit_evidence_kind": None if audit_evidence is None else audit_evidence.evidence_kind,
        "audit_token_pass": None if audit_evidence is None else audit_evidence.token_pass,
        "remote_only_content_evidence_gap": False,
    }


def _audit_one_safe(
    *,
    ctx: AuditContext,
    destination_id: DestinationId,
    flow: FlowName,
    mode: ModeName,
) -> dict[str, object]:
    try:
        return _audit_one(ctx=ctx, destination_id=destination_id, flow=flow, mode=mode)
    except Exception as exc:
        return {
            "destination": destination_id,
            "flow": flow,
            "mode": mode,
            "validation_mode": _validation_mode_for_mode(mode),
            "artifacts_pre_dumped": True,
            "product_direct_claim_safe": False,
            "user_seeded_marker_proof": False,
            "token_strategy": "synthetic_local_marker",
            "run_id": "",
            "sha256": "",
            "delivery_identity": "",
            "executed": False,
            "delivery_ok": False,
            "fanout_top_level_ok": None,
            "fanout_child_ok": None,
            "endpoint_readback_ok": False,
            "content_evidence_found": False,
            "evidence_kind": "missing",
            "delivery_payload_visible": False,
            "delivery_payload_core_metadata_check": False,
            "delivery_locator_found": False,
            "structured_state_log_status": "not_checked",
            "normalized_text_preview_found": False,
            "records_preview_found": False,
            "token_pass": False,
            "issue": _sanitize_issue(exc),
            "severity": "major",
            "audit_child_destination": None,
            "audit_endpoint_readback_ok": None,
            "audit_content_evidence_found": None,
            "audit_evidence_kind": None,
            "audit_token_pass": None,
            "remote_only_content_evidence_gap": False,
        }


def build_remote_only_direct_results(
    *,
    ctx: AuditContext,
    flows: list[FlowName] | None = None,
    destinations: list[DestinationId] | None = None,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    selected_flows = ["uns", "it"] if flows is None else list(flows)
    selected_destinations = (
        ["kafka", "opensearch"] if destinations is None else list(destinations)
    )
    for flow in selected_flows:
        for destination_id in selected_destinations:
            if destination_id not in {"kafka", "opensearch"}:
                continue
            case_root = (
                ctx.out_root
                / "remote_only_direct"
                / cast(str, flow)
                / cast(str, destination_id)
                / uuid.uuid4().hex[:8]
            )
            try:
                case = _build_case(
                    flow=cast(FlowName, flow),
                    root=case_root,
                    destination=cast(DestinationId, destination_id),
                    mode="direct",
                    validation_mode="remote_only_direct",
                    artifacts_pre_dumped=False,
                )
                primary_destination = _build_primary_destination(
                    destination_id=cast(DestinationId, destination_id),
                    ctx=ctx,
                    case=case,
                )
                receipt = primary_destination(
                    out_root=case.out_root,
                    sha256=case.sha256,
                    meta=case.meta,
                    result=case.result,
                )
                target_evidence = _readback_target_evidence(
                    destination_id=cast(DestinationId, destination_id),
                    destination=primary_destination,
                    ctx=ctx,
                    case=case,
                )
                issue, severity = _severity_and_issue(
                    executed=True,
                    delivery_ok=receipt.ok,
                    endpoint_readback_ok=target_evidence.endpoint_readback_ok,
                    content_evidence_found=target_evidence.content_evidence_found,
                    token_pass=target_evidence.token_pass,
                    validation_mode=case.validation_mode,
                    delivery_payload_core_metadata_check=target_evidence.delivery_payload_core_metadata_check,
                )
                results.append(
                    {
                        "destination": destination_id,
                        "flow": flow,
                        "mode": "direct",
                        "validation_mode": case.validation_mode,
                        "artifacts_pre_dumped": case.artifacts_pre_dumped,
                        "product_direct_claim_safe": _product_direct_claim_safe(
                            validation_mode=case.validation_mode,
                            issue=issue,
                            delivery_payload_core_metadata_check=target_evidence.delivery_payload_core_metadata_check,
                        ),
                        "user_seeded_marker_proof": case.user_seeded_marker_proof,
                        "token_strategy": case.token_strategy,
                        "run_id": case.run_id,
                        "sha256": case.sha256,
                        "delivery_identity": _delivery_identity(case),
                        "executed": True,
                        "delivery_ok": receipt.ok,
                        "fanout_top_level_ok": None,
                        "fanout_child_ok": None,
                        "endpoint_readback_ok": target_evidence.endpoint_readback_ok,
                        "content_evidence_found": target_evidence.content_evidence_found,
                        "evidence_kind": target_evidence.evidence_kind,
                        "delivery_payload_visible": target_evidence.delivery_payload_visible,
                        "delivery_payload_core_metadata_check": (
                            target_evidence.delivery_payload_core_metadata_check
                        ),
                        "delivery_locator_found": target_evidence.delivery_locator_found,
                        "structured_state_log_status": target_evidence.structured_state_log_status,
                        "normalized_text_preview_found": (
                            target_evidence.normalized_text_preview_found
                        ),
                        "records_preview_found": target_evidence.records_preview_found,
                        "token_pass": target_evidence.token_pass,
                        "issue": issue,
                        "severity": severity,
                        "note": target_evidence.note,
                        "audit_child_destination": None,
                        "audit_endpoint_readback_ok": None,
                        "audit_content_evidence_found": None,
                        "audit_evidence_kind": None,
                        "audit_token_pass": None,
                        "remote_only_content_evidence_gap": False,
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "destination": destination_id,
                        "flow": flow,
                        "mode": "direct",
                        "validation_mode": "remote_only_direct",
                        "artifacts_pre_dumped": False,
                        "product_direct_claim_safe": False,
                        "user_seeded_marker_proof": False,
                        "token_strategy": "synthetic_local_marker",
                        "run_id": "",
                        "sha256": "",
                        "delivery_identity": "",
                        "executed": False,
                        "delivery_ok": False,
                        "fanout_top_level_ok": None,
                        "fanout_child_ok": None,
                        "endpoint_readback_ok": False,
                        "content_evidence_found": False,
                        "evidence_kind": "missing",
                        "delivery_payload_visible": False,
                        "delivery_payload_core_metadata_check": False,
                        "delivery_locator_found": False,
                        "structured_state_log_status": "not_checked",
                        "normalized_text_preview_found": False,
                        "records_preview_found": False,
                        "token_pass": False,
                        "issue": _sanitize_issue(exc),
                        "severity": "major",
                        "note": "",
                        "audit_child_destination": None,
                        "audit_endpoint_readback_ok": None,
                        "audit_content_evidence_found": None,
                        "audit_evidence_kind": None,
                        "audit_token_pass": None,
                        "remote_only_content_evidence_gap": False,
                    }
                )
    return results


def _apply_remote_only_gap(results: list[dict[str, object]]) -> None:
    by_pair: dict[tuple[str, str], list[dict[str, object]]] = {}
    for item in results:
        pair = (cast(str, item["destination"]), cast(str, item["flow"]))
        by_pair.setdefault(pair, []).append(item)
    for items in by_pair.values():
        artifact_direct = next(
            (item for item in items if item.get("validation_mode") == "artifact_prepared_direct"),
            None,
        )
        fanout = next(
            (item for item in items if item.get("validation_mode") == "fanout_audit"),
            None,
        )
        remote_only = next(
            (item for item in items if item.get("validation_mode") == "remote_only_direct"),
            None,
        )
        if remote_only is None:
            continue
        remote_gap = (
            remote_only.get("executed") is not True
            or remote_only.get("delivery_ok") is not True
            or remote_only.get("content_evidence_found") is not True
            or remote_only.get("token_pass") is not True
            or remote_only.get("delivery_payload_core_metadata_check") is not True
            or remote_only.get("evidence_kind") == "artifact_reference_only_v1"
        )
        if not remote_gap:
            continue
        remote_only["remote_only_content_evidence_gap"] = True
        remote_only["product_direct_claim_safe"] = False
        if cast(str, remote_only["issue"]) in {
            "",
            "content_evidence_missing",
            "token_missing_from_evidence",
            "delivery_payload_core_metadata_missing",
        }:
            remote_only["issue"] = "remote_only_content_evidence_gap"
            remote_only["severity"] = "major"
        if artifact_direct is not None:
            artifact_direct["remote_only_content_evidence_gap"] = True
        if fanout is not None:
            fanout["remote_only_content_evidence_gap"] = True


def build_audit_report(
    *,
    env_file: Path,
    out_root: Path,
    mode: ModeOption,
    flow: FlowName | Literal["both"],
    destinations: list[DestinationId],
    include_remote_only_direct: bool,
) -> dict[str, object]:
    env = _load_env_values(env_file)
    ctx = AuditContext(env=env, out_root=out_root, suffix=uuid.uuid4().hex[:8])
    out_root.mkdir(parents=True, exist_ok=True)

    flows: list[FlowName] = ["uns", "it"] if flow == "both" else [flow]
    modes: list[ModeName]
    if mode == "both":
        modes = ["direct", "fanout"]
    elif mode == "remote-only":
        modes = []
        include_remote_only_direct = True
    else:
        modes = [mode]
    results: list[dict[str, object]] = []
    for destination_id in destinations:
        for flow_name in flows:
            for mode_name in modes:
                results.append(
                    _audit_one_safe(
                        ctx=ctx,
                        destination_id=destination_id,
                        flow=flow_name,
                        mode=mode_name,
                    )
                )
    if include_remote_only_direct:
        results.extend(
            build_remote_only_direct_results(ctx=ctx, flows=flows, destinations=destinations)
        )
    _apply_remote_only_gap(results)

    return {
        "catalog_id": "zephyr.p5.1.destination_evidence_audit.v1",
        "generated_at_utc": current_timestamp_utc(),
        "generated_from": _repo_relative(Path(__file__)),
        "env_file": env_file.as_posix(),
        "out_root": out_root.as_posix(),
        "mode": mode,
        "flow": flow,
        "destinations": list(destinations),
        "secrets_redacted": True,
        "product_direct_note": (
            "artifact_prepared_direct uses pre-dumped artifacts and does not prove "
            "remote-only direct product pass"
        ),
        "summary": {
            "result_count": len(results),
            "executed_count": sum(1 for item in results if item["executed"] is True),
            "delivery_ok_count": sum(1 for item in results if item["delivery_ok"] is True),
            "token_pass_count": sum(1 for item in results if item["token_pass"] is True),
            "remote_only_direct_count": sum(
                1 for item in results if item["validation_mode"] == "remote_only_direct"
            ),
            "remote_only_content_evidence_gap_count": sum(
                1 for item in results if item["remote_only_content_evidence_gap"] is True
            ),
        },
        "results": results,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = _object_dict(report["summary"])
    results = cast(list[dict[str, object]], report["results"])
    lines = [
        "# Zephyr P5.1 Destination Evidence Audit",
        "",
        f"- env_file: `{report['env_file']}`",
        f"- out_root: `{report['out_root']}`",
        f"- mode: `{report['mode']}`",
        f"- flow: `{report['flow']}`",
        f"- result_count: {summary['result_count']}",
        f"- delivery_ok_count: {summary['delivery_ok_count']}",
        f"- token_pass_count: {summary['token_pass_count']}",
        f"- remote_only_direct_count: {summary['remote_only_direct_count']}",
        (
            "- remote_only_content_evidence_gap_count: "
            f"{summary['remote_only_content_evidence_gap_count']}"
        ),
        f"- product_direct_note: {report['product_direct_note']}",
        "",
        (
            "| destination | flow | mode | validation_mode | artifacts_pre_dumped | "
            "product_direct_claim_safe | token_strategy | user_seeded_marker_proof | "
            "executed | delivery_ok | endpoint_readback_ok | "
            "delivery_payload_core_metadata_check | "
            "evidence_kind | token_pass | issue | severity | "
            "remote_only_content_evidence_gap |"
        ),
        (
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | "
            "--- | --- | --- | --- | --- | --- |"
        ),
    ]
    for item in results:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(item["destination"]),
                    str(item["flow"]),
                    str(item["mode"]),
                    str(item["validation_mode"]),
                    str(item["artifacts_pre_dumped"]),
                    str(item["product_direct_claim_safe"]),
                    str(item["token_strategy"]),
                    str(item["user_seeded_marker_proof"]),
                    str(item["executed"]),
                    str(item["delivery_ok"]),
                    str(item["endpoint_readback_ok"]),
                    str(item["delivery_payload_core_metadata_check"]),
                    str(item["evidence_kind"]),
                    str(item["token_pass"]),
                    str(item["issue"]),
                    str(item["severity"]),
                    str(item["remote_only_content_evidence_gap"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _resolve_destinations(values: list[str] | None) -> list[DestinationId]:
    if not values:
        return list(ALL_DESTINATIONS)
    resolved: list[DestinationId] = []
    for raw_value in values:
        normalized = DESTINATION_ALIASES.get(raw_value)
        if normalized is None:
            raise ValueError(f"Unsupported destination id: {raw_value}")
        if normalized not in resolved:
            resolved.append(normalized)
    return resolved


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="p5_destination_evidence_audit")
    parser.add_argument("--env-file", type=Path, required=True, help="External P45 env file.")
    parser.add_argument("--out-root", type=Path, required=True, help="Audit workspace root.")
    parser.add_argument(
        "--mode",
        choices=("direct", "fanout", "both", "remote-only"),
        default="both",
        help="Audit artifact-prepared direct, fanout audit delivery, remote-only direct, or both.",
    )
    parser.add_argument(
        "--flow",
        choices=("uns", "it", "both"),
        default="both",
        help="Audit uns flow, it flow, or both.",
    )
    parser.add_argument(
        "--include-remote-only-direct",
        action="store_true",
        help="Also run bounded remote-only direct checks for Kafka and OpenSearch.",
    )
    parser.add_argument(
        "--dest",
        action="append",
        default=None,
        help="Destination id. May be repeated. Defaults to all retained destinations.",
    )
    render_group = parser.add_mutually_exclusive_group()
    render_group.add_argument("--json", action="store_true", help="Render JSON output.")
    render_group.add_argument("--markdown", action="store_true", help="Render markdown output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.env_file.exists() or not args.env_file.is_file():
        raise FileNotFoundError(f"Env file does not exist: {args.env_file}")
    destinations = _resolve_destinations(cast(list[str] | None, args.dest))
    report = build_audit_report(
        env_file=args.env_file,
        out_root=args.out_root,
        mode=cast(ModeOption, args.mode),
        flow=cast(FlowName | Literal["both"], args.flow),
        destinations=destinations,
        include_remote_only_direct=args.include_remote_only_direct,
    )
    if args.markdown:
        print(render_markdown(report), end="")
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
