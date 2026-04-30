from __future__ import annotations

import argparse
import json
import threading
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Literal, cast
from urllib.parse import parse_qs, urlsplit

from p5_connector_readiness import build_connector_readiness
from p5_destination_evidence_audit import (
    DESTINATION_ALIASES,
    AuditContext,
    EndpointEvidence,
    FlowName,
    ModeName,
    PreparedCase,
    _build_audit_destination,
    _build_primary_destination,
    _build_run_meta,
    _child_ok,
    _find_child_receipt,
    _load_env_values,
    _readback_audit_child_evidence,
    _readback_target_evidence,
    build_remote_only_direct_results,
)

from it_stream import normalize_it_input_identity_sha
from it_stream.service import process_file as process_it_file
from it_stream.tests._p45_source_wave1_helpers import (
    clickhouse_url,
    postgresql_dsn,
    reset_clickhouse_cursor_table,
    reset_postgresql_cursor_table,
    write_clickhouse_source_spec,
    write_postgresql_source_spec,
)
from it_stream.tests._p45_source_wave1_helpers import (
    write_http_source_spec as write_it_http_source_spec,
)
from it_stream.tests._p45_source_wave2_helpers import (
    create_kafka_topic,
    kafka_bootstrap_servers,
    make_kafka_topic_name,
    mongodb_uri,
    produce_kafka_messages,
    reset_mongodb_collection,
    write_kafka_source_spec,
    write_mongodb_source_spec,
)
from uns_stream.sources import normalize_uns_input_identity_sha
from uns_stream.sources import process_file as process_uns_file
from uns_stream.tests._p45_source_wave3_helpers import (
    http_fixture_base,
    init_git_text_repo,
    s3_access_key,
    s3_bucket,
    s3_endpoint,
    s3_region,
    s3_secret_key,
    seed_minio_text_object,
    write_git_source_spec,
    write_s3_source_spec,
)
from uns_stream.tests._p45_source_wave3_helpers import (
    write_http_source_spec as write_uns_http_source_spec,
)
from uns_stream.tests.test_p45_source_wave4_saas_live import (
    write_confluence_source_spec,
    write_google_drive_source_spec,
)
from zephyr_core import PartitionStrategy, RunContext, ZephyrError
from zephyr_ingest._internal.artifacts import dump_partition_artifacts
from zephyr_ingest._internal.utils import sha256_file
from zephyr_ingest.destinations.fanout import FanoutDestination
from zephyr_ingest.testing.p45 import LoadedP45Env
from zephyr_ingest.tests._p45_destination_wave2_helpers import current_timestamp_utc

SourceId = Literal[
    "source.uns.http_document.v1",
    "source.uns.s3_document.v1",
    "source.uns.git_document.v1",
    "source.uns.google_drive_document.v1",
    "source.uns.confluence_document.v1",
    "source.it.http_json_cursor.v1",
    "source.it.postgresql_incremental.v1",
    "source.it.clickhouse_incremental.v1",
    "source.it.kafka_partition_offset.v1",
    "source.it.mongodb_incremental.v1",
]
CatalogDestinationId = Literal[
    "filesystem",
    "destination.webhook.v1",
    "destination.kafka.v1",
    "destination.weaviate.v1",
    "destination.s3.v1",
    "destination.opensearch.v1",
    "destination.clickhouse.v1",
    "destination.mongodb.v1",
    "destination.loki.v1",
    "destination.sqlite.v1",
]
RouteSetName = Literal["representative"]
ReadinessStatus = Literal["pass", "blocked"]
Severity = Literal["none", "major"]
TokenStrategy = Literal[
    "seeded_source_marker",
    "derived_from_source_result",
    "synthetic_local_marker",
]
ShortDestinationId = Literal[
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


@dataclass(frozen=True, slots=True)
class RepresentativeRoute:
    source_id: SourceId
    destination_id: CatalogDestinationId
    flow: FlowName


@dataclass(frozen=True, slots=True)
class PreparedRouteCase:
    case: PreparedCase
    source_id: SourceId
    destination_id: CatalogDestinationId
    token_strategy: TokenStrategy


REPRESENTATIVE_ROUTES: tuple[RepresentativeRoute, ...] = (
    RepresentativeRoute("source.uns.http_document.v1", "filesystem", "uns"),
    RepresentativeRoute("source.uns.s3_document.v1", "destination.s3.v1", "uns"),
    RepresentativeRoute("source.uns.git_document.v1", "destination.sqlite.v1", "uns"),
    RepresentativeRoute("source.uns.google_drive_document.v1", "destination.webhook.v1", "uns"),
    RepresentativeRoute("source.uns.confluence_document.v1", "destination.opensearch.v1", "uns"),
    RepresentativeRoute("source.it.http_json_cursor.v1", "destination.kafka.v1", "it"),
    RepresentativeRoute(
        "source.it.postgresql_incremental.v1",
        "destination.clickhouse.v1",
        "it",
    ),
    RepresentativeRoute(
        "source.it.clickhouse_incremental.v1",
        "destination.loki.v1",
        "it",
    ),
    RepresentativeRoute(
        "source.it.kafka_partition_offset.v1",
        "destination.mongodb.v1",
        "it",
    ),
    RepresentativeRoute(
        "source.it.mongodb_incremental.v1",
        "destination.weaviate.v1",
        "it",
    ),
)


def _normalize_destination_id(destination_id: CatalogDestinationId) -> ShortDestinationId:
    return cast(ShortDestinationId, DESTINATION_ALIASES[destination_id])


def _slug(value: str) -> str:
    return value.replace(".", "_").replace("-", "_")


def _compact_text(value: str) -> str:
    return " ".join(value.split())


def _derive_token_from_normalized_text(normalized_text: str) -> str:
    compact = _compact_text(normalized_text)
    if compact == "":
        raise ValueError("normalized_text is empty; cannot derive token")
    return compact[:120]


def _retryable_process_uns(*, filename: str, sha256: str, attempts: int = 3) -> object:
    last_error: ZephyrError | None = None
    for _ in range(attempts):
        try:
            return process_uns_file(
                filename=filename,
                strategy=PartitionStrategy.AUTO,
                sha256=sha256,
            )
        except ZephyrError as err:
            details = err.details
            if details is None or details.get("retryable") is not True:
                raise
            last_error = err
    assert last_error is not None
    raise last_error


class _UnsHttpMarkerServer(AbstractContextManager["_UnsHttpMarkerServer"]):
    def __init__(self, *, token: str) -> None:
        self._token = token
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._build_handler())
        self.base_url = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def _build_handler(self) -> type[BaseHTTPRequestHandler]:
        token = self._token

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path != "/document.txt":
                    self.send_error(404)
                    return
                body = f"Zephyr P5.1 UNS HTTP marker {token}\n".encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Disposition", 'attachment; filename="document.txt"')
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                return None

        return Handler

    def __enter__(self) -> _UnsHttpMarkerServer:
        self._thread.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)
        return None


class _ItHttpCursorServer(AbstractContextManager["_ItHttpCursorServer"]):
    def __init__(self, *, token: str) -> None:
        self._token = token
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._build_handler())
        self.base_url = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def _build_handler(self) -> type[BaseHTTPRequestHandler]:
        token = self._token

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                split = urlsplit(self.path)
                if split.path != "/records/cursor":
                    self.send_error(404)
                    return
                query = parse_qs(split.query)
                cursor_values = query.get("cursor")
                cursor = cursor_values[0] if cursor_values else None
                if cursor is None:
                    payload = {
                        "records": [
                            {"id": 1, "marker": token, "message": f"IT {token}"},
                        ],
                        "next_cursor": "page-2",
                    }
                elif cursor == "page-2":
                    payload = {
                        "records": [
                            {"id": 2, "kind": "secondary", "nested": {"marker": token}},
                        ],
                        "next_cursor": None,
                    }
                else:
                    payload = {"records": [], "next_cursor": None}
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                return None

        return Handler

    def __enter__(self) -> _ItHttpCursorServer:
        self._thread.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)
        return None


def _make_run_meta_from_result(*, result: object, run_id: str) -> object:
    ctx = RunContext.new(
        pipeline_version="p5.1.nm_representative_smoke",
        run_id=run_id,
        timestamp_utc=current_timestamp_utc(),
    )
    result_obj = cast(object, result)
    result_document = getattr(result_obj, "document")
    result_engine = getattr(result_obj, "engine")
    normalized_text = getattr(result_obj, "normalized_text")
    elements = getattr(result_obj, "elements")
    assert isinstance(normalized_text, str)
    assert isinstance(elements, list)
    return _build_run_meta(
        ctx=ctx,
        result_document=result_document,
        result_engine=result_engine,
        normalized_text_len=len(normalized_text),
        elements_count=len(elements),
    )


def _build_prepared_case(
    *,
    source_id: SourceId,
    destination_id: CatalogDestinationId,
    mode: ModeName,
    flow: FlowName,
    token: str,
    sha256: str,
    result: object,
    case_root: Path,
    token_strategy: TokenStrategy,
) -> PreparedRouteCase:
    run_id = (
        f"p51_nm_{_slug(flow)}_{mode}_{_slug(source_id)}_"
        f"{_slug(destination_id)}_{uuid.uuid4().hex[:8]}"
    )
    meta = cast(object, _make_run_meta_from_result(result=result, run_id=run_id))
    out_root = case_root / "out"
    dump_partition_artifacts(
        out_root=out_root,
        sha256=sha256,
        meta=cast(object, meta),
        result=cast(object, result),
    )
    return PreparedRouteCase(
        case=PreparedCase(
            flow=flow,
            mode=mode,
            validation_mode="artifact_prepared_direct" if mode == "direct" else "fanout_audit",
            destination=_normalize_destination_id(destination_id),
            token=token,
            sha256=sha256,
            run_id=run_id,
            out_root=out_root,
            meta=cast(object, meta),
            result=cast(object, result),
            artifacts_pre_dumped=True,
            token_strategy=token_strategy,
            user_seeded_marker_proof=token_strategy == "seeded_source_marker",
        ),
        source_id=source_id,
        destination_id=destination_id,
        token_strategy=token_strategy,
    )


def _prepare_uns_http_case(
    *,
    env: LoadedP45Env,
    source_id: SourceId,
    destination_id: CatalogDestinationId,
    mode: ModeName,
    case_root: Path,
) -> PreparedRouteCase:
    _ = http_fixture_base(env)
    token = f"zephyr_p5_1_uns_http_{uuid.uuid4().hex}"
    spec_path = case_root / "http_document.json"
    with _UnsHttpMarkerServer(token=token) as server:
        write_uns_http_source_spec(
            spec_path,
            url=f"{server.base_url}/document.txt",
            accept="text/plain",
            timeout_s=5.0,
        )
        sha256 = normalize_uns_input_identity_sha(
            filename=str(spec_path),
            default_sha="p51-uns-http-fallback",
        )
        result = process_uns_file(
            filename=str(spec_path),
            strategy=PartitionStrategy.AUTO,
            sha256=sha256,
        )
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="uns",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="seeded_source_marker",
    )


def _prepare_uns_s3_case(
    *,
    env: LoadedP45Env,
    source_id: SourceId,
    destination_id: CatalogDestinationId,
    mode: ModeName,
    case_root: Path,
) -> PreparedRouteCase:
    token = f"zephyr_p5_1_uns_s3_{uuid.uuid4().hex}"
    object_key = f"p5_1_nm_representative/uns/{mode}/{uuid.uuid4().hex}/document.txt"
    seed_minio_text_object(
        env,
        bucket=s3_bucket(env),
        key=object_key,
        body=f"Zephyr P5.1 UNS S3 marker {token}\n",
    )
    spec_path = case_root / "s3_document.json"
    write_s3_source_spec(
        spec_path,
        bucket=s3_bucket(env),
        key=object_key,
        region=s3_region(env),
        access_key=s3_access_key(env),
        secret_key=s3_secret_key(env),
        endpoint_url=s3_endpoint(env),
        session_token=env.get("ZEPHYR_P45_S3_SESSION_TOKEN"),
    )
    sha256 = normalize_uns_input_identity_sha(filename=str(spec_path), default_sha="p51-uns-s3")
    result = _retryable_process_uns(filename=str(spec_path), sha256=sha256)
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="uns",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="seeded_source_marker",
    )


def _prepare_uns_git_case(
    *, source_id: SourceId, destination_id: CatalogDestinationId, mode: ModeName, case_root: Path
) -> PreparedRouteCase:
    token = f"zephyr_p5_1_uns_git_{uuid.uuid4().hex}"
    repo_root = (case_root / "git_repo").resolve()
    relative_path = "docs/report.txt"
    commit = init_git_text_repo(
        repo_root,
        relative_path=relative_path,
        content=f"Zephyr P5.1 UNS git marker {token}\n",
    )
    spec_path = case_root / "git_document.json"
    write_git_source_spec(
        spec_path,
        repo_root=str(repo_root),
        commit=commit,
        relative_path=relative_path,
    )
    sha256 = normalize_uns_input_identity_sha(filename=str(spec_path), default_sha="p51-uns-git")
    result = process_uns_file(
        filename=str(spec_path),
        strategy=PartitionStrategy.AUTO,
        sha256=sha256,
    )
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="uns",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="seeded_source_marker",
    )


def _prepare_uns_google_drive_case(
    *,
    env: LoadedP45Env,
    source_id: SourceId,
    destination_id: CatalogDestinationId,
    mode: ModeName,
    case_root: Path,
) -> PreparedRouteCase:
    spec_path = case_root / "google_drive_document.json"
    export_mime_type = env.get("ZEPHYR_P45_GOOGLE_DRIVE_EXPORT_MIME_TYPE")
    acquisition_mode = "export" if export_mime_type is not None else "download"
    write_google_drive_source_spec(
        spec_path,
        file_id=env.require("ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID"),
        access_token=env.require("ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN"),
        acquisition_mode=acquisition_mode,
        export_mime_type=export_mime_type,
        drive_id=env.get("ZEPHYR_P45_GOOGLE_DRIVE_DRIVE_ID"),
        timeout_s=10.0,
    )
    sha256 = normalize_uns_input_identity_sha(
        filename=str(spec_path),
        default_sha="p51-uns-google-drive",
    )
    result = _retryable_process_uns(filename=str(spec_path), sha256=sha256)
    token = _derive_token_from_normalized_text(cast(str, getattr(result, "normalized_text")))
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="uns",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="derived_from_source_result",
    )


def _prepare_uns_confluence_case(
    *,
    env: LoadedP45Env,
    source_id: SourceId,
    destination_id: CatalogDestinationId,
    mode: ModeName,
    case_root: Path,
) -> PreparedRouteCase:
    spec_path = case_root / "confluence_document.json"
    raw_page_version = env.get("ZEPHYR_P45_CONFLUENCE_PAGE_VERSION")
    page_version = None if raw_page_version is None else int(raw_page_version)
    write_confluence_source_spec(
        spec_path,
        site_url=env.require("ZEPHYR_P45_CONFLUENCE_SITE_URL"),
        page_id=env.require("ZEPHYR_P45_CONFLUENCE_PAGE_ID"),
        access_token=env.require("ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN"),
        email=env.get("ZEPHYR_P45_CONFLUENCE_EMAIL"),
        space_key=env.get("ZEPHYR_P45_CONFLUENCE_SPACE_KEY"),
        page_version=page_version,
        timeout_s=10.0,
    )
    sha256 = normalize_uns_input_identity_sha(
        filename=str(spec_path),
        default_sha="p51-uns-confluence",
    )
    result = _retryable_process_uns(filename=str(spec_path), sha256=sha256)
    token = _derive_token_from_normalized_text(cast(str, getattr(result, "normalized_text")))
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="uns",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="derived_from_source_result",
    )


def _prepare_it_http_case(
    *, source_id: SourceId, destination_id: CatalogDestinationId, mode: ModeName, case_root: Path
) -> PreparedRouteCase:
    token = f"zephyr_p5_1_it_http_{uuid.uuid4().hex}"
    spec_path = case_root / "http_json_cursor.json"
    with _ItHttpCursorServer(token=token) as server:
        write_it_http_source_spec(
            spec_path,
            stream="p5-1-http-json-cursor",
            url=f"{server.base_url}/records/cursor",
            query={"limit": "2"},
        )
        sha256 = normalize_it_input_identity_sha(
            filename=str(spec_path),
            default_sha=sha256_file(spec_path),
        )
        result = process_it_file(
            filename=str(spec_path),
            strategy=PartitionStrategy.AUTO,
            sha256=sha256,
            size_bytes=spec_path.stat().st_size,
        )
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="it",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="seeded_source_marker",
    )


def _prepare_it_postgresql_case(
    *,
    env: LoadedP45Env,
    source_id: SourceId,
    destination_id: CatalogDestinationId,
    mode: ModeName,
    case_root: Path,
) -> PreparedRouteCase:
    token = f"zephyr_p5_1_it_pg_{uuid.uuid4().hex}"
    table_name = f"p51_nm_pg_{uuid.uuid4().hex[:8]}"
    reset_postgresql_cursor_table(
        env,
        table=table_name,
        rows=(
            ("cust-1", "cust-0001", token),
            ("cust-2", "cust-0002", f"{token}-two"),
        ),
    )
    spec_path = case_root / "postgresql_incremental.json"
    write_postgresql_source_spec(
        spec_path,
        stream="p5-1-postgresql",
        connection_name="warehouse-primary",
        dsn=postgresql_dsn(env),
        schema="public",
        table=table_name,
        columns=("customer_id", "doc_cursor", "name"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
        batch_size=2,
    )
    sha256 = normalize_it_input_identity_sha(
        filename=str(spec_path),
        default_sha=sha256_file(spec_path),
    )
    result = process_it_file(
        filename=str(spec_path),
        strategy=PartitionStrategy.AUTO,
        sha256=sha256,
        size_bytes=spec_path.stat().st_size,
    )
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="it",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="seeded_source_marker",
    )


def _prepare_it_clickhouse_case(
    *,
    env: LoadedP45Env,
    source_id: SourceId,
    destination_id: CatalogDestinationId,
    mode: ModeName,
    case_root: Path,
) -> PreparedRouteCase:
    token = f"zephyr_p5_1_it_ch_{uuid.uuid4().hex}"
    table_name = f"p51_nm_ch_{uuid.uuid4().hex[:8]}"
    reset_clickhouse_cursor_table(
        env,
        table=table_name,
        rows=(
            ("cust-1", "cust-0001", token),
            ("cust-2", "cust-0002", f"{token}-two"),
        ),
    )
    spec_path = case_root / "clickhouse_incremental.json"
    write_clickhouse_source_spec(
        spec_path,
        stream="p5-1-clickhouse",
        connection_name="warehouse-clickhouse",
        url=clickhouse_url(env),
        database=env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE"),
        table=table_name,
        columns=("customer_id", "doc_cursor", "segment"),
        cursor_column="doc_cursor",
        cursor_start="cust-0000",
        username=env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME"),
        password=env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD"),
        batch_size=2,
    )
    sha256 = normalize_it_input_identity_sha(
        filename=str(spec_path),
        default_sha=sha256_file(spec_path),
    )
    result = process_it_file(
        filename=str(spec_path),
        strategy=PartitionStrategy.AUTO,
        sha256=sha256,
        size_bytes=spec_path.stat().st_size,
    )
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="it",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="seeded_source_marker",
    )


def _prepare_it_kafka_case(
    *,
    env: LoadedP45Env,
    source_id: SourceId,
    destination_id: CatalogDestinationId,
    mode: ModeName,
    case_root: Path,
) -> PreparedRouteCase:
    token = f"zephyr_p5_1_it_kafka_{uuid.uuid4().hex}"
    topic = make_kafka_topic_name(prefix="p51-nm-kafka", suffix=uuid.uuid4().hex[:8])
    create_kafka_topic(env=env, topic=topic)
    produce_kafka_messages(
        env=env,
        topic=topic,
        partition=0,
        values=(
            {"order_id": "ord-1", "marker": token},
            {"order_id": "ord-2", "nested": {"marker": token}},
        ),
    )
    spec_path = case_root / "kafka_partition_offset.json"
    write_kafka_source_spec(
        spec_path,
        stream="p5-1-kafka",
        connection_name="events-primary",
        brokers=kafka_bootstrap_servers(env),
        topic=topic,
        partition=0,
        offset_start=None,
        batch_size=2,
    )
    sha256 = normalize_it_input_identity_sha(
        filename=str(spec_path),
        default_sha=sha256_file(spec_path),
    )
    result = process_it_file(
        filename=str(spec_path),
        strategy=PartitionStrategy.AUTO,
        sha256=sha256,
        size_bytes=spec_path.stat().st_size,
    )
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="it",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="seeded_source_marker",
    )


def _prepare_it_mongodb_case(
    *,
    env: LoadedP45Env,
    source_id: SourceId,
    destination_id: CatalogDestinationId,
    mode: ModeName,
    case_root: Path,
) -> PreparedRouteCase:
    token = f"zephyr_p5_1_it_mongo_{uuid.uuid4().hex}"
    database = env.require("ZEPHYR_P45_MONGODB_DATABASE")
    collection = f"p51_nm_docs_{uuid.uuid4().hex[:8]}"
    reset_mongodb_collection(
        env,
        database=database,
        collection=collection,
        documents=(
            {"doc_id": "cust-1", "doc_cursor": "cust-0001", "segment": token},
            {"doc_id": "cust-2", "doc_cursor": "cust-0002", "segment": f"{token}-two"},
        ),
    )
    spec_path = case_root / "mongodb_incremental.json"
    write_mongodb_source_spec(
        spec_path,
        stream="p5-1-mongodb",
        connection_name="mongo-primary",
        uri=mongodb_uri(env),
        database=database,
        collection=collection,
        fields=("doc_id", "doc_cursor", "segment"),
        cursor_field="doc_cursor",
        cursor_start=None,
        batch_size=2,
    )
    sha256 = normalize_it_input_identity_sha(
        filename=str(spec_path),
        default_sha=sha256_file(spec_path),
    )
    result = process_it_file(
        filename=str(spec_path),
        strategy=PartitionStrategy.AUTO,
        sha256=sha256,
        size_bytes=spec_path.stat().st_size,
    )
    return _build_prepared_case(
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        flow="it",
        token=token,
        sha256=sha256,
        result=result,
        case_root=case_root,
        token_strategy="seeded_source_marker",
    )


def _prepare_source_case(
    *,
    env: LoadedP45Env,
    route: RepresentativeRoute,
    mode: ModeName,
    case_root: Path,
) -> PreparedRouteCase:
    source_id = route.source_id
    destination_id = route.destination_id
    if source_id == "source.uns.http_document.v1":
        return _prepare_uns_http_case(
            env=env,
            source_id=source_id,
            destination_id=destination_id,
            mode=mode,
            case_root=case_root,
        )
    if source_id == "source.uns.s3_document.v1":
        return _prepare_uns_s3_case(
            env=env,
            source_id=source_id,
            destination_id=destination_id,
            mode=mode,
            case_root=case_root,
        )
    if source_id == "source.uns.git_document.v1":
        return _prepare_uns_git_case(
            source_id=source_id,
            destination_id=destination_id,
            mode=mode,
            case_root=case_root,
        )
    if source_id == "source.uns.google_drive_document.v1":
        return _prepare_uns_google_drive_case(
            env=env,
            source_id=source_id,
            destination_id=destination_id,
            mode=mode,
            case_root=case_root,
        )
    if source_id == "source.uns.confluence_document.v1":
        return _prepare_uns_confluence_case(
            env=env,
            source_id=source_id,
            destination_id=destination_id,
            mode=mode,
            case_root=case_root,
        )
    if source_id == "source.it.http_json_cursor.v1":
        return _prepare_it_http_case(
            source_id=source_id,
            destination_id=destination_id,
            mode=mode,
            case_root=case_root,
        )
    if source_id == "source.it.postgresql_incremental.v1":
        return _prepare_it_postgresql_case(
            env=env,
            source_id=source_id,
            destination_id=destination_id,
            mode=mode,
            case_root=case_root,
        )
    if source_id == "source.it.clickhouse_incremental.v1":
        return _prepare_it_clickhouse_case(
            env=env,
            source_id=source_id,
            destination_id=destination_id,
            mode=mode,
            case_root=case_root,
        )
    if source_id == "source.it.kafka_partition_offset.v1":
        return _prepare_it_kafka_case(
            env=env,
            source_id=source_id,
            destination_id=destination_id,
            mode=mode,
            case_root=case_root,
        )
    return _prepare_it_mongodb_case(
        env=env,
        source_id=source_id,
        destination_id=destination_id,
        mode=mode,
        case_root=case_root,
    )


def _readiness_map(env_file: Path) -> dict[str, dict[str, object]]:
    readiness = build_connector_readiness(env_file=env_file, live=False)
    connectors_obj = readiness["connectors"]
    assert isinstance(connectors_obj, list)
    return {
        cast(str, cast(dict[str, object], item)["id"]): cast(dict[str, object], item)
        for item in connectors_obj
    }


def _route_readiness(
    *,
    readiness_map: dict[str, dict[str, object]],
    route: RepresentativeRoute,
) -> tuple[ReadinessStatus, str]:
    source_record = readiness_map[route.source_id]
    destination_record = readiness_map[route.destination_id]
    source_status = cast(str, source_record["status"])
    destination_status = cast(str, destination_record["status"])
    if source_status == "pass" and destination_status == "pass":
        return ("pass", "pass_ready")
    source_reason = cast(str, source_record["reason"])
    destination_reason = cast(str, destination_record["reason"])
    return (
        "blocked",
        f"source={source_status}:{source_reason};destination={destination_status}:{destination_reason}",
    )


def _blocked_result(
    *,
    route: RepresentativeRoute,
    mode: ModeName,
    readiness_detail: str,
) -> dict[str, object]:
    token_strategy: TokenStrategy = (
        "derived_from_source_result"
        if route.source_id
        in {"source.uns.google_drive_document.v1", "source.uns.confluence_document.v1"}
        else "seeded_source_marker"
    )
    return {
        "source_id": route.source_id,
        "destination_id": route.destination_id,
        "flow": route.flow,
        "mode": mode,
        "validation_mode": "artifact_prepared_direct" if mode == "direct" else "fanout_audit",
        "artifacts_pre_dumped": True,
        "product_direct_claim_safe": False,
        "user_seeded_marker_proof": token_strategy == "seeded_source_marker",
        "token_strategy": token_strategy,
        "route_set": "representative",
        "readiness_status": "blocked",
        "executed": False,
        "stats": {"total": 0, "success": 0, "failed": 0},
        "delivery_ok": False,
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
        "overall_pass": False,
        "issue": f"blocked_by_readiness:{readiness_detail}",
        "severity": "major",
        "remote_only_content_evidence_gap": False,
        "fanout_top_level_ok": None,
        "fanout_child_ok": None,
        "audit_child_destination": None,
        "audit_evidence_kind": None,
        "audit_token_pass": None,
    }


def _route_issue_for_fanout(
    *,
    base_issue: str,
    fanout_top_level_ok: bool | None,
    audit_evidence: EndpointEvidence | None,
) -> tuple[str, Severity]:
    if base_issue != "":
        return (base_issue, "major")
    if fanout_top_level_ok is not True:
        return ("fanout_top_level_failed", "major")
    if audit_evidence is None:
        return ("fanout_audit_missing", "major")
    if not audit_evidence.endpoint_readback_ok:
        return ("fanout_audit_readback_failed", "major")
    if not audit_evidence.content_evidence_found:
        return ("fanout_audit_content_evidence_missing", "major")
    if not audit_evidence.token_pass:
        return ("fanout_audit_token_missing", "major")
    return ("", "none")


def _run_one_route(
    *,
    ctx: AuditContext,
    readiness_map: dict[str, dict[str, object]],
    route: RepresentativeRoute,
    mode: ModeName,
) -> dict[str, object]:
    readiness_status, readiness_detail = _route_readiness(
        readiness_map=readiness_map,
        route=route,
    )
    if readiness_status != "pass":
        return _blocked_result(route=route, mode=mode, readiness_detail=readiness_detail)

    case_root = ctx.out_root / route.flow / mode / _slug(route.source_id)
    case_root.mkdir(parents=True, exist_ok=True)
    try:
        prepared = _prepare_source_case(
            env=ctx.env,
            route=route,
            mode=mode,
            case_root=case_root,
        )
        primary_destination = _build_primary_destination(
            destination_id=prepared.case.destination,
            ctx=ctx,
            case=prepared.case,
        )
        target_receipt_ok = False
        fanout_top_level_ok: bool | None = None
        fanout_child_ok: bool | None = None
        audit_destination_name: str | None = None
        audit_evidence: EndpointEvidence | None = None

        if mode == "direct":
            receipt = primary_destination(
                out_root=prepared.case.out_root,
                sha256=prepared.case.sha256,
                meta=prepared.case.meta,
                result=prepared.case.result,
            )
            target_receipt_ok = receipt.ok
        else:
            audit_destination, audit_destination_name = _build_audit_destination(case=prepared.case)
            fanout = FanoutDestination(destinations=(primary_destination, audit_destination))
            receipt = fanout(
                out_root=prepared.case.out_root,
                sha256=prepared.case.sha256,
                meta=prepared.case.meta,
                result=prepared.case.result,
            )
            fanout_top_level_ok = receipt.ok
            primary_child = _find_child_receipt(receipt, destination_name=primary_destination.name)
            audit_child = _find_child_receipt(receipt, destination_name=audit_destination_name)
            fanout_child_ok = _child_ok(primary_child)
            target_receipt_ok = fanout_child_ok is True
            if audit_destination_name is not None and _child_ok(audit_child):
                audit_evidence = _readback_audit_child_evidence(
                    audit_destination_name=audit_destination_name,
                    case=prepared.case,
                )

        target_evidence = _readback_target_evidence(
            destination_id=prepared.case.destination,
            destination=primary_destination,
            ctx=ctx,
            case=prepared.case,
        )
        base_issue = ""
        severity: Severity = "none"
        if not target_receipt_ok:
            base_issue = "delivery_failed"
            severity = "major"
        elif not target_evidence.endpoint_readback_ok:
            base_issue = "endpoint_readback_failed"
            severity = "major"
        elif not target_evidence.content_evidence_found:
            base_issue = "content_evidence_missing"
            severity = "major"
        elif not target_evidence.token_pass:
            base_issue = "token_missing_from_evidence"
            severity = "major"
        if mode == "fanout":
            issue, severity = _route_issue_for_fanout(
                base_issue=base_issue,
                fanout_top_level_ok=fanout_top_level_ok,
                audit_evidence=audit_evidence,
            )
        else:
            issue = base_issue
        success = issue == ""
        return {
            "source_id": route.source_id,
            "destination_id": route.destination_id,
            "flow": route.flow,
            "mode": mode,
            "validation_mode": prepared.case.validation_mode,
            "artifacts_pre_dumped": prepared.case.artifacts_pre_dumped,
            "product_direct_claim_safe": False,
            "user_seeded_marker_proof": prepared.case.user_seeded_marker_proof,
            "token_strategy": prepared.token_strategy,
            "route_set": "representative",
            "readiness_status": readiness_status,
            "executed": True,
            "stats": {"total": 1, "success": 1 if success else 0, "failed": 0 if success else 1},
            "delivery_ok": target_receipt_ok,
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
            "overall_pass": success,
            "issue": issue,
            "severity": severity,
            "remote_only_content_evidence_gap": False,
            "fanout_top_level_ok": fanout_top_level_ok,
            "fanout_child_ok": fanout_child_ok,
            "audit_child_destination": audit_destination_name,
            "audit_evidence_kind": None if audit_evidence is None else audit_evidence.evidence_kind,
            "audit_token_pass": None if audit_evidence is None else audit_evidence.token_pass,
        }
    except Exception as exc:
        return {
            "source_id": route.source_id,
            "destination_id": route.destination_id,
            "flow": route.flow,
            "mode": mode,
            "validation_mode": "artifact_prepared_direct" if mode == "direct" else "fanout_audit",
            "artifacts_pre_dumped": True,
            "product_direct_claim_safe": False,
            "user_seeded_marker_proof": False,
            "token_strategy": None,
            "route_set": "representative",
            "readiness_status": readiness_status,
            "executed": False,
            "stats": {"total": 1, "success": 0, "failed": 1},
            "delivery_ok": False,
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
            "overall_pass": False,
            "issue": type(exc).__name__,
            "severity": "major",
            "remote_only_content_evidence_gap": False,
            "fanout_top_level_ok": None,
            "fanout_child_ok": None,
            "audit_child_destination": None,
            "audit_evidence_kind": None,
            "audit_token_pass": None,
        }


def _apply_remote_only_gap(results: list[dict[str, object]]) -> None:
    for item in results:
        if item.get("validation_mode") != "remote_only_direct":
            continue
        remote_gap = (
            item.get("executed") is not True
            or item.get("delivery_ok") is not True
            or item.get("content_evidence_found") is not True
            or item.get("token_pass") is not True
            or item.get("delivery_payload_core_metadata_check") is not True
            or item.get("evidence_kind") == "artifact_reference_only_v1"
        )
        if remote_gap:
            item["remote_only_content_evidence_gap"] = True
            if cast(str, item["issue"]) in {
                "",
                "content_evidence_missing",
                "token_missing_from_evidence",
                "delivery_failed",
            }:
                item["issue"] = "remote_only_content_evidence_gap"
                item["severity"] = "major"
    by_pair: dict[tuple[str, str], dict[str, dict[str, object]]] = {}
    for item in results:
        pair = (cast(str, item["source_id"]), cast(str, item["destination_id"]))
        pair_map = by_pair.setdefault(pair, {})
        pair_map[cast(str, item["mode"])] = item
    for pair_map in by_pair.values():
        direct = pair_map.get("direct")
        fanout = pair_map.get("fanout")
        if direct is None or fanout is None:
            continue
        if direct.get("executed") is not True or fanout.get("executed") is not True:
            continue
        if direct.get("validation_mode") != "artifact_prepared_direct":
            continue
        direct_kind = direct.get("evidence_kind")
        fanout_kind = fanout.get("audit_evidence_kind")
        if (
            direct_kind == "artifact_reference_only_v1"
            and isinstance(fanout_kind, str)
            and fanout_kind
            in {"normalized_text_preview_v1", "normalized_text_and_records_preview_v1"}
        ):
            direct["remote_only_content_evidence_gap"] = True
            fanout["remote_only_content_evidence_gap"] = True
            direct_failed = direct.get("overall_pass") is not True
            if fanout.get("overall_pass") is True and direct_failed:
                direct["issue"] = "remote_only_content_evidence_gap"
                direct["severity"] = "major"


def _remote_only_destination_to_catalog_id(destination_id: str) -> str:
    if destination_id == "kafka":
        return "destination.kafka.v1"
    if destination_id == "opensearch":
        return "destination.opensearch.v1"
    raise ValueError(f"Unsupported remote-only destination id: {destination_id}")


def _remote_only_source_id(flow: FlowName) -> str:
    if flow == "uns":
        return "synthetic.uns.local_text"
    return "synthetic.it.local_json"


def _remote_only_route_results(*, ctx: AuditContext) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for item in build_remote_only_direct_results(ctx=ctx):
        issue = cast(str, item["issue"])
        results.append(
            {
                "source_id": _remote_only_source_id(cast(FlowName, item["flow"])),
                "destination_id": _remote_only_destination_to_catalog_id(
                    cast(str, item["destination"])
                ),
                "flow": item["flow"],
                "mode": item["mode"],
                "validation_mode": item["validation_mode"],
                "artifacts_pre_dumped": item["artifacts_pre_dumped"],
                "product_direct_claim_safe": item["product_direct_claim_safe"],
                "user_seeded_marker_proof": item["user_seeded_marker_proof"],
                "token_strategy": item["token_strategy"],
                "route_set": "remote_only_direct_small",
                "readiness_status": "pass",
                "executed": item["executed"],
                "stats": {
                    "total": 1,
                    "success": 1 if issue == "" else 0,
                    "failed": 0 if issue == "" else 1,
                },
                "delivery_ok": item["delivery_ok"],
                "endpoint_readback_ok": item["endpoint_readback_ok"],
                "content_evidence_found": item["content_evidence_found"],
                "evidence_kind": item["evidence_kind"],
                "delivery_payload_visible": item["delivery_payload_visible"],
                "delivery_payload_core_metadata_check": item[
                    "delivery_payload_core_metadata_check"
                ],
                "delivery_locator_found": item["delivery_locator_found"],
                "structured_state_log_status": item["structured_state_log_status"],
                "normalized_text_preview_found": item["normalized_text_preview_found"],
                "records_preview_found": item["records_preview_found"],
                "token_pass": item["token_pass"],
                "overall_pass": issue == "",
                "issue": issue,
                "severity": item["severity"],
                "remote_only_content_evidence_gap": item["remote_only_content_evidence_gap"],
                "fanout_top_level_ok": None,
                "fanout_child_ok": None,
                "audit_child_destination": None,
                "audit_evidence_kind": None,
                "audit_token_pass": None,
            }
        )
    return results


def build_report(
    *,
    env_file: Path,
    out_root: Path,
    mode: ModeName | Literal["both"],
    route_set: RouteSetName,
    include_remote_only_direct: bool,
) -> dict[str, object]:
    assert route_set == "representative"
    env = _load_env_values(env_file)
    readiness_map = _readiness_map(env_file)
    ctx = AuditContext(env=env, out_root=out_root, suffix=uuid.uuid4().hex[:8])
    out_root.mkdir(parents=True, exist_ok=True)
    modes: list[ModeName] = ["direct", "fanout"] if mode == "both" else [mode]
    results: list[dict[str, object]] = []
    for route in REPRESENTATIVE_ROUTES:
        for mode_name in modes:
            results.append(
                _run_one_route(
                    ctx=ctx,
                    readiness_map=readiness_map,
                    route=route,
                    mode=mode_name,
                )
            )
    if include_remote_only_direct:
        results.extend(_remote_only_route_results(ctx=ctx))
    _apply_remote_only_gap(results)
    overall_pass_count = sum(1 for item in results if item["overall_pass"] is True)
    blocked_count = sum(1 for item in results if item["readiness_status"] == "blocked")
    executed_count = sum(1 for item in results if item["executed"] is True)
    return {
        "catalog_id": "zephyr.p5.1.nm_representative_smoke.v1",
        "env_file": env_file.as_posix(),
        "out_root": out_root.as_posix(),
        "route_set": route_set,
        "mode": mode,
        "secrets_redacted": True,
        "fanout_mode_note": (
            "fanout mode is audit mode and does not replace direct-mode product pass"
        ),
        "artifact_prepared_direct_note": (
            "artifact_prepared_direct uses pre-dumped artifacts and is not equivalent "
            "to remote-only direct product proof"
        ),
        "summary": {
            "route_count": len(results),
            "executed_count": executed_count,
            "blocked_count": blocked_count,
            "overall_pass_count": overall_pass_count,
            "overall_fail_count": len(results) - overall_pass_count,
            "remote_only_direct_count": sum(
                1 for item in results if item["validation_mode"] == "remote_only_direct"
            ),
        },
        "routes": results,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(dict[str, object], report["summary"])
    routes = cast(list[dict[str, object]], report["routes"])
    lines = [
        "# Zephyr P5.1 Representative N×M Smoke",
        "",
        f"- env_file: `{report['env_file']}`",
        f"- out_root: `{report['out_root']}`",
        f"- route_set: `{report['route_set']}`",
        f"- mode: `{report['mode']}`",
        f"- route_count: {summary['route_count']}",
        f"- executed_count: {summary['executed_count']}",
        f"- blocked_count: {summary['blocked_count']}",
        f"- overall_pass_count: {summary['overall_pass_count']}",
        f"- overall_fail_count: {summary['overall_fail_count']}",
        f"- remote_only_direct_count: {summary['remote_only_direct_count']}",
        "",
        "> fanout mode is audit mode and does not replace direct-mode product pass.",
        f"> {report['artifact_prepared_direct_note']}",
        "",
        (
            "| source_id | destination_id | mode | validation_mode | artifacts_pre_dumped | "
            "product_direct_claim_safe | token_strategy | user_seeded_marker_proof | "
            "readiness_status | executed | delivery_ok | endpoint_readback_ok | "
            "delivery_payload_core_metadata_check | evidence_kind | token_pass | overall_pass | "
            "issue | severity | gap |"
        ),
        (
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | "
            "--- | --- | --- | --- | --- | --- | --- |"
        ),
    ]
    for item in routes:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(item["source_id"]),
                    str(item["destination_id"]),
                    str(item["mode"]),
                    str(item["validation_mode"]),
                    str(item["artifacts_pre_dumped"]),
                    str(item["product_direct_claim_safe"]),
                    str(item["token_strategy"]),
                    str(item["user_seeded_marker_proof"]),
                    str(item["readiness_status"]),
                    str(item["executed"]),
                    str(item["delivery_ok"]),
                    str(item["endpoint_readback_ok"]),
                    str(item["delivery_payload_core_metadata_check"]),
                    str(item["evidence_kind"]),
                    str(item["token_pass"]),
                    str(item["overall_pass"]),
                    str(item["issue"]),
                    str(item["severity"]),
                    str(item["remote_only_content_evidence_gap"]),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _emit(*, text: str, out_path: Path | None) -> None:
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        return
    print(text, end="")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a bounded representative P5.1 source-to-destination smoke."
    )
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument(
        "--out-root",
        type=Path,
        default=Path(".tmp/p5_1_nm_representative"),
    )
    parser.add_argument(
        "--mode",
        choices=("direct", "fanout", "both"),
        default="both",
    )
    parser.add_argument(
        "--route-set",
        choices=("representative",),
        default="representative",
    )
    parser.add_argument(
        "--include-remote-only-direct",
        action="store_true",
        help="Also run bounded remote-only direct synthetic local checks for Kafka and OpenSearch.",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--markdown", action="store_true")
    parser.add_argument("--out", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.json and args.markdown:
        parser.error("--json and --markdown are mutually exclusive")
    report = build_report(
        env_file=args.env_file.resolve(),
        out_root=args.out_root,
        mode=cast(ModeName | Literal["both"], args.mode),
        route_set=cast(RouteSetName, args.route_set),
        include_remote_only_direct=args.include_remote_only_direct,
    )
    if args.markdown:
        _emit(text=render_markdown(report), out_path=args.out)
        return 0
    _emit(text=json.dumps(report, ensure_ascii=False, indent=2) + "\n", out_path=args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
