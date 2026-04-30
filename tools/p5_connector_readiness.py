from __future__ import annotations

import argparse
import importlib
import json
import os
import shutil
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast
from urllib.parse import urlsplit

import httpx
from p5_connector_catalog import build_connector_catalog

from zephyr_ingest.testing.p5_fuv_d_fix import collect_p45_s3_bucket_status
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

ReadinessStatus = Literal["pass", "fail", "skip"]
ReadinessReason = Literal[
    "pass_ready",
    "skip_env",
    "fail_dependency",
    "fail_service",
    "fail_missing_implementation",
]
ProbeKind = Literal[
    "http_fixture",
    "s3",
    "git_local",
    "google_drive",
    "confluence",
    "postgresql",
    "clickhouse",
    "kafka",
    "mongodb",
    "filesystem",
    "webhook",
    "weaviate",
    "opensearch",
    "loki",
    "sqlite",
    "backend_uns_api",
]

IMPORT_NAME_BY_DEPENDENCY: dict[str, str] = {
    "boto3": "boto3",
    "botocore": "botocore",
    "clickhouse-connect": "clickhouse_connect",
    "confluent-kafka": "confluent_kafka",
    "httpx": "httpx",
    "psycopg": "psycopg",
    "pymongo": "pymongo",
    "unstructured": "unstructured",
    "weaviate-client": "weaviate",
}


@dataclass(frozen=True, slots=True)
class ConnectorReadinessProfile:
    required_env: tuple[str, ...]
    optional_env: tuple[str, ...]
    probe_kind: ProbeKind
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ServiceProbeResult:
    ok: bool
    detail: str


CONNECTOR_READINESS_PROFILES: dict[str, ConnectorReadinessProfile] = {
    "source.uns.http_document.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_HTTP_FIXTURE_BASE",),
        optional_env=("ZEPHYR_P45_HTTP_FIXTURE_HOST", "ZEPHYR_P45_HTTP_FIXTURE_PORT"),
        probe_kind="http_fixture",
    ),
    "source.uns.s3_document.v1": ConnectorReadinessProfile(
        required_env=(
            "ZEPHYR_P45_S3_ENDPOINT",
            "ZEPHYR_P45_S3_REGION",
            "ZEPHYR_P45_S3_BUCKET",
            "ZEPHYR_P45_S3_ACCESS_KEY",
            "ZEPHYR_P45_S3_SECRET_KEY",
        ),
        optional_env=("ZEPHYR_P45_S3_SESSION_TOKEN",),
        probe_kind="s3",
    ),
    "source.uns.git_document.v1": ConnectorReadinessProfile(
        required_env=(),
        optional_env=(),
        probe_kind="git_local",
        notes=(
            "Git source has no retained P45 live env surface; readiness checks "
            "git availability only.",
        ),
    ),
    "source.uns.google_drive_document.v1": ConnectorReadinessProfile(
        required_env=(
            "ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID",
            "ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN",
        ),
        optional_env=(
            "ZEPHYR_P45_GOOGLE_DRIVE_DRIVE_ID",
            "ZEPHYR_P45_GOOGLE_DRIVE_EXPORT_MIME_TYPE",
        ),
        probe_kind="google_drive",
    ),
    "source.uns.confluence_document.v1": ConnectorReadinessProfile(
        required_env=(
            "ZEPHYR_P45_CONFLUENCE_SITE_URL",
            "ZEPHYR_P45_CONFLUENCE_PAGE_ID",
            "ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN",
        ),
        optional_env=(
            "ZEPHYR_P45_CONFLUENCE_SPACE_KEY",
            "ZEPHYR_P45_CONFLUENCE_PAGE_VERSION",
            "ZEPHYR_P45_CONFLUENCE_EMAIL",
        ),
        probe_kind="confluence",
    ),
    "source.it.http_json_cursor.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_HTTP_FIXTURE_BASE",),
        optional_env=("ZEPHYR_P45_HTTP_FIXTURE_HOST", "ZEPHYR_P45_HTTP_FIXTURE_PORT"),
        probe_kind="http_fixture",
    ),
    "source.it.postgresql_incremental.v1": ConnectorReadinessProfile(
        required_env=(
            "ZEPHYR_P45_POSTGRES_HOST",
            "ZEPHYR_P45_POSTGRES_PORT",
            "ZEPHYR_P45_POSTGRES_DB",
            "ZEPHYR_P45_POSTGRES_USER",
            "ZEPHYR_P45_POSTGRES_PASSWORD",
        ),
        optional_env=("ZEPHYR_P45_POSTGRES_TABLE",),
        probe_kind="postgresql",
    ),
    "source.it.clickhouse_incremental.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_CLICKHOUSE_URL",),
        optional_env=(
            "ZEPHYR_P45_CLICKHOUSE_DATABASE",
            "ZEPHYR_P45_CLICKHOUSE_TABLE",
            "ZEPHYR_P45_CLICKHOUSE_USERNAME",
            "ZEPHYR_P45_CLICKHOUSE_PASSWORD",
        ),
        probe_kind="clickhouse",
    ),
    "source.it.kafka_partition_offset.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_KAFKA_BROKERS", "ZEPHYR_P45_KAFKA_TOPIC"),
        optional_env=(),
        probe_kind="kafka",
    ),
    "source.it.mongodb_incremental.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_MONGODB_URI",),
        optional_env=("ZEPHYR_P45_MONGODB_DATABASE", "ZEPHYR_P45_MONGODB_COLLECTION"),
        probe_kind="mongodb",
    ),
    "filesystem": ConnectorReadinessProfile(
        required_env=(),
        optional_env=(),
        probe_kind="filesystem",
    ),
    "destination.webhook.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_WEBHOOK_ECHO_URL",),
        optional_env=("ZEPHYR_P45_WEBHOOK_ECHO_HOST", "ZEPHYR_P45_WEBHOOK_ECHO_PORT"),
        probe_kind="webhook",
    ),
    "destination.kafka.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_KAFKA_BROKERS", "ZEPHYR_P45_KAFKA_TOPIC"),
        optional_env=(),
        probe_kind="kafka",
    ),
    "destination.weaviate.v1": ConnectorReadinessProfile(
        required_env=(
            "ZEPHYR_P45_WEAVIATE_HTTP_HOST",
            "ZEPHYR_P45_WEAVIATE_HTTP_PORT",
            "ZEPHYR_P45_WEAVIATE_COLLECTION",
        ),
        optional_env=(
            "ZEPHYR_P45_WEAVIATE_GRPC_HOST",
            "ZEPHYR_P45_WEAVIATE_GRPC_PORT",
            "ZEPHYR_P45_WEAVIATE_API_KEY",
        ),
        probe_kind="weaviate",
    ),
    "destination.s3.v1": ConnectorReadinessProfile(
        required_env=(
            "ZEPHYR_P45_S3_ENDPOINT",
            "ZEPHYR_P45_S3_REGION",
            "ZEPHYR_P45_S3_BUCKET",
            "ZEPHYR_P45_S3_ACCESS_KEY",
            "ZEPHYR_P45_S3_SECRET_KEY",
        ),
        optional_env=("ZEPHYR_P45_S3_SESSION_TOKEN",),
        probe_kind="s3",
    ),
    "destination.opensearch.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_OPENSEARCH_URL", "ZEPHYR_P45_OPENSEARCH_INDEX"),
        optional_env=(
            "ZEPHYR_P45_OPENSEARCH_USERNAME",
            "ZEPHYR_P45_OPENSEARCH_PASSWORD",
            "ZEPHYR_P45_OPENSEARCH_SKIP_TLS_VERIFY",
        ),
        probe_kind="opensearch",
    ),
    "destination.clickhouse.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_CLICKHOUSE_URL",),
        optional_env=(
            "ZEPHYR_P45_CLICKHOUSE_DATABASE",
            "ZEPHYR_P45_CLICKHOUSE_TABLE",
            "ZEPHYR_P45_CLICKHOUSE_USERNAME",
            "ZEPHYR_P45_CLICKHOUSE_PASSWORD",
        ),
        probe_kind="clickhouse",
    ),
    "destination.mongodb.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_MONGODB_URI",),
        optional_env=("ZEPHYR_P45_MONGODB_DATABASE", "ZEPHYR_P45_MONGODB_COLLECTION"),
        probe_kind="mongodb",
    ),
    "destination.loki.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_P45_LOKI_URL", "ZEPHYR_P45_LOKI_STREAM"),
        optional_env=("ZEPHYR_P45_LOKI_TENANT_ID",),
        probe_kind="loki",
    ),
    "destination.sqlite.v1": ConnectorReadinessProfile(
        required_env=(),
        optional_env=(),
        probe_kind="sqlite",
    ),
    "backend.uns_api.v1": ConnectorReadinessProfile(
        required_env=("ZEPHYR_UNS_API_KEY",),
        optional_env=("UNS_API_KEY", "UNSTRUCTURED_API_KEY", "ZEPHYR_UNS_API_URL"),
        probe_kind="backend_uns_api",
        notes=(
            "Backend readiness is bounded; without an explicit backend URL, "
            "the check stays env-gated.",
        ),
    ),
}


def _repo_root() -> Path:
    return repo_root()


def _repo_relative(path: Path) -> str:
    return path.relative_to(_repo_root()).as_posix()


def _normalize_env_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized if normalized != "" else None


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
        repo_root_path=_repo_root(),
        environ={P45_HOME_ENV_NAME: str(env_file.parent.parent)},
    )
    return LoadedP45Env(values=merged, sources=sources, runtime_paths=runtime_paths)


def _import_name(dependency_name: str) -> str:
    return IMPORT_NAME_BY_DEPENDENCY.get(dependency_name, dependency_name.replace("-", "_"))


def _dependency_status(dependencies: list[str]) -> tuple[list[str], list[str]]:
    available: list[str] = []
    missing: list[str] = []
    for dependency in dependencies:
        module_name = _import_name(dependency)
        try:
            importlib.import_module(module_name)
        except ImportError:
            missing.append(dependency)
            continue
        available.append(dependency)
    return available, missing


def _safe_env_status(env: LoadedP45Env, names: tuple[str, ...]) -> tuple[list[str], list[str]]:
    present: list[str] = []
    missing: list[str] = []
    for name in names:
        if env.get(name) is None:
            missing.append(name)
        else:
            present.append(name)
    return present, missing


def _http_probe(
    *,
    url: str,
    expected_statuses: tuple[int, ...] = (200,),
    method: Literal["GET", "HEAD"] = "GET",
    headers: dict[str, str] | None = None,
    auth: tuple[str, str] | None = None,
    verify: bool = True,
    params: dict[str, str] | None = None,
    timeout_s: float = 3.0,
) -> ServiceProbeResult:
    try:
        response = httpx.request(
            method,
            url,
            headers=headers,
            auth=auth,
            verify=verify,
            params=params,
            timeout=timeout_s,
            trust_env=False,
        )
    except httpx.HTTPError as exc:
        return ServiceProbeResult(ok=False, detail=f"http_probe_failed:{type(exc).__name__}")
    if response.status_code in expected_statuses:
        return ServiceProbeResult(ok=True, detail=f"http_status:{response.status_code}")
    return ServiceProbeResult(ok=False, detail=f"http_status:{response.status_code}")


def _http_fixture_probe(env: LoadedP45Env) -> ServiceProbeResult:
    base = env.get("ZEPHYR_P45_HTTP_FIXTURE_BASE")
    if base is None:
        return ServiceProbeResult(ok=False, detail="missing_http_fixture_base")
    return _http_probe(url=f"{base.rstrip('/')}/healthz")


def _google_drive_probe(env: LoadedP45Env) -> ServiceProbeResult:
    file_id = env.get("ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID")
    token = env.get("ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN")
    if file_id is None or token is None:
        return ServiceProbeResult(ok=False, detail="missing_google_drive_env")
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    headers = {"Authorization": f"Bearer {token}"}
    return _http_probe(
        url=url,
        headers=headers,
        params={"fields": "id,mimeType,name"},
    )


def _confluence_probe(env: LoadedP45Env) -> ServiceProbeResult:
    site_url = env.get("ZEPHYR_P45_CONFLUENCE_SITE_URL")
    page_id = env.get("ZEPHYR_P45_CONFLUENCE_PAGE_ID")
    token = env.get("ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN")
    if site_url is None or page_id is None or token is None:
        return ServiceProbeResult(ok=False, detail="missing_confluence_env")
    email = env.get("ZEPHYR_P45_CONFLUENCE_EMAIL")
    auth: tuple[str, str] | None = None if email is None else (email, token)
    headers: dict[str, str] | None = None
    if auth is None:
        headers = {"Authorization": f"Bearer {token}"}
    url = f"{site_url.rstrip('/')}/wiki/api/v2/pages/{page_id}"
    return _http_probe(url=url, auth=auth, headers=headers)


def _postgresql_probe(env: LoadedP45Env) -> ServiceProbeResult:
    psycopg = importlib.import_module("psycopg")
    connect = getattr(psycopg, "connect")
    try:
        connection = connect(
            host=env.require("ZEPHYR_P45_POSTGRES_HOST"),
            port=int(env.require("ZEPHYR_P45_POSTGRES_PORT")),
            dbname=env.require("ZEPHYR_P45_POSTGRES_DB"),
            user=env.require("ZEPHYR_P45_POSTGRES_USER"),
            password=env.require("ZEPHYR_P45_POSTGRES_PASSWORD"),
            connect_timeout=3,
        )
    except Exception as exc:
        return ServiceProbeResult(ok=False, detail=f"postgres_connect_failed:{type(exc).__name__}")
    try:
        with cast(object, connection):
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT 1")
                row = cursor.fetchone()
            finally:
                cursor.close()
    except Exception as exc:
        return ServiceProbeResult(ok=False, detail=f"postgres_query_failed:{type(exc).__name__}")
    return ServiceProbeResult(ok=row == (1,), detail="postgres_select_1")


def _clickhouse_probe(env: LoadedP45Env) -> ServiceProbeResult:
    url = env.get("ZEPHYR_P45_CLICKHOUSE_URL")
    if url is None:
        return ServiceProbeResult(ok=False, detail="missing_clickhouse_url")
    username = env.get("ZEPHYR_P45_CLICKHOUSE_USERNAME")
    password = env.get("ZEPHYR_P45_CLICKHOUSE_PASSWORD")
    auth = None if username is None else (username, password or "")
    return _http_probe(
        url=url,
        auth=auth,
        params={"query": "SELECT 1"},
        expected_statuses=(200,),
    )


def _kafka_probe(env: LoadedP45Env) -> ServiceProbeResult:
    confluent_kafka_admin = importlib.import_module("confluent_kafka.admin")
    admin_client_cls = getattr(confluent_kafka_admin, "AdminClient")
    brokers = env.require("ZEPHYR_P45_KAFKA_BROKERS")
    topic = env.require("ZEPHYR_P45_KAFKA_TOPIC")
    try:
        admin_client = admin_client_cls({"bootstrap.servers": brokers})
        metadata = admin_client.list_topics(topic=topic, timeout=3.0)
        topics_obj = getattr(metadata, "topics", None)
        if isinstance(topics_obj, dict) and topic in topics_obj:
            return ServiceProbeResult(ok=True, detail="kafka_topic_metadata_ready")
        return ServiceProbeResult(ok=False, detail="kafka_topic_missing_from_metadata")
    except Exception as exc:
        return ServiceProbeResult(ok=False, detail=f"kafka_metadata_failed:{type(exc).__name__}")


def _mongodb_probe(env: LoadedP45Env) -> ServiceProbeResult:
    pymongo = importlib.import_module("pymongo")
    mongo_client_cls = getattr(pymongo, "MongoClient")
    try:
        with closing(
            mongo_client_cls(
                env.require("ZEPHYR_P45_MONGODB_URI"),
                serverSelectionTimeoutMS=3000,
            )
        ) as client:
            client.admin.command("ping")
    except Exception as exc:
        return ServiceProbeResult(ok=False, detail=f"mongodb_ping_failed:{type(exc).__name__}")
    return ServiceProbeResult(ok=True, detail="mongodb_ping_ok")


def _filesystem_probe() -> ServiceProbeResult:
    probe_root = _repo_root()
    if os.access(probe_root, os.W_OK):
        return ServiceProbeResult(ok=True, detail="filesystem_access_writable")
    return ServiceProbeResult(ok=False, detail="filesystem_access_not_writable")


def _webhook_probe(env: LoadedP45Env, *, live: bool) -> ServiceProbeResult:
    raw_url = env.get("ZEPHYR_P45_WEBHOOK_ECHO_URL")
    if raw_url is None:
        return ServiceProbeResult(ok=False, detail="missing_webhook_url")
    parsed = urlsplit(raw_url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc == "":
        return ServiceProbeResult(ok=False, detail="webhook_url_invalid")
    if not live:
        return ServiceProbeResult(ok=True, detail="webhook_url_valid_live_probe_disabled")
    health_url = raw_url.rstrip("/")
    if health_url.endswith("/ingest"):
        health_url = f"{health_url.removesuffix('/ingest')}/healthz"
    return _http_probe(url=health_url)


def _weaviate_probe(env: LoadedP45Env) -> ServiceProbeResult:
    host = env.get("ZEPHYR_P45_WEAVIATE_HTTP_HOST")
    port = env.get("ZEPHYR_P45_WEAVIATE_HTTP_PORT")
    if host is None or port is None:
        return ServiceProbeResult(ok=False, detail="missing_weaviate_http_surface")
    url = f"http://{host}:{port}/v1/.well-known/ready"
    return _http_probe(url=url)


def _opensearch_probe(env: LoadedP45Env) -> ServiceProbeResult:
    return _http_probe(
        url=get_p45_opensearch_url(env),
        auth=get_p45_opensearch_auth(env),
        verify=get_p45_opensearch_verify_tls(env),
        expected_statuses=(200,),
    )


def _loki_probe(env: LoadedP45Env) -> ServiceProbeResult:
    base = env.get("ZEPHYR_P45_LOKI_URL")
    if base is None:
        return ServiceProbeResult(ok=False, detail="missing_loki_url")
    return _http_probe(url=f"{base.rstrip('/')}/ready")


def _git_local_probe() -> ServiceProbeResult:
    return ServiceProbeResult(
        ok=shutil.which("git") is not None,
        detail="git_on_path" if shutil.which("git") is not None else "git_not_on_path",
    )


def _backend_uns_api_probe(env: LoadedP45Env) -> ServiceProbeResult:
    backend_url = env.get("ZEPHYR_UNS_API_URL")
    if backend_url is None:
        return ServiceProbeResult(ok=False, detail="missing_backend_uns_api_url")
    parsed = urlsplit(backend_url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc == "":
        return ServiceProbeResult(ok=False, detail="backend_uns_api_url_invalid")
    return _http_probe(url=backend_url, method="HEAD", expected_statuses=(200, 405))


def _sqlite_probe() -> ServiceProbeResult:
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("SELECT 1")
    finally:
        connection.close()
    return ServiceProbeResult(ok=True, detail="sqlite_select_1")


def _s3_probe(env: LoadedP45Env) -> ServiceProbeResult:
    status = collect_p45_s3_bucket_status(loaded_env=env, ensure_bucket=False)
    dependency_installed = status.get("dependency_installed")
    if dependency_installed is not True:
        return ServiceProbeResult(ok=False, detail="s3_dependency_unavailable")
    bucket_exists = status.get("bucket_exists")
    if bucket_exists is True:
        return ServiceProbeResult(ok=True, detail="s3_bucket_exists")
    error = status.get("error")
    if isinstance(error, str):
        return ServiceProbeResult(ok=False, detail=f"s3_check_failed:{error}")
    return ServiceProbeResult(ok=False, detail="s3_bucket_missing")


def _probe_service(*, probe_kind: ProbeKind, env: LoadedP45Env, live: bool) -> ServiceProbeResult:
    if probe_kind == "http_fixture":
        return _http_fixture_probe(env)
    if probe_kind == "s3":
        return _s3_probe(env)
    if probe_kind == "git_local":
        return _git_local_probe()
    if probe_kind == "google_drive":
        return _google_drive_probe(env)
    if probe_kind == "confluence":
        return _confluence_probe(env)
    if probe_kind == "postgresql":
        return _postgresql_probe(env)
    if probe_kind == "clickhouse":
        return _clickhouse_probe(env)
    if probe_kind == "kafka":
        return _kafka_probe(env)
    if probe_kind == "mongodb":
        return _mongodb_probe(env)
    if probe_kind == "filesystem":
        return _filesystem_probe()
    if probe_kind == "webhook":
        return _webhook_probe(env, live=live)
    if probe_kind == "weaviate":
        return _weaviate_probe(env)
    if probe_kind == "opensearch":
        return _opensearch_probe(env)
    if probe_kind == "loki":
        return _loki_probe(env)
    if probe_kind == "sqlite":
        return _sqlite_probe()
    if probe_kind == "backend_uns_api":
        return _backend_uns_api_probe(env)
    raise ValueError(f"Unsupported probe kind: {probe_kind}")


def _sanitize_probe_detail(detail: str) -> str:
    if "://" in detail:
        parsed = urlsplit(detail)
        if parsed.scheme != "" and parsed.netloc != "":
            safe_host = parsed.hostname or parsed.netloc
            safe_port = f":{parsed.port}" if parsed.port is not None else ""
            safe_path = parsed.path or ""
            return f"{parsed.scheme}://{safe_host}{safe_port}{safe_path}"
    return detail


def _build_record(
    *,
    connector: dict[str, object],
    env: LoadedP45Env,
    live: bool,
) -> dict[str, object]:
    connector_id = cast(str, connector["id"])
    profile = CONNECTOR_READINESS_PROFILES[connector_id]
    implementation_status = cast(str, connector["implementation_status"])
    available_deps, missing_deps = _dependency_status(
        cast(list[str], connector["python_dependencies"])
    )
    present_env, missing_env = _safe_env_status(env, profile.required_env)
    optional_present, optional_missing = _safe_env_status(env, profile.optional_env)

    status: ReadinessStatus
    reason: ReadinessReason
    detail: str
    service_probe_ok = False

    if implementation_status == "missing_or_unregistered":
        status = "fail"
        reason = "fail_missing_implementation"
        detail = implementation_status
    elif missing_deps:
        status = "fail"
        reason = "fail_dependency"
        detail = "missing_python_dependencies"
    elif missing_env:
        status = "skip"
        reason = "skip_env"
        detail = "missing_required_env"
    else:
        probe = _probe_service(probe_kind=profile.probe_kind, env=env, live=live)
        service_probe_ok = probe.ok
        detail = _sanitize_probe_detail(probe.detail)
        if probe.ok:
            status = "pass"
            reason = "pass_ready"
        else:
            status = "fail"
            reason = "fail_service"

    record_notes = cast(list[str], connector["notes"]) + list(profile.notes)
    return {
        "id": connector_id,
        "family": connector["family"],
        "flow_kind": connector["flow_kind"],
        "implementation_status": implementation_status,
        "severity": connector["severity"],
        "python_dependencies": connector["python_dependencies"],
        "dependency_importable": available_deps,
        "dependency_missing": missing_deps,
        "required_env": list(profile.required_env),
        "required_env_present": present_env,
        "required_env_missing": missing_env,
        "optional_env": list(profile.optional_env),
        "optional_env_present": optional_present,
        "optional_env_missing": optional_missing,
        "service_readiness_kind": connector["service_readiness_kind"],
        "endpoint_readback_kind": connector["endpoint_readback_kind"],
        "status": status,
        "reason": reason,
        "service_probe_ok": service_probe_ok,
        "detail": detail,
        "notes": record_notes,
    }


def build_connector_readiness(*, env_file: Path, live: bool) -> dict[str, object]:
    catalog = build_connector_catalog()
    env = _load_env_values(env_file)
    connectors_obj = catalog["connectors"]
    assert isinstance(connectors_obj, list)
    records = [
        _build_record(connector=cast(dict[str, object], item), env=env, live=live)
        for item in connectors_obj
    ]
    pass_count = sum(1 for item in records if item["status"] == "pass")
    fail_count = sum(1 for item in records if item["status"] == "fail")
    skip_count = sum(1 for item in records if item["status"] == "skip")
    return {
        "catalog_id": "zephyr.p5.1.connector_readiness.v1",
        "generated_from": _repo_relative(Path(__file__)),
        "catalog_source": "tools/p5_connector_catalog.py",
        "env_file": env_file.as_posix(),
        "secrets_redacted": True,
        "live_webhook_probe_enabled": live,
        "summary": {
            "connector_count": len(records),
            "pass_count": pass_count,
            "fail_count": fail_count,
            "skip_count": skip_count,
            "readiness_pass_does_not_imply_product_direct_pass": True,
        },
        "connectors": records,
    }


def _markdown_cell_list(items: list[str]) -> str:
    if not items:
        return "-"
    return "<br>".join(items)


def render_markdown(readiness: dict[str, object]) -> str:
    summary = cast(dict[str, object], readiness["summary"])
    connectors = cast(list[dict[str, object]], readiness["connectors"])
    lines = [
        "# Zephyr P5.1 Connector Readiness",
        "",
        f"- env_file: `{readiness['env_file']}`",
        f"- connector_count: {summary['connector_count']}",
        f"- pass: {summary['pass_count']}",
        f"- fail: {summary['fail_count']}",
        f"- skip: {summary['skip_count']}",
        (
            "- readiness_pass_does_not_imply_product_direct_pass: "
            f"{summary['readiness_pass_does_not_imply_product_direct_pass']}"
        ),
        "",
        "| id | family | flow_kind | status | reason | required_env_missing | "
        "dependency_missing | detail | notes |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in connectors:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(item["id"]),
                    str(item["family"]),
                    str(item["flow_kind"]),
                    str(item["status"]),
                    str(item["reason"]),
                    _markdown_cell_list(
                        [str(value) for value in cast(list[object], item["required_env_missing"])]
                    ),
                    _markdown_cell_list(
                        [str(value) for value in cast(list[object], item["dependency_missing"])]
                    ),
                    str(item["detail"]),
                    _markdown_cell_list(
                        [str(value) for value in cast(list[object], item["notes"])]
                    ),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _emit(*, content: str, out_path: Path | None) -> None:
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(content, encoding="utf-8")
        return
    print(content, end="")


def _validate_env_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Env file does not exist: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"Env file is not a file: {path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="p5_connector_readiness")
    render_group = parser.add_mutually_exclusive_group()
    render_group.add_argument("--json", action="store_true", help="Render JSON output.")
    render_group.add_argument("--markdown", action="store_true", help="Render markdown output.")
    parser.add_argument(
        "--env-file",
        type=Path,
        required=True,
        help="Path to the external P45 env file.",
    )
    parser.add_argument("--out", type=Path, default=None, help="Write output to a file path.")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Allow live webhook readiness probe instead of URL-format-only validation.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_env_file(args.env_file)
    readiness = build_connector_readiness(env_file=args.env_file, live=args.live)
    if args.markdown:
        content = render_markdown(readiness)
    else:
        content = json.dumps(readiness, ensure_ascii=False, indent=2) + "\n"
    _emit(content=content, out_path=args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
