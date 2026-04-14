from __future__ import annotations

import os
import shlex
import socket
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

import httpx

from zephyr_ingest.testing.p45_matrix import P45Tier

P45_MARKERS: Final[dict[P45Tier, str]] = {
    "contract": "auth_contract",
    "orchestration": "auth_orchestration",
    "local-real": "auth_local_real",
    "service-live": "auth_service_live",
    "saas-live": "auth_saas_live",
    "recovery-drill": "auth_recovery_drill",
}
P45_GATED_TIERS: Final[frozenset[P45Tier]] = frozenset(
    {"local-real", "service-live", "saas-live", "recovery-drill"}
)
P45_ENV_FILE_NAMES: Final[tuple[str, ...]] = (".env", ".env.p45", ".env.p45.local")
P45_COMPOSE_FILE_NAME: Final[str] = "docker-compose.p45-validation.yml"

ServiceTier = Literal["local-real", "service-live"]
ServiceName = Literal[
    "postgresql",
    "kafka-redpanda",
    "minio-s3",
    "opensearch",
    "clickhouse",
    "mongodb",
    "loki",
    "weaviate",
    "http-fixture",
    "webhook-echo",
]
ProbeKind = Literal["http", "tcp"]

DEFAULT_P45_ENV: Final[dict[str, str]] = {
    "ZEPHYR_P45_POSTGRES_HOST": "127.0.0.1",
    "ZEPHYR_P45_POSTGRES_PORT": "15432",
    "ZEPHYR_P45_POSTGRES_USER": "zephyr",
    "ZEPHYR_P45_POSTGRES_PASSWORD": "zephyr",
    "ZEPHYR_P45_POSTGRES_DB": "zephyr",
    "ZEPHYR_P45_KAFKA_HOST": "127.0.0.1",
    "ZEPHYR_P45_KAFKA_PORT": "19092",
    "ZEPHYR_P45_REDPANDA_ADMIN_HOST": "127.0.0.1",
    "ZEPHYR_P45_REDPANDA_ADMIN_PORT": "19644",
    "ZEPHYR_P45_S3_HOST": "127.0.0.1",
    "ZEPHYR_P45_S3_PORT": "19000",
    "ZEPHYR_P45_S3_ACCESS_KEY": "minioadmin",
    "ZEPHYR_P45_S3_SECRET_KEY": "minioadmin",
    "ZEPHYR_P45_OPENSEARCH_HOST": "127.0.0.1",
    "ZEPHYR_P45_OPENSEARCH_PORT": "19200",
    "ZEPHYR_P45_OPENSEARCH_USERNAME": "admin",
    "ZEPHYR_P45_OPENSEARCH_PASSWORD": "",
    "ZEPHYR_P45_CLICKHOUSE_HOST": "127.0.0.1",
    "ZEPHYR_P45_CLICKHOUSE_PORT": "18123",
    "ZEPHYR_P45_CLICKHOUSE_USERNAME": "default",
    "ZEPHYR_P45_CLICKHOUSE_PASSWORD": "",
    "ZEPHYR_P45_MONGODB_HOST": "127.0.0.1",
    "ZEPHYR_P45_MONGODB_PORT": "27018",
    "ZEPHYR_P45_MONGODB_USERNAME": "",
    "ZEPHYR_P45_MONGODB_PASSWORD": "",
    "ZEPHYR_P45_LOKI_HOST": "127.0.0.1",
    "ZEPHYR_P45_LOKI_PORT": "13100",
    "ZEPHYR_P45_WEAVIATE_HOST": "127.0.0.1",
    "ZEPHYR_P45_WEAVIATE_PORT": "18080",
    "ZEPHYR_P45_WEAVIATE_GRPC_PORT": "15051",
    "ZEPHYR_P45_WEAVIATE_API_KEY": "",
    "ZEPHYR_P45_HTTP_FIXTURE_HOST": "127.0.0.1",
    "ZEPHYR_P45_HTTP_FIXTURE_PORT": "18081",
    "ZEPHYR_P45_WEBHOOK_ECHO_HOST": "127.0.0.1",
    "ZEPHYR_P45_WEBHOOK_ECHO_PORT": "18082",
    "ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID": "",
    "ZEPHYR_P45_GOOGLE_DRIVE_DRIVE_ID": "",
    "ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN": "",
    "ZEPHYR_P45_CONFLUENCE_SITE_URL": "",
    "ZEPHYR_P45_CONFLUENCE_PAGE_ID": "",
    "ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN": "",
}
SAAS_REQUIRED_ENV: Final[dict[str, tuple[str, ...]]] = {
    "google-drive": (
        "ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID",
        "ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN",
    ),
    "confluence": (
        "ZEPHYR_P45_CONFLUENCE_SITE_URL",
        "ZEPHYR_P45_CONFLUENCE_PAGE_ID",
        "ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN",
    ),
}
SECRET_NAME_FRAGMENTS: Final[tuple[str, ...]] = (
    "ACCESS_KEY",
    "ACCESS_TOKEN",
    "API_KEY",
    "PASSWORD",
    "SECRET",
    "SESSION_TOKEN",
    "TOKEN",
    "_DSN",
    "_URI",
)


@dataclass(frozen=True, slots=True)
class LoadedP45Env:
    values: Mapping[str, str]
    sources: Mapping[str, str]

    def get(self, name: str, default: str | None = None) -> str | None:
        value = self.values.get(name)
        if value is None or value == "":
            return default
        return value

    def require(self, name: str) -> str:
        value = self.get(name)
        if value is None:
            raise KeyError(f"Missing required P4.5 env setting: {name}")
        return value


@dataclass(frozen=True, slots=True)
class ProbeDefinition:
    label: str
    kind: ProbeKind
    host_env: str
    default_host: str
    port_env: str
    default_port: int
    path: str = ""
    expected_statuses: tuple[int, ...] = (200,)


@dataclass(frozen=True, slots=True)
class ServiceDefinition:
    name: ServiceName
    tier: ServiceTier
    compose_service: str
    description: str
    probes: tuple[ProbeDefinition, ...]


@dataclass(frozen=True, slots=True)
class ProbeResult:
    service: ServiceName
    probe: str
    ok: bool
    detail: str


_REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[5]
_SERVICE_DEFINITIONS: Final[tuple[ServiceDefinition, ...]] = (
    ServiceDefinition(
        name="postgresql",
        tier="service-live",
        compose_service="postgresql",
        description="Local PostgreSQL substrate for retained source validation.",
        probes=(
            ProbeDefinition(
                label="tcp",
                kind="tcp",
                host_env="ZEPHYR_P45_POSTGRES_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_POSTGRES_PORT",
                default_port=15432,
            ),
        ),
    ),
    ServiceDefinition(
        name="kafka-redpanda",
        tier="service-live",
        compose_service="redpanda",
        description="Redpanda-backed Kafka substrate for retained source/destination validation.",
        probes=(
            ProbeDefinition(
                label="broker-tcp",
                kind="tcp",
                host_env="ZEPHYR_P45_KAFKA_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_KAFKA_PORT",
                default_port=19092,
            ),
            ProbeDefinition(
                label="admin-ready",
                kind="http",
                host_env="ZEPHYR_P45_REDPANDA_ADMIN_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_REDPANDA_ADMIN_PORT",
                default_port=19644,
                path="/v1/status/ready",
            ),
        ),
    ),
    ServiceDefinition(
        name="minio-s3",
        tier="service-live",
        compose_service="minio",
        description="S3-compatible substrate for retained source/destination validation.",
        probes=(
            ProbeDefinition(
                label="health",
                kind="http",
                host_env="ZEPHYR_P45_S3_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_S3_PORT",
                default_port=19000,
                path="/minio/health/live",
            ),
        ),
    ),
    ServiceDefinition(
        name="opensearch",
        tier="service-live",
        compose_service="opensearch",
        description="OpenSearch substrate for retained destination validation.",
        probes=(
            ProbeDefinition(
                label="root",
                kind="http",
                host_env="ZEPHYR_P45_OPENSEARCH_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_OPENSEARCH_PORT",
                default_port=19200,
                path="/",
            ),
        ),
    ),
    ServiceDefinition(
        name="clickhouse",
        tier="service-live",
        compose_service="clickhouse",
        description="ClickHouse substrate for retained source/destination validation.",
        probes=(
            ProbeDefinition(
                label="ping",
                kind="http",
                host_env="ZEPHYR_P45_CLICKHOUSE_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_CLICKHOUSE_PORT",
                default_port=18123,
                path="/ping",
            ),
        ),
    ),
    ServiceDefinition(
        name="mongodb",
        tier="service-live",
        compose_service="mongodb",
        description="MongoDB substrate for retained source/destination validation.",
        probes=(
            ProbeDefinition(
                label="tcp",
                kind="tcp",
                host_env="ZEPHYR_P45_MONGODB_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_MONGODB_PORT",
                default_port=27018,
            ),
        ),
    ),
    ServiceDefinition(
        name="loki",
        tier="service-live",
        compose_service="loki",
        description="Loki substrate for retained destination validation.",
        probes=(
            ProbeDefinition(
                label="ready",
                kind="http",
                host_env="ZEPHYR_P45_LOKI_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_LOKI_PORT",
                default_port=13100,
                path="/ready",
            ),
        ),
    ),
    ServiceDefinition(
        name="weaviate",
        tier="service-live",
        compose_service="weaviate",
        description="Weaviate substrate for retained destination validation.",
        probes=(
            ProbeDefinition(
                label="ready",
                kind="http",
                host_env="ZEPHYR_P45_WEAVIATE_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_WEAVIATE_PORT",
                default_port=18080,
                path="/v1/.well-known/ready",
            ),
        ),
    ),
    ServiceDefinition(
        name="http-fixture",
        tier="local-real",
        compose_service="http-fixture",
        description="HTTP fixture server for retained HTTP source validation.",
        probes=(
            ProbeDefinition(
                label="healthz",
                kind="http",
                host_env="ZEPHYR_P45_HTTP_FIXTURE_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_HTTP_FIXTURE_PORT",
                default_port=18081,
                path="/healthz",
            ),
        ),
    ),
    ServiceDefinition(
        name="webhook-echo",
        tier="local-real",
        compose_service="webhook-echo",
        description="Webhook echo server for retained webhook destination validation.",
        probes=(
            ProbeDefinition(
                label="healthz",
                kind="http",
                host_env="ZEPHYR_P45_WEBHOOK_ECHO_HOST",
                default_host="127.0.0.1",
                port_env="ZEPHYR_P45_WEBHOOK_ECHO_PORT",
                default_port=18082,
                path="/healthz",
            ),
        ),
    ),
)


def repo_root() -> Path:
    return _REPO_ROOT


def default_compose_path() -> Path:
    return repo_root() / P45_COMPOSE_FILE_NAME


def _normalize_env_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return None if normalized == "" else normalized


def _parse_env_assignment(raw_value: str) -> str:
    stripped = raw_value.strip()
    if stripped == "":
        return ""
    try:
        tokens = shlex.split(stripped, comments=True, posix=True)
    except ValueError:
        return stripped
    return "" if not tokens else tokens[0]


def parse_env_file(path: Path) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line == "" or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        name_part, raw_value = line.split("=", 1)
        name = name_part.strip()
        if name == "":
            continue
        parsed[name] = _parse_env_assignment(raw_value)
    return parsed


def load_p45_env(
    *,
    repo_root_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> LoadedP45Env:
    root = repo_root() if repo_root_path is None else repo_root_path
    merged: dict[str, str] = dict(DEFAULT_P45_ENV)
    sources: dict[str, str] = dict.fromkeys(merged, "default")
    for file_name in P45_ENV_FILE_NAMES:
        env_path = root / file_name
        if not env_path.exists():
            continue
        for name, value in parse_env_file(env_path).items():
            normalized = _normalize_env_value(value)
            if normalized is None:
                continue
            merged[name] = normalized
            sources[name] = env_path.name
    active_env = os.environ if environ is None else environ
    for name, value in active_env.items():
        normalized = _normalize_env_value(value)
        if normalized is None:
            continue
        merged[name] = normalized
        sources[name] = "env"
    return LoadedP45Env(values=merged, sources=sources)


def is_secret_env_name(name: str) -> bool:
    upper_name = name.upper()
    return any(fragment in upper_name for fragment in SECRET_NAME_FRAGMENTS)


def redact_value(value: str) -> str:
    if value == "":
        return ""
    return "***"


def redact_mapping(values: Mapping[str, str]) -> dict[str, str]:
    redacted: dict[str, str] = {}
    for name, value in values.items():
        redacted[name] = redact_value(value) if is_secret_env_name(name) else value
    return redacted


def format_redacted_env_summary(env: LoadedP45Env, *, names: Sequence[str] | None = None) -> str:
    selected_names = sorted(env.values) if names is None else sorted(set(names))
    redacted = redact_mapping(env.values)
    lines = ["P4.5 env summary:"]
    for name in selected_names:
        value = redacted.get(name, "")
        source = env.sources.get(name, "unset")
        lines.append(f"- {name}={value!r} ({source})")
    return "\n".join(lines)


def get_service_definitions(*, tier: ServiceTier | None = None) -> tuple[ServiceDefinition, ...]:
    if tier is None:
        return _SERVICE_DEFINITIONS
    return tuple(service for service in _SERVICE_DEFINITIONS if service.tier == tier)


def _resolve_host(env: LoadedP45Env, probe: ProbeDefinition) -> str:
    return env.get(probe.host_env, probe.default_host) or probe.default_host


def _resolve_port(env: LoadedP45Env, probe: ProbeDefinition) -> int:
    raw_port = env.get(probe.port_env, str(probe.default_port)) or str(probe.default_port)
    return int(raw_port)


def _check_tcp_probe(
    *, service: ServiceName, probe: ProbeDefinition, env: LoadedP45Env, timeout_s: float
) -> ProbeResult:
    host = _resolve_host(env, probe)
    port = _resolve_port(env, probe)
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return ProbeResult(service=service, probe=probe.label, ok=True, detail=f"{host}:{port}")
    except OSError as exc:
        return ProbeResult(
            service=service,
            probe=probe.label,
            ok=False,
            detail=f"{host}:{port} ({exc})",
        )


def _check_http_probe(
    *, service: ServiceName, probe: ProbeDefinition, env: LoadedP45Env, timeout_s: float
) -> ProbeResult:
    host = _resolve_host(env, probe)
    port = _resolve_port(env, probe)
    url = f"http://{host}:{port}{probe.path}"
    try:
        response = httpx.get(url, timeout=timeout_s)
    except httpx.HTTPError as exc:
        return ProbeResult(service=service, probe=probe.label, ok=False, detail=f"{url} ({exc})")
    if response.status_code in probe.expected_statuses:
        return ProbeResult(
            service=service,
            probe=probe.label,
            ok=True,
            detail=f"{url} [{response.status_code}]",
        )
    return ProbeResult(
        service=service,
        probe=probe.label,
        ok=False,
        detail=f"{url} [{response.status_code}]",
    )


def check_services(
    env: LoadedP45Env,
    *,
    tier: ServiceTier | None = None,
    service_names: Sequence[str] | None = None,
    timeout_s: float = 3.0,
) -> list[ProbeResult]:
    selected = set(service_names) if service_names is not None else None
    results: list[ProbeResult] = []
    for service in get_service_definitions(tier=tier):
        if selected is not None and service.name not in selected:
            continue
        for probe in service.probes:
            if probe.kind == "http":
                results.append(
                    _check_http_probe(
                        service=service.name,
                        probe=probe,
                        env=env,
                        timeout_s=timeout_s,
                    )
                )
            else:
                results.append(
                    _check_tcp_probe(
                        service=service.name,
                        probe=probe,
                        env=env,
                        timeout_s=timeout_s,
                    )
                )
    return results


def format_probe_results(results: Sequence[ProbeResult]) -> str:
    if not results:
        return "No P4.5 substrate probes selected."
    lines = ["P4.5 substrate probe results:"]
    for result in results:
        state = "ok" if result.ok else "failed"
        lines.append(f"- {result.service}:{result.probe} -> {state} ({result.detail})")
    return "\n".join(lines)


def missing_saas_env(service: str, env: LoadedP45Env) -> tuple[str, ...]:
    required = SAAS_REQUIRED_ENV.get(service, ())
    missing: list[str] = []
    for name in required:
        if env.get(name) is None:
            missing.append(name)
    return tuple(missing)
