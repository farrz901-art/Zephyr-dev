from __future__ import annotations

import os
import shlex
import socket
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal
from urllib.parse import urlsplit

import httpx

from zephyr_ingest.testing.p45_matrix import P45Tier

P45_HOME_ENV_NAME: Final[str] = "ZEPHYR_P45_HOME"
P45_COMPOSE_FILE_NAME: Final[str] = "docker-compose.p45-validation.yml"
P45_ENV_FILE_NAMES: Final[tuple[str, ...]] = (".env", ".env.p45", ".env.p45.local")
P45_RUNTIME_COMPOSE_FILE_NAMES: Final[tuple[str, ...]] = (
    "docker-compose.yml",
    "docker-compose.yaml",
    P45_COMPOSE_FILE_NAME,
)
P45_RUNTIME_ENV_DIR_NAME: Final[str] = "env"
P45_RUNTIME_LOG_DIR_NAME: Final[str] = "logs"
P45_RUNTIME_DATA_DIR_NAME: Final[str] = "data"
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
DEFAULT_P45_HOME_RELATIVE: Final[Path] = Path(".config") / "zephyr" / "p45"

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
    "ZEPHYR_P45_GOOGLE_DRIVE_EXPORT_MIME_TYPE": "",
    "ZEPHYR_P45_CONFLUENCE_SITE_URL": "",
    "ZEPHYR_P45_CONFLUENCE_PAGE_ID": "",
    "ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN": "",
}
P45_ENV_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    "ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID": (
        "ZEPHYR_P45_GDRIVE_FILE_ID_EXPORT",
        "ZEPHYR_P45_GDRIVE_FILE_ID_DOC",
        "ZEPHYR_P45_GDRIVE_FILE_ID_BINARY",
    ),
    "ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN": ("ZEPHYR_P45_GDRIVE_ACCESS_TOKEN",),
    "ZEPHYR_P45_GOOGLE_DRIVE_EXPORT_MIME_TYPE": ("ZEPHYR_P45_GDRIVE_EXPORT_MIME_TYPE",),
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
EXPLICIT_SECRET_CARRIER_ENV_NAMES: Final[frozenset[str]] = frozenset(
    {
        "ZEPHYR_P45_OPENSEARCH_URL",
        "ZEPHYR_P45_CLICKHOUSE_URL",
        "ZEPHYR_P45_LOKI_URL",
        "ZEPHYR_P45_MONGODB_URI",
    }
)


@dataclass(frozen=True, slots=True)
class P45RuntimePaths:
    repo_root: Path
    home: Path
    home_source: str
    env_dir: Path
    logs_dir: Path
    data_dir: Path
    compose_path: Path
    compose_source: str
    repo_compose_example: Path


@dataclass(frozen=True, slots=True)
class LoadedP45Env:
    values: Mapping[str, str]
    sources: Mapping[str, str]
    runtime_paths: P45RuntimePaths

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


def _normalize_env_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return None if normalized == "" else normalized


def _normalize_source_scope(source: str) -> str:
    return source.split(":", 1)[0] if ":" in source else source


def _coerce_path(raw_path: str, *, base_dir: Path) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path


def default_p45_home(*, user_home_path: Path | None = None) -> Path:
    home_base = Path.home() if user_home_path is None else user_home_path
    return home_base / DEFAULT_P45_HOME_RELATIVE


def _resolve_p45_home_with_source(
    *,
    repo_root_path: Path,
    environ: Mapping[str, str],
    user_home_path: Path | None = None,
) -> tuple[Path, str]:
    configured_home = _normalize_env_value(environ.get(P45_HOME_ENV_NAME))
    if configured_home is not None:
        return (
            _coerce_path(configured_home, base_dir=repo_root_path),
            f"env:{P45_HOME_ENV_NAME}",
        )
    return (default_p45_home(user_home_path=user_home_path), "default-home")


def resolve_p45_home(
    *,
    repo_root_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
    user_home_path: Path | None = None,
) -> Path:
    repo_path = repo_root() if repo_root_path is None else repo_root_path
    active_env = os.environ if environ is None else environ
    home, _ = _resolve_p45_home_with_source(
        repo_root_path=repo_path,
        environ=active_env,
        user_home_path=user_home_path,
    )
    return home


def resolve_p45_env_dir(
    *,
    repo_root_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
    user_home_path: Path | None = None,
) -> Path:
    return (
        resolve_p45_home(
            repo_root_path=repo_root_path,
            environ=environ,
            user_home_path=user_home_path,
        )
        / P45_RUNTIME_ENV_DIR_NAME
    )


def _resolve_p45_compose_with_source(
    *,
    repo_root_path: Path,
    home: Path,
) -> tuple[Path, str]:
    for file_name in P45_RUNTIME_COMPOSE_FILE_NAMES:
        candidate = home / file_name
        if candidate.exists():
            return (candidate, f"runtime-home:{candidate.name}")
    repo_compose = repo_root_path / P45_COMPOSE_FILE_NAME
    return (repo_compose, f"repo-fallback:{repo_compose.name}")


def resolve_p45_compose_path(
    *,
    repo_root_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
    user_home_path: Path | None = None,
) -> Path:
    runtime_paths = resolve_p45_runtime_paths(
        repo_root_path=repo_root_path,
        environ=environ,
        user_home_path=user_home_path,
    )
    return runtime_paths.compose_path


def resolve_p45_runtime_paths(
    *,
    repo_root_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
    user_home_path: Path | None = None,
) -> P45RuntimePaths:
    repo_path = repo_root() if repo_root_path is None else repo_root_path
    active_env = os.environ if environ is None else environ
    home, home_source = _resolve_p45_home_with_source(
        repo_root_path=repo_path,
        environ=active_env,
        user_home_path=user_home_path,
    )
    compose_path, compose_source = _resolve_p45_compose_with_source(
        repo_root_path=repo_path,
        home=home,
    )
    return P45RuntimePaths(
        repo_root=repo_path,
        home=home,
        home_source=home_source,
        env_dir=home / P45_RUNTIME_ENV_DIR_NAME,
        logs_dir=home / P45_RUNTIME_LOG_DIR_NAME,
        data_dir=home / P45_RUNTIME_DATA_DIR_NAME,
        compose_path=compose_path,
        compose_source=compose_source,
        repo_compose_example=repo_path / P45_COMPOSE_FILE_NAME,
    )


def default_compose_path(
    *,
    repo_root_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
    user_home_path: Path | None = None,
) -> Path:
    return resolve_p45_compose_path(
        repo_root_path=repo_root_path,
        environ=environ,
        user_home_path=user_home_path,
    )


def _runtime_env_file_paths(runtime_paths: P45RuntimePaths) -> tuple[Path, ...]:
    return tuple(runtime_paths.env_dir / file_name for file_name in P45_ENV_FILE_NAMES)


def _repo_fallback_env_file_paths(repo_root_path: Path) -> tuple[Path, ...]:
    return tuple(repo_root_path / file_name for file_name in P45_ENV_FILE_NAMES)


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


def _apply_env_file(
    *,
    merged: dict[str, str],
    sources: dict[str, str],
    path: Path,
    source_kind: str,
    allowed_existing_scopes: set[str],
) -> None:
    if not path.exists():
        return
    for name, value in parse_env_file(path).items():
        normalized = _normalize_env_value(value)
        if normalized is None:
            continue
        existing_scope = _normalize_source_scope(sources.get(name, "default"))
        if existing_scope not in allowed_existing_scopes:
            continue
        merged[name] = normalized
        sources[name] = f"{source_kind}:{path}"


def load_p45_env(
    *,
    repo_root_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
    user_home_path: Path | None = None,
) -> LoadedP45Env:
    runtime_paths = resolve_p45_runtime_paths(
        repo_root_path=repo_root_path,
        environ=environ,
        user_home_path=user_home_path,
    )
    merged: dict[str, str] = dict(DEFAULT_P45_ENV)
    sources: dict[str, str] = dict.fromkeys(merged, "default")
    for env_path in _runtime_env_file_paths(runtime_paths):
        _apply_env_file(
            merged=merged,
            sources=sources,
            path=env_path,
            source_kind="runtime-home",
            allowed_existing_scopes={"default", "runtime-home"},
        )
    for env_path in _repo_fallback_env_file_paths(runtime_paths.repo_root):
        _apply_env_file(
            merged=merged,
            sources=sources,
            path=env_path,
            source_kind="repo-fallback",
            allowed_existing_scopes={"default", "repo-fallback"},
        )
    active_env = os.environ if environ is None else environ
    for name, value in active_env.items():
        normalized = _normalize_env_value(value)
        if normalized is None:
            continue
        merged[name] = normalized
        sources[name] = "env"
    for canonical_name, alias_names in P45_ENV_ALIASES.items():
        if _normalize_env_value(merged.get(canonical_name)) is not None:
            continue
        for alias_name in alias_names:
            alias_value = _normalize_env_value(merged.get(alias_name))
            if alias_value is None:
                continue
            merged[canonical_name] = alias_value
            alias_source = sources.get(alias_name, "alias")
            sources[canonical_name] = f"{alias_source}|alias:{alias_name}"
            break
    return LoadedP45Env(values=merged, sources=sources, runtime_paths=runtime_paths)


def is_secret_env_name(name: str) -> bool:
    upper_name = name.upper()
    return upper_name in EXPLICIT_SECRET_CARRIER_ENV_NAMES or any(
        fragment in upper_name for fragment in SECRET_NAME_FRAGMENTS
    )


def redact_value(value: str) -> str:
    if value == "":
        return ""
    return "***"


def redact_mapping(values: Mapping[str, str]) -> dict[str, str]:
    redacted: dict[str, str] = {}
    for name, value in values.items():
        redacted[name] = redact_value(value) if is_secret_env_name(name) else value
    return redacted


def format_runtime_resolution(runtime_paths: P45RuntimePaths) -> str:
    return "\n".join(
        (
            "P4.5 runtime resolution:",
            f"- control plane repo: {runtime_paths.repo_root}",
            f"- runtime home: {runtime_paths.home} ({runtime_paths.home_source})",
            f"- runtime env dir: {runtime_paths.env_dir}",
            f"- runtime logs dir: {runtime_paths.logs_dir}",
            f"- runtime data dir: {runtime_paths.data_dir}",
            f"- compose file: {runtime_paths.compose_path} ({runtime_paths.compose_source})",
        )
    )


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


def _env_flag(raw_value: str | None, *, default: bool = False) -> bool:
    normalized = _normalize_env_value(raw_value)
    if normalized is None:
        return default
    return normalized.lower() in {"1", "true", "yes", "on"}


def get_p45_opensearch_url(env: LoadedP45Env) -> str:
    explicit_url = env.get("ZEPHYR_P45_OPENSEARCH_URL")
    if explicit_url is not None:
        return explicit_url.rstrip("/")
    host = env.get("ZEPHYR_P45_OPENSEARCH_HOST", "127.0.0.1") or "127.0.0.1"
    port = env.get("ZEPHYR_P45_OPENSEARCH_PORT", "19200") or "19200"
    scheme = env.get("ZEPHYR_P45_OPENSEARCH_SCHEME")
    if scheme is None:
        scheme = "https" if env.get("ZEPHYR_P45_OPENSEARCH_PASSWORD") is not None else "http"
    return f"{scheme}://{host}:{port}".rstrip("/")


def get_p45_opensearch_auth(env: LoadedP45Env) -> tuple[str, str] | None:
    username = env.get("ZEPHYR_P45_OPENSEARCH_USERNAME")
    password = env.get("ZEPHYR_P45_OPENSEARCH_PASSWORD")
    if username is None or password is None:
        return None
    return (username, password)


def get_p45_opensearch_verify_tls(env: LoadedP45Env) -> bool:
    raw_skip_verify = env.values.get("ZEPHYR_P45_OPENSEARCH_SKIP_TLS_VERIFY")
    if raw_skip_verify is not None:
        return not _env_flag(raw_skip_verify)
    parsed = urlsplit(get_p45_opensearch_url(env))
    return not (parsed.scheme == "https" and parsed.hostname in {"127.0.0.1", "localhost", "::1"})


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
    auth: tuple[str, str] | None = None
    verify_tls = True
    if service == "opensearch":
        base_url = get_p45_opensearch_url(env)
        url = f"{base_url.rstrip('/')}{probe.path}"
        auth = get_p45_opensearch_auth(env)
        verify_tls = get_p45_opensearch_verify_tls(env)
    try:
        response = httpx.get(
            url,
            timeout=timeout_s,
            trust_env=False,
            auth=auth,
            verify=verify_tls,
        )
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
