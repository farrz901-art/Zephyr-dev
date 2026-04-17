from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast
from urllib.parse import urlsplit

from zephyr_ingest.testing.p45 import (
    EXPLICIT_SECRET_CARRIER_ENV_NAMES,
    LoadedP45Env,
    check_services,
    format_probe_results,
    format_redacted_env_summary,
    format_runtime_resolution,
    get_p45_opensearch_url,
    get_p45_opensearch_verify_tls,
    redact_mapping,
    repo_root,
)

RuntimeContractSeverity = Literal["error", "warning", "info"]

P5_RUNTIME_CONTRACT_PATH: Final[Path] = repo_root() / "validation" / "p5_runtime_contract.json"
P5_RUNTIME_REPORT_PATH: Final[Path] = repo_root() / "validation" / "P5_M1_RUNTIME_CONTRACT.md"
P5_CANONICAL_RUNTIME_HOME: Final[Path] = Path(r"E:\zephyr_env\.config\zephyr\p45")
P5_REQUIRED_RUNTIME_DIRS: Final[tuple[str, ...]] = ("bin", "data", "env", "logs")
P5_CANONICAL_RUNTIME_FILES: Final[tuple[str, ...]] = (
    "docker-compose.yml",
    "env/.env.p45.local",
    "env/p45.local.env",
    "env/p45.gdrive.env",
    "env/p45.confluence.env",
)
P5_REPO_TEMPLATE_FILES: Final[tuple[str, ...]] = (
    ".env.example",
    ".env.p45.example",
    "docker-compose.p45-validation.yml",
)
P5_PROXY_ENV_NAMES: Final[tuple[str, ...]] = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)
P5_STARTUP_REQUIRED_GROUPS: Final[tuple[str, ...]] = (
    "runtime_identity",
    "postgresql_local_substrate",
    "kafka_redpanda_local_substrate",
    "s3_minio_local_substrate",
    "opensearch_local_substrate",
    "clickhouse_local_substrate",
    "mongodb_local_substrate",
    "loki_local_substrate",
    "weaviate_local_substrate",
    "http_fixture_local_surface",
    "webhook_echo_local_surface",
)
P5_VALIDATION_REQUIRED_GROUPS: Final[tuple[str, ...]] = (
    "google_drive_saas_auth",
    "confluence_saas_auth",
)
P5_ENV_GROUPS: Final[tuple[dict[str, object], ...]] = (
    {
        "name": "runtime_identity",
        "kind": "runtime-settings",
        "startup_required": True,
        "validation_required_for": ("preflight",),
        "required_names": ("ZEPHYR_P45_ENV", "ZEPHYR_P45_STACK_NAME"),
        "optional_names": (),
        "secret_names": (),
        "secret_carrier_names": (),
        "non_secret_names": ("ZEPHYR_P45_ENV", "ZEPHYR_P45_STACK_NAME"),
    },
    {
        "name": "postgresql_local_substrate",
        "kind": "local-substrate-auth",
        "startup_required": True,
        "validation_required_for": ("service-live",),
        "required_names": (
            "ZEPHYR_P45_POSTGRES_HOST",
            "ZEPHYR_P45_POSTGRES_PORT",
            "ZEPHYR_P45_POSTGRES_DB",
            "ZEPHYR_P45_POSTGRES_USER",
            "ZEPHYR_P45_POSTGRES_PASSWORD",
        ),
        "optional_names": (),
        "secret_names": ("ZEPHYR_P45_POSTGRES_PASSWORD",),
        "secret_carrier_names": (),
        "non_secret_names": (
            "ZEPHYR_P45_POSTGRES_HOST",
            "ZEPHYR_P45_POSTGRES_PORT",
            "ZEPHYR_P45_POSTGRES_DB",
            "ZEPHYR_P45_POSTGRES_USER",
        ),
    },
    {
        "name": "kafka_redpanda_local_substrate",
        "kind": "local-substrate-auth",
        "startup_required": True,
        "validation_required_for": ("service-live",),
        "required_names": (
            "ZEPHYR_P45_KAFKA_HOST",
            "ZEPHYR_P45_KAFKA_PORT",
            "ZEPHYR_P45_REDPANDA_ADMIN_HOST",
            "ZEPHYR_P45_REDPANDA_ADMIN_PORT",
            "ZEPHYR_P45_KAFKA_BROKERS",
            "ZEPHYR_P45_KAFKA_TOPIC",
        ),
        "optional_names": (),
        "secret_names": (),
        "secret_carrier_names": (),
        "non_secret_names": (
            "ZEPHYR_P45_KAFKA_HOST",
            "ZEPHYR_P45_KAFKA_PORT",
            "ZEPHYR_P45_REDPANDA_ADMIN_HOST",
            "ZEPHYR_P45_REDPANDA_ADMIN_PORT",
            "ZEPHYR_P45_KAFKA_BROKERS",
            "ZEPHYR_P45_KAFKA_TOPIC",
        ),
    },
    {
        "name": "s3_minio_local_substrate",
        "kind": "local-substrate-auth",
        "startup_required": True,
        "validation_required_for": ("service-live",),
        "required_names": (
            "ZEPHYR_P45_S3_HOST",
            "ZEPHYR_P45_S3_PORT",
            "ZEPHYR_P45_S3_ENDPOINT",
            "ZEPHYR_P45_S3_REGION",
            "ZEPHYR_P45_S3_ACCESS_KEY",
            "ZEPHYR_P45_S3_SECRET_KEY",
            "ZEPHYR_P45_S3_BUCKET",
        ),
        "optional_names": (),
        "secret_names": ("ZEPHYR_P45_S3_ACCESS_KEY", "ZEPHYR_P45_S3_SECRET_KEY"),
        "secret_carrier_names": (),
        "non_secret_names": (
            "ZEPHYR_P45_S3_HOST",
            "ZEPHYR_P45_S3_PORT",
            "ZEPHYR_P45_S3_ENDPOINT",
            "ZEPHYR_P45_S3_REGION",
            "ZEPHYR_P45_S3_BUCKET",
        ),
    },
    {
        "name": "opensearch_local_substrate",
        "kind": "local-substrate-auth",
        "startup_required": True,
        "validation_required_for": ("service-live",),
        "required_names": (
            "ZEPHYR_P45_OPENSEARCH_PORT",
            "ZEPHYR_P45_OPENSEARCH_URL",
            "ZEPHYR_P45_OPENSEARCH_INDEX",
            "ZEPHYR_P45_OPENSEARCH_USERNAME",
            "ZEPHYR_P45_OPENSEARCH_PASSWORD",
            "ZEPHYR_P45_OPENSEARCH_SKIP_TLS_VERIFY",
        ),
        "optional_names": ("ZEPHYR_P45_OPENSEARCH_HOST",),
        "secret_names": ("ZEPHYR_P45_OPENSEARCH_PASSWORD",),
        "secret_carrier_names": ("ZEPHYR_P45_OPENSEARCH_URL",),
        "non_secret_names": (
            "ZEPHYR_P45_OPENSEARCH_PORT",
            "ZEPHYR_P45_OPENSEARCH_INDEX",
            "ZEPHYR_P45_OPENSEARCH_USERNAME",
            "ZEPHYR_P45_OPENSEARCH_SKIP_TLS_VERIFY",
        ),
    },
    {
        "name": "clickhouse_local_substrate",
        "kind": "local-substrate-auth",
        "startup_required": True,
        "validation_required_for": ("service-live",),
        "required_names": (
            "ZEPHYR_P45_CLICKHOUSE_PORT",
            "ZEPHYR_P45_CLICKHOUSE_URL",
            "ZEPHYR_P45_CLICKHOUSE_DATABASE",
            "ZEPHYR_P45_CLICKHOUSE_TABLE",
            "ZEPHYR_P45_CLICKHOUSE_USERNAME",
            "ZEPHYR_P45_CLICKHOUSE_PASSWORD",
        ),
        "optional_names": ("ZEPHYR_P45_CLICKHOUSE_HOST",),
        "secret_names": ("ZEPHYR_P45_CLICKHOUSE_PASSWORD",),
        "secret_carrier_names": ("ZEPHYR_P45_CLICKHOUSE_URL",),
        "non_secret_names": (
            "ZEPHYR_P45_CLICKHOUSE_PORT",
            "ZEPHYR_P45_CLICKHOUSE_DATABASE",
            "ZEPHYR_P45_CLICKHOUSE_TABLE",
            "ZEPHYR_P45_CLICKHOUSE_USERNAME",
        ),
    },
    {
        "name": "mongodb_local_substrate",
        "kind": "local-substrate-auth",
        "startup_required": True,
        "validation_required_for": ("service-live",),
        "required_names": (
            "ZEPHYR_P45_MONGODB_PORT",
            "ZEPHYR_P45_MONGODB_URI",
            "ZEPHYR_P45_MONGODB_DATABASE",
            "ZEPHYR_P45_MONGODB_COLLECTION",
        ),
        "optional_names": (
            "ZEPHYR_P45_MONGODB_HOST",
            "ZEPHYR_P45_MONGODB_USERNAME",
            "ZEPHYR_P45_MONGODB_PASSWORD",
        ),
        "secret_names": ("ZEPHYR_P45_MONGODB_PASSWORD",),
        "secret_carrier_names": ("ZEPHYR_P45_MONGODB_URI",),
        "non_secret_names": (
            "ZEPHYR_P45_MONGODB_PORT",
            "ZEPHYR_P45_MONGODB_DATABASE",
            "ZEPHYR_P45_MONGODB_COLLECTION",
            "ZEPHYR_P45_MONGODB_USERNAME",
        ),
    },
    {
        "name": "loki_local_substrate",
        "kind": "local-substrate-auth",
        "startup_required": True,
        "validation_required_for": ("service-live",),
        "required_names": (
            "ZEPHYR_P45_LOKI_PORT",
            "ZEPHYR_P45_LOKI_URL",
            "ZEPHYR_P45_LOKI_STREAM",
        ),
        "optional_names": ("ZEPHYR_P45_LOKI_HOST", "ZEPHYR_P45_LOKI_TENANT_ID"),
        "secret_names": ("ZEPHYR_P45_LOKI_TENANT_ID",),
        "secret_carrier_names": ("ZEPHYR_P45_LOKI_URL",),
        "non_secret_names": (
            "ZEPHYR_P45_LOKI_PORT",
            "ZEPHYR_P45_LOKI_STREAM",
            "ZEPHYR_P45_LOKI_HOST",
        ),
    },
    {
        "name": "weaviate_local_substrate",
        "kind": "local-substrate-auth",
        "startup_required": True,
        "validation_required_for": ("service-live",),
        "required_names": (
            "ZEPHYR_P45_WEAVIATE_PORT",
            "ZEPHYR_P45_WEAVIATE_GRPC_PORT",
            "ZEPHYR_P45_WEAVIATE_HTTP_HOST",
            "ZEPHYR_P45_WEAVIATE_HTTP_PORT",
            "ZEPHYR_P45_WEAVIATE_GRPC_HOST",
            "ZEPHYR_P45_WEAVIATE_GRPC_PORT",
            "ZEPHYR_P45_WEAVIATE_COLLECTION",
            "ZEPHYR_P45_WEAVIATE_API_KEY",
        ),
        "optional_names": ("ZEPHYR_P45_WEAVIATE_HOST",),
        "secret_names": ("ZEPHYR_P45_WEAVIATE_API_KEY",),
        "secret_carrier_names": (),
        "non_secret_names": (
            "ZEPHYR_P45_WEAVIATE_PORT",
            "ZEPHYR_P45_WEAVIATE_GRPC_PORT",
            "ZEPHYR_P45_WEAVIATE_HTTP_HOST",
            "ZEPHYR_P45_WEAVIATE_HTTP_PORT",
            "ZEPHYR_P45_WEAVIATE_GRPC_HOST",
            "ZEPHYR_P45_WEAVIATE_COLLECTION",
            "ZEPHYR_P45_WEAVIATE_HOST",
        ),
    },
    {
        "name": "http_fixture_local_surface",
        "kind": "local-runtime-surface",
        "startup_required": True,
        "validation_required_for": ("local-real",),
        "required_names": (
            "ZEPHYR_P45_HTTP_FIXTURE_HOST",
            "ZEPHYR_P45_HTTP_FIXTURE_PORT",
            "ZEPHYR_P45_HTTP_FIXTURE_BASE",
        ),
        "optional_names": (),
        "secret_names": (),
        "secret_carrier_names": (),
        "non_secret_names": (
            "ZEPHYR_P45_HTTP_FIXTURE_HOST",
            "ZEPHYR_P45_HTTP_FIXTURE_PORT",
            "ZEPHYR_P45_HTTP_FIXTURE_BASE",
        ),
    },
    {
        "name": "webhook_echo_local_surface",
        "kind": "local-runtime-surface",
        "startup_required": True,
        "validation_required_for": ("local-real",),
        "required_names": (
            "ZEPHYR_P45_WEBHOOK_ECHO_HOST",
            "ZEPHYR_P45_WEBHOOK_ECHO_PORT",
            "ZEPHYR_P45_WEBHOOK_ECHO_URL",
        ),
        "optional_names": (),
        "secret_names": (),
        "secret_carrier_names": (),
        "non_secret_names": (
            "ZEPHYR_P45_WEBHOOK_ECHO_HOST",
            "ZEPHYR_P45_WEBHOOK_ECHO_PORT",
            "ZEPHYR_P45_WEBHOOK_ECHO_URL",
        ),
    },
    {
        "name": "google_drive_saas_auth",
        "kind": "saas-auth",
        "startup_required": False,
        "validation_required_for": ("saas-live",),
        "required_names": (
            "ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID",
            "ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN",
        ),
        "optional_names": (
            "ZEPHYR_P45_GOOGLE_DRIVE_DRIVE_ID",
            "ZEPHYR_P45_GOOGLE_DRIVE_EXPORT_MIME_TYPE",
        ),
        "secret_names": ("ZEPHYR_P45_GOOGLE_DRIVE_ACCESS_TOKEN",),
        "secret_carrier_names": (),
        "non_secret_names": (
            "ZEPHYR_P45_GOOGLE_DRIVE_FILE_ID",
            "ZEPHYR_P45_GOOGLE_DRIVE_DRIVE_ID",
            "ZEPHYR_P45_GOOGLE_DRIVE_EXPORT_MIME_TYPE",
        ),
    },
    {
        "name": "confluence_saas_auth",
        "kind": "saas-auth",
        "startup_required": False,
        "validation_required_for": ("saas-live",),
        "required_names": (
            "ZEPHYR_P45_CONFLUENCE_SITE_URL",
            "ZEPHYR_P45_CONFLUENCE_PAGE_ID",
            "ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN",
        ),
        "optional_names": (
            "ZEPHYR_P45_CONFLUENCE_SPACE_KEY",
            "ZEPHYR_P45_CONFLUENCE_PAGE_VERSION",
            "ZEPHYR_P45_CONFLUENCE_EMAIL",
        ),
        "secret_names": ("ZEPHYR_P45_CONFLUENCE_ACCESS_TOKEN",),
        "secret_carrier_names": (),
        "non_secret_names": (
            "ZEPHYR_P45_CONFLUENCE_SITE_URL",
            "ZEPHYR_P45_CONFLUENCE_PAGE_ID",
            "ZEPHYR_P45_CONFLUENCE_SPACE_KEY",
            "ZEPHYR_P45_CONFLUENCE_PAGE_VERSION",
            "ZEPHYR_P45_CONFLUENCE_EMAIL",
        ),
    },
)


@dataclass(frozen=True, slots=True)
class P5RuntimeContractCheck:
    name: str
    ok: bool
    detail: str
    severity: RuntimeContractSeverity = "error"


def build_p5_runtime_contract() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M1 production runtime contract",
        "runtime_home_env_var": "ZEPHYR_P45_HOME",
        "current_verified_runtime_home": str(P5_CANONICAL_RUNTIME_HOME),
        "current_verified_env_dir": str(P5_CANONICAL_RUNTIME_HOME / "env"),
        "current_verified_compose_path": str(P5_CANONICAL_RUNTIME_HOME / "docker-compose.yml"),
        "control_plane": {
            "owner": "repo",
            "repo_template_files": list(P5_REPO_TEMPLATE_FILES),
            "notes": [
                (
                    "Code, tests, helpers, validation tooling, templates, and audit artifacts "
                    "remain repo-owned."
                ),
                (
                    "Repo compose/env examples are templates/examples only; they are not "
                    "canonical live runtime files."
                ),
            ],
        },
        "runtime_plane": {
            "owner": "external-runtime-home",
            "required_dirs": list(P5_REQUIRED_RUNTIME_DIRS),
            "canonical_files": list(P5_CANONICAL_RUNTIME_FILES),
            "notes": [
                (
                    "Current verified runtime layout is Windows absolute-path coupled and external "
                    "to the repo."
                ),
                (
                    "Real env, compose, logs, data, and runtime-adjacent files remain outside "
                    "the repo."
                ),
            ],
        },
        "env_groups": [dict(group) for group in P5_ENV_GROUPS],
        "redaction_contract": {
            "secret_name_fragments": [
                "ACCESS_KEY",
                "ACCESS_TOKEN",
                "API_KEY",
                "PASSWORD",
                "SECRET",
                "SESSION_TOKEN",
                "TOKEN",
                "_DSN",
                "_URI",
            ],
            "explicit_secret_carrier_env_names": sorted(EXPLICIT_SECRET_CARRIER_ENV_NAMES),
            "rule": (
                "Secret values and explicit secret-carrier variables must be fully redacted in "
                "summaries, reports, and preflight output."
            ),
        },
        "startup_preflight_contract": {
            "startup_required_groups": list(P5_STARTUP_REQUIRED_GROUPS),
            "validation_required_groups": list(P5_VALIDATION_REQUIRED_GROUPS),
            "repo_side_healthcheck_tool": "tools/p45_substrate_healthcheck.py",
            "notes": [
                (
                    "Current readiness is primarily validated through repo-side healthcheck "
                    "tooling rather than compose-native healthcheck blocks."
                ),
                (
                    "Lifecycle closure was verified manually before P5-M1 and is now made "
                    "inspectable through repo-side preflight and health tooling."
                ),
            ],
        },
        "network_contract": {
            "trust_env_false_required": True,
            "proxy_env_expected_absent_in_checked_terminal": True,
            "opensearch_local_https_skip_tls_verify": True,
            "google_drive_depends_on_tun_or_vpn_or_network_condition": True,
        },
        "deferred_realities": [
            "No cloud/Kubernetes/Helm packaging layer is claimed here.",
            (
                "No deployment matrix beyond the current external runtime-home contract is claimed "
                "here."
            ),
            (
                "Windows absolute-path coupling is current reality, not a permanent cross-platform "
                "architecture promise."
            ),
            (
                "This milestone does not redesign queue/lock/runtime ownership or invent a "
                "broader deployment platform."
            ),
        ],
    }


def iter_contract_env_names() -> tuple[str, ...]:
    names: list[str] = []
    for group in P5_ENV_GROUPS:
        for field_name in (
            "required_names",
            "optional_names",
            "secret_names",
            "secret_carrier_names",
            "non_secret_names",
        ):
            values = group[field_name]
            assert isinstance(values, tuple)
            typed_values = cast(tuple[str, ...], values)
            for name in typed_values:
                names.append(name)
    return tuple(sorted(set(names)))


def _missing_env_names(env: LoadedP45Env, names: Sequence[str]) -> tuple[str, ...]:
    missing: list[str] = []
    for name in names:
        if env.get(name) is None:
            missing.append(name)
    return tuple(missing)


def _repo_fallback_env_names(env: LoadedP45Env, names: Sequence[str]) -> tuple[str, ...]:
    repo_fallback: list[str] = []
    for name in names:
        if env.sources.get(name, "").startswith("repo-fallback"):
            repo_fallback.append(name)
    return tuple(repo_fallback)


def validate_p5_runtime_contract(
    env: LoadedP45Env,
    *,
    environ: Mapping[str, str] | None = None,
    expected_runtime_home: Path | None = None,
) -> list[P5RuntimeContractCheck]:
    active_env = os.environ if environ is None else environ
    runtime_paths = env.runtime_paths
    checks: list[P5RuntimeContractCheck] = []
    resolved_home = runtime_paths.home.resolve()
    repo_path = runtime_paths.repo_root.resolve()

    checks.append(
        P5RuntimeContractCheck(
            name="runtime_home_external_to_repo",
            ok=resolved_home != repo_path and repo_path not in resolved_home.parents,
            detail=f"resolved home={resolved_home}; repo={repo_path}",
        )
    )
    if expected_runtime_home is not None:
        checks.append(
            P5RuntimeContractCheck(
                name="runtime_home_matches_current_verified_home",
                ok=resolved_home == expected_runtime_home.resolve(),
                detail=f"resolved home={resolved_home}; expected={expected_runtime_home.resolve()}",
            )
        )
    checks.append(
        P5RuntimeContractCheck(
            name="runtime_home_is_absolute_windows_coupled_path",
            ok=runtime_paths.home.is_absolute(),
            detail=f"runtime home={runtime_paths.home}",
            severity="info",
        )
    )

    for dir_name in P5_REQUIRED_RUNTIME_DIRS:
        dir_path = runtime_paths.home / dir_name
        checks.append(
            P5RuntimeContractCheck(
                name=f"runtime_dir:{dir_name}",
                ok=dir_path.is_dir(),
                detail=str(dir_path),
            )
        )

    checks.append(
        P5RuntimeContractCheck(
            name="canonical_compose_path",
            ok=runtime_paths.compose_path.exists()
            and runtime_paths.compose_source == "runtime-home:docker-compose.yml",
            detail=f"{runtime_paths.compose_path} ({runtime_paths.compose_source})",
        )
    )

    for relative_path in P5_CANONICAL_RUNTIME_FILES:
        file_path = runtime_paths.home / Path(relative_path)
        checks.append(
            P5RuntimeContractCheck(
                name=f"runtime_file:{relative_path}",
                ok=file_path.exists(),
                detail=str(file_path),
            )
        )

    for group in P5_ENV_GROUPS:
        group_name = group["name"]
        required_names = group["required_names"]
        assert isinstance(group_name, str)
        assert isinstance(required_names, tuple)
        required_name_tuple = cast(tuple[str, ...], required_names)
        missing = _missing_env_names(env, required_name_tuple)
        checks.append(
            P5RuntimeContractCheck(
                name=f"env_group:{group_name}",
                ok=not missing,
                detail="missing=" + (", ".join(missing) if missing else "none"),
            )
        )
        repo_fallback = _repo_fallback_env_names(env, required_name_tuple)
        checks.append(
            P5RuntimeContractCheck(
                name=f"env_group:{group_name}:repo_fallback",
                ok=not repo_fallback,
                detail="repo-fallback=" + (", ".join(repo_fallback) if repo_fallback else "none"),
            )
        )

    present_proxy_env = [
        name for name in P5_PROXY_ENV_NAMES if active_env.get(name, "").strip() != ""
    ]
    checks.append(
        P5RuntimeContractCheck(
            name="proxy_env_absent",
            ok=not present_proxy_env,
            detail="present="
            + (", ".join(sorted(present_proxy_env)) if present_proxy_env else "none"),
        )
    )

    opensearch_url = get_p45_opensearch_url(env)
    verify_tls = get_p45_opensearch_verify_tls(env)
    checks.append(
        P5RuntimeContractCheck(
            name="opensearch_https_skip_tls_verify_local_reality",
            ok=urlsplit(opensearch_url).scheme == "https" and not verify_tls,
            detail=f"url={opensearch_url}; verify_tls={verify_tls}",
        )
    )

    return checks


def format_p5_runtime_contract_summary() -> str:
    return "\n".join(
        (
            "P5-M1 runtime contract:",
            "- runtime home env var: ZEPHYR_P45_HOME",
            f"- current verified runtime home: {P5_CANONICAL_RUNTIME_HOME}",
            f"- canonical runtime files: {', '.join(P5_CANONICAL_RUNTIME_FILES)}",
            f"- startup-required env groups: {', '.join(P5_STARTUP_REQUIRED_GROUPS)}",
            f"- validation-required env groups: {', '.join(P5_VALIDATION_REQUIRED_GROUPS)}",
            (
                "- network realities: "
                "trust_env_false=True, "
                "proxy_absent=True, "
                "opensearch_https_skip_tls_verify=True, "
                "gdrive_depends_on_tun_or_vpn=True"
            ),
        )
    )


def format_p5_preflight_results(results: Sequence[P5RuntimeContractCheck]) -> str:
    lines = ["P5-M1 runtime preflight:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def render_p5_preflight_report(
    env: LoadedP45Env,
    *,
    checks: Sequence[P5RuntimeContractCheck],
    include_env: bool,
    include_health: bool,
    health_tier: Literal["all", "local-real", "service-live"],
) -> str:
    lines = [
        format_runtime_resolution(env.runtime_paths),
        format_p5_runtime_contract_summary(),
        format_p5_preflight_results(checks),
    ]
    if include_env:
        lines.append(format_redacted_env_summary(env, names=iter_contract_env_names()))
    if include_health:
        tier = None if health_tier == "all" else health_tier
        results = check_services(env, tier=tier)
        lines.append(format_probe_results(results))
    return "\n\n".join(lines)


def render_p5_preflight_json(
    env: LoadedP45Env,
    *,
    checks: Sequence[P5RuntimeContractCheck],
    include_env: bool,
    include_health: bool,
    health_tier: Literal["all", "local-real", "service-live"],
) -> str:
    payload: dict[str, object] = {
        "runtime_paths": {
            "home": str(env.runtime_paths.home),
            "env_dir": str(env.runtime_paths.env_dir),
            "compose_path": str(env.runtime_paths.compose_path),
            "compose_source": env.runtime_paths.compose_source,
        },
        "contract": build_p5_runtime_contract(),
        "checks": [
            {
                "name": check.name,
                "ok": check.ok,
                "detail": check.detail,
                "severity": check.severity,
            }
            for check in checks
        ],
    }
    if include_env:
        selected = {name: env.values.get(name, "") for name in iter_contract_env_names()}
        payload["redacted_env"] = redact_mapping(selected)
    if include_health:
        tier = None if health_tier == "all" else health_tier
        payload["health"] = [
            {
                "service": result.service,
                "probe": result.probe,
                "ok": result.ok,
                "detail": result.detail,
            }
            for result in check_services(env, tier=tier)
        ]
    return json.dumps(payload, ensure_ascii=False, indent=2)
