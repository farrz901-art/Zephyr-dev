from __future__ import annotations

import argparse
import importlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol, cast

import httpx

from zephyr_ingest.testing.p45 import (
    DEFAULT_P45_ENV,
    P45_ENV_ALIASES,
    P45_HOME_ENV_NAME,
    LoadedP45Env,
    parse_env_file,
    repo_root,
    resolve_p45_runtime_paths,
)

FinalCategory = Literal[
    "bucket_missing",
    "object_missing",
    "auth_failed",
    "endpoint_unreachable",
    "permission_failed",
    "ready",
    "unknown",
]

SOURCE_KEY_ENV_CANDIDATES: tuple[str, ...] = (
    "ZEPHYR_P45_S3_SOURCE_KEY",
    "ZEPHYR_P45_UNS_S3_SOURCE_KEY",
    "ZEPHYR_P45_S3_OBJECT_KEY",
)

AUTH_ERROR_CODES: frozenset[str] = frozenset(
    {
        "AccessDenied",
        "AuthFailure",
        "AuthorizationHeaderMalformed",
        "ExpiredToken",
        "InvalidAccessKeyId",
        "InvalidClientTokenId",
        "InvalidSecurity",
        "RequestExpired",
        "SignatureDoesNotMatch",
        "TokenRefreshRequired",
    }
)

PERMISSION_ERROR_CODES: frozenset[str] = frozenset(
    {
        "AccessDenied",
        "AllAccessDisabled",
        "UnauthorizedAccess",
    }
)


class S3AdminClientProtocol(Protocol):
    def list_buckets(self) -> dict[str, object]: ...

    def head_bucket(self, *, Bucket: str) -> dict[str, object]: ...

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class DiagnoseResult:
    endpoint_reachable: bool
    endpoint_probe_detail: str
    credentials_accepted: bool
    list_buckets_ok: bool
    available_buckets: list[str]
    expected_bucket_exists: bool
    source_object_key_checked: bool
    source_object_key_present: bool
    source_object_key_exists: bool
    final_category: FinalCategory
    recommended_action: str
    error_detail: str


def _repo_root() -> Path:
    return repo_root()


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
        repo_root_path=_repo_root(),
        environ={P45_HOME_ENV_NAME: str(env_file.parent.parent)},
    )
    return LoadedP45Env(values=merged, sources=sources, runtime_paths=runtime_paths)


def _safe_bool_env_present(env: LoadedP45Env, name: str) -> bool:
    return env.get(name) is not None


def _effective_endpoint_url(env: LoadedP45Env) -> str:
    endpoint_url = env.get("ZEPHYR_P45_S3_ENDPOINT")
    if endpoint_url is not None:
        return endpoint_url.rstrip("/")
    host = env.get("ZEPHYR_P45_S3_HOST", "127.0.0.1") or "127.0.0.1"
    port = env.get("ZEPHYR_P45_S3_PORT", "19000") or "19000"
    return f"http://{host}:{port}"


def _source_object_key(env: LoadedP45Env) -> str | None:
    for name in SOURCE_KEY_ENV_CANDIDATES:
        value = env.get(name)
        if value is not None:
            return value
    return None


def _http_endpoint_probe(endpoint_url: str) -> tuple[bool, str]:
    safe_url = endpoint_url
    try:
        response = httpx.get(
            safe_url,
            timeout=3.0,
            trust_env=False,
            follow_redirects=False,
        )
    except httpx.HTTPError as exc:
        return False, f"http_probe_failed:{type(exc).__name__}"
    return True, f"http_status:{response.status_code}"


def _import_boto3_client(env: LoadedP45Env) -> S3AdminClientProtocol:
    boto3 = importlib.import_module("boto3")
    session_module = getattr(boto3, "session")
    session_cls = getattr(session_module, "Session")
    session = session_cls(
        aws_access_key_id=env.require("ZEPHYR_P45_S3_ACCESS_KEY"),
        aws_secret_access_key=env.require("ZEPHYR_P45_S3_SECRET_KEY"),
        aws_session_token=env.get("ZEPHYR_P45_S3_SESSION_TOKEN"),
        region_name=env.get("ZEPHYR_P45_S3_REGION", "us-east-1") or "us-east-1",
    )
    client = session.client(
        "s3",
        endpoint_url=_effective_endpoint_url(env),
        region_name=env.get("ZEPHYR_P45_S3_REGION", "us-east-1") or "us-east-1",
    )
    return cast(S3AdminClientProtocol, client)


def _extract_error_code(exc: Exception) -> str | None:
    response_obj = getattr(exc, "response", None)
    if not isinstance(response_obj, dict):
        return None
    response = cast(dict[str, object], response_obj)
    error_obj = response.get("Error")
    if not isinstance(error_obj, dict):
        return None
    error = cast(dict[str, object], error_obj)
    code_obj = error.get("Code")
    return code_obj if isinstance(code_obj, str) else None


def _diagnose_with_client(
    *,
    client: S3AdminClientProtocol,
    bucket: str,
    source_key: str | None,
) -> DiagnoseResult:
    try:
        buckets_obj = client.list_buckets()
    except Exception as exc:
        error_code = _extract_error_code(exc)
        if error_code in AUTH_ERROR_CODES:
            return DiagnoseResult(
                endpoint_reachable=True,
                endpoint_probe_detail="boto3_request_reached_endpoint",
                credentials_accepted=False,
                list_buckets_ok=False,
                available_buckets=[],
                expected_bucket_exists=False,
                source_object_key_checked=False,
                source_object_key_present=source_key is not None,
                source_object_key_exists=False,
                final_category="auth_failed",
                recommended_action=(
                    "Verify S3 access key, secret key, session token, and signer compatibility."
                ),
                error_detail=f"list_buckets_failed:{error_code or type(exc).__name__}",
            )
        return DiagnoseResult(
            endpoint_reachable=True,
            endpoint_probe_detail="boto3_request_reached_endpoint",
            credentials_accepted=False,
            list_buckets_ok=False,
            available_buckets=[],
            expected_bucket_exists=False,
            source_object_key_checked=False,
            source_object_key_present=source_key is not None,
            source_object_key_exists=False,
            final_category="unknown",
            recommended_action=(
                "Inspect S3-compatible service error details; this is not yet a connector logic "
                "claim."
            ),
            error_detail=f"list_buckets_failed:{error_code or type(exc).__name__}",
        )

    raw_buckets = buckets_obj.get("Buckets", [])
    available_buckets: list[str] = []
    if isinstance(raw_buckets, list):
        typed_buckets = cast(list[object], raw_buckets)
        for bucket_obj in typed_buckets:
            if not isinstance(bucket_obj, dict):
                continue
            name_obj = cast(dict[str, object], bucket_obj).get("Name")
            if isinstance(name_obj, str):
                available_buckets.append(name_obj)
    bucket_exists = bucket in available_buckets
    if not bucket_exists:
        return DiagnoseResult(
            endpoint_reachable=True,
            endpoint_probe_detail="boto3_request_reached_endpoint",
            credentials_accepted=True,
            list_buckets_ok=True,
            available_buckets=available_buckets,
            expected_bucket_exists=False,
            source_object_key_checked=False,
            source_object_key_present=source_key is not None,
            source_object_key_exists=False,
            final_category="bucket_missing",
            recommended_action=(
                "Create the configured bucket in the runtime substrate, then rerun readiness. "
                "No connector code change is indicated by this diagnosis."
            ),
            error_detail="bucket_missing",
        )

    if source_key is None:
        return DiagnoseResult(
            endpoint_reachable=True,
            endpoint_probe_detail="boto3_request_reached_endpoint",
            credentials_accepted=True,
            list_buckets_ok=True,
            available_buckets=available_buckets,
            expected_bucket_exists=True,
            source_object_key_checked=False,
            source_object_key_present=False,
            source_object_key_exists=False,
            final_category="ready",
            recommended_action=(
                "Bucket substrate is ready. If you want to validate the UNS S3 source path next, "
                "seed a test object key and point the source spec at it."
            ),
            error_detail="no_static_source_key_configured",
        )

    try:
        client.head_object(Bucket=bucket, Key=source_key)
    except Exception as exc:
        error_code = _extract_error_code(exc)
        if error_code in {"NoSuchKey", "404", "NotFound"}:
            return DiagnoseResult(
                endpoint_reachable=True,
                endpoint_probe_detail="boto3_request_reached_endpoint",
                credentials_accepted=True,
                list_buckets_ok=True,
                available_buckets=available_buckets,
                expected_bucket_exists=True,
                source_object_key_checked=True,
                source_object_key_present=True,
                source_object_key_exists=False,
                final_category="object_missing",
                recommended_action=(
                    "Seed the configured source object key into the existing bucket before "
                    "running the S3 source path."
                ),
                error_detail=f"head_object_failed:{error_code}",
            )
        if error_code in PERMISSION_ERROR_CODES:
            return DiagnoseResult(
                endpoint_reachable=True,
                endpoint_probe_detail="boto3_request_reached_endpoint",
                credentials_accepted=True,
                list_buckets_ok=True,
                available_buckets=available_buckets,
                expected_bucket_exists=True,
                source_object_key_checked=True,
                source_object_key_present=True,
                source_object_key_exists=False,
                final_category="permission_failed",
                recommended_action=(
                    "Bucket exists, but object-level permission is insufficient. Fix runtime "
                    "policy/credentials before blaming connector logic."
                ),
                error_detail=f"head_object_failed:{error_code}",
            )
        return DiagnoseResult(
            endpoint_reachable=True,
            endpoint_probe_detail="boto3_request_reached_endpoint",
            credentials_accepted=True,
            list_buckets_ok=True,
            available_buckets=available_buckets,
            expected_bucket_exists=True,
            source_object_key_checked=True,
            source_object_key_present=True,
            source_object_key_exists=False,
            final_category="unknown",
            recommended_action=(
                "Inspect object-level S3 service error details before changing connector code."
            ),
            error_detail=f"head_object_failed:{error_code or type(exc).__name__}",
        )

    return DiagnoseResult(
        endpoint_reachable=True,
        endpoint_probe_detail="boto3_request_reached_endpoint",
        credentials_accepted=True,
        list_buckets_ok=True,
        available_buckets=available_buckets,
        expected_bucket_exists=True,
        source_object_key_checked=True,
        source_object_key_present=True,
        source_object_key_exists=True,
        final_category="ready",
        recommended_action=(
            "S3 substrate is ready. Remaining failures would need to come from later execution "
            "stages, not basic readiness."
        ),
        error_detail="ready",
    )


def _diagnose_s3(env: LoadedP45Env) -> tuple[bool, DiagnoseResult]:
    endpoint_url = _effective_endpoint_url(env)
    endpoint_reachable, endpoint_probe_detail = _http_endpoint_probe(endpoint_url)
    source_key = _source_object_key(env)
    try:
        client = _import_boto3_client(env)
    except ImportError as exc:
        return False, DiagnoseResult(
            endpoint_reachable=endpoint_reachable,
            endpoint_probe_detail=endpoint_probe_detail,
            credentials_accepted=False,
            list_buckets_ok=False,
            available_buckets=[],
            expected_bucket_exists=False,
            source_object_key_checked=False,
            source_object_key_present=source_key is not None,
            source_object_key_exists=False,
            final_category="unknown",
            recommended_action=(
                "boto3 is not importable in the locked runtime; do not treat this as a "
                "connector logic failure."
            ),
            error_detail=f"boto3_import_failed:{type(exc).__name__}",
        )
    if not endpoint_reachable:
        return True, DiagnoseResult(
            endpoint_reachable=False,
            endpoint_probe_detail=endpoint_probe_detail,
            credentials_accepted=False,
            list_buckets_ok=False,
            available_buckets=[],
            expected_bucket_exists=False,
            source_object_key_checked=False,
            source_object_key_present=source_key is not None,
            source_object_key_exists=False,
            final_category="endpoint_unreachable",
            recommended_action=(
                "Bring up or expose the configured S3-compatible endpoint, then rerun readiness."
            ),
            error_detail=endpoint_probe_detail,
        )
    result = _diagnose_with_client(
        client=client,
        bucket=env.require("ZEPHYR_P45_S3_BUCKET"),
        source_key=source_key,
    )
    return True, result


def _build_bucket_create_commands(*, out_root: Path) -> Path:
    commands_path = out_root / "s3_bucket_create_commands.ps1"
    content = "\n".join(
        (
            "# P5.1 M4-A S3 bucket creation suggestions",
            "# Do not run unless you explicitly want to initialize the runtime substrate.",
            "",
            "# Option A: AWS CLI",
            'aws --endpoint-url $env:ZEPHYR_P45_S3_ENDPOINT s3 mb "s3://$env:ZEPHYR_P45_S3_BUCKET"',
            "",
            "# Option B: Python / boto3",
            "$script = @'",
            "import os",
            "import boto3",
            "",
            "session = boto3.session.Session(",
            "    aws_access_key_id=os.environ['ZEPHYR_P45_S3_ACCESS_KEY'],",
            "    aws_secret_access_key=os.environ['ZEPHYR_P45_S3_SECRET_KEY'],",
            "    aws_session_token=os.environ.get('ZEPHYR_P45_S3_SESSION_TOKEN'),",
            "    region_name=os.environ.get('ZEPHYR_P45_S3_REGION', 'us-east-1'),",
            ")",
            "client = session.client(",
            "    's3',",
            "    endpoint_url=os.environ['ZEPHYR_P45_S3_ENDPOINT'],",
            "    region_name=os.environ.get('ZEPHYR_P45_S3_REGION', 'us-east-1'),",
            ")",
            "client.create_bucket(Bucket=os.environ['ZEPHYR_P45_S3_BUCKET'])",
            "print('bucket created')",
            "'@",
            "$script | uv run --locked --no-sync python -",
            "",
            "# Optional next source seed suggestion after bucket creation",
            "$seedKey = 'p5_1_uns_s3_seed/document.txt'",
            (
                "# Upload a small marker-bearing text object under $seedKey before running "
                "source.uns.s3_document.v1"
            ),
        )
    )
    commands_path.write_text(content + "\n", encoding="utf-8")
    return commands_path


def build_report(*, env_file: Path, out_root: Path) -> dict[str, object]:
    env = _load_env_values(env_file)
    out_root.mkdir(parents=True, exist_ok=True)
    boto3_importable, diagnose = _diagnose_s3(env)
    commands_path: str | None = None
    if diagnose.final_category == "bucket_missing":
        commands_path = _build_bucket_create_commands(out_root=out_root).as_posix()
    endpoint_url = _effective_endpoint_url(env)
    bucket = env.require("ZEPHYR_P45_S3_BUCKET")
    region = env.get("ZEPHYR_P45_S3_REGION", "us-east-1") or "us-east-1"
    source_key = _source_object_key(env)
    return {
        "catalog_id": "zephyr.p5.1.s3_readiness_diagnose.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "generated_from": "tools/p5_s3_readiness_diagnose.py",
        "env_file": env_file.as_posix(),
        "runtime_home": str(env.runtime_paths.home),
        "secrets_redacted": True,
        "endpoint_url": endpoint_url,
        "bucket": bucket,
        "region": region,
        "access_key_present": _safe_bool_env_present(env, "ZEPHYR_P45_S3_ACCESS_KEY"),
        "secret_key_present": _safe_bool_env_present(env, "ZEPHYR_P45_S3_SECRET_KEY"),
        "session_token_present": _safe_bool_env_present(env, "ZEPHYR_P45_S3_SESSION_TOKEN"),
        "boto3_import": boto3_importable,
        "endpoint_reachable": diagnose.endpoint_reachable,
        "credentials_accepted": diagnose.credentials_accepted,
        "list_buckets_ok": diagnose.list_buckets_ok,
        "expected_bucket": bucket,
        "available_buckets": diagnose.available_buckets,
        "bucket_exists": diagnose.expected_bucket_exists,
        "source_object_key_checked": diagnose.source_object_key_checked,
        "source_key_present": source_key is not None,
        "source_key_exists": diagnose.source_object_key_exists,
        "final_category": diagnose.final_category,
        "recommended_action": diagnose.recommended_action,
        "endpoint_probe_detail": diagnose.endpoint_probe_detail,
        "error_detail": diagnose.error_detail,
        "bucket_create_commands_path": commands_path,
        "notes": [
            "readiness pass does not imply product direct pass",
            (
                "S3 readiness failure is diagnosed as substrate/auth/object state until proven "
                "otherwise"
            ),
            "no secrets are printed by this tool",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    lines = [
        "# P5.1 S3 Readiness Diagnose",
        "",
        f"- env_file: `{report['env_file']}`",
        f"- endpoint_url: `{report['endpoint_url']}`",
        f"- bucket: `{report['bucket']}`",
        f"- region: `{report['region']}`",
        f"- access_key_present: {report['access_key_present']}",
        f"- secret_key_present: {report['secret_key_present']}",
        f"- session_token_present: {report['session_token_present']}",
        f"- boto3_import: {report['boto3_import']}",
        f"- endpoint_reachable: {report['endpoint_reachable']}",
        f"- credentials_accepted: {report['credentials_accepted']}",
        f"- list_buckets_ok: {report['list_buckets_ok']}",
        (
            "- available_buckets: "
            f"{', '.join(cast(list[str], report['available_buckets'])) or '(none)'}"
        ),
        f"- expected_bucket_exists: {report['bucket_exists']}",
        f"- source_key_present: {report['source_key_present']}",
        f"- source_object_key_checked: {report['source_object_key_checked']}",
        f"- source_key_exists: {report['source_key_exists']}",
        f"- final_category: {report['final_category']}",
        f"- recommended_action: {report['recommended_action']}",
    ]
    commands_path = report.get("bucket_create_commands_path")
    if isinstance(commands_path, str):
        lines.append(f"- bucket_create_commands_path: `{commands_path}`")
    lines.extend(
        (
            "",
            "## Notes",
            "",
            f"- endpoint_probe_detail: `{report['endpoint_probe_detail']}`",
            f"- error_detail: `{report['error_detail']}`",
            "- readiness pass does not imply product direct pass",
            (
                "- this tool diagnoses substrate/auth/object state only; it does not blame "
                "connector logic by default"
            ),
        )
    )
    if report["final_category"] == "bucket_missing":
        lines.extend(
            (
                "",
                "## Suggested next step",
                "",
                (
                    "Create the configured bucket in the runtime substrate, then rerun "
                    "connector readiness."
                ),
                (
                    "If you later want to validate the UNS S3 source path, seed a small text "
                    "object such as `p5_1_uns_s3_seed/document.txt` after bucket creation."
                ),
            )
        )
    return "\n".join(lines) + "\n"


def _emit(*, content: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="p5_s3_readiness_diagnose",
        description="Diagnose retained P5.1 S3 readiness without printing secrets.",
    )
    render_group = parser.add_mutually_exclusive_group()
    render_group.add_argument("--json", action="store_true")
    render_group.add_argument("--markdown", action="store_true")
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument(
        "--out-root",
        type=Path,
        default=Path(".tmp/p5_1_s3_diagnose"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = build_report(env_file=args.env_file, out_root=args.out_root)
    json_path = args.out_root / "s3_readiness_diagnose.json"
    markdown_path = args.out_root / "s3_readiness_diagnose.md"
    _emit(content=json.dumps(report, ensure_ascii=False, indent=2) + "\n", out_path=json_path)
    _emit(content=render_markdown(report), out_path=markdown_path)
    if args.markdown:
        print(render_markdown(report), end="")
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
