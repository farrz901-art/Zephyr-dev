from __future__ import annotations

import importlib
import json
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Final, Literal, Mapping, Protocol, cast

from zephyr_ingest._internal.delivery_payload import (
    DELIVERY_NORMALIZED_TEXT_PREVIEW_MAX_CHARS,
    DELIVERY_RECORDS_PREVIEW_MAX_RECORDS,
)
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.testing.p45 import LoadedP45Env, load_p45_env, repo_root

P5FuvDFixSeverity = Literal["error", "warning", "info"]

P5_FUV_D_FIX_CONTENT_EVIDENCE_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_fuv_d_fix_delivery_payload_content_evidence.json"
)
P5_FUV_D_FIX_REPORT_PATH: Final[Path] = repo_root() / "validation" / "P5_FUV_D_FIX_DELIVERY.md"
P5_FUV_D_FIX_SUCCESS_CRITERIA_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_fuv_d_fix_success_criteria.json"
)
PACKAGE_PYPROJECT_PATH: Final[Path] = repo_root() / "packages" / "zephyr-ingest" / "pyproject.toml"


class S3BucketAdminClientProtocol(Protocol):
    def list_buckets(self) -> Mapping[str, object]: ...

    def create_bucket(self, *, Bucket: str) -> Mapping[str, object]: ...


ImportModuleFunc = Callable[[str], object]


@dataclass(frozen=True, slots=True)
class P5FuvDFixCheck:
    name: str
    ok: bool
    detail: str
    severity: P5FuvDFixSeverity = "error"


def build_p5_fuv_d_fix_content_evidence_artifact() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5 final user validation D fix",
        "status": "delivery_content_visibility_and_s3_readiness_fix",
        "delivery_payload_contract": {
            "schema_version": 1,
            "backward_compatible": True,
            "existing_required_fields": [
                "schema_version",
                "sha256",
                "run_meta",
                "artifacts",
            ],
            "additive_optional_fields": ["content_evidence"],
        },
        "content_evidence": {
            "field_name": "content_evidence",
            "normalized_preview_max_chars": DELIVERY_NORMALIZED_TEXT_PREVIEW_MAX_CHARS,
            "records_preview_max_records": DELIVERY_RECORDS_PREVIEW_MAX_RECORDS,
            "required_visible_fields": [
                "evidence_kind",
                "normalized_text_preview",
                "normalized_text_len",
                "normalized_text_sha256",
                "normalized_text_truncated",
                "records_preview",
                "records_count",
                "records_truncated",
                "elements_count",
            ],
            "safe_missing_artifact_behavior": [
                "missing normalized.txt does not crash payload building",
                "missing records.jsonl for non-IT does not fail delivery payload building",
                (
                    "invalid or missing records preview is surfaced through status "
                    "fields instead of crashing unrelated delivery paths"
                ),
            ],
            "destination_visibility_targets": [
                "filesystem",
                "webhook",
                "kafka",
                "s3",
                "opensearch",
            ],
        },
        "fanout_readability": {
            "top_level_destination": "fanout",
            "children_required": True,
            "child_summary_required": True,
            "single_sink_connector": False,
        },
        "s3_readiness": {
            "dependency_surface": "packages/zephyr-ingest[ s3 ]".replace(" ", ""),
            "bucket_helper": "tools/p45_s3_bootstrap.py",
            "runtime_home_authoritative": True,
            "secrets_printed": False,
        },
        "non_claims": [
            "P6 product shell",
            "installer implementation",
            "deployment packaging",
            "distributed runtime",
            "cloud or Kubernetes readiness",
            "commercial or entitlement semantics",
        ],
    }


def build_p5_fuv_d_fix_success_criteria() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5 final user validation D fix success criteria",
        "scope": "local bounded delivery user-validation blocker fix only",
        "criteria": [
            {
                "criterion_id": "delivery_payload_content_evidence",
                "success": [
                    "delivery payload keeps schema_version=1 and existing artifacts paths",
                    "bounded normalized text evidence becomes visible when normalized.txt exists",
                    "bounded IT records evidence becomes visible when records.jsonl exists",
                    "missing optional evidence files do not crash unrelated delivery flows",
                ],
            },
            {
                "criterion_id": "destination_marker_visibility",
                "success": [
                    "webhook, kafka, opensearch, and s3 payload paths carry content_evidence",
                    (
                        "marker-bearing normalized or IT record content can be found "
                        "from destination payload surfaces"
                    ),
                    "sha256/idempotency semantics remain intact",
                ],
            },
            {
                "criterion_id": "s3_durability_and_readiness",
                "success": [
                    "boto3 is reachable through durable repo dependency declaration",
                    "bucket readiness can be checked from external runtime-home configuration",
                    "explicit ensure mode can create the configured bucket without leaking secrets",
                ],
            },
            {
                "criterion_id": "fanout_readability",
                "success": [
                    "top-level fanout receipt remains semantically valid",
                    "child destinations are explicitly visible",
                    "child summaries remain visible for user/operator interpretation",
                ],
            },
        ],
        "live_revalidation_claimed": False,
        "non_claims": [
            "no live cloud or distributed validation",
            "no installer implementation",
            "no P6 product shell work",
        ],
    }


def render_p5_fuv_d_fix_report() -> str:
    return "\n".join(
        (
            "# P5 FUV D Delivery Fix",
            "",
            (
                "This patch fixes retained P5 final user validation D delivery "
                "blockers; it does not start P6 work."
            ),
            (
                "It keeps the current TaskV1 -> FlowProcessor -> Delivery execution "
                "chain and only hardens bounded local delivery visibility, S3 "
                "readiness, and fanout readability."
            ),
            "",
            "## What This Fix Changes",
            "",
            (
                "- Delivery payloads keep `schema_version=1`, `sha256`, `run_meta`, "
                "and `artifacts`, and add an optional bounded `content_evidence` "
                "field."
            ),
            (
                "- Kafka, OpenSearch, Webhook, and S3 can now carry marker-visible "
                "normalized text or IT record previews without removing "
                "artifact-reference payload behavior."
            ),
            (
                "- S3 dependency durability is declared through the "
                "`zephyr-ingest` optional `s3` extra backed by `boto3`."
            ),
            (
                "- S3 bucket readiness is checkable or explicitly ensureable from "
                "the external runtime-home configuration without printing secrets."
            ),
            "- fanout remains composition/orchestration, not a sink connector.",
            "",
            "## What This Fix Does Not Claim",
            "",
            (
                "- No installer implementation, deployment packaging, or "
                "product-shell work is added here."
            ),
            (
                "- No distributed runtime, cloud, Kubernetes, or large-scale "
                "load-testing claim is made here."
            ),
            "- No billing, entitlement, RBAC, or enterprise connector claim is introduced here.",
            "",
            "## User Revalidation Intent",
            "",
            (
                "- Filesystem visibility still comes from delivered artifacts such as "
                "`normalized.txt` and `records.jsonl` rather than from a remote "
                "endpoint."
            ),
            (
                "- Remote endpoint validation becomes easier because payload content "
                "evidence now carries bounded marker-visible previews."
            ),
            "- S3 readiness is durable only when dependency and bucket checks are both green.",
            (
                "- Fanout readability now includes explicit child destination and "
                "summary surfaces at the top-level receipt layer."
            ),
        )
    )


def summarize_fanout_receipt(*, receipt: DeliveryReceipt) -> dict[str, object]:
    details = receipt.details or {}
    raw_children = details.get("children")
    children: list[dict[str, object]] = []
    if isinstance(raw_children, list):
        typed_children = cast(list[object], raw_children)
        for child_obj in typed_children:
            if isinstance(child_obj, dict):
                child = cast(dict[str, object], child_obj)
                children.append(
                    {
                        "destination": child.get("destination"),
                        "ok": child.get("ok"),
                        "summary": child.get("summary"),
                    }
                )
    return {
        "destination": receipt.destination,
        "ok": receipt.ok,
        "child_count": details.get("child_count"),
        "children": children,
    }


def s3_optional_dependency_declared() -> bool:
    parsed = tomllib.loads(PACKAGE_PYPROJECT_PATH.read_text(encoding="utf-8"))
    project = cast(dict[str, object], parsed["project"])
    optional_obj = cast(dict[str, object], project["optional-dependencies"])
    s3_obj = optional_obj.get("s3")
    if not isinstance(s3_obj, list):
        return False
    typed_s3_entries = cast(list[object], s3_obj)
    return any(isinstance(item, str) and item.startswith("boto3>=") for item in typed_s3_entries)


def _import_boto3(import_module_func: ImportModuleFunc) -> object:
    return import_module_func("boto3")


def _make_bucket_admin_client(
    *,
    loaded_env: LoadedP45Env,
    import_module_func: ImportModuleFunc = importlib.import_module,
) -> S3BucketAdminClientProtocol:
    boto3 = _import_boto3(import_module_func)
    session_module = getattr(boto3, "session")
    session_cls = getattr(session_module, "Session")
    session = session_cls(
        aws_access_key_id=loaded_env.require("ZEPHYR_P45_S3_ACCESS_KEY"),
        aws_secret_access_key=loaded_env.require("ZEPHYR_P45_S3_SECRET_KEY"),
        aws_session_token=loaded_env.get("ZEPHYR_P45_S3_SESSION_TOKEN"),
        region_name=loaded_env.get("ZEPHYR_P45_S3_REGION", "us-east-1") or "us-east-1",
    )
    endpoint_url = loaded_env.get("ZEPHYR_P45_S3_ENDPOINT")
    if endpoint_url is None:
        host = loaded_env.get("ZEPHYR_P45_S3_HOST", "127.0.0.1") or "127.0.0.1"
        port = loaded_env.get("ZEPHYR_P45_S3_PORT", "59100") or "59100"
        endpoint_url = f"http://{host}:{port}"
    client = session.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=loaded_env.get("ZEPHYR_P45_S3_REGION", "us-east-1") or "us-east-1",
    )
    return cast(S3BucketAdminClientProtocol, client)


def collect_p45_s3_bucket_status(
    *,
    ensure_bucket: bool = False,
    loaded_env: LoadedP45Env | None = None,
    client: S3BucketAdminClientProtocol | None = None,
    import_module_func: ImportModuleFunc = importlib.import_module,
) -> dict[str, object]:
    env = load_p45_env() if loaded_env is None else loaded_env
    endpoint_url = env.get("ZEPHYR_P45_S3_ENDPOINT")
    if endpoint_url is None:
        host = env.get("ZEPHYR_P45_S3_HOST", "127.0.0.1") or "127.0.0.1"
        port = env.get("ZEPHYR_P45_S3_PORT", "59100") or "59100"
        endpoint_url = f"http://{host}:{port}"
    bucket = env.get("ZEPHYR_P45_S3_BUCKET")
    region = env.get("ZEPHYR_P45_S3_REGION", "us-east-1") or "us-east-1"
    result: dict[str, object] = {
        "runtime_home": str(env.runtime_paths.home),
        "runtime_env_dir": str(env.runtime_paths.env_dir),
        "endpoint_url": endpoint_url,
        "region": region,
        "bucket": bucket,
        "bucket_exists": False,
        "available_buckets": [],
        "dependency_installed": False,
        "created_bucket": False,
        "action": "ensure" if ensure_bucket else "check",
        "secrets_redacted": True,
    }
    if bucket is None:
        result["error"] = "missing_bucket_configuration"
        return result

    client_obj = client
    if client_obj is None:
        try:
            client_obj = _make_bucket_admin_client(
                loaded_env=env,
                import_module_func=import_module_func,
            )
            result["dependency_installed"] = True
        except ImportError as exc:
            result["error"] = f"boto3_not_installed:{type(exc).__name__}"
            return result
    else:
        result["dependency_installed"] = True

    try:
        bucket_response = client_obj.list_buckets()
        buckets_obj = bucket_response.get("Buckets")
        bucket_names: list[str] = []
        if isinstance(buckets_obj, list):
            typed_buckets = cast(list[object], buckets_obj)
            for bucket_obj in typed_buckets:
                if not isinstance(bucket_obj, dict):
                    continue
                name_obj = cast(dict[str, object], bucket_obj).get("Name")
                if isinstance(name_obj, str):
                    bucket_names.append(name_obj)
        result["available_buckets"] = bucket_names
        bucket_exists = bucket in bucket_names
        result["bucket_exists"] = bucket_exists
        if ensure_bucket and not bucket_exists:
            client_obj.create_bucket(Bucket=bucket)
            bucket_names = list(bucket_names)
            bucket_names.append(bucket)
            result["available_buckets"] = bucket_names
            result["bucket_exists"] = True
            result["created_bucket"] = True
        return result
    except Exception as exc:
        result["error"] = type(exc).__name__
        response_obj = getattr(exc, "response", None)
        if isinstance(response_obj, dict):
            response = cast(dict[str, object], response_obj)
            error_obj = response.get("Error")
            if isinstance(error_obj, dict):
                error = cast(dict[str, object], error_obj)
                code_obj = error.get("Code")
                if isinstance(code_obj, str):
                    result["backend_error_code"] = code_obj
        return result


def _read_json_object(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded_obj, dict):
        return None
    return cast(dict[str, object], loaded_obj)


def _json_artifact_check(*, name: str, path: Path, expected: dict[str, object]) -> P5FuvDFixCheck:
    observed = _read_json_object(path)
    return P5FuvDFixCheck(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def validate_p5_fuv_d_fix_artifacts() -> list[P5FuvDFixCheck]:
    checks = [
        _json_artifact_check(
            name="p5_fuv_d_fix_content_evidence_matches_helper",
            path=P5_FUV_D_FIX_CONTENT_EVIDENCE_PATH,
            expected=build_p5_fuv_d_fix_content_evidence_artifact(),
        ),
        _json_artifact_check(
            name="p5_fuv_d_fix_success_criteria_matches_helper",
            path=P5_FUV_D_FIX_SUCCESS_CRITERIA_PATH,
            expected=build_p5_fuv_d_fix_success_criteria(),
        ),
        P5FuvDFixCheck(
            name="p5_fuv_d_fix_s3_optional_dependency_declared",
            ok=s3_optional_dependency_declared(),
            detail=str(PACKAGE_PYPROJECT_PATH),
        ),
    ]
    if P5_FUV_D_FIX_REPORT_PATH.exists():
        report_text = P5_FUV_D_FIX_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            (
                "This patch fixes retained P5 final user validation D delivery "
                "blockers; it does not start P6 work."
            ),
            (
                "Remote endpoint validation becomes easier because payload content "
                "evidence now carries bounded marker-visible previews."
            ),
            "S3 readiness is durable only when dependency and bucket checks are both green.",
            "fanout remains composition/orchestration, not a sink connector.",
            (
                "No distributed runtime, cloud, Kubernetes, or large-scale "
                "load-testing claim is made here."
            ),
        )
        missing = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5FuvDFixCheck(
                name="p5_fuv_d_fix_report_required_phrases",
                ok=not missing,
                detail="missing=" + (", ".join(missing) if missing else "none"),
            )
        )
    else:
        checks.append(
            P5FuvDFixCheck(
                name="p5_fuv_d_fix_report_required_phrases",
                ok=False,
                detail=str(P5_FUV_D_FIX_REPORT_PATH),
            )
        )
    return checks


def format_p5_fuv_d_fix_results(results: list[P5FuvDFixCheck]) -> str:
    lines = ["P5 final user validation D-fix checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_fuv_d_fix_summary() -> str:
    return "\n".join(
        (
            "P5 final user validation D fix:",
            "- scope: delivery payload content evidence, S3 readiness, fanout readability",
            "- additive payload extension only; schema_version=1 remains unchanged",
            "- no installer, cloud, distributed runtime, or P6 claim is added",
            "- external runtime-home env remains authoritative for S3 readiness checks",
        )
    )


def render_p5_fuv_d_fix_json() -> str:
    bundle = {
        "delivery_payload_content_evidence": build_p5_fuv_d_fix_content_evidence_artifact(),
        "success_criteria": build_p5_fuv_d_fix_success_criteria(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
