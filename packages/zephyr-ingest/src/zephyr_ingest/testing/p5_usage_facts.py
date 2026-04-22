from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p5_capability_domains import EXTRA_RESERVED_CAPABILITIES
from zephyr_ingest.testing.p45 import repo_root

UsageFactSeverity = Literal["error", "warning", "info"]

P5_USAGE_FACT_MANIFEST_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_usage_fact_manifest.json"
)
P5_USAGE_OUTPUT_SURFACE_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_usage_output_surface.json"
)
P5_USAGE_LINKAGE_CONTRACT_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_usage_linkage_contract.json"
)
P5_USAGE_SUCCESS_FAILURE_CLASSIFICATION_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_usage_success_failure_classification.json"
)
P5_USAGE_FACT_REPORT_PATH: Final[Path] = repo_root() / "validation" / "P5_M4_S11_USAGE_FACTS.md"


def build_p5_usage_fact_manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S11 usage raw-fact hardening",
        "usage_kind": "raw_technical_usage_fact",
        "not": [
            "billing record",
            "pricing input",
            "license entitlement",
            "subscription allowance",
            "quota decision",
        ],
        "usage_record_shape": {
            "record_kind": "raw_usage_fact_v1_skeleton",
            "runtime_emitted_today": False,
            "minimum_linkage": [
                "task.kind",
                "task.task_id",
                "run_meta.run_id",
                "run_meta.provenance.task_id",
                "run_meta.provenance.run_origin",
                "run_meta.provenance.delivery_origin",
                "run_meta.provenance.execution_mode",
            ],
            "result_linkage": [
                "run_meta.outcome",
                "run_meta.error.code",
                "delivery_receipt.ok",
                "delivery_receipt.shared_summary",
                "batch_report docs/delivery counters",
            ],
            "capability_domains_used_marker": "capability_domains_used",
        },
        "raw_processing_units": {
            "uns": {
                "primary_raw_unit": "document",
                "stable_today": True,
                "not_primary_raw_units": [
                    "page",
                    "elements_count",
                    "normalized_text_len",
                    "delivery_success",
                ],
                "reason": (
                    "one document/blob/page-body selection is the stable current unit; pages are "
                    "not universal, and elements/text length are derivative metrics"
                ),
            },
            "it": {
                "primary_raw_unit": "record_or_emitted_item",
                "stable_today": "bounded",
                "not_fully_first_class_today": True,
                "useful_linkage": [
                    "checkpoint_identity_key",
                    "task_identity_key",
                    "run_origin=resume",
                ],
                "reason": (
                    "structured emitted records/items are the conservative raw unit, but current "
                    "runtime artifacts have not promoted it into a universal first-class field"
                ),
            },
        },
        "capability_domains_used": {
            "marker_kind": "technical_capability_domain_used_not_entitlement",
            "shape": {
                "base": "bool",
                "pro": "bool",
                "extra": "bool",
                "evidence": "list[str]",
            },
            "base_rule": "current Base connectors and Base flow surfaces set Base evidence",
            "pro_rule": (
                "only explicit current uns-stream optional-extra usage or future mature "
                "enterprise connectors set Pro evidence"
            ),
            "extra_rule": (
                "only explicit reserved/incubating Extra capability usage sets Extra evidence"
            ),
            "mixed_uns_stream_rule": (
                "uns-stream direct non-extra document processing is Base and must not be marked "
                "Pro just because the package has optional extras"
            ),
            "root_dev_all_docs_rule": (
                "root dev uns-stream[all-docs] is engineering convenience and must not mark "
                "runtime usage as Pro"
            ),
            "extra_reserved_capabilities": list(EXTRA_RESERVED_CAPABILITIES),
            "default_marker_for_current_base_path": {
                "base": True,
                "pro": False,
                "extra": False,
            },
        },
        "not_claimed": [
            "runtime RunMetaV1 already emits usage_record",
            "runtime RunMetaV1 already emits capability_domains_used",
            "page is a universal raw unit",
            "elements_count is a primary raw unit",
            "normalized_text_len is a primary raw unit",
            "retry/replay billing semantics",
        ],
    }


def build_p5_usage_linkage_contract() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S11 usage linkage contract",
        "task_contract": {
            "kind_values": ["uns", "it"],
            "do_not_add_runtime_observation_kinds": [
                "replay",
                "delivery",
                "batch",
                "worker_task",
            ],
            "stable_fields": [
                "task_id",
                "kind",
                "inputs.document",
                "execution.strategy",
                "execution.unique_element_ids",
                "identity.pipeline_version",
                "identity.sha256",
            ],
        },
        "run_meta_contract": {
            "stable_linkage_fields": [
                "run_id",
                "pipeline_version",
                "provenance.task_id",
                "provenance.task_identity_key",
                "provenance.checkpoint_identity_key",
                "provenance.run_origin",
                "provenance.delivery_origin",
                "provenance.execution_mode",
            ],
            "raw_runtime_fact_fields": [
                "metrics.duration_ms",
                "metrics.attempts",
                "metrics.elements_count",
                "metrics.normalized_text_len",
            ],
            "failure_fields": [
                "error.code",
                "error.message",
                "error.details",
                "outcome",
            ],
            "not_first_class_today": ["usage_record", "capability_domains_used"],
        },
        "recovery_linkage": {
            "resume": {
                "run_origin": "resume",
                "delivery_origin": "primary",
                "decisive_fields": ["checkpoint_identity_key", "task_identity_key"],
            },
            "replay": {
                "delivery_origin": "replay",
                "decisive_fields": ["run_origin", "task_id", "task_identity_key"],
            },
            "requeue": {
                "run_origin": "requeue",
                "delivery_origin": "primary",
                "decisive_fields": ["task_id", "task_identity_key"],
            },
        },
    }


def build_p5_usage_output_surface() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S11 usage output surface",
        "current_usage_like_surfaces": {
            "run_meta": {
                "available": True,
                "fields": [
                    "run_id",
                    "pipeline_version",
                    "metrics.duration_ms",
                    "metrics.attempts",
                    "metrics.elements_count",
                    "metrics.normalized_text_len",
                    "error.code",
                    "provenance.task_id",
                    "provenance.run_origin",
                    "provenance.delivery_origin",
                ],
                "usage_record_field_present_today": False,
                "capability_domains_used_field_present_today": False,
            },
            "delivery_receipt": {
                "available": True,
                "fields": [
                    "destination",
                    "ok",
                    "details.retryable",
                    "details.failure_kind",
                    "details.error_code",
                    "shared_summary",
                ],
            },
            "batch_report": {
                "available": True,
                "fields": [
                    "docs.total",
                    "docs.success",
                    "docs.failed",
                    "docs.skipped",
                    "delivery.total",
                    "delivery.ok",
                    "delivery.failed",
                ],
            },
        },
        "future_usage_record_surface": {
            "status": "manifested_not_runtime_emitted",
            "must_remain_redaction_safe": True,
            "must_not_include": [
                "secret values",
                "tokens",
                "billing price",
                "customer entitlement decision",
            ],
        },
    }


def build_p5_usage_success_failure_classification() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S11 usage success/failure classification",
        "success_surfaces": {
            "run_success": {
                "required_facts": ["run_meta.run_id", "run_meta.outcome", "metrics.duration_ms"],
                "usage_recordable": True,
            },
            "delivery_success": {
                "required_facts": ["delivery_receipt.ok=true", "destination"],
                "usage_recordable": True,
                "not_raw_unit": True,
            },
            "batch_success_or_partial": {
                "required_facts": ["docs counters", "delivery counters"],
                "usage_recordable": True,
            },
        },
        "failure_surfaces": {
            "run_failure": {
                "required_facts": ["run_meta.run_id", "error.code", "error.message"],
                "usage_recordable": True,
            },
            "delivery_failure": {
                "required_facts": [
                    "delivery_receipt.ok=false",
                    "details.retryable",
                    "details.failure_kind",
                    "details.error_code",
                ],
                "usage_recordable": True,
            },
            "batch_failure_or_partial": {
                "required_facts": ["docs.failed", "delivery.failed", "error_code aggregates"],
                "usage_recordable": True,
            },
        },
        "recovery_surfaces": {
            "resume": "preserve run_origin=resume and checkpoint linkage",
            "replay": "preserve delivery_origin=replay and delivery/DLQ linkage",
            "requeue": "preserve run_origin=requeue and queue governance linkage",
            "redrive": "do not collapse into replay/requeue unless explicitly modeled",
        },
        "not_claimed": [
            "retry billing",
            "replay billing",
            "failure chargeability",
            "entitlement decision",
        ],
    }


@dataclass(frozen=True, slots=True)
class P5UsageFactCheck:
    name: str
    ok: bool
    detail: str
    severity: UsageFactSeverity = "error"


def _read_json_object(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded_obj, dict):
        return None
    return cast(dict[str, object], loaded_obj)


def _json_artifact_check(
    *,
    name: str,
    path: Path,
    expected: dict[str, object],
) -> P5UsageFactCheck:
    observed = _read_json_object(path)
    return P5UsageFactCheck(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def validate_p5_usage_fact_artifacts() -> list[P5UsageFactCheck]:
    checks = [
        _json_artifact_check(
            name="usage_fact_manifest_matches_helper",
            path=P5_USAGE_FACT_MANIFEST_PATH,
            expected=build_p5_usage_fact_manifest(),
        ),
        _json_artifact_check(
            name="usage_output_surface_matches_helper",
            path=P5_USAGE_OUTPUT_SURFACE_PATH,
            expected=build_p5_usage_output_surface(),
        ),
        _json_artifact_check(
            name="usage_linkage_contract_matches_helper",
            path=P5_USAGE_LINKAGE_CONTRACT_PATH,
            expected=build_p5_usage_linkage_contract(),
        ),
        _json_artifact_check(
            name="usage_success_failure_classification_matches_helper",
            path=P5_USAGE_SUCCESS_FAILURE_CLASSIFICATION_PATH,
            expected=build_p5_usage_success_failure_classification(),
        ),
    ]

    manifest = build_p5_usage_fact_manifest()
    raw_units = cast(dict[str, object], manifest["raw_processing_units"])
    uns_unit = cast(dict[str, object], raw_units["uns"])
    it_unit = cast(dict[str, object], raw_units["it"])
    checks.append(
        P5UsageFactCheck(
            name="usage_raw_units_are_conservative",
            ok=uns_unit.get("primary_raw_unit") == "document"
            and "page" in cast(list[object], uns_unit["not_primary_raw_units"])
            and it_unit.get("primary_raw_unit") == "record_or_emitted_item"
            and it_unit.get("not_fully_first_class_today") is True,
            detail=(
                f"uns={uns_unit.get('primary_raw_unit')!r}; it={it_unit.get('primary_raw_unit')!r}"
            ),
        )
    )

    marker = cast(dict[str, object], manifest["capability_domains_used"])
    default_marker = cast(dict[str, object], marker["default_marker_for_current_base_path"])
    checks.append(
        P5UsageFactCheck(
            name="usage_domain_marker_keeps_base_not_naive_pro",
            ok=default_marker == {"base": True, "pro": False, "extra": False}
            and "root dev uns-stream[all-docs]" in cast(str, marker["root_dev_all_docs_rule"]),
            detail=str(default_marker),
        )
    )
    checks.append(
        P5UsageFactCheck(
            name="usage_extra_marker_is_non_default",
            ok=marker.get("extra_reserved_capabilities") == list(EXTRA_RESERVED_CAPABILITIES)
            and default_marker.get("extra") is False,
            detail=f"extra={default_marker.get('extra')!r}",
        )
    )

    classification = build_p5_usage_success_failure_classification()
    success = cast(dict[str, object], classification["success_surfaces"])
    failure = cast(dict[str, object], classification["failure_surfaces"])
    checks.append(
        P5UsageFactCheck(
            name="usage_success_failure_symmetry_present",
            ok={"run_success", "delivery_success", "batch_success_or_partial"}.issubset(success)
            and {"run_failure", "delivery_failure", "batch_failure_or_partial"}.issubset(failure),
            detail=f"success={list(success)}; failure={list(failure)}",
        )
    )

    linkage = build_p5_usage_linkage_contract()
    task_contract = cast(dict[str, object], linkage["task_contract"])
    checks.append(
        P5UsageFactCheck(
            name="usage_task_kind_stays_flow_kind_only",
            ok=task_contract.get("kind_values") == ["uns", "it"],
            detail=str(task_contract.get("kind_values")),
        )
    )

    if P5_USAGE_FACT_REPORT_PATH.exists():
        report_text = P5_USAGE_FACT_REPORT_PATH.read_text(encoding="utf-8")
        required_phrases = (
            "raw usage facts, not billing",
            "uns raw unit: document",
            "page is not universal",
            "capability_domains_used",
            "success and failure",
        )
        missing = [phrase for phrase in required_phrases if phrase not in report_text]
        checks.append(
            P5UsageFactCheck(
                name="usage_report_required_phrases",
                ok=not missing,
                detail="missing=" + (", ".join(missing) if missing else "none"),
            )
        )
    else:
        checks.append(
            P5UsageFactCheck(
                name="usage_report_required_phrases",
                ok=False,
                detail=str(P5_USAGE_FACT_REPORT_PATH),
            )
        )

    return checks


def format_p5_usage_fact_results(results: list[P5UsageFactCheck]) -> str:
    lines = ["P5-M4-S11 usage fact checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_usage_fact_summary() -> str:
    return "\n".join(
        (
            "P5-M4-S11 usage raw facts:",
            "- kind: raw technical usage facts, not billing/pricing/entitlement",
            "- linkage: task_id, run_id, provenance, result, delivery, and batch counters",
            "- uns raw unit: document; page is not universal",
            "- it raw unit: record_or_emitted_item, still bounded/not fully first-class",
            "- capability_domains_used: Base/Pro/Extra marker shape, non-entitlement",
            "- success and failure usage facts are both recordable as raw facts",
        )
    )


def render_p5_usage_fact_json() -> str:
    bundle = {
        "usage_fact_manifest": build_p5_usage_fact_manifest(),
        "usage_output_surface": build_p5_usage_output_surface(),
        "usage_linkage_contract": build_p5_usage_linkage_contract(),
        "usage_success_failure_classification": (build_p5_usage_success_failure_classification()),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
