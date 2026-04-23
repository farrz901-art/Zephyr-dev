from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from zephyr_ingest.testing.p5_capability_domains import (
    EXTRA_RESERVED_CAPABILITIES,
    IT_STREAM_SOURCE_CONNECTORS,
    UNS_STREAM_SOURCE_CONNECTORS,
)
from zephyr_ingest.testing.p45 import repo_root

SourceContractSeverity = Literal["error", "warning", "info"]

P5_SOURCE_CONTRACT_MATRIX_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_source_contract_matrix.json"
)
P5_SOURCE_SURFACE_CLASSIFICATION_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_source_surface_classification.json"
)
P5_SOURCE_CONTRACT_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M4_S12_SOURCE_CONTRACTS.md"
)
P5_USAGE_RUNTIME_CONTRACT_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_usage_runtime_contract.json"
)
P5_USAGE_RUNTIME_OUTPUT_SURFACE_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_usage_runtime_output_surface.json"
)
P5_USAGE_RUNTIME_REPORT_PATH: Final[Path] = (
    repo_root() / "validation" / "P5_M4_S12_USAGE_RUNTIME.md"
)
P5_SOURCE_USAGE_LINKAGE_PATH: Final[Path] = (
    repo_root() / "validation" / "p5_source_usage_linkage.json"
)

_UNS_SOURCE_AUTH: Final[dict[str, str]] = {
    "http_document_v1": "optional request headers; no secret material in task identity",
    "s3_document_v1": "runtime S3 credentials/session; bucket/key/version are identity facts",
    "git_document_v1": "optional repository credentials; repo/ref/path are identity facts",
    "google_drive_document_v1": "runtime OAuth/bearer token; trust_env remains disabled",
    "confluence_document_v1": "runtime token/session; site/page/version are identity facts",
}
_IT_SOURCE_AUTH: Final[dict[str, str]] = {
    "http_json_cursor_v1": "optional request headers; cursor selector is identity/progress fact",
    "postgresql_incremental_v1": (
        "runtime database credentials; table/query selector is identity fact"
    ),
    "clickhouse_incremental_v1": (
        "runtime database credentials; query/window selector is identity fact"
    ),
    "kafka_partition_offset_v1": (
        "runtime broker/auth config; topic/partition/offset are progress facts"
    ),
    "mongodb_incremental_v1": (
        "runtime database credentials; collection/cursor selector is identity fact"
    ),
}


def _source_entry(
    *,
    source_id: str,
    package: str,
    flow_family: Literal["uns", "it"],
    auth_surface: str,
) -> dict[str, object]:
    if flow_family == "uns":
        raw_unit_relation = {
            "primary_raw_unit": "document",
            "relation": "one explicit document/blob/page-body selection enters partitioning",
            "not_primary": ["page", "elements_count", "normalized_text_len", "delivery_success"],
        }
        source_model = "document_acquisition_snapshot"
        boundedness = [
            "one document-like unit per task",
            "no long-lived sync or webhook delta ownership",
            "source-native handles are not shared runtime artifacts",
        ]
    else:
        raw_unit_relation = {
            "primary_raw_unit": "record_or_emitted_item",
            "relation": "structured records/items emitted from bounded cursor/incremental reads",
            "not_primary": ["delivery_success", "checkpoint"],
            "maturity": "bounded_not_fully_first_class",
        }
        source_model = "structured_progress_snapshot"
        boundedness = [
            "explicit cursor/progress-family subset",
            "no CDC/oplog/consumer-group ownership",
            "checkpoint/resume remains bounded current cursor_v1 evidence",
        ]

    return {
        "source_id": source_id,
        "source_kind": source_id,
        "package": package,
        "flow_family": flow_family,
        "retained": True,
        "technical_capability_domain": "base",
        "enterprise_grade_today": False,
        "auth_surface": auth_surface,
        "raw_unit_relation": raw_unit_relation,
        "source_model": source_model,
        "boundedness": boundedness,
        "not_claimed": [
            "enterprise connector",
            "fully generalized source platform",
            "billing or entitlement domain",
        ],
    }


def build_p5_source_contract_matrix() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S12 source contract alignment",
        "domain_kind": "technical_capability_domain_not_entitlement",
        "source_contract_status": "machine_readable_retained_source_truth",
        "uns_stream_sources": [
            _source_entry(
                source_id=source_id,
                package="uns-stream",
                flow_family="uns",
                auth_surface=_UNS_SOURCE_AUTH[source_id],
            )
            for source_id in UNS_STREAM_SOURCE_CONNECTORS
        ],
        "it_stream_sources": [
            _source_entry(
                source_id=source_id,
                package="it-stream",
                flow_family="it",
                auth_surface=_IT_SOURCE_AUTH[source_id],
            )
            for source_id in IT_STREAM_SOURCE_CONNECTORS
        ],
        "source_model_asymmetry": {
            "uns": "document acquisition to partition-entry path",
            "it": "structured cursor/incremental progression to emitted records/items",
            "intentionally_not_claimed": "uns and it sources are fully symmetric implementations",
        },
        "capability_domain_rule": {
            "all_current_retained_sources": "base",
            "pro_not_default_for_uns_stream": True,
            "extra_not_default": True,
            "root_dev_all_docs_does_not_mark_runtime_usage_pro": True,
        },
        "not_claimed": [
            "commercial entitlement",
            "enterprise connector implementation",
            "distributed runtime or fleet source ownership",
        ],
    }


def build_p5_source_surface_classification() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S12 source surface classification",
        "uns_document_acquisition_sources": list(UNS_STREAM_SOURCE_CONNECTORS),
        "it_structured_progress_sources": list(IT_STREAM_SOURCE_CONNECTORS),
        "classification_rules": {
            "uns": [
                "document/file/content acquisition",
                "snapshot-style source selection",
                "handoff into document partitioning",
            ],
            "it": [
                "structured record transport",
                "cursor/checkpoint/resume progression",
                "bounded emitted records/items",
            ],
        },
        "misread_guards": [
            "do not treat staging/materialization details as source contract",
            "do not mix document acquisition sources with structured progress sources",
            "do not infer enterprise-grade support from retained non-enterprise source truth",
        ],
    }


def build_p5_usage_runtime_contract() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S12 runtime-native usage emission",
        "usage_record": {
            "artifact": "usage_record.json",
            "record_kinds": [
                "raw_technical_usage_fact_v1",
                "raw_technical_usage_batch_summary_v1",
            ],
            "authoritative_usage_truth": True,
            "placement": [
                "per task/run artifact directory beside run_meta.json and delivery_receipt.json",
                "batch root aggregate beside batch_report.json",
            ],
            "not": [
                "billing record",
                "pricing input",
                "license entitlement",
                "subscription allowance",
                "quota decision",
            ],
        },
        "capability_domains_used": {
            "runtime_emitted": True,
            "shape": {"base": "bool", "pro": "bool", "extra": "bool", "evidence": "list[str]"},
            "current_default_for_retained_sources": {"base": True, "pro": False, "extra": False},
            "mixed_uns_stream_rule": (
                "Base uns-stream runtime use is not marked Pro unless explicit optional-extra "
                "capability evidence is present"
            ),
            "extra_reserved": list(EXTRA_RESERVED_CAPABILITIES),
        },
        "raw_units": {
            "uns": {
                "primary_raw_unit": "document",
                "not_primary": ["page", "elements_count", "normalized_text_len"],
            },
            "it": {
                "primary_raw_unit": "record_or_emitted_item",
                "maturity": "bounded_not_fully_first_class",
            },
        },
        "recovery_symmetry": {
            "success": "status=succeeded and failure_stage=none",
            "failure": "status=failed/skipped with run or delivery failure_stage",
            "batch": "batch root usage_record.json records aggregate success/partial/empty status",
            "resume": "recovery_kind=resume via run_origin=resume",
            "replay": "recovery_kind=replay via delivery_origin=replay",
            "requeue": "recovery_kind=requeue via run_origin=requeue",
            "redrive": "recovery_kind=redrive; not collapsed into replay/requeue",
        },
    }


def build_p5_usage_runtime_output_surface() -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": "P5-M4-S12 usage runtime output surface",
        "authoritative_artifact": "usage_record.json",
        "runtime_emitted_today": True,
        "lightweight_discovery": {
            "run_meta_json": (
                "usage_record links to run_id/task_id/provenance; run_meta is not overstuffed"
            ),
            "delivery_receipt_json": "usage_record records delivery outcome/failure summary",
            "batch_report_json": "batch root usage_record records aggregate non-billing outcome",
        },
        "redaction_boundary": {
            "must_not_include": [
                "secret values",
                "tokens",
                "credential strings",
                "billing price",
                "license or entitlement decision",
            ],
        },
        "bounded_claim": "local artifact-backed runtime usage facts, not fleet billing telemetry",
    }


def build_p5_source_usage_linkage() -> dict[str, object]:
    source_linkages: list[dict[str, object]] = []
    for source_id in UNS_STREAM_SOURCE_CONNECTORS:
        source_linkages.append(
            {
                "source_contract_id": source_id,
                "flow_family": "uns",
                "usage_raw_unit": "document",
                "usage_raw_unit_relation": "document_selection",
                "default_capability_domains_used": {"base": True, "pro": False, "extra": False},
                "runtime_linkage_fields": [
                    "usage_record.source.source_contract_id",
                    "usage_record.linkage.task_id",
                    "usage_record.linkage.run_id",
                    "run_meta.provenance.task_id",
                ],
            }
        )
    for source_id in IT_STREAM_SOURCE_CONNECTORS:
        source_linkages.append(
            {
                "source_contract_id": source_id,
                "flow_family": "it",
                "usage_raw_unit": "record_or_emitted_item",
                "usage_raw_unit_relation": "structured_emitted_item",
                "default_capability_domains_used": {"base": True, "pro": False, "extra": False},
                "runtime_linkage_fields": [
                    "usage_record.source.source_contract_id",
                    "usage_record.linkage.task_id",
                    "usage_record.linkage.run_id",
                    "run_meta.provenance.task_id",
                    "run_meta.provenance.checkpoint_identity_key",
                ],
            }
        )
    return {
        "schema_version": 1,
        "phase": "P5-M4-S12 source to usage linkage",
        "source_linkages": source_linkages,
        "linkage_status_notes": {
            "linked": "TaskV1.inputs.document.source maps to a retained source contract id",
            "flow_family_only": (
                "runtime task lacks a retained source id; usage record still preserves flow, "
                "task, run, and raw-unit truth"
            ),
        },
        "not_claimed": [
            "source contract and usage emission are complete billing records",
            "source auth material is emitted in usage records",
        ],
    }


@dataclass(frozen=True, slots=True)
class P5SourceContractCheck:
    name: str
    ok: bool
    detail: str
    severity: SourceContractSeverity = "error"


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
) -> P5SourceContractCheck:
    observed = _read_json_object(path)
    return P5SourceContractCheck(
        name=name,
        ok=observed == expected,
        detail=str(path) if observed == expected else "missing or not aligned with helper truth",
    )


def validate_p5_source_contract_artifacts() -> list[P5SourceContractCheck]:
    checks = [
        _json_artifact_check(
            name="source_contract_matrix_matches_helper",
            path=P5_SOURCE_CONTRACT_MATRIX_PATH,
            expected=build_p5_source_contract_matrix(),
        ),
        _json_artifact_check(
            name="source_surface_classification_matches_helper",
            path=P5_SOURCE_SURFACE_CLASSIFICATION_PATH,
            expected=build_p5_source_surface_classification(),
        ),
        _json_artifact_check(
            name="usage_runtime_contract_matches_helper",
            path=P5_USAGE_RUNTIME_CONTRACT_PATH,
            expected=build_p5_usage_runtime_contract(),
        ),
        _json_artifact_check(
            name="usage_runtime_output_surface_matches_helper",
            path=P5_USAGE_RUNTIME_OUTPUT_SURFACE_PATH,
            expected=build_p5_usage_runtime_output_surface(),
        ),
        _json_artifact_check(
            name="source_usage_linkage_matches_helper",
            path=P5_SOURCE_USAGE_LINKAGE_PATH,
            expected=build_p5_source_usage_linkage(),
        ),
    ]

    matrix = build_p5_source_contract_matrix()
    uns_sources = cast(list[dict[str, object]], matrix["uns_stream_sources"])
    it_sources = cast(list[dict[str, object]], matrix["it_stream_sources"])
    checks.append(
        P5SourceContractCheck(
            name="source_contract_matrix_covers_retained_sources",
            ok=len(uns_sources) == len(UNS_STREAM_SOURCE_CONNECTORS)
            and len(it_sources) == len(IT_STREAM_SOURCE_CONNECTORS),
            detail=f"uns={len(uns_sources)} it={len(it_sources)}",
        )
    )
    checks.append(
        P5SourceContractCheck(
            name="source_contract_domains_remain_base_non_enterprise",
            ok=all(source["technical_capability_domain"] == "base" for source in uns_sources)
            and all(source["technical_capability_domain"] == "base" for source in it_sources)
            and all(source["enterprise_grade_today"] is False for source in uns_sources)
            and all(source["enterprise_grade_today"] is False for source in it_sources),
            detail="current retained sources are Base and non-enterprise",
        )
    )

    runtime_contract = build_p5_usage_runtime_contract()
    domains = cast(dict[str, object], runtime_contract["capability_domains_used"])
    checks.append(
        P5SourceContractCheck(
            name="runtime_domain_marker_keeps_mixed_uns_not_naive_pro",
            ok=domains["current_default_for_retained_sources"]
            == {"base": True, "pro": False, "extra": False}
            and "not marked Pro" in cast(str, domains["mixed_uns_stream_rule"]),
            detail=str(domains["current_default_for_retained_sources"]),
        )
    )

    linkage = build_p5_source_usage_linkage()
    source_linkages = cast(list[dict[str, object]], linkage["source_linkages"])
    checks.append(
        P5SourceContractCheck(
            name="source_usage_linkage_covers_all_current_sources",
            ok=len(source_linkages)
            == len(UNS_STREAM_SOURCE_CONNECTORS) + len(IT_STREAM_SOURCE_CONNECTORS),
            detail=f"linkages={len(source_linkages)}",
        )
    )

    for report_path, required_phrases in (
        (
            P5_SOURCE_CONTRACT_REPORT_PATH,
            (
                "source contracts are technical, non-entitlement truth",
                "uns sources remain document-acquisition surfaces",
                "it sources remain structured progress surfaces",
            ),
        ),
        (
            P5_USAGE_RUNTIME_REPORT_PATH,
            (
                "usage_record.json",
                "not billing",
                "capability_domains_used",
                "source to usage linkage",
            ),
        ),
    ):
        if report_path.exists():
            report_text = report_path.read_text(encoding="utf-8")
            missing = [phrase for phrase in required_phrases if phrase not in report_text]
            checks.append(
                P5SourceContractCheck(
                    name=f"{report_path.name}_required_phrases",
                    ok=not missing,
                    detail="missing=" + (", ".join(missing) if missing else "none"),
                )
            )
        else:
            checks.append(
                P5SourceContractCheck(
                    name=f"{report_path.name}_required_phrases",
                    ok=False,
                    detail=str(report_path),
                )
            )

    return checks


def format_p5_source_contract_results(results: list[P5SourceContractCheck]) -> str:
    lines = ["P5-M4-S12 source/usage runtime checks:"]
    for result in results:
        status = "ok" if result.ok else "failed"
        lines.append(f"- {result.name} -> {status} ({result.detail})")
    return "\n".join(lines)


def format_p5_source_contract_summary() -> str:
    return "\n".join(
        (
            "P5-M4-S12 source contract and runtime usage truth:",
            "- source contracts are technical, non-entitlement truth",
            "- uns sources remain document-acquisition surfaces",
            "- it sources remain structured progress surfaces",
            "- usage_record.json is the dedicated runtime raw usage fact artifact",
            "- capability_domains_used is emitted as technical-domain evidence, not billing",
            "- source to usage linkage preserves source contract id when TaskV1 carries it",
        )
    )


def render_p5_source_contract_json() -> str:
    bundle = {
        "source_contract_matrix": build_p5_source_contract_matrix(),
        "source_surface_classification": build_p5_source_surface_classification(),
        "usage_runtime_contract": build_p5_usage_runtime_contract(),
        "usage_runtime_output_surface": build_p5_usage_runtime_output_surface(),
        "source_usage_linkage": build_p5_source_usage_linkage(),
    }
    return json.dumps(bundle, ensure_ascii=False, indent=2)
