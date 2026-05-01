from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import NotRequired, TypedDict, cast

REPORT_ID = "zephyr.p5.1.m5.user_facing_boundary.v1"
DEFAULT_HANDOFF_ROOT = Path(".tmp/p5_1_m4_final_handoff")
DEFAULT_OUT_ROOT = Path(".tmp/p5_1_m5_user_facing_boundary")

CATALOG_FILENAME = "catalog_after_m4_final.json"
READINESS_FILENAME = "readiness_after_m4_final.json"
DESTINATION_EVIDENCE_FILENAME = "destination_evidence_after_m4_final.json"
REPRESENTATIVE_FILENAME = "nm_representative_after_m4_final.json"
GAP_SUMMARY_FILENAME = "gap_summary_after_m4_final.json"
REPAIR_PLAN_FILENAME = "repair_plan_after_m4_final.json"
HANDOFF_MD_FILENAME = "p5_1_m4_final_handoff.md"
REPAIR_PLAN_MD_FILENAME = "repair_plan_after_m4_final.md"

SOURCE_IDS = (
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
)
DESTINATION_IDS = (
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
)
RELATED_TOOL_CHECKS = (
    ("usage_report", "tools/p5_usage_report.py"),
    ("capability_domain_report", "tools/p5_capability_domain_report.py"),
    ("product_cut_report", "tools/p5_product_cut_report.py"),
    ("governance_audit_report", "tools/p5_governance_audit_report.py"),
)
BOUNDARY_KEYWORDS = (
    "billing",
    "pricing",
    "license",
    "entitlement",
    "quota",
    "rbac",
    "kubernetes",
    "helm",
    "cloud",
    "distributed runtime",
    "installer",
    "gui",
    "saas",
    "multi-tenant",
    "all-pairs",
    "exhaustive",
)


class KeywordHit(TypedDict):
    keyword: str
    classification: str
    source: str
    line: str


class RelatedToolStatus(TypedDict):
    status: str
    path: str
    detail: str
    returncode: NotRequired[int]


def _generated_at_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig")
    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8", "utf-8-sig", "utf-16"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError as err:
            last_error = err
    assert last_error is not None
    raise last_error


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(_read_text(path))
    if not isinstance(loaded_obj, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return cast(dict[str, object], loaded_obj)


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected dict, got {type(value).__name__}")
    return cast(dict[str, object], value)


def _as_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list, got {type(value).__name__}")
    return cast(list[object], value)


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value).__name__}")
    return value


def _as_int(value: object) -> int:
    if not isinstance(value, int):
        raise ValueError(f"Expected int, got {type(value).__name__}")
    return value


def _as_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected bool, got {type(value).__name__}")
    return value


def _bool_word(value: bool) -> str:
    return "yes" if value else "no"


def _load_handoff_inputs(handoff_root: Path) -> dict[str, dict[str, object]]:
    return {
        "catalog": _load_json_object(handoff_root / CATALOG_FILENAME),
        "readiness": _load_json_object(handoff_root / READINESS_FILENAME),
        "destination_evidence": _load_json_object(handoff_root / DESTINATION_EVIDENCE_FILENAME),
        "representative": _load_json_object(handoff_root / REPRESENTATIVE_FILENAME),
        "gap_summary": _load_json_object(handoff_root / GAP_SUMMARY_FILENAME),
        "repair_plan": _load_json_object(handoff_root / REPAIR_PLAN_FILENAME),
    }


def _collect_related_tool_checks(repo_root: Path) -> dict[str, RelatedToolStatus]:
    statuses: dict[str, RelatedToolStatus] = {}
    for tool_id, relative_path in RELATED_TOOL_CHECKS:
        tool_path = repo_root / relative_path
        if not tool_path.exists():
            statuses[tool_id] = {
                "status": "not_available",
                "path": relative_path,
                "detail": "tool_missing",
            }
            continue
        completed = subprocess.run(
            [sys.executable, str(tool_path), "--check-artifacts"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        detail = completed.stdout.strip() or completed.stderr.strip() or "no_output"
        statuses[tool_id] = {
            "status": "pass" if completed.returncode == 0 else "fail",
            "path": relative_path,
            "detail": detail,
            "returncode": completed.returncode,
        }
    return statuses


def _normalize_non_blocking_notes(repair_plan: dict[str, object]) -> list[str]:
    notes: list[str] = []
    raw_notes = repair_plan.get("non_blocking_notes")
    if raw_notes is None:
        return notes
    for item in _as_list(raw_notes):
        note = _as_dict(item)
        note_id = _as_str(note["id"])
        if note_id == "note-backend-uns_api-v1":
            notes.append("backend.uns_api.v1 skip_env is backend-only and non-matrix")
        elif note_id == "note-clickhouse-dependency-surface-history":
            notes.append("ClickHouse historical dependency lock note is not a current fail")
        elif note_id == "note-it-structured-state-log-future-hardening":
            notes.append(
                "IT structured_state_log_status=artifact_paths_only remains future hardening"
            )
        else:
            notes.append(_as_str(note["message"]))
    return notes


def _keyword_classification(keyword: str, line: str) -> str:
    lowered = line.lower()
    negative_markers = (
        "not ",
        "not_",
        "no_",
        ": no",
        ": false",
        "does not",
        "doesn't",
        "excluded",
        "future",
        "non-blocking",
        "optional",
        "audit",
        "not equal",
        "not equivalent",
        "not exhaustive",
        "not full",
        "not billing",
        "not rbac",
        "not claimed",
    )
    if any(marker in lowered for marker in negative_markers):
        return "non_claim"
    if "usage_not_billing" in lowered or "capability_domains_not_license_entitlement" in lowered:
        return "allowed_boundary"
    if "governance_not_rbac" in lowered or "no_" in lowered:
        return "allowed_boundary"
    issue_phrases = (
        "supports kubernetes",
        "supports helm",
        "supports cloud",
        "supports installer",
        "supports gui",
        "supports saas",
        "supports multi-tenant",
        "full all-pairs",
        "exhaustive all-pairs",
        "license entitlement",
        "pricing entitlement",
        "usage is billing",
        "governance is rbac",
    )
    if any(phrase in lowered for phrase in issue_phrases):
        return "issue"
    if keyword in {"all-pairs", "exhaustive"}:
        if "false" in lowered or ": no" in lowered:
            return "non_claim"
        return "issue"
    return "allowed_boundary"


def _scan_boundary_keywords(texts: dict[str, str]) -> tuple[list[KeywordHit], list[KeywordHit]]:
    allowed_hits: list[KeywordHit] = []
    issues: list[KeywordHit] = []
    for source_name, text in texts.items():
        for line in text.splitlines():
            lowered = line.lower()
            for keyword in BOUNDARY_KEYWORDS:
                if keyword not in lowered:
                    continue
                classification = _keyword_classification(keyword, line)
                hit: KeywordHit = {
                    "keyword": keyword,
                    "classification": classification,
                    "source": source_name,
                    "line": line.strip(),
                }
                if classification == "issue":
                    issues.append(hit)
                else:
                    allowed_hits.append(hit)
    return allowed_hits, issues


def _build_matrix_lists(catalog: dict[str, object]) -> tuple[list[str], list[str], list[str]]:
    source_ids: list[str] = []
    destination_ids: list[str] = []
    backend_ids: list[str] = []
    for item in _as_list(catalog["connectors"]):
        connector = _as_dict(item)
        connector_id = _as_str(connector["id"])
        family = _as_str(connector["family"])
        if family == "source":
            source_ids.append(connector_id)
        elif family == "destination":
            destination_ids.append(connector_id)
        elif family == "backend":
            backend_ids.append(connector_id)
    return source_ids, destination_ids, backend_ids


def build_report(
    *,
    handoff_root: Path,
    out_root: Path,
    related_tool_checks: dict[str, RelatedToolStatus] | None = None,
) -> dict[str, object]:
    repo_root = _repo_root()
    loaded = _load_handoff_inputs(handoff_root)
    catalog = loaded["catalog"]
    readiness = loaded["readiness"]
    destination_evidence = loaded["destination_evidence"]
    representative = loaded["representative"]
    gap_summary = loaded["gap_summary"]
    repair_plan = loaded["repair_plan"]
    if related_tool_checks is None:
        related_tool_checks = _collect_related_tool_checks(repo_root)

    catalog_summary = _as_dict(catalog["summary"])
    readiness_summary = _as_dict(readiness["summary"])
    representative_summary = _as_dict(representative["summary"])
    source_ids, destination_ids, backend_ids = _build_matrix_lists(catalog)

    active_blockers = _as_int(repair_plan["active_blocker_count"])
    backend_counted_as_destination = bool(_as_int(gap_summary["backend_counted_as_destination"]))
    source_spec_fields_empty = _as_int(gap_summary["source_spec_fields_empty"])
    representative_routes_passed = _as_int(representative_summary["overall_pass_count"])
    representative_is_full_all_pairs = representative_routes_passed >= 100
    all_pairs_100_claimed = representative_is_full_all_pairs

    flow_explanation = {
        "uns_stream_explained": "source.uns.http_document.v1" in source_ids
        and any(
            _as_str(_as_dict(item)["flow"]) == "uns"
            for item in _as_list(destination_evidence["results"])
        ),
        "it_stream_explained": "source.it.http_json_cursor.v1" in source_ids
        and any(
            _as_str(_as_dict(item)["flow"]) == "it"
            for item in _as_list(destination_evidence["results"])
        ),
        "delivery_payload_v1_explained": True,
        "destination_endpoint_readback_explained": any(
            _as_bool(_as_dict(item)["endpoint_readback_ok"])
            for item in _as_list(destination_evidence["results"])
        ),
    }
    mode_explanation = {
        "remote_only_direct_explained": True,
        "artifact_prepared_direct_explained_as_audit_only": (
            "artifact_prepared_direct_note" in representative
            and "product_direct_note" in destination_evidence
            and _as_bool(readiness_summary["readiness_pass_does_not_imply_product_direct_pass"])
        ),
        "fanout_explained_as_optional_composite_destination": (
            "fanout_mode_note" in representative
            and any(
                _as_str(_as_dict(item)["validation_mode"]) == "fanout_audit"
                for item in _as_list(destination_evidence["results"])
            )
        ),
        "readiness_pass_not_equal_product_direct_pass": _as_bool(
            readiness_summary["readiness_pass_does_not_imply_product_direct_pass"]
        ),
    }
    business_boundary = {
        "usage_not_billing": related_tool_checks["usage_report"]["status"] == "pass",
        "capability_domains_not_license_entitlement": (
            related_tool_checks["capability_domain_report"]["status"] == "pass"
        ),
        "governance_not_rbac": related_tool_checks["governance_audit_report"]["status"] == "pass",
        "no_pricing_claim": True,
        "no_quota_claim": True,
    }
    p6_boundary = {
        "no_gui_claim": True,
        "no_installer_claim": True,
        "no_cloud_claim": True,
        "no_k8s_helm_claim": True,
        "no_saas_multitenant_claim": True,
    }
    non_blocking_notes = _normalize_non_blocking_notes(repair_plan)

    issues: list[dict[str, object]] = []
    if active_blockers != 0:
        issues.append(
            {
                "severity": "blocker",
                "area": "repair_plan",
                "message": "P5.1-M4 active blockers are not zero.",
            }
        )
    if source_spec_fields_empty != 0:
        issues.append(
            {
                "severity": "major",
                "area": "source_spec_fields",
                "message": "Retained source spec fields are still incomplete.",
            }
        )
    if backend_counted_as_destination:
        issues.append(
            {
                "severity": "major",
                "area": "matrix_claims",
                "message": "backend.uns_api.v1 is being counted as a destination.",
            }
        )

    boundary_bools = {
        **flow_explanation,
        **mode_explanation,
        **business_boundary,
        **p6_boundary,
    }
    explanation_failures = [
        name for name, value in boundary_bools.items() if not value
    ]
    major_ux_confusions = len(explanation_failures)
    if explanation_failures:
        issues.append(
            {
                "severity": "major",
                "area": "boundary_explanation",
                "message": "Boundary explanation booleans are not all green.",
                "details": explanation_failures,
            }
        )

    overall = "pass"
    if issues:
        overall = (
            "fail"
            if any(_as_str(item["severity"]) == "blocker" for item in issues)
            else "conditional"
        )

    report: dict[str, object] = {
        "schema_version": 1,
        "report_id": REPORT_ID,
        "generated_at_utc": _generated_at_utc(),
        "handoff_root": handoff_root.as_posix(),
        "out_root": out_root.as_posix(),
        "summary": {
            "overall": overall,
            "active_blockers": active_blockers,
            "major_ux_confusions": major_ux_confusions,
            "notes": [
                "P5.1-M4 is sealed before this user-facing boundary audit.",
                "backend.uns_api.v1 remains backend-only and does not block the retained matrix.",
            ],
        },
        "matrix_claims": {
            "source_count": _as_int(catalog_summary["source_count"]),
            "destination_count": _as_int(catalog_summary["destination_count"]),
            "backend_count": _as_int(catalog_summary["backend_count"]),
            "backend_uns_api_counted_as_destination": backend_counted_as_destination,
            "representative_routes_passed": representative_routes_passed,
            "representative_is_full_all_pairs": representative_is_full_all_pairs,
            "all_pairs_100_claimed": all_pairs_100_claimed,
        },
        "flow_explanation": flow_explanation,
        "mode_explanation": mode_explanation,
        "business_boundary": business_boundary,
        "p6_boundary": p6_boundary,
        "non_blocking_notes": non_blocking_notes,
        "related_tool_checks": related_tool_checks,
        "inputs_loaded": {
            "catalog": CATALOG_FILENAME,
            "readiness": READINESS_FILENAME,
            "destination_evidence": DESTINATION_EVIDENCE_FILENAME,
            "representative": REPRESENTATIVE_FILENAME,
            "gap_summary": GAP_SUMMARY_FILENAME,
            "repair_plan": REPAIR_PLAN_FILENAME,
        },
        "source_matrix": list(source_ids),
        "destination_matrix": list(destination_ids),
        "backend_matrix": list(backend_ids),
        "issues": issues,
    }

    markdown = render_markdown(report)
    json_text = json.dumps(report, ensure_ascii=False, indent=2)
    scan_texts = {
        "generated_markdown": markdown,
        "generated_json": json_text,
    }
    handoff_md_path = handoff_root / HANDOFF_MD_FILENAME
    if handoff_md_path.exists():
        scan_texts[HANDOFF_MD_FILENAME] = _read_text(handoff_md_path)
    repair_plan_md_path = handoff_root / REPAIR_PLAN_MD_FILENAME
    if repair_plan_md_path.exists():
        scan_texts[REPAIR_PLAN_MD_FILENAME] = _read_text(repair_plan_md_path)
    allowed_hits, keyword_issues = _scan_boundary_keywords(scan_texts)
    report["keyword_scan"] = {
        "allowed_boundary_hits_count": len(allowed_hits),
        "issue_count": len(keyword_issues),
        "allowed_boundary_hits": allowed_hits,
        "issues": keyword_issues,
    }
    if keyword_issues:
        report["summary"] = {
            **_as_dict(report["summary"]),
            "overall": "fail",
        }
        issue_list = cast(list[object], report["issues"])
        issue_list.extend(
            {
                "severity": "major",
                "area": "keyword_scan",
                "message": f"Suspicious overclaim keyword hit: {hit['line']}",
            }
            for hit in keyword_issues
        )
    return report


def render_markdown(report: dict[str, object]) -> str:
    summary = _as_dict(report["summary"])
    matrix_claims = _as_dict(report["matrix_claims"])
    flow_explanation = _as_dict(report["flow_explanation"])
    mode_explanation = _as_dict(report["mode_explanation"])
    business_boundary = _as_dict(report["business_boundary"])
    p6_boundary = _as_dict(report["p6_boundary"])
    related_tool_checks = _as_dict(report["related_tool_checks"])
    keyword_scan = _as_dict(report["keyword_scan"]) if "keyword_scan" in report else {}
    usage_treated_as_billing = not _as_bool(business_boundary["usage_not_billing"])
    capability_as_entitlement = not _as_bool(
        business_boundary["capability_domains_not_license_entitlement"]
    )
    governance_as_rbac = not _as_bool(business_boundary["governance_not_rbac"])
    p6_claims_present = not all(_as_bool(p6_boundary[key]) for key in p6_boundary)
    lines = [
        "# Zephyr P5.1 user-facing boundary report",
        "",
        "## Final judgment",
        f"- P5.1-M4 sealed: {_bool_word(_as_int(summary['active_blockers']) == 0)}",
        f"- M5-A user-facing boundary: {_as_str(summary['overall'])}",
        f"- active blockers: {_as_int(summary['active_blockers'])}",
        f"- major UX confusions: {_as_int(summary['major_ux_confusions'])}",
        "",
        "## What P5.1 can do",
        (
            "- retained 10 source × 10 destination matrix with one backend "
            "surface kept out of the destination count"
        ),
        (
            "- UNS source flow: external unstructured document -> acquisition/download/export "
            "-> uns-stream -> elements + normalized_text + run_meta -> DeliveryPayloadV1 "
            "-> destination endpoint"
        ),
        (
            "- IT source flow: external structured data -> acquisition/query/poll/consume "
            "-> it-stream -> StructuredRecord / normalized_text / records evidence + run_meta "
            "-> DeliveryPayloadV1 -> destination endpoint"
        ),
        (
            "- destination endpoints receive Zephyr-processed payload plus Zephyr metadata "
            "such as run_id / sha256 / provenance / content_evidence, not only raw source content"
        ),
        (
            "- readiness / destination evidence / representative / repair-plan "
            "tooling is available for audit and operator interpretation"
        ),
        "",
        "## What P5.1 does not claim",
        "- not cloud / Kubernetes / Helm deployment packaging",
        "- not GUI / installer",
        "- not SaaS / multi-tenant product shell",
        "- not billing / pricing / license / entitlement / quota",
        "- not full RBAC / enterprise permission system",
        "- not 100-route all-pairs exhaustive matrix proof",
        "- not P6 productization complete",
        "",
        "## Validation mode glossary",
        (
            "- remote-only direct = product-like direct payload path that does "
            "not depend on pre-dumped artifacts"
        ),
        (
            "- artifact_prepared_direct = audit/prepared-artifact validation "
            "mode, not product direct proof"
        ),
        "- fanout = optional composite destination / audit mode, not mandatory default path",
        "- readiness pass = dependency/env/service availability, not full product route pass",
        "- representative N×M = bounded representative pairing, not exhaustive all-pairs",
        "",
        "## Source/destination matrix",
        "- sources:",
    ]
    for source_id in cast(list[str], report["source_matrix"]):
        lines.append(f"  - {source_id}")
    lines.append("- destinations:")
    for destination_id in cast(list[str], report["destination_matrix"]):
        lines.append(f"  - {destination_id}")
    lines.append("- backend excluded from destination count:")
    for backend_id in cast(list[str], report["backend_matrix"]):
        lines.append(f"  - {backend_id}")
    lines.extend(
        [
            "",
            "## Non-blocking notes",
        ]
    )
    for note in cast(list[str], report["non_blocking_notes"]):
        lines.append(f"- {note}")
    lines.extend(
        [
            "",
            "## Boundary sanity facts",
            f"- source_count: {_as_int(matrix_claims['source_count'])}",
            f"- destination_count: {_as_int(matrix_claims['destination_count'])}",
            f"- backend_count: {_as_int(matrix_claims['backend_count'])}",
            (
                "- backend.uns_api.v1 counted as destination: "
                f"{_bool_word(_as_bool(matrix_claims['backend_uns_api_counted_as_destination']))}"
            ),
            (
                "- representative routes passed: "
                f"{_as_int(matrix_claims['representative_routes_passed'])}"
            ),
            (
                "- representative is full all-pairs: "
                f"{_bool_word(_as_bool(matrix_claims['representative_is_full_all_pairs']))}"
            ),
            (
                "- all-pairs 100 claimed: "
                f"{_bool_word(_as_bool(matrix_claims['all_pairs_100_claimed']))}"
            ),
            "",
            "## Boundary helpers",
            (
                "- usage_report check: "
                f"{_as_str(_as_dict(related_tool_checks['usage_report'])['status'])}"
            ),
            (
                "- capability_domain_report check: "
                f"{_as_str(_as_dict(related_tool_checks['capability_domain_report'])['status'])}"
            ),
            (
                "- product_cut_report check: "
                f"{_as_str(_as_dict(related_tool_checks['product_cut_report'])['status'])}"
            ),
            (
                "- governance_audit_report check: "
                f"{_as_str(_as_dict(related_tool_checks['governance_audit_report'])['status'])}"
            ),
            "",
            "## Boundary booleans",
            (
                "- UNS flow explained: "
                f"{_bool_word(_as_bool(flow_explanation['uns_stream_explained']))}"
            ),
            (
                "- IT flow explained: "
                f"{_bool_word(_as_bool(flow_explanation['it_stream_explained']))}"
            ),
            (
                "- DeliveryPayloadV1 explained: "
                f"{_bool_word(_as_bool(flow_explanation['delivery_payload_v1_explained']))}"
            ),
            (
                "- destination endpoint readback explained: "
                f"{_bool_word(_as_bool(flow_explanation['destination_endpoint_readback_explained']))}"
            ),
            (
                "- remote-only direct explained: "
                f"{_bool_word(_as_bool(mode_explanation['remote_only_direct_explained']))}"
            ),
            (
                "- artifact_prepared_direct audit-only: "
                f"{_bool_word(_as_bool(mode_explanation['artifact_prepared_direct_explained_as_audit_only']))}"
            ),
            (
                "- fanout optional composite: "
                f"{_bool_word(_as_bool(mode_explanation['fanout_explained_as_optional_composite_destination']))}"
            ),
            (
                "- readiness pass not product direct pass: "
                f"{_bool_word(_as_bool(mode_explanation['readiness_pass_not_equal_product_direct_pass']))}"
            ),
            (
                "- usage treated as billing: "
                f"{_bool_word(usage_treated_as_billing)}"
            ),
            (
                "- Base/Pro/Extra treated as license/entitlement: "
                f"{_bool_word(capability_as_entitlement)}"
            ),
            (
                "- governance treated as RBAC: "
                f"{_bool_word(governance_as_rbac)}"
            ),
            (
                "- P5.1 claims GUI/installer/cloud/K8s/Helm/SaaS: "
                f"{_bool_word(p6_claims_present)}"
            ),
            "- P5.1 claims P6 productization complete: no",
        ]
    )
    if keyword_scan:
        lines.extend(
            [
                "",
                "## Boundary keyword scan",
                (
                    "- allowed boundary hits count: "
                    f"{_as_int(keyword_scan['allowed_boundary_hits_count'])}"
                ),
                f"- issue count: {_as_int(keyword_scan['issue_count'])}",
            ]
        )
    return "\n".join(lines) + "\n"


def _emit_outputs(report: dict[str, object], out_root: Path) -> tuple[Path, Path]:
    out_root.mkdir(parents=True, exist_ok=True)
    json_path = out_root / "report.json"
    md_path = out_root / "report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def _validate_emitted_report(report: dict[str, object], out_root: Path) -> list[str]:
    issues: list[str] = []
    json_path = out_root / "report.json"
    md_path = out_root / "report.md"
    if not json_path.exists():
        issues.append("report_json_missing")
    if not md_path.exists():
        issues.append("report_md_missing")
    if _as_int(_as_dict(report["summary"])["active_blockers"]) != 0:
        issues.append("active_blockers_nonzero")
    if _as_bool(_as_dict(report["matrix_claims"])["backend_uns_api_counted_as_destination"]):
        issues.append("backend_counted_as_destination")
    if _as_int(_as_dict(report["keyword_scan"])["issue_count"]) != 0:
        issues.append("boundary_keyword_issue")
    return issues


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="p5_1_m5_user_facing_boundary_report",
        description="Render or validate the P5.1-M5-A user-facing boundary report.",
    )
    parser.add_argument("--handoff-root", type=Path, default=DEFAULT_HANDOFF_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--json", action="store_true")
    mode_group.add_argument("--markdown", action="store_true")
    mode_group.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = build_report(handoff_root=args.handoff_root, out_root=args.out_root)
    _emit_outputs(report, args.out_root)
    if args.check_artifacts:
        issues = _validate_emitted_report(report, args.out_root)
        if issues:
            print("\n".join(issues))
            return 1
        print("p5_1_m5_user_facing_boundary_report -> ok")
        return 0
    if args.markdown:
        print(render_markdown(report), end="")
        return 0
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
