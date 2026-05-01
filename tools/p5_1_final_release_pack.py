from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict, cast

REPORT_ID = "zephyr.p5.1.final_release_pack.v1"
DEFAULT_HANDOFF_ROOT = Path(".tmp/p5_1_m4_final_handoff")
DEFAULT_M5A_REPORT = Path(".tmp/p5_1_m5_user_facing_boundary/report.json")
DEFAULT_M5B_REPORT = Path(".tmp/p5_1_m5_cli_env_ux/report.json")
DEFAULT_OUT_ROOT = Path(".tmp/p5_1_final_release_pack")

CATALOG_FILENAME = "catalog_after_m4_final.json"
READINESS_FILENAME = "readiness_after_m4_final.json"
DESTINATION_EVIDENCE_FILENAME = "destination_evidence_after_m4_final.json"
REPRESENTATIVE_FILENAME = "nm_representative_after_m4_final.json"
GAP_SUMMARY_FILENAME = "gap_summary_after_m4_final.json"
REPAIR_PLAN_FILENAME = "repair_plan_after_m4_final.json"
HANDOFF_SUMMARY_FILENAME = "p5_1_m4_final_handoff_summary.json"
HANDOFF_MD_FILENAME = "p5_1_m4_final_handoff.md"
REPAIR_PLAN_MD_FILENAME = "repair_plan_after_m4_final.md"

POSITIVE_OVERCLAIM_PHRASES = (
    "full all-pairs pass",
    "100-route exhaustive pass",
    "cloud deployment ready",
    "kubernetes ready",
    "helm chart complete",
    "gui complete",
    "installer complete",
    "billing system",
    "license entitlement",
    "rbac complete",
    "saas ready",
    "multi-tenant ready",
)
NEGATIVE_BOUNDARY_MARKERS = (
    "not ",
    "no ",
    "does not",
    "doesn't",
    "cannot claim",
    "excluded",
    ": false",
    ": no",
)


class ArtifactEntry(TypedDict):
    label: str
    path: str
    exists: bool


class OverclaimIssue(TypedDict):
    phrase: str
    source: str
    line: str


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
        "handoff_summary": _load_json_object(handoff_root / HANDOFF_SUMMARY_FILENAME),
    }


def _normalize_non_blocking_notes(
    repair_plan: dict[str, object],
    *,
    add_filesystem_note: bool,
    add_minio_port_note: bool,
    add_powershell_note: bool,
) -> list[str]:
    notes: list[str] = []
    raw_notes = repair_plan.get("non_blocking_notes")
    if raw_notes is not None:
        for item in _as_list(raw_notes):
            note = _as_dict(item)
            note_id = _as_str(note["id"])
            if note_id == "note-backend-uns_api-v1":
                notes.append("backend.uns_api.v1 skip_env is backend-only and non-matrix")
            elif note_id == "note-clickhouse-dependency-surface-history":
                notes.append("ClickHouse historical dependency lock note is not a current fail")
            elif note_id == "note-it-structured-state-log-future-hardening":
                notes.append("IT structured_state_log_status=artifact_paths_only future hardening")
            else:
                notes.append(_as_str(note["message"]))
    if add_filesystem_note:
        notes.append("filesystem is retained destination but not registry spec id")
    if add_minio_port_note:
        notes.append(
            "MinIO host port migrated from 59000 to 59100 due Windows excluded port range"
        )
    if add_powershell_note:
        notes.append("PowerShell output encoding/path caveat is documented")
    deduped: list[str] = []
    for note_text in notes:
        if note_text not in deduped:
            deduped.append(note_text)
    return deduped


def _active_repair_groups_empty(repair_plan: dict[str, object]) -> bool:
    repair_groups = _as_dict(repair_plan["repair_groups"])
    return all(len(_as_list(items)) == 0 for items in repair_groups.values())


def _backend_only_non_blocking(repair_plan: dict[str, object]) -> bool:
    for item in _as_list(repair_plan["non_blocking_notes"]):
        note = _as_dict(item)
        if _as_str(note["id"]) == "note-backend-uns_api-v1":
            return True
    return False


def _minio_port_migrated_to_59100() -> bool:
    source = _read_text(
        _repo_root() / "packages/zephyr-ingest/src/zephyr_ingest/testing/p45.py"
    )
    return '"ZEPHYR_P45_S3_PORT": "59100"' in source and "default_port=59100" in source


def _p45_substrate_healthcheck_status(
    handoff_summary: dict[str, object],
    m5b_report: dict[str, object],
) -> str:
    m5b_notes = [_as_str(value) for value in _as_list(m5b_report["non_blocking_notes"])]
    windows_note_present = any("excluded port range" in note.lower() for note in m5b_notes)
    if (
        _as_str(handoff_summary["judgment"]) == "PASS"
        and _as_str(_as_dict(m5b_report["summary"])["overall"]) == "pass"
        and windows_note_present
        and _minio_port_migrated_to_59100()
    ):
        return "pass"
    return "not_checked"


def _artifact_index(
    *,
    handoff_root: Path,
    m5a_report_path: Path,
    m5b_report_path: Path,
    out_root: Path,
) -> list[ArtifactEntry]:
    repo_root = _repo_root()
    paths = [
        ("M4 final handoff summary JSON", handoff_root / HANDOFF_SUMMARY_FILENAME),
        ("M4 final handoff markdown", handoff_root / HANDOFF_MD_FILENAME),
        ("catalog_after_m4_final.json", handoff_root / CATALOG_FILENAME),
        ("readiness_after_m4_final.json", handoff_root / READINESS_FILENAME),
        (
            "destination_evidence_after_m4_final.json",
            handoff_root / DESTINATION_EVIDENCE_FILENAME,
        ),
        ("nm_representative_after_m4_final.json", handoff_root / REPRESENTATIVE_FILENAME),
        ("gap_summary_after_m4_final.json", handoff_root / GAP_SUMMARY_FILENAME),
        ("repair_plan_after_m4_final.json", handoff_root / REPAIR_PLAN_FILENAME),
        ("repair_plan_after_m4_final.md", handoff_root / REPAIR_PLAN_MD_FILENAME),
        ("M5-A report JSON", m5a_report_path),
        ("M5-A report markdown", m5a_report_path.with_suffix(".md")),
        ("M5-B report JSON", m5b_report_path),
        ("M5-B report markdown", m5b_report_path.with_suffix(".md")),
        ("final release pack JSON", out_root / "release_pack.json"),
        ("final release pack markdown", out_root / "release_pack.md"),
    ]
    final_json = out_root / "release_pack.json"
    final_md = out_root / "release_pack.md"
    results: list[ArtifactEntry] = []
    for label, path in paths:
        exists = path.exists() or path in {final_json, final_md}
        results.append(
            {
                "label": label,
                "path": path.relative_to(repo_root).as_posix()
                if path.is_relative_to(repo_root)
                else path.as_posix(),
                "exists": exists,
            }
        )
    return results


def _scan_positive_overclaims(texts: dict[str, str]) -> list[OverclaimIssue]:
    issues: list[OverclaimIssue] = []
    for source_name, text in texts.items():
        for raw_line in text.splitlines():
            line = raw_line.strip()
            lowered = line.lower()
            for phrase in POSITIVE_OVERCLAIM_PHRASES:
                if phrase not in lowered:
                    continue
                if any(marker in lowered for marker in NEGATIVE_BOUNDARY_MARKERS):
                    continue
                issues.append(
                    {
                        "phrase": phrase,
                        "source": source_name,
                        "line": line,
                    }
                )
    return issues


def scan_positive_overclaims(texts: dict[str, str]) -> list[OverclaimIssue]:
    return _scan_positive_overclaims(texts)


def build_report(
    *,
    handoff_root: Path,
    m5a_report_path: Path,
    m5b_report_path: Path,
    out_root: Path,
) -> dict[str, object]:
    loaded = _load_handoff_inputs(handoff_root)
    catalog = loaded["catalog"]
    representative = loaded["representative"]
    gap_summary = loaded["gap_summary"]
    repair_plan = loaded["repair_plan"]
    handoff_summary = loaded["handoff_summary"]
    m5a_report = _load_json_object(m5a_report_path)
    m5b_report = _load_json_object(m5b_report_path)

    catalog_summary = _as_dict(catalog["summary"])
    representative_summary = _as_dict(representative["summary"])
    gap = _as_dict(gap_summary)
    m5a_summary = _as_dict(m5a_report["summary"])
    m5b_summary = _as_dict(m5b_report["summary"])
    m5a_matrix_claims = _as_dict(m5a_report["matrix_claims"])
    m5a_business_boundary = _as_dict(m5a_report["business_boundary"])
    m5a_keyword_scan = _as_dict(m5a_report["keyword_scan"])
    m5b_spec_show = _as_dict(m5b_report["spec_show"])
    m5b_secret_safety = _as_dict(m5b_report["secret_safety"])
    m5b_error_taxonomy = _as_dict(m5b_report["error_taxonomy"])

    issues: list[dict[str, object]] = []

    zero_expectations = {
        "readiness_fail_dependency": 0,
        "readiness_fail_service": 0,
        "readiness_missing_implementation": 0,
        "destination_evidence_failed": 0,
        "remote_only_content_evidence_gap": 0,
        "nm_blocked": 0,
        "nm_failed_executed": 0,
        "source_spec_fields_empty": 0,
    }
    for key, expected in zero_expectations.items():
        if _as_int(gap[key]) != expected:
            issues.append(
                {
                    "severity": "blocker",
                    "area": "m4_gap_summary",
                    "message": f"{key} expected {expected}, got {_as_int(gap[key])}",
                }
            )

    if _as_int(repair_plan["active_blocker_count"]) != 0:
        issues.append(
            {
                "severity": "blocker",
                "area": "repair_plan",
                "message": "repair_plan active_blocker_count is not zero",
            }
        )
    if len(_as_list(repair_plan["prioritized_actions"])) != 0:
        issues.append(
            {
                "severity": "major",
                "area": "repair_plan",
                "message": "repair_plan prioritized_actions is not empty",
            }
        )
    if not _active_repair_groups_empty(repair_plan):
        issues.append(
            {
                "severity": "major",
                "area": "repair_plan",
                "message": "repair_plan repair_groups still contain active items",
            }
        )
    if not _backend_only_non_blocking(repair_plan):
        issues.append(
            {
                "severity": "major",
                "area": "repair_plan",
                "message": "backend.uns_api.v1 backend-only note missing from non_blocking_notes",
            }
        )

    if _as_str(m5a_summary["overall"]) != "pass":
        issues.append(
            {
                "severity": "blocker",
                "area": "m5a",
                "message": "M5-A overall is not pass",
            }
        )
    if _as_int(m5a_summary["active_blockers"]) != 0:
        issues.append(
            {
                "severity": "blocker",
                "area": "m5a",
                "message": "M5-A active_blockers is not zero",
            }
        )
    if _as_int(m5a_summary["major_ux_confusions"]) != 0:
        issues.append(
            {
                "severity": "major",
                "area": "m5a",
                "message": "M5-A major_ux_confusions is not zero",
            }
        )
    if _as_bool(m5a_matrix_claims["representative_is_full_all_pairs"]):
        issues.append(
            {
                "severity": "major",
                "area": "m5a",
                "message": "M5-A incorrectly claims representative routes are full all-pairs",
            }
        )
    if _as_bool(m5a_matrix_claims["all_pairs_100_claimed"]):
        issues.append(
            {
                "severity": "major",
                "area": "m5a",
                "message": "M5-A incorrectly claims 100-route exhaustive proof",
            }
        )
    if _as_bool(m5a_matrix_claims["backend_uns_api_counted_as_destination"]):
        issues.append(
            {
                "severity": "major",
                "area": "m5a",
                "message": "M5-A counts backend.uns_api.v1 as destination",
            }
        )

    if _as_str(m5b_summary["overall"]) != "pass":
        issues.append(
            {
                "severity": "blocker",
                "area": "m5b",
                "message": "M5-B overall is not pass",
            }
        )
    if _as_int(m5b_summary["active_blockers"]) != 0:
        issues.append(
            {
                "severity": "blocker",
                "area": "m5b",
                "message": "M5-B active_blockers is not zero",
            }
        )
    if _as_int(m5b_summary["major_ux_gaps"]) != 0:
        issues.append(
            {
                "severity": "major",
                "area": "m5b",
                "message": "M5-B major_ux_gaps is not zero",
            }
        )
    if _as_int(m5b_spec_show["source_specs_readable"]) != 10:
        issues.append(
            {
                "severity": "major",
                "area": "m5b",
                "message": "M5-B source_specs_readable is not 10",
            }
        )
    if _as_int(m5b_spec_show["destination_specs_readable"]) != 10:
        issues.append(
            {
                "severity": "major",
                "area": "m5b",
                "message": "M5-B destination_specs_readable is not 10",
            }
        )
    if _as_bool(m5b_secret_safety["secret_values_printed"]):
        issues.append(
            {
                "severity": "major",
                "area": "m5b",
                "message": "M5-B secret_values_printed must stay false",
            }
        )
    if "backend.uns_api.v1" not in _as_str(m5b_spec_show["backend_note"]):
        issues.append(
            {
                "severity": "major",
                "area": "m5b",
                "message": "M5-B backend note is missing backend.uns_api.v1 exclusion",
            }
        )

    runtime_substrate_status = _p45_substrate_healthcheck_status(handoff_summary, m5b_report)
    minio_59100 = _minio_port_migrated_to_59100()
    non_blocking_notes = _normalize_non_blocking_notes(
        repair_plan,
        add_filesystem_note=True,
        add_minio_port_note=minio_59100,
        add_powershell_note=True,
    )

    error_taxonomy_classifiable = all(_as_bool(value) for value in m5b_error_taxonomy.values())
    repair_plan_active_blockers = _as_int(repair_plan["active_blocker_count"])
    representative_routes_passed = _as_int(representative_summary["overall_pass_count"])

    can_claim = [
        "bounded local runtime/operator readiness",
        "retained 10 source × 10 destination matrix",
        "representative N×M validation, not exhaustive all-pairs",
        (
            "UNS stream: external unstructured document -> acquisition/download/export -> "
            "uns-stream -> elements + normalized_text + run_meta -> "
            "DeliveryPayloadV1 -> destination"
        ),
        (
            "IT stream: external structured data -> acquisition/query/poll/consume -> it-stream "
            "-> StructuredRecord / normalized_text / records evidence + run_meta "
            "-> DeliveryPayloadV1 -> destination"
        ),
        "destination endpoints receive Zephyr processed payload and Zephyr metadata",
        "readiness/evidence/repair-plan/boundary/CLI UX reports available",
    ]
    cannot_claim = [
        "not full 100 all-pairs exhaustive proof",
        "not cloud/K8s/Helm deployment",
        "not GUI / installer",
        "not SaaS / multi-tenant product shell",
        "not billing/pricing/license/entitlement/quota system",
        "not full RBAC / enterprise permission system",
        "not P6 productization complete",
        "not fanout as mandatory default path",
        "not artifact_prepared_direct as remote-only direct product proof",
    ]

    report: dict[str, object] = {
        "schema_version": 1,
        "report_id": REPORT_ID,
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": "pass",
            "p5_1_final_status": "sealed",
            "active_blockers": 0,
            "major_gaps": 0,
            "release_pack_complete": True,
        },
        "matrix": {
            "retained_source_count": _as_int(catalog_summary["source_count"]),
            "retained_destination_count": _as_int(catalog_summary["destination_count"]),
            "backend_count": _as_int(catalog_summary["backend_count"]),
            "backend_uns_api_counted_as_destination": False,
            "filesystem_destination_note": True,
            "representative_routes_passed": representative_routes_passed,
            "representative_is_full_all_pairs": False,
            "all_pairs_100_claimed": False,
        },
        "runtime_substrate": {
            "p45_substrate_healthcheck_status": runtime_substrate_status,
            "minio_port_migrated_to_59100": minio_59100,
            "runtime_home_modified_by_this_tool": False,
        },
        "evidence": {
            "catalog_loaded": True,
            "readiness_loaded": True,
            "destination_evidence_loaded": True,
            "representative_loaded": True,
            "gap_summary_loaded": True,
            "repair_plan_loaded": True,
            "m5a_loaded": True,
            "m5b_loaded": True,
        },
        "m4_final": {
            "readiness_fail_dependency": _as_int(gap["readiness_fail_dependency"]),
            "readiness_fail_service": _as_int(gap["readiness_fail_service"]),
            "readiness_missing_implementation": _as_int(
                gap["readiness_missing_implementation"]
            ),
            "destination_evidence_failed": _as_int(gap["destination_evidence_failed"]),
            "remote_only_content_evidence_gap": _as_int(
                gap["remote_only_content_evidence_gap"]
            ),
            "nm_blocked": _as_int(gap["nm_blocked"]),
            "nm_failed_executed": _as_int(gap["nm_failed_executed"]),
            "source_spec_fields_empty": _as_int(gap["source_spec_fields_empty"]),
            "repair_plan_active_blockers": repair_plan_active_blockers,
        },
        "m5a_user_boundary": {
            "overall": _as_str(m5a_summary["overall"]),
            "active_blockers": _as_int(m5a_summary["active_blockers"]),
            "major_ux_confusions": _as_int(m5a_summary["major_ux_confusions"]),
            "usage_not_billing": _as_bool(m5a_business_boundary["usage_not_billing"]),
            "capability_domains_not_license_entitlement": _as_bool(
                m5a_business_boundary["capability_domains_not_license_entitlement"]
            ),
            "governance_not_rbac": _as_bool(m5a_business_boundary["governance_not_rbac"]),
            "p6_overclaim_found": _as_int(m5a_keyword_scan["issue_count"]) != 0,
        },
        "m5b_cli_env_ux": {
            "overall": _as_str(m5b_summary["overall"]),
            "active_blockers": _as_int(m5b_summary["active_blockers"]),
            "major_ux_gaps": _as_int(m5b_summary["major_ux_gaps"]),
            "source_specs_readable": _as_int(m5b_spec_show["source_specs_readable"]),
            "destination_specs_readable": _as_int(
                m5b_spec_show["destination_specs_readable"]
            ),
            "secret_values_printed": _as_bool(m5b_secret_safety["secret_values_printed"]),
            "error_taxonomy_classifiable": error_taxonomy_classifiable,
        },
        "can_claim": can_claim,
        "cannot_claim": cannot_claim,
        "non_blocking_notes": non_blocking_notes,
        "artifact_index": _artifact_index(
            handoff_root=handoff_root,
            m5a_report_path=m5a_report_path,
            m5b_report_path=m5b_report_path,
            out_root=out_root,
        ),
        "issues": issues,
    }

    markdown = render_markdown(report)
    json_text = json.dumps(report, ensure_ascii=False, indent=2)
    overclaim_issues = _scan_positive_overclaims(
        {"generated_markdown": markdown, "generated_json": json_text}
    )
    report["boundary_scan"] = {
        "suspicious_overclaim_found": len(overclaim_issues) > 0,
        "issue_count": len(overclaim_issues),
        "issues": overclaim_issues,
    }
    if issues or overclaim_issues:
        severity_values = {_as_str(item["severity"]) for item in issues}
        overall = (
            "fail" if "blocker" in severity_values or overclaim_issues else "conditional"
        )
        summary = {
            "overall": overall,
            "p5_1_final_status": "sealed",
            "active_blockers": sum(
                1 for item in issues if _as_str(item["severity"]) == "blocker"
            ),
            "major_gaps": sum(1 for item in issues if _as_str(item["severity"]) == "major")
            + len(overclaim_issues),
            "release_pack_complete": False,
        }
        report["summary"] = summary
    return report


def render_markdown(report: dict[str, object]) -> str:
    summary = _as_dict(report["summary"])
    matrix = _as_dict(report["matrix"])
    runtime_substrate = _as_dict(report["runtime_substrate"])
    m4_final = _as_dict(report["m4_final"])
    m5a = _as_dict(report["m5a_user_boundary"])
    m5b = _as_dict(report["m5b_cli_env_ux"])
    artifact_index = [_as_dict(item) for item in _as_list(report["artifact_index"])]

    lines = [
        "# Zephyr P5.1 final release pack",
        "",
        "## Final status",
        (
            f"- P5.1 final status: {_as_str(summary['p5_1_final_status'])} / "
            f"{_as_str(summary['overall'])}"
        ),
        f"- active blockers: {_as_int(summary['active_blockers'])}",
        f"- major gaps: {_as_int(summary['major_gaps'])}",
        f"- release pack complete: {_bool_word(_as_bool(summary['release_pack_complete']))}",
        "",
        "## What is completed",
        (
            "- P4.5 bounded local substrate health restored: "
            f"{_as_str(runtime_substrate['p45_substrate_healthcheck_status'])}"
        ),
        f"- 10 retained sources: {_as_int(matrix['retained_source_count'])}",
        f"- 10 retained destinations: {_as_int(matrix['retained_destination_count'])}",
        (
            "- backend.uns_api.v1 excluded from destination: "
            f"{_bool_word(not _as_bool(matrix['backend_uns_api_counted_as_destination']))}"
        ),
        (
            "- source spec fields UX complete: "
            f"{_bool_word(_as_int(m4_final['source_spec_fields_empty']) == 0)}"
        ),
        (
            "- readiness fail = 0: "
            f"{_bool_word(
                _as_int(m4_final['readiness_fail_dependency']) == 0
                and _as_int(m4_final['readiness_fail_service']) == 0
                and _as_int(m4_final['readiness_missing_implementation']) == 0
            )}"
        ),
        (
            "- destination evidence pass: "
            f"{_bool_word(_as_int(m4_final['destination_evidence_failed']) == 0)}"
        ),
        (
            "- representative N×M 20/20 pass: "
            f"{_bool_word(_as_int(matrix['representative_routes_passed']) == 20)}"
        ),
        (
            "- remote-only content_evidence gap fixed: "
            f"{_bool_word(_as_int(m4_final['remote_only_content_evidence_gap']) == 0)}"
        ),
        (
            "- repair plan active blockers = 0: "
            f"{_bool_word(_as_int(m4_final['repair_plan_active_blockers']) == 0)}"
        ),
        f"- M5-A user boundary pass: {_as_str(m5a['overall'])}",
        f"- M5-B CLI/spec/env UX pass: {_as_str(m5b['overall'])}",
        "",
        "## What P5.1 can claim",
    ]
    for claim in cast(list[str], report["can_claim"]):
        lines.append(f"- {claim}")
    lines.extend(
        [
            "",
            "## What P5.1 cannot claim",
        ]
    )
    for non_claim in cast(list[str], report["cannot_claim"]):
        lines.append(f"- {non_claim}")
    lines.extend(
        [
            "",
            "## Validation mode glossary",
            (
                "- remote-only direct = product-like direct payload path that does not depend on "
                "pre-dumped artifacts"
            ),
            (
            "- artifact_prepared_direct = audit/prepared-artifact validation mode, not "
            "remote-only direct product proof"
        ),
        "- fanout = optional composite destination / audit mode, not mandatory default path",
        "- readiness pass = dependency/env/service availability, not full product direct pass",
        "- representative N×M = bounded representative pairing, not exhaustive all-pairs",
        (
            "- destination evidence = endpoint readback plus DeliveryPayloadV1/"
            "content_evidence checks"
        ),
        (
            "- repair plan active-only semantics = resolved historical blockers stay "
            "out of active groups"
        ),
            "",
            "## Artifact index",
        ]
    )
    for artifact in artifact_index:
        lines.append(
            f"- {_as_str(artifact['label'])}: {_as_str(artifact['path'])} "
            f"(exists={_bool_word(_as_bool(artifact['exists']))})"
        )
    lines.extend(["", "## Non-blocking notes"])
    for note in cast(list[str], report["non_blocking_notes"]):
        lines.append(f"- {note}")
    lines.extend(
        [
            "",
            "## Next phase recommendation",
            "- Do not reopen P5.1 connector/runtime truth unless new regression evidence appears.",
            "- Next step is P6 productization planning.",
            (
                "- P6 can focus on GUI/installer/cloud/K8s/Helm, stronger state/log UX, "
                "product shell, packaging, and deployment model."
            ),
            "- backend.uns_api.v1 can be intentionally enabled or deferred.",
        ]
    )
    return "\n".join(lines) + "\n"


def _emit_outputs(report: dict[str, object], out_root: Path) -> tuple[Path, Path]:
    out_root.mkdir(parents=True, exist_ok=True)
    json_path = out_root / "release_pack.json"
    md_path = out_root / "release_pack.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def emit_outputs(report: dict[str, object], out_root: Path) -> tuple[Path, Path]:
    return _emit_outputs(report, out_root)


def _validate_emitted_report(report: dict[str, object], out_root: Path) -> list[str]:
    issues: list[str] = []
    json_path = out_root / "release_pack.json"
    md_path = out_root / "release_pack.md"
    if not json_path.exists():
        issues.append("release_pack_json_missing")
    if not md_path.exists():
        issues.append("release_pack_md_missing")
    summary = _as_dict(report["summary"])
    boundary_scan = _as_dict(report["boundary_scan"])
    if _as_str(summary["overall"]) != "pass":
        issues.append("release_pack_not_pass")
    if _as_int(summary["active_blockers"]) != 0:
        issues.append("active_blockers_nonzero")
    if _as_int(summary["major_gaps"]) != 0:
        issues.append("major_gaps_nonzero")
    if _as_bool(boundary_scan["suspicious_overclaim_found"]):
        issues.append("boundary_overclaim_detected")
    return issues


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="p5_1_final_release_pack",
        description="Render or validate the sealed P5.1 final release pack.",
    )
    parser.add_argument("--handoff-root", type=Path, default=DEFAULT_HANDOFF_ROOT)
    parser.add_argument("--m5a-report", type=Path, default=DEFAULT_M5A_REPORT)
    parser.add_argument("--m5b-report", type=Path, default=DEFAULT_M5B_REPORT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--json", action="store_true")
    mode_group.add_argument("--markdown", action="store_true")
    mode_group.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = build_report(
        handoff_root=args.handoff_root,
        m5a_report_path=args.m5a_report,
        m5b_report_path=args.m5b_report,
        out_root=args.out_root,
    )
    _emit_outputs(report, args.out_root)
    if args.check_artifacts:
        issues = _validate_emitted_report(report, args.out_root)
        if issues:
            print("\n".join(issues))
            return 1
        print("p5_1_final_release_pack -> ok")
        return 0
    if args.markdown:
        print(render_markdown(report), end="")
        return 0
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
