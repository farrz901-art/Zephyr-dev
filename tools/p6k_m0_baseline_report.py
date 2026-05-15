from __future__ import annotations

from pathlib import Path
from typing import Any

from _p6k_m0_common import (
    BASELINE_SHA,
    DOCS_DIR,
    REPOSITORY_NAME,
    TARGET_UNSTRUCTURED_VERSION,
    VALIDATION_DIR,
    benchmark_plan,
    current_branch,
    current_head_sha,
    current_unstructured_version,
    execution_plan_report,
    render_execution_plan_markdown,
    write_json,
)
from p6k_m0_base_non_regression_boundary import build_report as build_base_report
from p6k_m0_destination_package_mapping_gap import build_report as build_destination_report
from p6k_m0_it_stream_airbyte_gap import build_report as build_it_stream_report
from p6k_m0_package_manifest_gap import build_report as build_package_manifest_report
from p6k_m0_unstructured_param_audit import build_report as build_unstructured_report

BASELINE_OUTPUT = VALIDATION_DIR / "p6k_m0_baseline_audit.json"
EXECUTION_OUTPUT = VALIDATION_DIR / "p6k_m0_execution_plan.json"
MARKDOWN_OUTPUT = DOCS_DIR / "P6K_M0_BASELINE_AUDIT.md"
EXECUTION_DOC_OUTPUT = DOCS_DIR / "P6K_EXECUTION_PLAN.md"


def _write_markdown(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _status_from_sections(sections: dict[str, dict[str, Any]]) -> str:
    failing = [name for name, payload in sections.items() if payload.get("status") != "pass"]
    return "pass" if not failing else "conditional"


def build_report() -> dict[str, Any]:
    unstructured = build_unstructured_report()
    package_manifest = build_package_manifest_report()
    it_stream = build_it_stream_report()
    destination = build_destination_report()
    base_boundary = build_base_report()

    sections = {
        "S1_scope_roadmap_non_goals": {"status": "pass"},
        "S2_unstructured_gap": unstructured,
        "S3_package_manifest_gap": package_manifest,
        "S4_it_stream_airbyte_gap": it_stream,
        "S5_destination_package_mapping_gap": destination,
        "S6_base_non_regression_boundary": base_boundary,
        "S7_execution_plan": {"status": "pass"},
    }
    overall = _status_from_sections(sections)

    blockers: list[str] = []
    if unstructured["current_unstructured_version"] != TARGET_UNSTRUCTURED_VERSION:
        blockers.append(
            "Current environment is still on unstructured 0.21.5; M1 must "
            "handle 0.22.28 upgrade work."
        )
    blockers.append(
        "Enhanced Partition Profiles are not implemented yet; M0 is audit-only by design."
    )
    blockers.append(
        "PackageManifestV1 is not implemented yet; M2 is the first "
        "package-aware identity slice."
    )
    blockers.append(
        "Airbyte-like structured sync contracts are not implemented yet; "
        "M4 is the first bounded it-stream hardening slice."
    )

    report: dict[str, Any] = {
        "schema_version": 1,
        "report_id": "zephyr.dev.p6k.m0.baseline_audit.v1",
        "repository": REPOSITORY_NAME,
        "branch": current_branch(),
        "baseline_sha": BASELINE_SHA,
        "current_head_sha": current_head_sha(),
        "current_unstructured_version": current_unstructured_version(),
        "target_unstructured_version": TARGET_UNSTRUCTURED_VERSION,
        "dependency_changed": False,
        "uv_lock_changed": False,
        "runtime_behavior_changed": False,
        "base_changed": False,
        "commercial_logic_added": False,
        "sections": sections,
        "blockers": blockers,
        "next_milestone": "P6K-M1",
        "status": overall,
        "benchmark_plan": benchmark_plan(),
    }
    return report


def render_markdown(report: dict[str, Any]) -> str:
    unstructured = build_unstructured_report()
    package_manifest = build_package_manifest_report()
    it_stream = build_it_stream_report()
    destination = build_destination_report()
    lines = [
        "# P6K-M0 Baseline Audit",
        "",
        "- Repository: Zephyr-dev",
        f"- Branch: {report['branch']}",
        f"- Baseline SHA: {report['baseline_sha']}",
        f"- Current head SHA at report generation: {report['current_head_sha']}",
        f"- Current unstructured version: {report['current_unstructured_version']}",
        f"- Target unstructured version: {report['target_unstructured_version']}",
        "- dependency_changed: false",
        "- uv_lock_changed: false",
        "- runtime_behavior_changed: false",
        "- Base changed: false",
        "- commercial logic added: false",
        "",
        "## S1 Scope / Roadmap / Non-goals",
        "",
        "- P6K is a Zephyr-dev core hardening line for future Pro/Web common-kernel needs.",
        "- P6K does not modify Zephyr-base and does not add commercial logic.",
        "- P6K-M0 is audit-only; P6K-M1 is the first implementation slice.",
        "",
        "## S2 Unstructured 0.22.28 Enhanced Partition Profile gap",
        "",
        f"- Current version: {unstructured['current_unstructured_version']}",
        f"- Target version: {unstructured['target_unstructured_version']}",
        f"- Wrapper exposed params now: {', '.join(unstructured['wrapper_exposed_params'])}",
        f"- Missing wrapper params: {', '.join(unstructured['missing_wrapper_params'])}",
        f"- Missing CLI flags: {', '.join(unstructured['missing_cli_flags_for_m1'])}",
        f"- Missing metadata guards: {', '.join(unstructured['missing_metadata_guards_for_m1'])}",
        "",
        "## S3 PackageManifest gap",
        "",
        "- Missing package fields: "
        + ", ".join(package_manifest["missing_package_manifest_concepts"]),
        f"- Compatibility issue: {package_manifest['delivery_payload_compatibility_issue']}",
        "",
        "## S4 it-stream Airbyte-like gap",
        "",
        f"- Current supported concepts: {', '.join(it_stream['current_supported_concepts'])}",
        f"- Missing concepts: {', '.join(it_stream['missing_concepts'])}",
        "",
        "## S5 Destination package mapping gap",
        "",
        f"- Direct-compatible destinations: {', '.join(destination['direct_compatible'])}",
        "- Destinations needing mapping changes: "
        + ", ".join(destination["needs_mapping_changes"]),
        "",
        "## S6 Base non-regression boundary",
        "",
        "- Base remains public, local-only, and non-commercial.",
        "- P6K must not force Base to inherit heavy unstructured extras or "
        "package-aware outputs by default.",
        "",
        "## S7 Execution plan",
        "",
        "- Next milestone: P6K-M1",
        "- Execution plan frozen in docs/p6k/P6K_EXECUTION_PLAN.md and "
        "validation/p6k_m0_execution_plan.json.",
        "",
        "## Blockers",
        "",
    ]
    for blocker in report["blockers"]:
        lines.append(f"- {blocker}")
    lines.extend(
        [
            "",
            "## Judgment",
            "",
            f"- Status: {report['status']}",
            "- P6K-M0 intentionally documents the gap without implementing it.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    report = build_report()
    execution = execution_plan_report()
    write_json(BASELINE_OUTPUT, report)
    write_json(EXECUTION_OUTPUT, execution)
    _write_markdown(MARKDOWN_OUTPUT, render_markdown(report))
    _write_markdown(EXECUTION_DOC_OUTPUT, render_execution_plan_markdown())
    print(BASELINE_OUTPUT)
    print(EXECUTION_OUTPUT)


if __name__ == "__main__":
    main()
