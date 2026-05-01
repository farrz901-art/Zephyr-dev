from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, cast

import pytest
from tools import p5_1_final_release_pack as release_pack


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _build_fixture_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    handoff_root = tmp_path / "handoff"
    handoff_root.mkdir()
    _write_json(
        handoff_root / release_pack.CATALOG_FILENAME,
        {
            "summary": {
                "source_count": 10,
                "destination_count": 10,
                "backend_count": 1,
                "backend_ids_not_counted_as_destinations": ["backend.uns_api.v1"],
            }
        },
    )
    _write_json(
        handoff_root / release_pack.READINESS_FILENAME,
        {
            "summary": {
                "connector_count": 21,
                "pass_count": 20,
                "fail_count": 0,
                "skip_count": 1,
                "readiness_pass_does_not_imply_product_direct_pass": True,
            }
        },
    )
    _write_json(
        handoff_root / release_pack.DESTINATION_EVIDENCE_FILENAME,
        {
            "summary": {
                "result_count": 40,
                "executed_count": 40,
                "delivery_ok_count": 40,
                "token_pass_count": 40,
                "remote_only_direct_count": 0,
                "remote_only_content_evidence_gap_count": 0,
            }
        },
    )
    _write_json(
        handoff_root / release_pack.REPRESENTATIVE_FILENAME,
        {
            "summary": {
                "route_count": 20,
                "executed_count": 20,
                "blocked_count": 0,
                "overall_pass_count": 20,
                "overall_fail_count": 0,
                "remote_only_direct_count": 0,
            }
        },
    )
    _write_json(
        handoff_root / release_pack.GAP_SUMMARY_FILENAME,
        {
            "readiness_fail_dependency": 0,
            "readiness_fail_service": 0,
            "readiness_missing_implementation": 0,
            "readiness_skip_env": 1,
            "remote_only_content_evidence_gap": 0,
            "artifact_reference_only_v1": 0,
            "destination_evidence_failed": 0,
            "nm_blocked": 0,
            "nm_failed_executed": 0,
            "nm_passed": 20,
            "source_spec_fields_empty": 0,
            "backend_counted_as_destination": 0,
        },
    )
    _write_json(
        handoff_root / release_pack.REPAIR_PLAN_FILENAME,
        {
            "active_blocker_count": 0,
            "repair_groups": {
                "M4_A_dependency_and_readiness": [],
                "M4_B_missing_implementation": [],
                "M4_C_source_spec_fields": [],
                "M4_D_delivery_payload_visibility": [],
                "M4_E_representative_nm_unblock": [],
            },
            "prioritized_actions": [],
            "non_blocking_notes": [
                {
                    "id": "note-backend-uns_api-v1",
                    "message": "backend.uns_api.v1 remains skip_env and non-matrix",
                },
                {
                    "id": "note-clickhouse-dependency-surface-history",
                    "message": "ClickHouse history note",
                },
                {
                    "id": "note-it-structured-state-log-future-hardening",
                    "message": "artifact_paths_only future hardening",
                },
            ],
        },
    )
    _write_json(
        handoff_root / release_pack.HANDOFF_SUMMARY_FILENAME,
        {
            "phase": "P5.1-M4 final handoff",
            "judgment": "PASS",
        },
    )
    (handoff_root / release_pack.HANDOFF_MD_FILENAME).write_text(
        "# P5.1-M4 final handoff\n\n- not cloud\n",
        encoding="utf-8",
    )
    (handoff_root / release_pack.REPAIR_PLAN_MD_FILENAME).write_text(
        "# Repair plan\n\n- active blockers: 0\n",
        encoding="utf-8",
    )

    m5a_report = tmp_path / "m5a_report.json"
    _write_json(
        m5a_report,
        {
            "summary": {
                "overall": "pass",
                "active_blockers": 0,
                "major_ux_confusions": 0,
            },
            "matrix_claims": {
                "source_count": 10,
                "destination_count": 10,
                "backend_count": 1,
                "backend_uns_api_counted_as_destination": False,
                "representative_routes_passed": 20,
                "representative_is_full_all_pairs": False,
                "all_pairs_100_claimed": False,
            },
            "business_boundary": {
                "usage_not_billing": True,
                "capability_domains_not_license_entitlement": True,
                "governance_not_rbac": True,
            },
            "keyword_scan": {"issue_count": 0},
        },
    )
    m5a_report.with_suffix(".md").write_text(
        "# M5-A\n\n- not billing\n- not RBAC\n- not cloud\n",
        encoding="utf-8",
    )

    m5b_report = tmp_path / "m5b_report.json"
    _write_json(
        m5b_report,
        {
            "summary": {
                "overall": "pass",
                "active_blockers": 0,
                "major_ux_gaps": 0,
            },
            "spec_show": {
                "source_specs_readable": 10,
                "destination_specs_readable": 10,
                "backend_note": (
                    "backend.uns_api.v1 remains backend-only and is excluded from "
                    "destination_count."
                ),
            },
            "secret_safety": {
                "secret_values_printed": False,
            },
            "error_taxonomy": {
                "missing_env_classifiable": True,
                "dependency_missing_classifiable": True,
                "service_unreachable_classifiable": True,
                "bucket_missing_classifiable": True,
                "auth_permission_failure_classifiable": True,
                "token_expired_or_401_403_classifiable": True,
                "windows_port_exclusion_note_present": True,
                "powershell_encoding_note_present": True,
            },
            "non_blocking_notes": [
                (
                    "Windows excluded port ranges can block Docker binds; use a different "
                    "port or adjust the runtime substrate rather than blaming connector code."
                ),
                (
                    "PowerShell `>` may write UTF-16; prefer tool-managed report.json writes "
                    "or `Set-Content -Encoding utf8`."
                ),
            ],
        },
    )
    m5b_report.with_suffix(".md").write_text(
        "# M5-B\n\n- Windows excluded port ranges note\n- PowerShell UTF-16 note\n",
        encoding="utf-8",
    )
    return handoff_root, m5a_report, m5b_report


def _build_report(tmp_path: Path) -> dict[str, object]:
    handoff_root, m5a_report, m5b_report = _build_fixture_inputs(tmp_path)
    out_root = tmp_path / "out"
    return release_pack.build_report(
        handoff_root=handoff_root,
        m5a_report_path=m5a_report,
        m5b_report_path=m5b_report,
        out_root=out_root,
    )


@pytest.mark.auth_contract
def test_final_release_pack_overall_pass_and_zero_blockers(tmp_path: Path) -> None:
    report = _build_report(tmp_path)
    summary = cast(dict[str, object], report["summary"])
    assert summary["overall"] == "pass"
    assert summary["active_blockers"] == 0
    assert summary["major_gaps"] == 0
    assert summary["release_pack_complete"] is True


@pytest.mark.auth_contract
def test_final_release_pack_matrix_counts_and_boundary_flags(tmp_path: Path) -> None:
    report = _build_report(tmp_path)
    matrix = cast(dict[str, object], report["matrix"])
    assert matrix["retained_source_count"] == 10
    assert matrix["retained_destination_count"] == 10
    assert matrix["backend_count"] == 1
    assert matrix["backend_uns_api_counted_as_destination"] is False
    assert matrix["representative_is_full_all_pairs"] is False
    assert matrix["all_pairs_100_claimed"] is False


@pytest.mark.auth_contract
def test_final_release_pack_requires_m5a_and_m5b_green(tmp_path: Path) -> None:
    report = _build_report(tmp_path)
    m5a = cast(dict[str, object], report["m5a_user_boundary"])
    m5b = cast(dict[str, object], report["m5b_cli_env_ux"])
    assert m5a["overall"] == "pass"
    assert m5b["overall"] == "pass"
    assert m5b["secret_values_printed"] is False


@pytest.mark.auth_contract
def test_final_release_pack_overclaim_scan_catches_positive_claims() -> None:
    issues = release_pack.scan_positive_overclaims(
        {"bad": "P5.1 is cloud deployment ready and RBAC complete.\n"}
    )
    assert any(issue["phrase"] == "cloud deployment ready" for issue in issues)
    assert any(issue["phrase"] == "rbac complete" for issue in issues)


@pytest.mark.auth_contract
def test_final_release_pack_markdown_contains_claim_sections_and_next_phase(
    tmp_path: Path,
) -> None:
    report = _build_report(tmp_path)
    markdown = release_pack.render_markdown(report)
    assert "## What P5.1 can claim" in markdown
    assert "## What P5.1 cannot claim" in markdown
    assert "## Next phase recommendation" in markdown
    assert "Next step is P6 productization planning." in markdown


@pytest.mark.auth_contract
def test_final_release_pack_artifact_index_includes_m4_m5_and_final_outputs(
    tmp_path: Path,
) -> None:
    handoff_root, m5a_report, m5b_report = _build_fixture_inputs(tmp_path)
    out_root = tmp_path / "out"
    report = release_pack.build_report(
        handoff_root=handoff_root,
        m5a_report_path=m5a_report,
        m5b_report_path=m5b_report,
        out_root=out_root,
    )
    release_pack.emit_outputs(report, out_root)
    artifact_index = cast(list[dict[str, object]], report["artifact_index"])
    labels = {cast(str, item["label"]) for item in artifact_index}
    assert "M4 final handoff summary JSON" in labels
    assert "M5-A report JSON" in labels
    assert "M5-B report JSON" in labels
    assert "final release pack JSON" in labels
    assert "final release pack markdown" in labels


@pytest.mark.auth_contract
def test_final_release_pack_cli_emits_outputs_and_check_artifacts(tmp_path: Path) -> None:
    handoff_root, m5a_report, m5b_report = _build_fixture_inputs(tmp_path)
    out_root = tmp_path / "out"
    assert (
        release_pack.main(
            [
                "--handoff-root",
                str(handoff_root),
                "--m5a-report",
                str(m5a_report),
                "--m5b-report",
                str(m5b_report),
                "--out-root",
                str(out_root),
                "--check-artifacts",
            ]
        )
        == 0
    )
    report_json = json.loads((out_root / "release_pack.json").read_text(encoding="utf-8"))
    assert report_json["summary"]["overall"] == "pass"
