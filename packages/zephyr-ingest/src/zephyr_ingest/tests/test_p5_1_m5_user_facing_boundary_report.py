from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools import p5_1_m5_user_facing_boundary_report as boundary_report


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _build_fixture_handoff_root(tmp_path: Path) -> Path:
    handoff_root = tmp_path / "handoff"
    handoff_root.mkdir()
    connectors: list[dict[str, object]] = []
    for connector_id in boundary_report.SOURCE_IDS:
        connectors.append(
            {
                "id": connector_id,
                "family": "source",
                "flow_kind": "uns" if ".uns." in connector_id else "it",
                "spec_fields_status": "present",
                "field_count": 1,
                "severity": "none",
            }
        )
    for connector_id in boundary_report.DESTINATION_IDS:
        connectors.append(
            {
                "id": connector_id,
                "family": "destination",
                "flow_kind": "delivery",
                "spec_fields_status": "present",
                "field_count": 1,
                "severity": "none",
            }
        )
    connectors.append(
        {
            "id": "backend.uns_api.v1",
            "family": "backend",
            "flow_kind": "backend",
            "spec_fields_status": "present",
            "field_count": 1,
            "severity": "none",
        }
    )
    _write_json(
        handoff_root / boundary_report.CATALOG_FILENAME,
        {
            "summary": {
                "source_count": 10,
                "destination_count": 10,
                "backend_count": 1,
                "backend_ids_not_counted_as_destinations": ["backend.uns_api.v1"],
            },
            "connectors": connectors,
        },
    )
    _write_json(
        handoff_root / boundary_report.READINESS_FILENAME,
        {
            "summary": {
                "connector_count": 21,
                "pass_count": 20,
                "fail_count": 0,
                "skip_count": 1,
                "readiness_pass_does_not_imply_product_direct_pass": True,
            },
            "connectors": [
                {
                    "id": "backend.uns_api.v1",
                    "status": "skip",
                    "reason": "skip_env",
                    "required_env_missing": ["ZEPHYR_UNS_API_KEY"],
                }
            ],
        },
    )
    results = [
        {
            "destination": "destination.kafka.v1",
            "flow": "uns",
            "mode": "direct",
            "validation_mode": "artifact_prepared_direct",
            "artifacts_pre_dumped": True,
            "product_direct_claim_safe": False,
            "user_seeded_marker_proof": True,
            "token_strategy": "seeded_source_marker",
            "endpoint_readback_ok": True,
            "delivery_payload_visible": True,
            "delivery_payload_core_metadata_check": True,
            "delivery_locator_found": True,
            "content_evidence_found": True,
            "normalized_text_preview_found": True,
            "records_preview_found": False,
            "structured_state_log_status": "not_applicable",
            "token_pass": True,
            "issue": "",
            "severity": "none",
        },
        {
            "destination": "destination.kafka.v1",
            "flow": "uns",
            "mode": "fanout",
            "validation_mode": "fanout_audit",
            "artifacts_pre_dumped": True,
            "product_direct_claim_safe": False,
            "user_seeded_marker_proof": True,
            "token_strategy": "seeded_source_marker",
            "endpoint_readback_ok": True,
            "delivery_payload_visible": True,
            "delivery_payload_core_metadata_check": True,
            "delivery_locator_found": True,
            "content_evidence_found": True,
            "normalized_text_preview_found": True,
            "records_preview_found": False,
            "structured_state_log_status": "not_applicable",
            "token_pass": True,
            "issue": "",
            "severity": "none",
        },
        {
            "destination": "destination.opensearch.v1",
            "flow": "it",
            "mode": "direct",
            "validation_mode": "artifact_prepared_direct",
            "artifacts_pre_dumped": True,
            "product_direct_claim_safe": False,
            "user_seeded_marker_proof": True,
            "token_strategy": "seeded_source_marker",
            "endpoint_readback_ok": True,
            "delivery_payload_visible": True,
            "delivery_payload_core_metadata_check": True,
            "delivery_locator_found": True,
            "content_evidence_found": True,
            "normalized_text_preview_found": True,
            "records_preview_found": True,
            "structured_state_log_status": "artifact_paths_only",
            "token_pass": True,
            "issue": "",
            "severity": "none",
        },
        {
            "destination": "destination.opensearch.v1",
            "flow": "it",
            "mode": "fanout",
            "validation_mode": "fanout_audit",
            "artifacts_pre_dumped": True,
            "product_direct_claim_safe": False,
            "user_seeded_marker_proof": True,
            "token_strategy": "seeded_source_marker",
            "endpoint_readback_ok": True,
            "delivery_payload_visible": True,
            "delivery_payload_core_metadata_check": True,
            "delivery_locator_found": True,
            "content_evidence_found": True,
            "normalized_text_preview_found": True,
            "records_preview_found": True,
            "structured_state_log_status": "artifact_paths_only",
            "token_pass": True,
            "issue": "",
            "severity": "none",
        },
    ]
    _write_json(
        handoff_root / boundary_report.DESTINATION_EVIDENCE_FILENAME,
        {
            "product_direct_note": (
                "artifact_prepared_direct uses pre-dumped artifacts and does not prove "
                "remote-only direct product pass"
            ),
            "summary": {
                "result_count": 4,
                "executed_count": 4,
                "delivery_ok_count": 4,
                "token_pass_count": 4,
                "remote_only_direct_count": 0,
                "remote_only_content_evidence_gap_count": 0,
            },
            "results": results,
        },
    )
    routes = [
        {
            "source_id": "source.uns.http_document.v1",
            "destination_id": "destination.kafka.v1",
            "mode": "direct",
            "flow": "uns",
            "validation_mode": "artifact_prepared_direct",
            "artifacts_pre_dumped": True,
            "product_direct_claim_safe": False,
            "user_seeded_marker_proof": True,
            "token_strategy": "seeded_source_marker",
            "readiness_status": "pass",
            "delivery_ok": True,
            "endpoint_readback_ok": True,
            "content_evidence_found": True,
            "delivery_payload_visible": True,
            "delivery_payload_core_metadata_check": True,
            "delivery_locator_found": True,
            "normalized_text_preview_found": True,
            "records_preview_found": False,
            "structured_state_log_status": "not_applicable",
            "token_pass": True,
            "overall_pass": True,
            "issue": "",
            "severity": "none",
            "remote_only_content_evidence_gap": False,
        },
        {
            "source_id": "source.it.http_json_cursor.v1",
            "destination_id": "destination.opensearch.v1",
            "mode": "fanout",
            "flow": "it",
            "validation_mode": "fanout_audit",
            "artifacts_pre_dumped": True,
            "product_direct_claim_safe": False,
            "user_seeded_marker_proof": True,
            "token_strategy": "seeded_source_marker",
            "readiness_status": "pass",
            "delivery_ok": True,
            "endpoint_readback_ok": True,
            "content_evidence_found": True,
            "delivery_payload_visible": True,
            "delivery_payload_core_metadata_check": True,
            "delivery_locator_found": True,
            "normalized_text_preview_found": True,
            "records_preview_found": True,
            "structured_state_log_status": "artifact_paths_only",
            "token_pass": True,
            "overall_pass": True,
            "issue": "",
            "severity": "none",
            "remote_only_content_evidence_gap": False,
        },
    ]
    _write_json(
        handoff_root / boundary_report.REPRESENTATIVE_FILENAME,
        {
            "fanout_mode_note": (
                "fanout mode is audit mode and does not replace direct-mode product pass"
            ),
            "artifact_prepared_direct_note": (
                "artifact_prepared_direct uses pre-dumped artifacts and is not equivalent "
                "to remote-only direct product proof"
            ),
            "summary": {
                "route_count": 2,
                "executed_count": 2,
                "blocked_count": 0,
                "overall_pass_count": 2,
                "overall_fail_count": 0,
                "remote_only_direct_count": 0,
            },
            "routes": routes,
        },
    )
    _write_json(
        handoff_root / boundary_report.GAP_SUMMARY_FILENAME,
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
            "nm_passed": 2,
            "source_spec_fields_empty": 0,
            "backend_counted_as_destination": 0,
        },
    )
    _write_json(
        handoff_root / boundary_report.REPAIR_PLAN_FILENAME,
        {
            "active_blocker_count": 0,
            "non_blocking_notes": [
                {
                    "id": "note-backend-uns_api-v1",
                    "message": "backend.uns_api.v1 remains skip_env and is non-matrix",
                },
                {
                    "id": "note-clickhouse-dependency-surface-history",
                    "message": "ClickHouse dependency lock note",
                },
                {
                    "id": "note-it-structured-state-log-future-hardening",
                    "message": "structured_state_log_status note",
                },
            ],
        },
    )
    (handoff_root / boundary_report.HANDOFF_MD_FILENAME).write_text(
        "# P5.1-M4 final handoff\n\n- not billing\n- not RBAC\n- not exhaustive all-pairs\n",
        encoding="utf-8",
    )
    (handoff_root / boundary_report.REPAIR_PLAN_MD_FILENAME).write_text(
        "# Repair plan\n\n- active blockers: 0\n",
        encoding="utf-8",
    )
    return handoff_root


@pytest.fixture
def related_tool_checks() -> dict[str, boundary_report.RelatedToolStatus]:
    return {
        "usage_report": {"status": "pass", "path": "tools/p5_usage_report.py", "detail": "ok"},
        "capability_domain_report": {
            "status": "pass",
            "path": "tools/p5_capability_domain_report.py",
            "detail": "ok",
        },
        "product_cut_report": {
            "status": "pass",
            "path": "tools/p5_product_cut_report.py",
            "detail": "ok",
        },
        "governance_audit_report": {
            "status": "pass",
            "path": "tools/p5_governance_audit_report.py",
            "detail": "ok",
        },
    }


def _build_report(
    tmp_path: Path,
    related_tool_checks: dict[str, boundary_report.RelatedToolStatus],
) -> dict[str, object]:
    handoff_root = _build_fixture_handoff_root(tmp_path)
    out_root = tmp_path / "out"
    return boundary_report.build_report(
        handoff_root=handoff_root,
        out_root=out_root,
        related_tool_checks=related_tool_checks,
    )


@pytest.mark.auth_contract
def test_m5_boundary_report_summary_is_pass(
    tmp_path: Path,
    related_tool_checks: dict[str, boundary_report.RelatedToolStatus],
) -> None:
    report = _build_report(tmp_path, related_tool_checks)
    summary = cast(dict[str, object], report["summary"])
    assert summary["overall"] == "pass"
    assert summary["active_blockers"] == 0
    assert summary["major_ux_confusions"] == 0


@pytest.mark.auth_contract
def test_m5_boundary_report_keeps_backend_out_of_destination_and_not_all_pairs(
    tmp_path: Path,
    related_tool_checks: dict[str, boundary_report.RelatedToolStatus],
) -> None:
    report = _build_report(tmp_path, related_tool_checks)
    matrix_claims = cast(dict[str, object], report["matrix_claims"])
    assert matrix_claims["backend_uns_api_counted_as_destination"] is False
    assert matrix_claims["representative_is_full_all_pairs"] is False
    assert matrix_claims["all_pairs_100_claimed"] is False


@pytest.mark.auth_contract
def test_m5_boundary_report_business_and_mode_boundaries_are_true(
    tmp_path: Path,
    related_tool_checks: dict[str, boundary_report.RelatedToolStatus],
) -> None:
    report = _build_report(tmp_path, related_tool_checks)
    business_boundary = cast(dict[str, object], report["business_boundary"])
    mode_explanation = cast(dict[str, object], report["mode_explanation"])
    p6_boundary = cast(dict[str, object], report["p6_boundary"])
    assert business_boundary["usage_not_billing"] is True
    assert business_boundary["capability_domains_not_license_entitlement"] is True
    assert business_boundary["governance_not_rbac"] is True
    assert mode_explanation["artifact_prepared_direct_explained_as_audit_only"] is True
    assert mode_explanation["fanout_explained_as_optional_composite_destination"] is True
    assert mode_explanation["readiness_pass_not_equal_product_direct_pass"] is True
    assert p6_boundary["no_gui_claim"] is True
    assert p6_boundary["no_installer_claim"] is True
    assert p6_boundary["no_cloud_claim"] is True
    assert p6_boundary["no_k8s_helm_claim"] is True
    assert p6_boundary["no_saas_multitenant_claim"] is True


@pytest.mark.auth_contract
def test_m5_boundary_markdown_contains_required_sections_and_notes(
    tmp_path: Path,
    related_tool_checks: dict[str, boundary_report.RelatedToolStatus],
) -> None:
    report = _build_report(tmp_path, related_tool_checks)
    markdown = boundary_report.render_markdown(report)
    assert "# Zephyr P5.1 user-facing boundary report" in markdown
    assert "artifact_prepared_direct = audit/prepared-artifact validation mode" in markdown
    assert "fanout = optional composite destination / audit mode" in markdown
    assert "backend.uns_api.v1 skip_env is backend-only and non-matrix" in markdown


@pytest.mark.auth_contract
def test_m5_boundary_cli_emits_outputs_and_check_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    related_tool_checks: dict[str, boundary_report.RelatedToolStatus],
) -> None:
    def _fake_related_tool_checks(
        _repo_root: Path,
    ) -> dict[str, boundary_report.RelatedToolStatus]:
        return related_tool_checks

    handoff_root = _build_fixture_handoff_root(tmp_path)
    out_root = tmp_path / "out"
    monkeypatch.setattr(
        boundary_report,
        "_collect_related_tool_checks",
        _fake_related_tool_checks,
    )
    assert (
        boundary_report.main(
            [
                "--handoff-root",
                str(handoff_root),
                "--out-root",
                str(out_root),
                "--check-artifacts",
            ]
        )
        == 0
    )
    report_json = json.loads((out_root / "report.json").read_text(encoding="utf-8"))
    assert report_json["summary"]["overall"] == "pass"
