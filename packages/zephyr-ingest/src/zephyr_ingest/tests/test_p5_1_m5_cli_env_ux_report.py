from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, cast

import pytest
from tools import p5_1_m5_cli_env_ux_report as ux_report


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _build_fixture_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    handoff_root = tmp_path / "handoff"
    handoff_root.mkdir()
    catalog = {
        "summary": {
            "source_count": 10,
            "destination_count": 10,
            "backend_count": 1,
            "backend_ids_not_counted_as_destinations": ["backend.uns_api.v1"],
        }
    }
    readiness = {
        "summary": {
            "connector_count": 21,
            "pass_count": 20,
            "fail_count": 0,
            "skip_count": 1,
            "readiness_pass_does_not_imply_product_direct_pass": True,
        }
    }
    repair_plan = {
        "non_blocking_notes": [
            {
                "id": "note-backend-uns_api-v1",
                "message": "backend.uns_api.v1 backend-only note",
            }
        ]
    }
    _write_json(handoff_root / ux_report.CATALOG_FILENAME, catalog)
    _write_json(handoff_root / ux_report.READINESS_FILENAME, readiness)
    _write_json(handoff_root / ux_report.REPAIR_PLAN_FILENAME, repair_plan)

    m5a_report_path = tmp_path / "m5a_report.json"
    _write_json(
        m5a_report_path,
        {
            "summary": {
                "overall": "pass",
                "active_blockers": 0,
                "major_ux_confusions": 0,
            }
        },
    )
    env_file = tmp_path / ".env.p45.local"
    env_file.write_text("ZEPHYR_P45_EXAMPLE=1\n", encoding="utf-8")
    return handoff_root, m5a_report_path, env_file


def _build_report(tmp_path: Path) -> dict[str, object]:
    handoff_root, m5a_report_path, env_file = _build_fixture_inputs(tmp_path)
    out_root = tmp_path / "out"
    return ux_report.build_report(
        env_file=env_file,
        handoff_root=handoff_root,
        m5a_report_path=m5a_report_path,
        out_root=out_root,
    )


@pytest.mark.auth_contract
def test_m5_b_report_overall_pass_with_fixture_inputs(tmp_path: Path) -> None:
    report = _build_report(tmp_path)
    summary = cast(dict[str, object], report["summary"])
    assert summary["overall"] == "pass"
    assert summary["active_blockers"] == 0
    assert summary["major_ux_gaps"] == 0


@pytest.mark.auth_contract
def test_m5_b_spec_show_counts_and_backend_filesystem_notes(tmp_path: Path) -> None:
    report = _build_report(tmp_path)
    spec_show = cast(dict[str, object], report["spec_show"])
    assert spec_show["source_specs_readable"] == 10
    assert spec_show["destination_specs_readable"] == 10
    assert spec_show["destination_registry_specs_readable"] == 9
    assert spec_show["source_specs_empty"] == 0
    assert spec_show["destination_specs_empty"] == 0
    assert "filesystem" in cast(str, spec_show["filesystem_note"])
    assert "backend.uns_api.v1" in cast(str, spec_show["backend_note"])


@pytest.mark.auth_contract
def test_m5_b_secret_safety_marks_expected_fields(tmp_path: Path) -> None:
    report = _build_report(tmp_path)
    secret_safety = cast(dict[str, object], report["secret_safety"])
    expectations = cast(dict[str, object], secret_safety["expectations"])
    assert secret_safety["secret_values_printed"] is False
    google_drive = cast(dict[str, object], expectations["source.uns.google_drive_document.v1"])
    confluence = cast(dict[str, object], expectations["source.uns.confluence_document.v1"])
    s3_source = cast(dict[str, object], expectations["source.uns.s3_document.v1"])
    postgres = cast(dict[str, object], expectations["source.it.postgresql_incremental.v1"])
    mongodb = cast(dict[str, object], expectations["source.it.mongodb_incremental.v1"])
    backend = cast(dict[str, object], expectations["backend.uns_api.v1"])
    assert google_drive["status"] == "pass"
    assert confluence["status"] == "pass"
    assert s3_source["status"] == "pass"
    assert postgres["status"] == "pass"
    assert mongodb["status"] == "pass"
    assert backend["status"] == "pass"


@pytest.mark.auth_contract
def test_m5_b_error_taxonomy_and_backend_skip_env_non_blocking(tmp_path: Path) -> None:
    report = _build_report(tmp_path)
    env_readiness_ux = cast(dict[str, object], report["env_readiness_ux"])
    error_taxonomy = cast(dict[str, object], report["error_taxonomy"])
    assert env_readiness_ux["readiness_pass_not_product_direct_pass"] is True
    assert env_readiness_ux["backend_uns_api_skip_env_non_blocking"] is True
    assert error_taxonomy["missing_env_classifiable"] is True
    assert error_taxonomy["dependency_missing_classifiable"] is True
    assert error_taxonomy["service_unreachable_classifiable"] is True
    assert error_taxonomy["bucket_missing_classifiable"] is True
    assert error_taxonomy["auth_permission_failure_classifiable"] is True
    assert error_taxonomy["token_expired_or_401_403_classifiable"] is True
    assert error_taxonomy["windows_port_exclusion_note_present"] is True
    assert error_taxonomy["powershell_encoding_note_present"] is True


@pytest.mark.auth_contract
def test_m5_b_markdown_includes_required_operational_notes(tmp_path: Path) -> None:
    report = _build_report(tmp_path)
    markdown = ux_report.render_markdown(report)
    assert "PowerShell `>` may write UTF-16" in markdown
    assert "Windows excluded port ranges can block Docker binds" in markdown
    assert "S3 bucket missing is a runtime substrate initialization problem" in markdown
    assert "backend.uns_api.v1 skip_env is backend-only" in markdown


@pytest.mark.auth_contract
def test_m5_b_cli_emits_outputs_and_check_artifacts(tmp_path: Path) -> None:
    handoff_root, m5a_report_path, env_file = _build_fixture_inputs(tmp_path)
    out_root = tmp_path / "out"
    assert (
        ux_report.main(
            [
                "--env-file",
                str(env_file),
                "--handoff-root",
                str(handoff_root),
                "--m5a-report",
                str(m5a_report_path),
                "--out-root",
                str(out_root),
                "--check-artifacts",
            ]
        )
        == 0
    )
    report_json = json.loads((out_root / "report.json").read_text(encoding="utf-8"))
    assert report_json["summary"]["overall"] == "pass"
