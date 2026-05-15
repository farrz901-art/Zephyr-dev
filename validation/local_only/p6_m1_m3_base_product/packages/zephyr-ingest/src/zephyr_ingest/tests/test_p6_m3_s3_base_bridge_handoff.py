from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s3_base_bridge_handoff as handoff_tool


def _repo_root() -> Path:
    current = Path(__file__).resolve()
    for candidate in current.parents:
        if (
            ((candidate / "pyproject.toml").exists() or (candidate / ".git").exists())
            and (candidate / "docs/p6").exists()
            and (candidate / "packages/zephyr-ingest").exists()
        ):
            return candidate
    raise RuntimeError("Could not locate repository root from test file path")


def _as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _workspace_case_dir(case_name: str) -> Path:
    root = _repo_root() / ".tmp" / "p6_m3_s3_test" / case_name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _emit_base_fixture(base_root: Path) -> None:
    (base_root / ".tmp").mkdir(parents=True, exist_ok=True)
    (base_root / ".github/workflows").mkdir(parents=True, exist_ok=True)
    (base_root / "docs").mkdir(parents=True, exist_ok=True)
    (base_root / "manifests").mkdir(parents=True, exist_ok=True)
    (base_root / "public-core-bridge").mkdir(parents=True, exist_ok=True)
    (base_root / "scripts").mkdir(parents=True, exist_ok=True)
    (base_root / "tests/fixtures").mkdir(parents=True, exist_ok=True)
    (base_root / "README.md").write_text(
        "Source lineage points to Zephyr-dev SHA `780690a21ddc60bb5a01529d3d35358f08903685`.\n",
        encoding="utf-8",
    )
    (base_root / "docs/SOURCE_LINEAGE.md").write_text("source lineage doc\n", encoding="utf-8")
    _write_json(
        base_root / "manifests/public_export_lineage.json",
        {
            "actual_runtime_source_copied": False,
            "actual_product_runtime_implemented": False,
        },
    )
    _write_json(
        base_root / "public-core-bridge/bridge_contract.json",
        {
            "contracts": {
                "base_run_request_v1": {"input_kind": ["local_file", "local_text"]},
                "base_run_result_v1": {"usage_fact": {"billing_semantics": False}},
                "base_error_v1": {"secret_safe_required": True},
            }
        },
    )
    (base_root / "public-core-bridge/run_public_core_fixture.py").write_text(
        "# fixture runner\n", encoding="utf-8"
    )
    (base_root / "tests/fixtures/sample_request_file.json").write_text("{}", encoding="utf-8")
    (base_root / "tests/fixtures/sample_request_text.json").write_text("{}", encoding="utf-8")
    (base_root / "scripts/check_local_fixture_flow.py").write_text("# smoke\n", encoding="utf-8")
    (base_root / "scripts/check_boundary.py").write_text("# boundary\n", encoding="utf-8")
    (base_root / ".github/workflows/boundary.yml").write_text(
        (
            "python scripts/check_boundary.py --json\n"
            "python scripts/check_local_fixture_flow.py --json\n"
        ),
        encoding="utf-8",
    )
    _write_json(
        base_root / ".tmp/base_boundary_check.json",
        {"summary": {"pass": True, "blocked_count": 0, "review_required_count": 2}},
    )
    _write_json(
        base_root / ".tmp/local_fixture_flow_check.json",
        {
            "summary": {
                "pass": True,
                "file_fixture_pass": True,
                "text_fixture_pass": True,
                "marker_found": True,
                "production_runtime": False,
            }
        },
    )
    _write_json(
        base_root / ".tmp/s3_repo_status.json",
        {
            "repo_url": "https://github.com/farrz901-art/Zephyr-base.git",
            "branch": "main",
            "previous_head_sha": "prevsha",
            "current_head_sha": "currsha",
            "pushed": True,
            "source_lineage_readme_fixed": True,
            "source_lineage_docs_present": True,
            "boundary_check_pass": True,
            "local_fixture_flow_pass": True,
            "file_fixture_pass": True,
            "text_fixture_pass": True,
            "marker_found": True,
        },
    )


@pytest.mark.auth_contract
def test_s3_handoff_reads_bridge_fixture_outputs(tmp_path: Path) -> None:
    repo_root = _repo_root()
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    _emit_base_fixture(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    bridge = _as_dict(report["bridge"])
    fixture = _as_dict(report["fixture_flow"])
    boundary = _as_dict(report["boundary"])
    assert summary["overall"] == "pass"
    assert bridge["supports_local_file"] is True
    assert bridge["supports_local_text"] is True
    assert bridge["usage_billing_semantics"] is False
    assert bridge["error_contract_secret_safe"] is True
    assert fixture["file_fixture_pass"] is True
    assert fixture["text_fixture_pass"] is True
    assert boundary["boundary_check_pass"] is True


@pytest.mark.auth_contract
def test_s3_handoff_outputs_are_secret_safe(tmp_path: Path) -> None:
    repo_root = _repo_root()
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    out_root = _workspace_case_dir(f"{tmp_path.name}_out")
    _emit_base_fixture(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=False)
    handoff_tool.emit_outputs(report=report, out_root=out_root, markdown=True)
    json_text = (out_root / "report.json").read_text(encoding="utf-8")
    markdown_text = (out_root / "report.md").read_text(encoding="utf-8")
    assert "secret_value" not in json_text
    assert "secret_value" not in markdown_text
