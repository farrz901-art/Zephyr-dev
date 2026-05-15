from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools import p6_commercial_contamination_scan as contamination_tool
from tools import p6_m3_base_scaffold as base_tool
from tools import p6_m3_export_dry_run as export_tool
from tools import p6_m3_pro_scaffold as pro_tool


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


@pytest.mark.auth_contract
def test_base_and_pro_scaffolds_stay_dry_run_and_boundary_safe(tmp_path: Path) -> None:
    repo_root = _repo_root()
    base_report = base_tool.build_report(root=repo_root)
    pro_report = pro_tool.build_report(root=repo_root)
    base_summary = _as_dict(base_report["summary"])
    pro_summary = _as_dict(pro_report["summary"])
    assert base_summary["actual_export_performed"] is False
    assert pro_summary["actual_export_performed"] is False
    assert base_report["consumer_repo"] == "Zephyr-base"
    assert pro_report["consumer_repo"] == "Zephyr-Pro"
    assert base_report["boundary_checks"] == {
        "uses_public_core_only": True,
        "private_core_imported": False,
        "commercial_logic_written_to_zephyr_dev_runtime": False,
        "real_execution_performed": False,
    }
    pro_boundary = _as_dict(pro_report["boundary_checks"])
    assert pro_boundary["commercial_truth_owned_here"] is False
    base_tool.emit_outputs(report=base_report, out_root=tmp_path / "base", markdown=False)
    pro_tool.emit_outputs(report=pro_report, out_root=tmp_path / "pro", markdown=False)
    assert (tmp_path / "base" / "base_scaffold.json").exists()
    assert (tmp_path / "pro" / "pro_scaffold.json").exists()


@pytest.mark.auth_contract
def test_export_dry_run_builds_public_private_and_smoke_reports(tmp_path: Path) -> None:
    repo_root = _repo_root()
    public_export, private_export, smoke_report, report = export_tool.build_report(root=repo_root)
    report_summary = _as_dict(report["summary"])
    assert report_summary["actual_export_performed"] is False
    assert public_export["consumer_repo"] == "Zephyr-base"
    assert private_export["consumer_repo"] == "Zephyr-Pro"
    product_summaries = cast(list[dict[str, object]], smoke_report["product_summaries"])
    assert len(product_summaries) == 2
    assert all(summary["total_routes"] == 100 for summary in product_summaries)
    assert any(summary["placeholder_only_routes"] for summary in product_summaries)
    export_tool.emit_outputs(
        public_export=public_export,
        private_export=private_export,
        smoke_report=smoke_report,
        report=report,
        out_root=tmp_path / "export",
        markdown=False,
    )
    assert (tmp_path / "export" / "public_core_export.json").exists()
    assert (tmp_path / "export" / "private_core_export.json").exists()
    assert (tmp_path / "export" / "representative_nm_smoke.json").exists()
    assert (tmp_path / "export" / "report.json").exists()


@pytest.mark.auth_contract
def test_scaffold_outputs_do_not_trigger_contamination_blockers() -> None:
    repo_root = _repo_root()
    denylist = contamination_tool.load_denylist(
        repo_root / "docs/p6/commercial_contamination_denylist.json"
    )
    scan = contamination_tool.scan_repo(root=repo_root, denylist=denylist)
    summary = _as_dict(scan["summary"])
    assert summary["blocker_count"] == 0


@pytest.mark.auth_contract
def test_markdown_and_json_outputs_are_secret_safe(tmp_path: Path) -> None:
    repo_root = _repo_root()
    public_export, private_export, smoke_report, report = export_tool.build_report(root=repo_root)
    out_root = tmp_path / "export_md"
    export_tool.emit_outputs(
        public_export=public_export,
        private_export=private_export,
        smoke_report=smoke_report,
        report=report,
        out_root=out_root,
        markdown=True,
    )
    export_tool.emit_outputs(
        public_export=public_export,
        private_export=private_export,
        smoke_report=smoke_report,
        report=report,
        out_root=out_root,
        markdown=False,
    )
    combined = "\n".join(
        [
            (out_root / "public_core_export.md").read_text(encoding="utf-8"),
            (out_root / "private_core_export.md").read_text(encoding="utf-8"),
            json.dumps(report, ensure_ascii=False),
        ]
    ).lower()
    assert "secret_key" not in combined
    assert "access_key=" not in combined
    assert "password=" not in combined
