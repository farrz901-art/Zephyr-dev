from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, cast

import pytest
from tools import p6_notice_sbom_preflight as notice_tool
from tools import p6_private_core_export_dry_run as private_tool
from tools import p6_public_core_export_dry_run as public_tool
from tools import p6_public_private_leakage_preflight as leakage_tool
from tools import p6_release_source_manifest as source_tool


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


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


@pytest.mark.auth_contract
def test_public_and_private_export_dry_run_are_not_actual_exports(tmp_path: Path) -> None:
    repo_root = _repo_root()
    public_report = public_tool.build_report(
        root=repo_root,
        manifest_path=repo_root / "docs/p6/public_core_export_manifest.json",
        six_repo_manifest_path=repo_root / "docs/p6/six_repo_manifest.json",
    )
    private_report = private_tool.build_report(
        root=repo_root,
        manifest_path=repo_root / "docs/p6/private_core_export_manifest.json",
        six_repo_manifest_path=repo_root / "docs/p6/six_repo_manifest.json",
    )
    public_summary = _as_dict(public_report["summary"])
    private_summary = _as_dict(private_report["summary"])
    assert public_summary["actual_export_performed"] is False
    assert private_summary["actual_export_performed"] is False
    assert public_report["target_consumers"] == ["Zephyr-base", "Zephyr-Web"]
    assert private_report["target_consumers"] == ["Zephyr-Pro", "Zephyr-Web-core"]
    public_items = cast(list[dict[str, object]], public_report["export_items"])
    private_items = cast(list[dict[str, object]], private_report["export_items"])
    assert any(item["path_status"] == "draft_only" for item in public_items)
    assert any(item["path_status"] == "draft_only" for item in private_items)
    public_out = tmp_path / "public"
    private_out = tmp_path / "private"
    public_tool.emit_outputs(report=public_report, out_root=public_out, markdown=False)
    private_tool.emit_outputs(report=private_report, out_root=private_out, markdown=False)
    assert (public_out / "public_export_plan.json").exists()
    assert (private_out / "private_export_plan.json").exists()


@pytest.mark.auth_contract
def test_release_source_manifest_and_notice_preflight(tmp_path: Path) -> None:
    repo_root = _repo_root()
    report = source_tool.build_report(
        root=repo_root,
        public_manifest_path=repo_root / "docs/p6/public_core_export_manifest.json",
        private_manifest_path=repo_root / "docs/p6/private_core_export_manifest.json",
        six_repo_manifest_path=repo_root / "docs/p6/six_repo_manifest.json",
    )
    lineage = _as_dict(report["lineage"])
    assert lineage["source_sha_required"] is True
    assert lineage["manifest_hash_required"] is True
    assert lineage["combined_derivation_manifest_sha256"]
    notice = notice_tool.build_report(
        root=repo_root,
        public_plan_path=tmp_path / "missing_public.json",
        private_plan_path=tmp_path / "missing_private.json",
    )
    assert notice["license_file_found"] is True
    assert notice["dependency_manifest_found"] is True
    assert notice["public_release_legal_ready"] is False
    assert notice["private_release_legal_ready"] is False
    assert notice["preflight_only"] is True


@pytest.mark.auth_contract
def test_leakage_preflight_passes_and_blockers_propagate(tmp_path: Path) -> None:
    repo_root = _repo_root()
    public_plan = {
        "summary": {
            "actual_export_performed": False,
            "code_migration_performed": False,
            "target_repos_created": False,
        },
        "forbidden_export_check": {
            "commercial_logic_in_public_export": False,
            "private_core_in_public_export": False,
            "secret_risk_in_public_export": False,
        },
    }
    private_plan = {
        "summary": {
            "actual_export_performed": False,
            "code_migration_performed": False,
            "target_repos_created": False,
        },
        "forbidden_export_check": {
            "commercial_secrets_in_private_export_plan": False,
            "commercial_decision_code_back_to_zephyr_dev": False,
        },
    }
    commercial_scan = {"summary": {"blocker_count": 0}}
    forbidden_scan = {"summary": {"blocker_count": 0}}
    security_scan = {"summary": {"blocked_runtime_paths": 0}}
    public_path = tmp_path / "public_export_plan.json"
    private_path = tmp_path / "private_export_plan.json"
    commercial_path = tmp_path / "commercial.json"
    forbidden_path = tmp_path / "forbidden.json"
    security_path = tmp_path / "security.json"
    _write_json(public_path, public_plan)
    _write_json(private_path, private_plan)
    _write_json(commercial_path, commercial_scan)
    _write_json(forbidden_path, forbidden_scan)
    _write_json(security_path, security_scan)

    report = leakage_tool.build_report(
        root=repo_root,
        public_plan_path=public_path,
        private_plan_path=private_path,
        commercial_scan_path=commercial_path,
        forbidden_import_scan_path=forbidden_path,
        security_scan_path=security_path,
    )
    summary = _as_dict(report["summary"])
    assert summary["overall"] == "pass"
    assert summary["active_blockers"] == 0

    _write_json(commercial_path, {"summary": {"blocker_count": 1}})
    blocked_report = leakage_tool.build_report(
        root=repo_root,
        public_plan_path=public_path,
        private_plan_path=private_path,
        commercial_scan_path=commercial_path,
        forbidden_import_scan_path=forbidden_path,
        security_scan_path=security_path,
    )
    blocked_summary = _as_dict(blocked_report["summary"])
    assert blocked_summary["overall"] == "fail"
    assert blocked_summary["zephyr_dev_contamination_blockers"] == 1

    _write_json(commercial_path, {"summary": {"blocker_count": 0}})
    _write_json(forbidden_path, {"summary": {"blocker_count": 1}})
    forbidden_blocked = leakage_tool.build_report(
        root=repo_root,
        public_plan_path=public_path,
        private_plan_path=private_path,
        commercial_scan_path=commercial_path,
        forbidden_import_scan_path=forbidden_path,
        security_scan_path=security_path,
    )
    forbidden_summary = _as_dict(forbidden_blocked["summary"])
    assert forbidden_summary["forbidden_import_blockers"] == 1
