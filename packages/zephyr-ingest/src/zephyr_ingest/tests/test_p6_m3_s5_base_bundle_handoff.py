from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s5_base_bundle_handoff as handoff_tool


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
    root = _repo_root() / ".tmp" / "p6_m3_s5_test" / case_name
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
    (base_root / "public-core-bridge").mkdir(parents=True, exist_ok=True)
    (base_root / "runtime/public-core-bundle/manifest").mkdir(parents=True, exist_ok=True)
    (base_root / "runtime/public-core-bundle").mkdir(parents=True, exist_ok=True)
    (base_root / "docs/BRIDGE_RUNTIME_MODES.md").write_text("modes\n", encoding="utf-8")
    (base_root / "public-core-bridge/run_public_core_adapter.py").write_text(
        "# adapter\n",
        encoding="utf-8",
    )
    (base_root / "runtime/public-core-bundle/run_bundle_public_core.py").write_text(
        "# bundle entrypoint\n",
        encoding="utf-8",
    )
    (base_root / ".github/workflows/boundary.yml").write_text(
        (
            "python scripts/check_boundary.py --json\n"
            "python scripts/check_local_fixture_flow.py --json\n"
            "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json\n"
            "python scripts/check_bundled_adapter_flow.py --json\n"
        ),
        encoding="utf-8",
    )
    _write_json(
        base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json",
        {"schema_version": 1},
    )
    _write_json(
        base_root / "runtime/public-core-bundle/manifest/public_export_lineage.json",
        {"public_core_bundle_manifest_sha256": "hash"},
    )
    _write_json(
        base_root / "runtime/public-core-bundle/manifest/bundle_file_hashes.json",
        {"recorded_files_excluding_self": []},
    )
    _write_json(
        base_root / ".tmp/base_boundary_check.json",
        {"summary": {"pass": True, "blocked_count": 0, "review_required_count": 2}},
    )
    _write_json(base_root / ".tmp/local_fixture_flow_check.json", {"summary": {"pass": True}})
    _write_json(base_root / ".tmp/local_adapter_flow_check.json", {"summary": {"pass": True}})
    _write_json(
        base_root / ".tmp/bundled_adapter_flow_check.json",
        {
            "summary": {
                "pass": True,
                "bundled_runtime_used": True,
                "fixture_runner_used_for_s5_pass": False,
                "contains_dev_root": False,
                "requires_network": False,
                "requires_p45_substrate": False,
            }
        },
    )
    _write_json(
        base_root / ".tmp/s5_repo_status.json",
        {
            "repo_url": "https://github.com/farrz901-art/Zephyr-base.git",
            "branch": "main",
            "previous_head_sha": "prevsha",
            "current_head_sha": "currsha",
            "pushed": False,
            "origin_head_sha": "oldsha",
        },
    )


def _emit_fake_repo_root(repo_root: Path) -> None:
    (repo_root / "docs/p6").mkdir(parents=True, exist_ok=True)
    (repo_root / "packages/zephyr-ingest").mkdir(parents=True, exist_ok=True)
    (repo_root / ".tmp/p6_m3_public_core_bundle").mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[project]\nname='fake'\n", encoding="utf-8")
    (repo_root / "docs/p6/P6_M3_S5_PUBLIC_CORE_BUNDLE_POLICY.md").write_text(
        "policy\n",
        encoding="utf-8",
    )
    _write_json(
        repo_root / "docs/p6/p6_m3_s5_public_core_bundle_manifest.json",
        {"schema_version": 1},
    )
    (repo_root / "tools").mkdir(parents=True, exist_ok=True)
    (repo_root / "tools/p6_m3_public_core_bundle_export.py").write_text(
        "# tool\n",
        encoding="utf-8",
    )
    _write_json(
        repo_root / ".tmp/p6_m3_public_core_bundle/report.json",
        {
            "summary": {
                "bundle_exported": True,
                "bundle_execution_requires_zephyr_dev_working_tree": False,
                "bundle_generation_requires_zephyr_dev_working_tree": True,
            }
        },
    )


@pytest.mark.auth_contract
def test_s5_handoff_reads_bundled_outputs(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    _emit_fake_repo_root(repo_root)
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    _emit_base_fixture(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    adapter = _as_dict(report["adapter"])
    bundle = _as_dict(report["bundle"])
    assert summary["overall"] == "conditional"
    assert adapter["bundled_runtime_used"] is True
    assert adapter["zephyr_dev_root_used_for_s5_pass"] is False
    assert bundle["bundle_execution_requires_zephyr_dev_working_tree"] is False


@pytest.mark.auth_contract
def test_s5_handoff_outputs_are_secret_safe(tmp_path: Path) -> None:
    repo_root = _workspace_case_dir(f"{tmp_path.name}_repo")
    _emit_fake_repo_root(repo_root)
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
