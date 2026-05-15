from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import cast

import pytest
from tools import p6_m3_s4_base_real_adapter_handoff as handoff_tool


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
    root = _repo_root() / ".tmp" / "p6_m3_s4_test" / case_name
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
    (base_root / "scripts").mkdir(parents=True, exist_ok=True)
    (base_root / "docs/BRIDGE_RUNTIME_MODES.md").write_text("modes\n", encoding="utf-8")
    (base_root / "docs/SOURCE_LINEAGE.md").write_text("source lineage\n", encoding="utf-8")
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
    (base_root / "public-core-bridge/run_public_core_adapter.py").write_text(
        "# real adapter\n",
        encoding="utf-8",
    )
    (base_root / "public-core-bridge/run_public_core_fixture.py").write_text(
        "# fixture adapter\n",
        encoding="utf-8",
    )
    (base_root / "scripts/check_local_fixture_flow.py").write_text(
        "# fixture smoke\n",
        encoding="utf-8",
    )
    (base_root / "scripts/check_real_adapter_flow.py").write_text(
        "# real smoke\n",
        encoding="utf-8",
    )
    (base_root / "scripts/check_boundary.py").write_text("# boundary\n", encoding="utf-8")
    (base_root / ".github/workflows/boundary.yml").write_text(
        (
            "python scripts/check_boundary.py --json\n"
            "python scripts/check_local_fixture_flow.py --json\n"
            "test -f public-core-bridge/run_public_core_adapter.py\n"
        ),
        encoding="utf-8",
    )
    _write_json(
        base_root / ".tmp/base_boundary_check.json",
        {"summary": {"pass": True, "blocked_count": 0, "review_required_count": 3}},
    )
    _write_json(
        base_root / ".tmp/local_fixture_flow_check.json",
        {"summary": {"pass": True}},
    )
    _write_json(
        base_root / ".tmp/local_adapter_flow_check.json",
        {
            "summary": {
                "pass": True,
                "fixture_runner_used_for_s4_pass": False,
                "zephyr_dev_public_core_invoked": True,
                "content_evidence_kind_is_fixture": False,
            }
        },
    )
    _write_json(
        base_root / ".tmp/s4_repo_status.json",
        {
            "repo_url": "https://github.com/farrz901-art/Zephyr-base.git",
            "branch": "main",
            "previous_head_sha": "prevsha",
            "current_head_sha": "currsha",
            "pushed": True,
        },
    )


@pytest.mark.auth_contract
def test_s4_handoff_reads_real_adapter_outputs(tmp_path: Path) -> None:
    repo_root = _repo_root()
    base_root = _workspace_case_dir(f"{tmp_path.name}_base")
    _emit_base_fixture(base_root)
    report = handoff_tool.build_report(root=repo_root, base_root=base_root)
    summary = _as_dict(report["summary"])
    adapter = _as_dict(report["adapter"])
    boundary = _as_dict(report["boundary"])
    assert summary["overall"] == "pass"
    assert adapter["fixture_runner_used_for_s4_pass"] is False
    assert adapter["zephyr_dev_public_core_invoked"] is True
    assert adapter["content_evidence_kind_is_fixture"] is False
    assert boundary["blocked_count"] == 0


@pytest.mark.auth_contract
def test_s4_handoff_outputs_are_secret_safe(tmp_path: Path) -> None:
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
