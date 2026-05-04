from __future__ import annotations

import json
from pathlib import Path

import pytest
from tools import p6_forbidden_import_scan as scan_tool


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


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


def _fixture_root(tmp_path: Path, case_name: str) -> tuple[Path, Path]:
    repo_root = _repo_root()
    root = tmp_path / case_name / "repo"
    map_path = root / "docs/p6/forbidden_import_map.json"
    map_path.parent.mkdir(parents=True, exist_ok=True)
    map_path.write_text(
        (repo_root / "docs/p6/forbidden_import_map.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return root, map_path


def _fixture_root_under_dot_tmp(tmp_path: Path, case_name: str) -> tuple[Path, Path]:
    return _fixture_root(tmp_path / ".tmp" / "pytest" / case_name, case_name)


def _findings_for(report: scan_tool.ReportDict, pattern: str) -> list[scan_tool.FindingDict]:
    return [item for item in report["findings"] if item["pattern"] == pattern]


@pytest.mark.auth_contract
def test_docs_boundary_pattern_is_allowed(tmp_path: Path) -> None:
    root, map_path = _fixture_root(tmp_path, "docs_boundary")
    _write(root / "docs/p6/notes.md", "P6 boundary: not import web_core.entitlement here.\n")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(map_path))
    hits = _findings_for(report, "web_core.entitlement")
    assert hits
    assert all(item["classification"] == "allowed_boundary" for item in hits)


@pytest.mark.auth_contract
def test_packages_and_tools_block_real_downstream_imports(tmp_path: Path) -> None:
    root, map_path = _fixture_root(tmp_path, "blocked_imports")
    _write(root / "packages/app/src/runtime.py", "from web_core.entitlement import x\n")
    _write(root / "tools/helper.py", "import zephyr_pro\n")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(map_path))
    assert any(
        item["classification"] == "blocked"
        for item in _findings_for(report, "web_core.entitlement")
    )
    assert any(item["classification"] == "blocked" for item in _findings_for(report, "zephyr_pro"))


@pytest.mark.auth_contract
def test_fixture_context_and_skip_dirs_are_non_blocking(tmp_path: Path) -> None:
    root, map_path = _fixture_root(tmp_path, "fixture_skip")
    _write(
        root / "packages/app/src/tests/test_fixture.py",
        "from zephyr_web_core import test_only\n",
    )
    _write(root / ".tmp/blocked.py", "import zephyr_pro\n")
    _write(root / "node_modules/blocked.js", "require('zephyr_base')\n")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(map_path))
    hits = _findings_for(report, "zephyr_web_core")
    assert hits
    assert all(item["classification"] == "allowed_fixture" for item in hits)
    assert report["summary"]["blocked_hits"] == 0


@pytest.mark.auth_contract
def test_root_under_dot_tmp_still_scans_packages_and_skips_inner_tmp(tmp_path: Path) -> None:
    root, map_path = _fixture_root_under_dot_tmp(tmp_path, "root_under_dot_tmp")
    _write(root / "packages/app/src/runtime.py", "from web_core.entitlement import x\n")
    _write(root / ".tmp/out/runtime.py", "from web_core.entitlement import x\n")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(map_path))
    hits = _findings_for(report, "web_core.entitlement")
    assert any(item["path"] == "packages/app/src/runtime.py" for item in hits)
    assert all(not item["path"].startswith(".tmp/") for item in hits)


@pytest.mark.auth_contract
def test_fail_on_blocker_and_current_repo_pass(tmp_path: Path) -> None:
    root, map_path = _fixture_root(tmp_path, "fail_on_blocker")
    _write(root / "packages/app/src/runtime.py", "import zephyr_base\n")
    out_path = root / "report.json"
    rc = scan_tool.main(
        [
            "--root",
            str(root),
            "--map",
            str(map_path),
            "--out",
            str(out_path),
            "--json",
            "--fail-on-blocker",
        ]
    )
    assert rc == 1
    assert json.loads(out_path.read_text(encoding="utf-8"))["summary"]["overall"] == "fail"

    repo_root = _repo_root()
    report = scan_tool.scan_repo(
        root=repo_root,
        rules=scan_tool.load_rules(repo_root / "docs/p6/forbidden_import_map.json"),
    )
    assert report["summary"]["overall"] == "pass"
