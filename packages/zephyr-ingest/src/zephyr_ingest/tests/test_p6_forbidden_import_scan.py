from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest
from tools import p6_forbidden_import_scan as scan_tool


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _fixture_root(case_name: str) -> tuple[Path, Path]:
    root = (
        Path("E:/Github_Projects/Zephyr/codex_p6_forbidden_import_fixtures")
        / f"{case_name}_{uuid4().hex}"
        / "repo"
    )
    map_path = root / "docs/p6/forbidden_import_map.json"
    map_path.parent.mkdir(parents=True, exist_ok=True)
    map_path.write_text(
        Path("E:/Github_Projects/Zephyr/docs/p6/forbidden_import_map.json").read_text(
            encoding="utf-8"
        ),
        encoding="utf-8",
    )
    return root, map_path


def _findings_for(report: scan_tool.ReportDict, pattern: str) -> list[scan_tool.FindingDict]:
    return [item for item in report["findings"] if item["pattern"] == pattern]


@pytest.mark.auth_contract
def test_docs_boundary_pattern_is_allowed() -> None:
    root, map_path = _fixture_root("docs_boundary")
    _write(root / "docs/p6/notes.md", "P6 boundary: not import web_core.entitlement here.\n")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(map_path))
    hits = _findings_for(report, "web_core.entitlement")
    assert hits
    assert all(item["classification"] == "allowed_boundary" for item in hits)


@pytest.mark.auth_contract
def test_packages_and_tools_block_real_downstream_imports() -> None:
    root, map_path = _fixture_root("blocked_imports")
    _write(root / "packages/app/src/runtime.py", "from web_core.entitlement import x\n")
    _write(root / "tools/helper.py", "import zephyr_pro\n")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(map_path))
    assert any(
        item["classification"] == "blocked"
        for item in _findings_for(report, "web_core.entitlement")
    )
    assert any(item["classification"] == "blocked" for item in _findings_for(report, "zephyr_pro"))


@pytest.mark.auth_contract
def test_fixture_context_and_skip_dirs_are_non_blocking() -> None:
    root, map_path = _fixture_root("fixture_skip")
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
def test_fail_on_blocker_and_current_repo_pass() -> None:
    root, map_path = _fixture_root("fail_on_blocker")
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

    repo_root = Path("E:/Github_Projects/Zephyr")
    report = scan_tool.scan_repo(
        root=repo_root,
        rules=scan_tool.load_rules(repo_root / "docs/p6/forbidden_import_map.json"),
    )
    assert report["summary"]["overall"] == "pass"
