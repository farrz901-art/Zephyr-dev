from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest
from tools import p6_security_sensitive_path_scan as scan_tool


def _write(path: Path, text: str = "x\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _fixture_root(case_name: str) -> tuple[Path, Path]:
    root = (
        Path("E:/Github_Projects/Zephyr/codex_p6_security_path_fixtures")
        / f"{case_name}_{uuid4().hex}"
        / "repo"
    )
    policy_path = root / "docs/p6/security_sensitive_paths.json"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        Path("E:/Github_Projects/Zephyr/docs/p6/security_sensitive_paths.json").read_text(
            encoding="utf-8"
        ),
        encoding="utf-8",
    )
    return root, policy_path


@pytest.mark.auth_contract
def test_boundary_docs_and_tooling_are_boundary_tooling() -> None:
    root, policy = _fixture_root("boundary")
    _write(root / "docs/p6/SECURITY_REVIEW_REQUIRED.md")
    _write(root / "tools/p6_security_sensitive_path_scan.py")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(policy))
    assert report["summary"]["sensitive_boundary_paths"] >= 2
    assert report["summary"]["blocked_runtime_paths"] == 0


@pytest.mark.auth_contract
def test_runtime_sensitive_and_future_private_paths_classify_correctly() -> None:
    root, policy = _fixture_root("runtime")
    _write(root / "packages/foo/payment_callback.py")
    _write(root / "Zephyr-Web-core/internal/payment_callback.py")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(policy))
    blocked = [item for item in report["findings"] if item["classification"] == "blocked_runtime"]
    review = [item for item in report["findings"] if item["classification"] == "review_required"]
    assert any(item["path"] == "packages/foo/payment_callback.py" for item in blocked)
    assert any(item["path"] == "Zephyr-Web-core/internal/payment_callback.py" for item in review)


@pytest.mark.auth_contract
def test_json_and_markdown_outputs_work() -> None:
    root, policy = _fixture_root("outputs")
    _write(root / "docs/p6/SECURITY_REVIEW_REQUIRED.md")
    json_out = root / "out.json"
    md_out = root / "out.md"
    assert (
        scan_tool.main(
            ["--root", str(root), "--policy", str(policy), "--out", str(json_out), "--json"]
        )
        == 0
    )
    assert (
        scan_tool.main(
            ["--root", str(root), "--policy", str(policy), "--out", str(md_out), "--markdown"]
        )
        == 0
    )
    assert json.loads(json_out.read_text(encoding="utf-8"))["summary"]["overall"] == "pass"
    assert "# P6 security-sensitive path scan" in md_out.read_text(encoding="utf-8")
