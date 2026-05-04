from __future__ import annotations

import json
from pathlib import Path

import pytest
from tools import p6_security_sensitive_path_scan as scan_tool


def _write(path: Path, text: str = "x\n") -> None:
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
    policy_path = root / "docs/p6/security_sensitive_paths.json"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        (repo_root / "docs/p6/security_sensitive_paths.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return root, policy_path


def _fixture_root_under_dot_tmp(tmp_path: Path, case_name: str) -> tuple[Path, Path]:
    return _fixture_root(tmp_path / ".tmp" / "pytest" / case_name, case_name)


@pytest.mark.auth_contract
def test_boundary_docs_and_tooling_are_boundary_tooling(tmp_path: Path) -> None:
    root, policy = _fixture_root(tmp_path, "boundary")
    _write(root / "docs/p6/SECURITY_REVIEW_REQUIRED.md")
    _write(root / "tools/p6_security_sensitive_path_scan.py")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(policy))
    assert report["summary"]["sensitive_boundary_paths"] >= 2
    assert report["summary"]["blocked_runtime_paths"] == 0


@pytest.mark.auth_contract
def test_runtime_sensitive_and_future_private_paths_classify_correctly(
    tmp_path: Path,
) -> None:
    root, policy = _fixture_root(tmp_path, "runtime")
    _write(root / "packages/foo/payment_callback.py")
    _write(root / "Zephyr-Web-core/internal/payment_callback.py")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(policy))
    blocked = [item for item in report["findings"] if item["classification"] == "blocked_runtime"]
    review = [item for item in report["findings"] if item["classification"] == "review_required"]
    assert any(item["path"] == "packages/foo/payment_callback.py" for item in blocked)
    assert any(item["path"] == "Zephyr-Web-core/internal/payment_callback.py" for item in review)


@pytest.mark.auth_contract
def test_root_under_dot_tmp_still_scans_runtime_children(tmp_path: Path) -> None:
    root, policy = _fixture_root_under_dot_tmp(tmp_path, "root_under_dot_tmp")
    _write(root / "packages/foo/payment_callback.py")
    _write(root / ".tmp/out/payment_callback.py")
    report = scan_tool.scan_repo(root=root, rules=scan_tool.load_rules(policy))
    blocked = [item for item in report["findings"] if item["classification"] == "blocked_runtime"]
    assert any(item["path"] == "packages/foo/payment_callback.py" for item in blocked)
    assert all(not item["path"].startswith(".tmp/") for item in blocked)


@pytest.mark.auth_contract
def test_json_and_markdown_outputs_work(tmp_path: Path) -> None:
    root, policy = _fixture_root(tmp_path, "outputs")
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
