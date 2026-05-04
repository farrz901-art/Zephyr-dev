from __future__ import annotations

import json
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from tools import p6_commercial_contamination_scan as scan_tool


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


def _fixture_root(case_name: str) -> tuple[Path, Path]:
    repo_root = _repo_root()
    root = repo_root / "codex_p6_scan_test_fixtures" / f"{case_name}_{uuid4().hex}" / "repo"
    denylist_path = root / "docs/p6/commercial_contamination_denylist.json"
    denylist_path.parent.mkdir(parents=True, exist_ok=True)
    denylist_path.write_text(
        (repo_root / "docs/p6/commercial_contamination_denylist.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return root, denylist_path


def _load_report(root: Path, denylist_path: Path) -> scan_tool.ReportDict:
    denylist = scan_tool.load_denylist(denylist_path)
    return scan_tool.scan_repo(root=root, denylist=denylist)


def _findings_for(report: scan_tool.ReportDict, term: str) -> list[dict[str, object]]:
    findings = cast(list[dict[str, object]], report["findings"])
    return [item for item in findings if item["term"] == term]


@pytest.mark.auth_contract
def test_boundary_docs_are_allowed_and_runtime_hits_can_block() -> None:
    root, denylist_path = _fixture_root("boundary_allowed_blocked")
    _write(root / "docs/p6/BOUNDARY.md", "billing\nlicense entitlement\nentitlement\n")
    _write(
        root / "PURE_CORE_BOUNDARY.md",
        "payment\nlicense entitlement\nquota decision\nrisk score\n",
    )
    _write(root / "packages/app/src/runtime.py", "paid_user = True\nlicense_verify = True\n")
    report = _load_report(root, denylist_path)

    billing_hits = _findings_for(report, "billing")
    license_hits = _findings_for(report, "license entitlement")
    paid_user_hits = _findings_for(report, "paid_user")
    license_verify_hits = _findings_for(report, "license_verify")

    assert any(hit["classification"] == "allowed_boundary" for hit in billing_hits)
    assert any(hit["classification"] == "allowed_boundary" for hit in license_hits)
    assert any(hit["classification"] == "blocked" for hit in paid_user_hits)
    assert any(hit["classification"] == "blocked" for hit in license_verify_hits)


@pytest.mark.auth_contract
def test_negative_boundary_phrases_do_not_become_runtime_blockers() -> None:
    root, denylist_path = _fixture_root("negative_boundary")
    _write(
        root / "packages/app/src/usage_runtime.py",
        "\n".join(
            [
                "usage_not_billing = True",
                'record = {"not_claimed": [',
                '    "billing",',
                '    "license entitlement",',
                '    "quota decision",',
                '    "RBAC"',
                "]}",
                'note = "not billing and not RBAC"',
            ]
        )
        + "\n",
    )
    report = _load_report(root, denylist_path)

    for term in ("billing", "license entitlement", "quota decision", "rbac"):
        hits = _findings_for(report, term)
        assert hits
        assert all(hit["classification"] != "blocked" for hit in hits)


@pytest.mark.auth_contract
def test_skip_directories_are_not_scanned() -> None:
    root, denylist_path = _fixture_root("skip_directories")
    _write(root / ".tmp/blocked.py", "paid_user = True\n")
    _write(root / ".git/blocked.txt", "license_verify = True\n")
    _write(root / "node_modules/blocked.js", "quota decision\n")
    report = _load_report(root, denylist_path)
    assert report["summary"]["blocked_hits"] == 0
    assert report["summary"]["blocker_count"] == 0


@pytest.mark.auth_contract
def test_fail_on_blocker_and_output_generation() -> None:
    root, denylist_path = _fixture_root("fail_on_blocker")
    _write(root / "packages/app/src/runtime.py", "paid_user = True\nquota decision = 1\n")
    json_out = root / ".tmp/out/report.json"
    md_out = root / ".tmp/out/report.md"

    rc_json = scan_tool.main(
        [
            "--root",
            str(root),
            "--denylist",
            str(denylist_path),
            "--out",
            str(json_out),
            "--json",
            "--fail-on-blocker",
        ]
    )
    rc_md = scan_tool.main(
        [
            "--root",
            str(root),
            "--denylist",
            str(denylist_path),
            "--out",
            str(md_out),
            "--markdown",
        ]
    )
    assert rc_json == 1
    assert rc_md == 0
    assert json.loads(json_out.read_text(encoding="utf-8"))["summary"]["overall"] == "fail"
    markdown = md_out.read_text(encoding="utf-8")
    assert "# Zephyr-dev commercial contamination scan" in markdown
    assert "## Blocked findings" in markdown
    assert "## How to fix" in markdown


@pytest.mark.auth_contract
def test_tool_and_fixture_contexts_are_allowed() -> None:
    root, denylist_path = _fixture_root("tool_and_fixture")
    _write(root / "tools/p6_commercial_contamination_scan.py", 'TERM = "billing"\n')
    _write(root / "packages/x/src/x/tests/test_fixture.py", 'example = "not billing"\n')
    report = _load_report(root, denylist_path)

    billing_hits = _findings_for(report, "billing")
    assert any(hit["classification"] == "allowed_tooling" for hit in billing_hits)
    assert any(hit["classification"] == "allowed_fixture" for hit in billing_hits)


@pytest.mark.auth_contract
def test_historical_docs_become_review_or_boundary_not_runtime_blockers() -> None:
    root, denylist_path = _fixture_root("historical_docs")
    _write(root / "docs/history.md", "billing and license entitlement are not claimed here\n")
    _write(root / "packages/app/src/usage_runtime.py", 'record = {"not_claimed": ["billing"]}\n')
    report = _load_report(root, denylist_path)

    billing_hits = _findings_for(report, "billing")
    assert billing_hits
    assert any(hit["classification"] == "allowed_boundary" for hit in billing_hits)
    assert all(
        hit["classification"] in {"allowed_boundary", "review_required"} for hit in billing_hits
    )
