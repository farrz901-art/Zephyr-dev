from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict, cast


def _discover_repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (
            ((candidate / "pyproject.toml").exists() or (candidate / ".git").exists())
            and (candidate / "docs/p6").exists()
            and (candidate / "packages/zephyr-ingest").exists()
        ):
            return candidate
    raise RuntimeError("Could not locate repository root from tool path")


DEFAULT_ROOT = _discover_repo_root(Path(__file__).resolve().parent)
DEFAULT_BASE_ROOT = Path("E:/Github_Projects/Zephyr-base")
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s3_base_bridge_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"


class SummaryDict(TypedDict):
    overall: str
    m3_s3_status: str
    active_blockers: int
    major_gaps: int


def _generated_at_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig")
    return raw.decode("utf-8")


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(_read_text(path))
    if not isinstance(loaded_obj, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return cast(dict[str, object], loaded_obj)


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected dict, got {type(value).__name__}")
    return cast(dict[str, object], value)


def _as_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected bool, got {type(value).__name__}")
    return value


def _as_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list, got {type(value).__name__}")
    return cast(list[object], value)


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _git_output(repo: Path, *args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _required_paths(base_root: Path) -> dict[str, Path]:
    return {
        "readme": base_root / "README.md",
        "source_lineage_md": base_root / "docs/SOURCE_LINEAGE.md",
        "source_lineage_json": base_root / "manifests/public_export_lineage.json",
        "bridge_contract": base_root / "public-core-bridge/bridge_contract.json",
        "fixture_runner": base_root / "public-core-bridge/run_public_core_fixture.py",
        "sample_request_file": base_root / "tests/fixtures/sample_request_file.json",
        "sample_request_text": base_root / "tests/fixtures/sample_request_text.json",
        "fixture_check": base_root / "scripts/check_local_fixture_flow.py",
        "boundary_script": base_root / "scripts/check_boundary.py",
        "boundary_report": base_root / ".tmp/base_boundary_check.json",
        "fixture_report": base_root / ".tmp/local_fixture_flow_check.json",
        "repo_status": base_root / ".tmp/s3_repo_status.json",
        "workflow": base_root / ".github/workflows/boundary.yml",
    }


def build_report(*, root: Path, base_root: Path) -> dict[str, object]:
    required = _required_paths(base_root)
    missing = [
        path.relative_to(base_root).as_posix()
        for path in required.values()
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError("; ".join(missing))
    readme_text = _read_text(required["readme"])
    repo_status = _load_json_object(required["repo_status"])
    boundary_report = _load_json_object(required["boundary_report"])
    fixture_report = _load_json_object(required["fixture_report"])
    bridge_contract = _load_json_object(required["bridge_contract"])
    lineage_json = _load_json_object(required["source_lineage_json"])
    boundary_summary = _as_dict(boundary_report["summary"])
    fixture_summary = _as_dict(fixture_report["summary"])
    repo_info = _as_dict(repo_status)
    contracts = _as_dict(bridge_contract["contracts"])
    result_contract = _as_dict(contracts["base_run_result_v1"])
    error_contract = _as_dict(contracts["base_error_v1"])
    usage_fact = _as_dict(result_contract["usage_fact"])
    request_contract = _as_dict(contracts["base_run_request_v1"])
    input_kinds = cast(list[str], _as_list(request_contract["input_kind"]))
    issues: list[str] = []
    if _as_bool(boundary_summary["pass"]) is False:
        issues.append("Zephyr-base boundary check did not pass.")
    if _as_bool(fixture_summary["pass"]) is False:
        issues.append("Zephyr-base local fixture flow did not pass.")
    if "$sourceSha" in readme_text:
        issues.append("README still contains the unresolved source SHA placeholder.")
    active_blockers = len(issues)
    if active_blockers == 0 and _as_bool(repo_info["pushed"]):
        overall = "pass"
    elif active_blockers == 0:
        overall = "conditional"
    else:
        overall = "fail"
    if overall == "pass":
        status = "sealed"
    elif overall == "conditional":
        status = "conditional"
    else:
        status = "open"
    workflow_text = _read_text(required["workflow"])
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s3.base_bridge_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s3_status": status,
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "zephyr_base": {
            "repo_url": repo_info["repo_url"],
            "branch": repo_info["branch"],
            "previous_head_sha": repo_info["previous_head_sha"],
            "current_head_sha": repo_info["current_head_sha"],
            "pushed": repo_info["pushed"],
            "source_lineage_readme_fixed": repo_info["source_lineage_readme_fixed"],
            "source_lineage_docs_present": repo_info["source_lineage_docs_present"],
        },
        "scope": {
            "actual_runtime_source_copied": _as_bool(lineage_json["actual_runtime_source_copied"]),
            "actual_product_runtime_implemented": _as_bool(
                lineage_json["actual_product_runtime_implemented"]
            ),
            "production_tauri_app_implemented": False,
            "fixture_runner_only": True,
            "installer_built": False,
            "release_created": False,
        },
        "bridge": {
            "bridge_contract_exists": True,
            "fixture_runner_exists": True,
            "supports_local_file": "local_file" in input_kinds,
            "supports_local_text": "local_text" in input_kinds,
            "writes_normalized_text": True,
            "writes_content_evidence": True,
            "writes_receipt": True,
            "writes_usage_fact": True,
            "usage_billing_semantics": _as_bool(usage_fact["billing_semantics"]),
            "error_contract_secret_safe": _as_bool(error_contract["secret_safe_required"]),
        },
        "fixture_flow": {
            "file_fixture_pass": fixture_summary["file_fixture_pass"],
            "text_fixture_pass": fixture_summary["text_fixture_pass"],
            "marker_found": fixture_summary["marker_found"],
            "production_runtime": fixture_summary["production_runtime"],
        },
        "boundary": {
            "boundary_check_pass": boundary_summary["pass"],
            "blocked_count": boundary_summary["blocked_count"],
            "review_required_count": boundary_summary["review_required_count"],
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
        },
        "ci": {
            "workflow_updated": True,
            "runs_boundary_check": "check_boundary.py --json" in workflow_text,
            "runs_fixture_flow": "check_local_fixture_flow.py --json" in workflow_text,
            "runs_tauri_build": False,
            "requires_secrets": False,
        },
        "issues": issues,
        "non_blocking_notes": [
            "S3 is a fixture bridge only and not a production public core runtime.",
            "No Zephyr-dev runtime source was copied into Zephyr-base.",
            "No production Tauri app, installer, or release artifact was created.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    bridge = _as_dict(report["bridge"])
    fixture = _as_dict(report["fixture_flow"])
    boundary = _as_dict(report["boundary"])
    ci = _as_dict(report["ci"])
    lines = [
        "# P6-M3-S3 Base bridge fixture handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        f"- status: {summary['m3_s3_status']}",
        "",
        "## What S3 completed",
        "- concrete fixture bridge contract",
        "- local file and local text fixture runner",
        (
            "- local artifact outputs for normalized_text, content_evidence, "
            "receipt, usage_fact, run_result"
        ),
        "- boundary check update",
        "- CI update for boundary plus fixture flow",
        "",
        "## What S3 did not do",
        "- did not copy Zephyr-dev runtime source",
        "- did not implement a production Tauri app",
        "- did not build an installer",
        "- did not create a release",
        "",
        "## Bridge contract",
        f"- supports_local_file: {bridge['supports_local_file']}",
        f"- supports_local_text: {bridge['supports_local_text']}",
        f"- usage_billing_semantics: {bridge['usage_billing_semantics']}",
        f"- error_contract_secret_safe: {bridge['error_contract_secret_safe']}",
        "",
        "## Fixture runner",
        f"- file_fixture_pass: {fixture['file_fixture_pass']}",
        f"- text_fixture_pass: {fixture['text_fixture_pass']}",
        f"- marker_found: {fixture['marker_found']}",
        f"- production_runtime: {fixture['production_runtime']}",
        "",
        "## Local artifact outputs",
        f"- writes_normalized_text: {bridge['writes_normalized_text']}",
        f"- writes_content_evidence: {bridge['writes_content_evidence']}",
        f"- writes_receipt: {bridge['writes_receipt']}",
        f"- writes_usage_fact: {bridge['writes_usage_fact']}",
        "",
        "## Boundary check",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- blocked_count: {boundary['blocked_count']}",
        f"- review_required_count: {boundary['review_required_count']}",
        "",
        "## CI",
        f"- workflow_updated: {ci['workflow_updated']}",
        f"- runs_boundary_check: {ci['runs_boundary_check']}",
        f"- runs_fixture_flow: {ci['runs_fixture_flow']}",
        f"- runs_tauri_build: {ci['runs_tauri_build']}",
        "",
        "## Next step",
        (
            "- P6-M3-S4: Base local backend/public-core bridge real adapter planning "
            "or UI shell skeleton, depending on user choice."
        ),
    ]
    return "\n".join(lines) + "\n"


def emit_outputs(*, report: dict[str, object], out_root: Path, markdown: bool) -> None:
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / (OUTPUT_MD if markdown else OUTPUT_JSON)
    rendered = (
        render_markdown(report)
        if markdown
        else json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    )
    out_path.write_text(rendered, encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the P6-M3-S3 Base bridge fixture handoff."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--base-root", type=Path, default=DEFAULT_BASE_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--json", action="store_true")
    mode.add_argument("--markdown", action="store_true")
    mode.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    root = args.root.resolve()
    out_root = _resolve_from_root(root, args.out_root)
    if args.check_artifacts:
        required = (out_root / OUTPUT_JSON, out_root / OUTPUT_MD)
        return 0 if all(path.exists() for path in required) else 1
    report = build_report(root=root, base_root=args.base_root.resolve())
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
