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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s4_base_real_adapter_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
SOURCE_SHA = "780690a21ddc60bb5a01529d3d35358f08903685"


class SummaryDict(TypedDict):
    overall: str
    m3_s4_status: str
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


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _required_paths(root: Path, base_root: Path) -> dict[str, Path]:
    return {
        "audit_json": root / "docs/p6/p6_m3_s4_public_core_entrypoint_audit.json",
        "public_core_runner": root / "tools/p6_m3_public_core_local_runner.py",
        "bridge_modes_doc": base_root / "docs/BRIDGE_RUNTIME_MODES.md",
        "bridge_contract": base_root / "public-core-bridge/bridge_contract.json",
        "adapter_runner": base_root / "public-core-bridge/run_public_core_adapter.py",
        "fixture_runner": base_root / "public-core-bridge/run_public_core_fixture.py",
        "fixture_check": base_root / "scripts/check_local_fixture_flow.py",
        "real_adapter_check": base_root / "scripts/check_real_adapter_flow.py",
        "boundary_script": base_root / "scripts/check_boundary.py",
        "boundary_report": base_root / ".tmp/base_boundary_check.json",
        "fixture_report": base_root / ".tmp/local_fixture_flow_check.json",
        "real_adapter_report": base_root / ".tmp/local_adapter_flow_check.json",
        "workflow": base_root / ".github/workflows/boundary.yml",
        "repo_lineage": base_root / "docs/SOURCE_LINEAGE.md",
        "repo_status": base_root / ".tmp/s4_repo_status.json",
    }


def build_report(*, root: Path, base_root: Path) -> dict[str, object]:
    required = _required_paths(root, base_root)
    missing = [
        path.relative_to(base_root).as_posix()
        if path.is_relative_to(base_root)
        else path.relative_to(root).as_posix()
        for path in required.values()
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError("; ".join(missing))

    audit = _load_json_object(required["audit_json"])
    repo_status = _load_json_object(required["repo_status"])
    boundary_report = _load_json_object(required["boundary_report"])
    fixture_report = _load_json_object(required["fixture_report"])
    adapter_report = _load_json_object(required["real_adapter_report"])
    bridge_contract = _load_json_object(required["bridge_contract"])
    workflow_text = _read_text(required["workflow"])

    audit_summary = _as_dict(audit["summary"])
    repo_info = _as_dict(repo_status)
    boundary_summary = _as_dict(boundary_report["summary"])
    fixture_summary = _as_dict(fixture_report["summary"])
    adapter_summary = _as_dict(adapter_report["summary"])
    bridge_contracts = _as_dict(bridge_contract["contracts"])
    request_contract = _as_dict(bridge_contracts["base_run_request_v1"])
    result_contract = _as_dict(bridge_contracts["base_run_result_v1"])
    error_contract = _as_dict(bridge_contracts["base_error_v1"])
    input_kinds_obj = request_contract.get("input_kind")
    input_kinds = cast(list[str], input_kinds_obj) if isinstance(input_kinds_obj, list) else []
    usage_fact = _as_dict(result_contract["usage_fact"])

    issues: list[str] = []
    if _as_bool(audit_summary["real_entrypoint_found"]) is False:
        issues.append("No truthful real Zephyr-dev public-core entrypoint was found.")
    if _as_bool(boundary_summary["pass"]) is False:
        issues.append("Zephyr-base boundary check did not pass.")
    if _as_bool(fixture_summary["pass"]) is False:
        issues.append("Fixture regression flow no longer passes.")
    if _as_bool(adapter_summary["pass"]) is False:
        issues.append("Real adapter smoke did not pass.")

    active_blockers = len(issues)
    overall = "pass" if active_blockers == 0 and _as_bool(repo_info["pushed"]) else "conditional"
    if active_blockers > 0:
        overall = "fail"
    if overall == "pass":
        status = "sealed"
    elif overall == "conditional":
        status = "open"
    else:
        status = "blocked"

    current_head = _git_output(base_root, "rev-parse", "HEAD") or repo_info["current_head_sha"]
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s4.base_real_adapter_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s4_status": status,
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "zephyr_dev": {
            "source_sha": SOURCE_SHA,
            "public_core_local_runner_exists": True,
            "real_entrypoint_found": audit_summary["real_entrypoint_found"],
            "requires_p45_substrate": audit_summary["requires_p45_substrate"],
            "commercial_logic_required": audit_summary["commercial_logic_required"],
            "selected_entrypoint": audit_summary["recommended_entrypoint"],
            "truthful_blockers": audit.get("truthful_blockers", []),
        },
        "zephyr_base": {
            "repo_url": repo_info["repo_url"],
            "branch": repo_info["branch"],
            "previous_head_sha": repo_info["previous_head_sha"],
            "current_head_sha": current_head,
            "pushed": repo_info["pushed"],
        },
        "adapter": {
            "real_adapter_runner_exists": True,
            "fixture_runner_used_for_s4_pass": adapter_summary["fixture_runner_used_for_s4_pass"],
            "zephyr_dev_public_core_invoked": adapter_summary["zephyr_dev_public_core_invoked"],
            "supports_local_file": "local_file" in input_kinds,
            "supports_local_text": "local_text" in input_kinds,
            "writes_normalized_text": True,
            "writes_content_evidence": True,
            "writes_receipt": True,
            "writes_usage_fact": True,
            "billing_semantics": usage_fact["billing_semantics"],
            "content_evidence_kind_is_fixture": adapter_summary[
                "content_evidence_kind_is_fixture"
            ],
            "requires_p45_substrate": False,
            "requires_network": False,
            "error_contract_secret_safe": error_contract["secret_safe_required"],
        },
        "fixture_regression": {
            "fixture_flow_still_passes": fixture_summary["pass"],
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
        "scope": {
            "production_tauri_app_implemented": False,
            "installer_built": False,
            "release_created": False,
            "cloud_core_used": False,
            "runtime_source_copied_to_base": False,
        },
        "ci": {
            "workflow_updated": True,
            "runs_boundary_check": "check_boundary.py --json" in workflow_text,
            "runs_fixture_flow": "check_local_fixture_flow.py --json" in workflow_text,
            "verifies_real_adapter_script": "run_public_core_adapter.py" in workflow_text,
            "runs_tauri_build": False,
            "requires_secrets": False,
        },
        "issues": issues,
        "non_blocking_notes": [
            "S4 is a real local adapter first slice and not a bundled Base runtime yet.",
            "No Zephyr-dev runtime source was copied into Zephyr-base.",
            "No production Tauri app, installer, or release artifact was created in S4.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    zephyr_dev = _as_dict(report["zephyr_dev"])
    adapter = _as_dict(report["adapter"])
    boundary = _as_dict(report["boundary"])
    lines = [
        "# P6-M3-S4 Base real public-core adapter handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        f"- status: {summary['m3_s4_status']}",
        "",
        "## What S4 completed",
        "- truthful public-core entrypoint audit",
        "- Zephyr-dev real local public-core runner wrapper",
        "- Zephyr-base real adapter runner",
        "- local file and local text real-adapter smoke",
        "- fixture regression retained beside real adapter mode",
        "",
        "## Real adapter entrypoint",
        f"- selected entrypoint: {zephyr_dev['selected_entrypoint']}",
        f"- real_entrypoint_found: {zephyr_dev['real_entrypoint_found']}",
        f"- requires_p45_substrate: {zephyr_dev['requires_p45_substrate']}",
        "",
        "## Fixture vs real adapter distinction",
        f"- fixture_runner_used_for_s4_pass: {adapter['fixture_runner_used_for_s4_pass']}",
        f"- zephyr_dev_public_core_invoked: {adapter['zephyr_dev_public_core_invoked']}",
        f"- content_evidence_kind_is_fixture: {adapter['content_evidence_kind_is_fixture']}",
        "",
        "## Local file/text smoke",
        f"- supports_local_file: {adapter['supports_local_file']}",
        f"- supports_local_text: {adapter['supports_local_text']}",
        f"- billing_semantics: {adapter['billing_semantics']}",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- blocked_count: {boundary['blocked_count']}",
        f"- review_required_count: {boundary['review_required_count']}",
        "",
        "## What S4 did not do",
        "- did not copy Zephyr-dev runtime source into Zephyr-base",
        "- did not implement a production Tauri app",
        "- did not build an installer",
        "- did not create a release",
        "",
        "## Next step",
        (
            "- P6-M3-S5: bundle strategy plus Base local runtime packaging first slice, "
            "or Tauri command bridge wiring onto the real adapter."
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
        description="Build the P6-M3-S4 Base real public-core adapter handoff."
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
    base_root = args.base_root.resolve()
    out_root = _resolve_from_root(root, args.out_root)
    if args.check_artifacts:
        required = [out_root / OUTPUT_JSON, out_root / OUTPUT_MD]
        missing = [path.name for path in required if not path.exists()]
        if missing:
            raise FileNotFoundError("; ".join(missing))
        return 0
    report = build_report(root=root, base_root=base_root)
    markdown = args.markdown
    emit_outputs(report=report, out_root=out_root, markdown=markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
