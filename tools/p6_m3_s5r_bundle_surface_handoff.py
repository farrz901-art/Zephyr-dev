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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s5r_bundle_surface_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "345a6d3ba12fd74798ee3ad225bba0efdec93391"


class SummaryDict(TypedDict):
    overall: str
    m3_s5r_status: str
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


def _repo_status(base_root: Path) -> dict[str, object]:
    status_path = base_root / ".tmp/s5r_repo_status.json"
    status_file = _load_json_object(status_path) if status_path.exists() else None
    current_head = _git_output(base_root, "rev-parse", "HEAD")
    origin_head = _git_output(base_root, "rev-parse", "origin/main")
    repo_url = _git_output(base_root, "remote", "get-url", "origin")
    branch = _git_output(base_root, "branch", "--show-current")
    if current_head is None:
        if status_file is None:
            return {
                "previous_head_sha": PREVIOUS_HEAD_SHA,
                "current_head_sha": "unknown",
                "pushed": False,
                "repo_url": "",
                "branch": "",
            }
        return {
            "previous_head_sha": status_file.get("previous_head_sha", PREVIOUS_HEAD_SHA),
            "current_head_sha": status_file.get("current_head_sha", "unknown"),
            "pushed": status_file.get("pushed", False),
            "repo_url": status_file.get("repo_url", ""),
            "branch": status_file.get("branch", ""),
        }
    return {
        "previous_head_sha": PREVIOUS_HEAD_SHA,
        "current_head_sha": current_head,
        "pushed": origin_head == current_head and origin_head is not None,
        "repo_url": repo_url or "",
        "branch": branch or "",
    }


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _required_paths(root: Path, base_root: Path) -> dict[str, Path]:
    return {
        "bundle_manifest_doc": root / "docs/p6/p6_m3_s5_public_core_bundle_manifest.json",
        "bundle_export_tool": root / "tools/p6_m3_public_core_bundle_export.py",
        "bundle_export_report": root / ".tmp/p6_m3_public_core_bundle/report.json",
        "workflow": base_root / ".github/workflows/boundary.yml",
        "modes_doc": base_root / "docs/BRIDGE_RUNTIME_MODES.md",
        "bundle_manifest": (
            base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json"
        ),
        "bundle_hashes": base_root / "runtime/public-core-bundle/manifest/bundle_file_hashes.json",
        "bundle_readme": base_root / "runtime/public-core-bundle/README.md",
        "adapter": base_root / "public-core-bridge/run_public_core_adapter.py",
        "audit_script": base_root / "scripts/audit_public_core_bundle_surface.py",
        "negative_script": base_root / "scripts/check_bundle_unsupported_surface.py",
        "audit_report": base_root / ".tmp/public_core_bundle_surface_audit.json",
        "boundary_report": base_root / ".tmp/base_boundary_check.json",
        "fixture_report": base_root / ".tmp/local_fixture_flow_check.json",
        "dev_mode_report": base_root / ".tmp/local_adapter_flow_check.json",
        "bundled_report": base_root / ".tmp/bundled_adapter_flow_check.json",
        "negative_report": base_root / ".tmp/bundle_unsupported_surface_check.json",
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

    bundle_export_report = _load_json_object(required["bundle_export_report"])
    bundle_manifest = _load_json_object(required["bundle_manifest"])
    audit_report = _load_json_object(required["audit_report"])
    boundary_report = _load_json_object(required["boundary_report"])
    fixture_report = _load_json_object(required["fixture_report"])
    dev_mode_report = _load_json_object(required["dev_mode_report"])
    bundled_report = _load_json_object(required["bundled_report"])
    negative_report = _load_json_object(required["negative_report"])
    workflow_text = _read_text(required["workflow"])
    repo_status = _repo_status(base_root)

    export_summary = _as_dict(bundle_export_report["summary"])
    audit_summary = _as_dict(audit_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    fixture_summary = _as_dict(fixture_report["summary"])
    dev_mode_summary = _as_dict(dev_mode_report["summary"])
    bundled_summary = _as_dict(bundled_report["summary"])
    negative_summary = _as_dict(negative_report["summary"])
    removed_surface = _as_dict(bundle_manifest["removed_surface"])

    issues: list[str] = []
    if _as_bool(export_summary["bundle_exported"]) is False:
        issues.append("Zephyr-dev bundle export did not complete.")
    if _as_bool(audit_summary["bundle_surface_matches_manifest"]) is False:
        issues.append("Zephyr-base bundle surface does not match the trimmed manifest.")
    if int(audit_summary["blocked_count"]) != 0:
        issues.append("Zephyr-base bundle surface audit still has blockers.")
    if _as_bool(boundary_summary["pass"]) is False:
        issues.append("Zephyr-base boundary check did not pass.")
    if _as_bool(fixture_summary["pass"]) is False:
        issues.append("Fixture regression no longer passes.")
    if _as_bool(dev_mode_summary["pass"]) is False:
        issues.append("Dev-mode real adapter regression no longer passes.")
    if _as_bool(bundled_summary["pass"]) is False:
        issues.append("Bundled adapter smoke does not pass.")
    if _as_bool(negative_summary["pass"]) is False:
        issues.append("Unsupported surface negative smoke did not reject pdf correctly.")
    if _as_bool(bundled_summary["fixture_runner_used_for_s5_pass"]):
        issues.append("Bundled adapter pass incorrectly used fixture mode.")
    if _as_bool(bundled_summary["contains_dev_root"]):
        issues.append("Bundled adapter output still references the Zephyr-dev working tree.")
    if _as_bool(bundled_summary["zephyr_dev_working_tree_required"]):
        issues.append("Bundled adapter still requires the Zephyr-dev working tree at execution.")

    active_blockers = len(issues)
    pushed = (
        _as_bool(repo_status["pushed"])
        if isinstance(repo_status.get("pushed"), bool)
        else False
    )
    if active_blockers > 0:
        overall = "fail"
        status = "blocked"
    elif pushed:
        overall = "pass"
        status = "sealed"
    else:
        overall = "conditional"
        status = "open"

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s5r.bundle_surface_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s5r_status": status,
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": pushed,
        },
        "bundle_surface": {
            "bundle_surface_matches_manifest": audit_summary["bundle_surface_matches_manifest"],
            "supported_input_extensions": bundle_manifest["supported_input_extensions"],
            "allowed_partition_kinds": bundle_manifest["allowed_partition_kinds"],
            "allowed_sources": bundle_manifest["allowed_sources"],
            "allowed_destinations": bundle_manifest["allowed_destinations"],
            "non_txt_md_partition_modules_removed": removed_surface[
                "non_txt_md_partition_modules_removed"
            ],
            "remote_sources_removed": removed_surface["remote_sources_removed"],
            "remote_destinations_removed": removed_surface["remote_destinations_removed"],
            "testing_helpers_removed": removed_surface["testing_helpers_removed"],
            "queue_worker_observability_governance_removed": removed_surface[
                "queue_worker_observability_governance_removed"
            ],
            "blocked_count": audit_summary["blocked_count"],
            "review_required_count": audit_summary["review_required_count"],
        },
        "runtime": {
            "bundled_mode_still_passes": bundled_summary["pass"],
            "fixture_runner_used_for_s5r_pass": bundled_summary["fixture_runner_used_for_s5_pass"],
            "zephyr_dev_root_used_for_s5r_pass": False,
            "bundle_execution_requires_zephyr_dev_working_tree": bundled_summary[
                "zephyr_dev_working_tree_required"
            ],
            "uses_current_python_environment": True,
            "installer_runtime_complete": False,
        },
        "negative_smoke": {
            "unsupported_pdf_rejected": negative_summary["unsupported_pdf_rejected"],
            "hidden_pdf_route_available": negative_summary["hidden_pdf_route_available"],
            "secret_safe_error": negative_summary["secret_safe_error"],
        },
        "boundary": {
            "boundary_check_pass": boundary_summary["pass"],
            "blocked_count": boundary_summary["blocked_count"],
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
        },
        "ci": {
            "workflow_updated": True,
            "runs_bundle_surface_audit": "audit_public_core_bundle_surface.py --json"
            in workflow_text,
            "runs_unsupported_surface_negative_smoke": "check_bundle_unsupported_surface.py --json"
            in workflow_text,
            "runs_tauri_build": False,
            "requires_secrets": False,
        },
        "issues": issues,
        "non_blocking_notes": [
            "S5-R is a public bundle surface correction on top of the S5 bundled runtime slice.",
            "The bundled runtime still uses the current Python environment.",
            "Installer packaging and the Tauri command bridge remain later M3 work.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    bundle_surface = _as_dict(report["bundle_surface"])
    runtime = _as_dict(report["runtime"])
    negative = _as_dict(report["negative_smoke"])
    boundary = _as_dict(report["boundary"])
    lines = [
        "# P6-M3-S5-R Bundle surface hardening handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        f"- status: {summary['m3_s5r_status']}",
        "",
        "## Why S5-R was needed",
        (
            "- S5 proved bundled execution, but the imported bundle surface was still wider "
            "than the Base txt/md first-slice manifest."
        ),
        "",
        "## What was removed",
        (
            "- non_txt_md_partition_modules_removed: "
            f"{bundle_surface['non_txt_md_partition_modules_removed']}"
        ),
        f"- remote_sources_removed: {bundle_surface['remote_sources_removed']}",
        f"- remote_destinations_removed: {bundle_surface['remote_destinations_removed']}",
        f"- testing_helpers_removed: {bundle_surface['testing_helpers_removed']}",
        (
            "- queue_worker_observability_governance_removed: "
            f"{bundle_surface['queue_worker_observability_governance_removed']}"
        ),
        "",
        "## What remains in Base bundle",
        f"- supported_input_extensions: {bundle_surface['supported_input_extensions']}",
        f"- allowed_partition_kinds: {bundle_surface['allowed_partition_kinds']}",
        f"- allowed_sources: {bundle_surface['allowed_sources']}",
        f"- allowed_destinations: {bundle_surface['allowed_destinations']}",
        "",
        "## Manifest alignment",
        (
            "- bundle_surface_matches_manifest: "
            f"{bundle_surface['bundle_surface_matches_manifest']}"
        ),
        f"- blocked_count: {bundle_surface['blocked_count']}",
        f"- review_required_count: {bundle_surface['review_required_count']}",
        "",
        "## Regression validation",
        f"- bundled_mode_still_passes: {runtime['bundled_mode_still_passes']}",
        f"- fixture_runner_used_for_s5r_pass: {runtime['fixture_runner_used_for_s5r_pass']}",
        f"- zephyr_dev_root_used_for_s5r_pass: {runtime['zephyr_dev_root_used_for_s5r_pass']}",
        "",
        "## Negative unsupported surface validation",
        f"- unsupported_pdf_rejected: {negative['unsupported_pdf_rejected']}",
        f"- hidden_pdf_route_available: {negative['hidden_pdf_route_available']}",
        f"- secret_safe_error: {negative['secret_safe_error']}",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- blocked_count: {boundary['blocked_count']}",
        "",
        "## Current limitations",
        "- S5-R is still not a final installer runtime.",
        "- S5-R is still not a Tauri app.",
        "- The bundled runtime still uses the current Python environment.",
        "",
        "## Next step",
        "- P6-M3-S6: Tauri/Rust command bridge calls the bundled adapter.",
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
        description="Build the P6-M3-S5-R bundle surface hardening handoff."
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
    emit_outputs(report=report, out_root=out_root, markdown=args.markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
