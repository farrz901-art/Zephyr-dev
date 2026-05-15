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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s5_base_bundle_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
ADAPTER_COMMIT_SHA = "1b53c126131ce457c43e7ebeb55f77612911cd92"


class SummaryDict(TypedDict):
    overall: str
    m3_s5_status: str
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
    status_path = base_root / ".tmp/s5_repo_status.json"
    status_file = _load_json_object(status_path) if status_path.exists() else None
    current_head = _git_output(base_root, "rev-parse", "HEAD")
    if current_head is None:
        if status_file is None:
            raise FileNotFoundError(str(status_path))
        return status_file
    remote_head = _git_output(base_root, "rev-parse", "origin/main")
    repo_url = _git_output(base_root, "remote", "get-url", "origin")
    branch = _git_output(base_root, "branch", "--show-current")
    previous_head = None
    if status_file is not None:
        previous_head_obj = status_file.get("previous_head_sha")
        if isinstance(previous_head_obj, str):
            previous_head = previous_head_obj
    if previous_head is None:
        previous_head = _git_output(base_root, "rev-parse", "HEAD~2")
    return {
        "repo_url": repo_url or "",
        "branch": branch or "",
        "previous_head_sha": previous_head or current_head,
        "current_head_sha": current_head,
        "pushed": remote_head == current_head and remote_head is not None,
        "origin_head_sha": remote_head,
    }


def _required_paths(root: Path, base_root: Path) -> dict[str, Path]:
    return {
        "bundle_policy": root / "docs/p6/P6_M3_S5_PUBLIC_CORE_BUNDLE_POLICY.md",
        "bundle_manifest": root / "docs/p6/p6_m3_s5_public_core_bundle_manifest.json",
        "bundle_tool": root / "tools/p6_m3_public_core_bundle_export.py",
        "bundle_report": root / ".tmp/p6_m3_public_core_bundle/report.json",
        "base_bundle_manifest": (
            base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json"
        ),
        "base_bundle_lineage": (
            base_root / "runtime/public-core-bundle/manifest/public_export_lineage.json"
        ),
        "base_bundle_hashes": (
            base_root / "runtime/public-core-bundle/manifest/bundle_file_hashes.json"
        ),
        "base_bundle_runner": base_root / "runtime/public-core-bundle/run_bundle_public_core.py",
        "base_adapter": base_root / "public-core-bridge/run_public_core_adapter.py",
        "base_modes_doc": base_root / "docs/BRIDGE_RUNTIME_MODES.md",
        "base_boundary": base_root / ".tmp/base_boundary_check.json",
        "base_fixture": base_root / ".tmp/local_fixture_flow_check.json",
        "base_dev_mode": base_root / ".tmp/local_adapter_flow_check.json",
        "base_bundled": base_root / ".tmp/bundled_adapter_flow_check.json",
        "base_workflow": base_root / ".github/workflows/boundary.yml",
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

    bundle_report = _load_json_object(required["bundle_report"])
    boundary_report = _load_json_object(required["base_boundary"])
    fixture_report = _load_json_object(required["base_fixture"])
    dev_mode_report = _load_json_object(required["base_dev_mode"])
    bundled_report = _load_json_object(required["base_bundled"])
    bundle_lineage = _load_json_object(required["base_bundle_lineage"])
    repo_status = _repo_status(base_root)
    workflow_text = _read_text(required["base_workflow"])

    bundle_summary = _as_dict(bundle_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    fixture_summary = _as_dict(fixture_report["summary"])
    dev_mode_summary = _as_dict(dev_mode_report["summary"])
    bundled_summary = _as_dict(bundled_report["summary"])

    issues: list[str] = []
    if _as_bool(bundle_summary["bundle_exported"]) is False:
        issues.append("Zephyr-dev public-core bundle was not exported.")
    if _as_bool(boundary_summary["pass"]) is False:
        issues.append("Zephyr-base boundary check did not pass.")
    if _as_bool(fixture_summary["pass"]) is False:
        issues.append("Zephyr-base fixture regression no longer passes.")
    if _as_bool(dev_mode_summary["pass"]) is False:
        issues.append("Zephyr-base dev-mode real adapter no longer passes.")
    if _as_bool(bundled_summary["pass"]) is False:
        issues.append("Zephyr-base bundled adapter flow did not pass.")
    if _as_bool(bundled_summary["contains_dev_root"]):
        issues.append("Bundled adapter output still references the Zephyr-dev working tree.")

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
        "report_id": "zephyr.p6.m3.s5.base_bundle_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s5_status": status,
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "zephyr_dev": {
            "adapter_commit_sha": ADAPTER_COMMIT_SHA,
            "bundle_export_tool_exists": True,
            "bundle_exported": bundle_summary["bundle_exported"],
            "bundle_generation_requires_zephyr_dev_working_tree": bundle_summary[
                "bundle_generation_requires_zephyr_dev_working_tree"
            ],
        },
        "zephyr_base": {
            "repo_url": repo_status["repo_url"],
            "branch": repo_status["branch"],
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": pushed,
        },
        "bundle": {
            "bundle_root": "runtime/public-core-bundle",
            "bundle_manifest_exists": True,
            "bundle_file_hashes_recorded": True,
            "bundle_execution_requires_zephyr_dev_working_tree": bundle_summary[
                "bundle_execution_requires_zephyr_dev_working_tree"
            ],
            "uses_current_python_environment": True,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "installer_runtime_complete": False,
            "bundle_lineage_hash_present": bool(
                bundle_lineage.get("public_core_bundle_manifest_sha256")
            ),
        },
        "adapter": {
            "bundled_mode_supported": True,
            "dev_mode_still_supported": _as_bool(dev_mode_summary["pass"]),
            "fixture_runner_used_for_s5_pass": bundled_summary[
                "fixture_runner_used_for_s5_pass"
            ],
            "zephyr_dev_root_used_for_s5_pass": False,
            "bundled_runtime_used": bundled_summary["bundled_runtime_used"],
            "supports_local_file": True,
            "supports_local_text": True,
            "writes_normalized_text": True,
            "writes_content_evidence": True,
            "writes_receipt": True,
            "writes_usage_fact": True,
            "billing_semantics": False,
            "requires_network": bundled_summary["requires_network"],
            "requires_p45_substrate": bundled_summary["requires_p45_substrate"],
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
        },
        "ci": {
            "workflow_updated": True,
            "runs_boundary_check": "check_boundary.py --json" in workflow_text,
            "runs_fixture_flow": "check_local_fixture_flow.py --json" in workflow_text,
            "validates_bundle_manifest": (
                "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json"
                in workflow_text
            ),
            "runs_bundled_adapter_smoke": "check_bundled_adapter_flow.py" in workflow_text,
            "runs_tauri_build": False,
            "requires_secrets": False,
        },
        "issues": issues,
        "non_blocking_notes": [
            "S5 is a bundled public-core runtime first slice, not a final installer runtime.",
            "S5 still uses the current Python environment.",
            "S5 does not implement the Tauri UI or installer packaging yet.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    zephyr_dev = _as_dict(report["zephyr_dev"])
    bundle = _as_dict(report["bundle"])
    adapter = _as_dict(report["adapter"])
    boundary = _as_dict(report["boundary"])
    lines = [
        "# P6-M3-S5 Base bundled public-core runtime handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        f"- status: {summary['m3_s5_status']}",
        "",
        "## What S5 completed",
        "- Zephyr-dev public-core bundle policy and manifest",
        "- Zephyr-dev bundle export tool",
        "- Zephyr-base runtime/public-core-bundle import",
        "- Zephyr-base bundled adapter mode",
        "- Zephyr-base bundled adapter smoke",
        "",
        "## Bundle export",
        f"- adapter_commit_sha: {zephyr_dev['adapter_commit_sha']}",
        f"- bundle_exported: {zephyr_dev['bundle_exported']}",
        (
            "- bundle_generation_requires_zephyr_dev_working_tree: "
            f"{zephyr_dev['bundle_generation_requires_zephyr_dev_working_tree']}"
        ),
        "",
        "## Bundled adapter mode",
        f"- bundled_mode_supported: {adapter['bundled_mode_supported']}",
        f"- dev_mode_still_supported: {adapter['dev_mode_still_supported']}",
        f"- bundled_runtime_used: {adapter['bundled_runtime_used']}",
        "",
        "## Dev mode vs bundled mode",
        "- dev mode still requires a local Zephyr-dev working tree",
        "- bundled mode does not require a Zephyr-dev working tree at execution",
        "- S5 pass does not use fixture mode or --zephyr-dev-root execution",
        "",
        "## Current limitations",
        f"- uses_current_python_environment: {bundle['uses_current_python_environment']}",
        f"- embedded_python_runtime: {bundle['embedded_python_runtime']}",
        f"- wheelhouse_bundled: {bundle['wheelhouse_bundled']}",
        f"- installer_runtime_complete: {bundle['installer_runtime_complete']}",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- blocked_count: {boundary['blocked_count']}",
        f"- review_required_count: {boundary['review_required_count']}",
        "",
        "## What S5 did not do",
        "- did not implement a Tauri UI",
        "- did not build an installer",
        "- did not create a release",
        "- did not add cloud, Pro, or Web-core dependencies",
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
    parser = argparse.ArgumentParser(description="Build the P6-M3-S5 Base bundle handoff.")
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
    out_root = args.out_root if args.out_root.is_absolute() else (root / args.out_root).resolve()
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
