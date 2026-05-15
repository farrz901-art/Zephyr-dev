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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s6_tauri_bridge_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "1805a1d4f973ba635029b0c6837f406ee4314c3d"


class SummaryDict(TypedDict):
    overall: str
    m3_s6_status: str
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
    status_path = base_root / ".tmp/s6_repo_status.json"
    status_file = _load_json_object(status_path) if status_path.exists() else None
    current_head = _git_output(base_root, "rev-parse", "HEAD")
    origin_head = _git_output(base_root, "rev-parse", "origin/main")
    repo_url = _git_output(base_root, "remote", "get-url", "origin")
    if current_head is None:
        if status_file is not None:
            return {
                "previous_head_sha": status_file.get("previous_head_sha", PREVIOUS_HEAD_SHA),
                "current_head_sha": status_file.get("current_head_sha", "unknown"),
                "pushed": status_file.get("pushed", False),
                "repo_url": status_file.get("repo_url", ""),
            }
        return {
            "previous_head_sha": PREVIOUS_HEAD_SHA,
            "current_head_sha": "unknown",
            "pushed": False,
            "repo_url": "",
        }
    return {
        "previous_head_sha": PREVIOUS_HEAD_SHA,
        "current_head_sha": current_head,
        "pushed": origin_head == current_head and origin_head is not None,
        "repo_url": repo_url or "",
    }


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _required_paths(base_root: Path) -> dict[str, Path]:
    return {
        "tauri_doc": base_root / "docs/TAURI_COMMAND_BRIDGE.md",
        "cargo_toml": base_root / "src-tauri/Cargo.toml",
        "tauri_conf": base_root / "src-tauri/tauri.conf.json",
        "main_rs": base_root / "src-tauri/src/main.rs",
        "commands_rs": base_root / "src-tauri/src/commands.rs",
        "bridge_rs": base_root / "src-tauri/src/bridge.rs",
        "errors_rs": base_root / "src-tauri/src/errors.rs",
        "lineage_rs": base_root / "src-tauri/src/lineage.rs",
        "tauri_check": base_root / ".tmp/tauri_command_bridge_check.json",
        "boundary_report": base_root / ".tmp/base_boundary_check.json",
        "bundle_audit": base_root / ".tmp/public_core_bundle_surface_audit.json",
        "unsupported_report": base_root / ".tmp/bundle_unsupported_surface_check.json",
        "bundled_report": base_root / ".tmp/bundled_adapter_flow_check.json",
        "workflow": base_root / ".github/workflows/boundary.yml",
        "bundle_manifest": (
            base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json"
        ),
    }


def _load_local_validation(base_root: Path) -> dict[str, object] | None:
    validation_path = base_root / ".tmp/s6_local_validation.json"
    if not validation_path.exists():
        return None
    return _load_json_object(validation_path)


def build_report(*, root: Path, base_root: Path) -> dict[str, object]:
    required = _required_paths(base_root)
    missing = [
        path.relative_to(base_root).as_posix()
        for path in required.values()
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError("; ".join(missing))

    tauri_check = _load_json_object(required["tauri_check"])
    boundary_report = _load_json_object(required["boundary_report"])
    bundle_audit = _load_json_object(required["bundle_audit"])
    unsupported_report = _load_json_object(required["unsupported_report"])
    bundled_report = _load_json_object(required["bundled_report"])
    bundle_manifest = _load_json_object(required["bundle_manifest"])
    workflow_text = _read_text(required["workflow"])
    repo_status = _repo_status(base_root)
    local_validation = _load_local_validation(base_root)

    tauri_summary = _as_dict(tauri_check["summary"])
    tauri_bridge = _as_dict(tauri_check["bridge"])
    boundary_summary = _as_dict(boundary_report["summary"])
    audit_summary = _as_dict(bundle_audit["summary"])
    unsupported_summary = _as_dict(unsupported_report["summary"])
    bundled_summary = _as_dict(bundled_report["summary"])

    issues: list[str] = []
    if _as_bool(boundary_summary["pass"]) is False:
        issues.append("Zephyr-base boundary check did not pass.")
    if int(boundary_summary["blocked_count"]) != 0:
        issues.append("Zephyr-base boundary check still has blockers.")
    if audit_summary["overall"] != "pass":
        issues.append("Bundle surface audit did not pass.")
    if _as_bool(unsupported_summary["pass"]) is False:
        issues.append("Unsupported surface negative smoke did not pass.")
    if _as_bool(bundled_summary["pass"]) is False:
        issues.append("Bundled adapter smoke did not pass.")
    if _as_bool(tauri_bridge["commands_exist"]) is False:
        issues.append("Rust/Tauri commands were not detected.")
    if _as_bool(tauri_bridge["bundled_adapter_invocation_detected"]) is False:
        issues.append("Rust/Tauri bundled adapter invocation was not detected.")
    if _as_bool(tauri_bridge["uses_zephyr_dev_root"]):
        issues.append("Rust/Tauri bridge still references a Zephyr-dev root.")
    if _as_bool(tauri_bridge["fixture_fallback_used"]):
        issues.append("Rust/Tauri bridge still references fixture fallback.")
    cargo_available = _as_bool(tauri_summary["cargo_available"])
    cargo_check_pass = _as_bool(tauri_summary["cargo_check_pass"])
    static_bridge_check_pass = _as_bool(tauri_summary["static_bridge_check_pass"])
    commercial_terms_blocked = int(tauri_summary["commercial_terms_blocked"])
    local_validation_source = ""
    cargo_version = ""
    non_blocking_warnings: list[object] = []
    if local_validation is not None:
        cargo_available = bool(local_validation.get("cargo_available", cargo_available))
        cargo_check_pass = bool(local_validation.get("cargo_check_pass", cargo_check_pass))
        static_bridge_check_pass = bool(
            local_validation.get("static_bridge_check_pass", static_bridge_check_pass)
        )
        commercial_terms_blocked = int(
            local_validation.get("commercial_terms_blocked", commercial_terms_blocked)
        )
        local_validation_source = str(local_validation.get("local_validation_source", ""))
        cargo_version = str(local_validation.get("cargo_version", ""))
        raw_warnings = local_validation.get("non_blocking_warnings", [])
        if isinstance(raw_warnings, list):
            non_blocking_warnings = raw_warnings
    if commercial_terms_blocked != 0:
        issues.append("Rust/Tauri bridge still contains blocked commercial terms.")
    pushed = (
        _as_bool(repo_status["pushed"])
        if isinstance(repo_status.get("pushed"), bool)
        else False
    )

    active_blockers = len(issues)
    if active_blockers > 0 or static_bridge_check_pass is False:
        overall = "fail"
        status = "open"
    elif cargo_available is False or cargo_check_pass is False or pushed is False:
        overall = "conditional"
        status = "conditional"
    else:
        overall = "pass"
        status = "sealed"

    non_blocking_notes = [
        (
            "S6 moves Base from Python-script-only invocation to a Rust/Tauri "
            "command bridge first slice."
        ),
        "The bundled adapter still uses the current Python environment.",
        "No production UI, installer, or embedded Python runtime is implemented in S6.",
    ]
    if cargo_available is False:
        non_blocking_notes.append(
            "cargo is not available in the current environment, so local cargo check was skipped."
        )
    elif cargo_check_pass is False:
        non_blocking_notes.append("cargo is available but local cargo check did not pass.")
    if pushed is False:
        non_blocking_notes.append("Zephyr-base local HEAD is not confirmed on origin/main.")

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s6.tauri_bridge_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s6_status": status,
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": pushed,
            "repo_url": repo_status["repo_url"],
        },
        "tauri_bridge": {
            "src_tauri_exists": True,
            "commands_exist": tauri_bridge["commands_exist"],
            "run_local_file_command": tauri_bridge["run_local_file_command"],
            "run_local_text_command": tauri_bridge["run_local_text_command"],
            "read_run_result_command": tauri_bridge["read_run_result_command"],
            "bundled_adapter_invocation": tauri_bridge["bundled_adapter_invocation_detected"],
            "uses_zephyr_dev_root": tauri_bridge["uses_zephyr_dev_root"],
            "fixture_fallback_used": tauri_bridge["fixture_fallback_used"],
            "cargo_check_pass": cargo_check_pass,
            "static_bridge_check_pass": static_bridge_check_pass,
        },
        "s6_seal_update": {
            "cargo_available": cargo_available,
            "cargo_check_pass": cargo_check_pass,
            "static_bridge_check_pass": static_bridge_check_pass,
            "commercial_terms_blocked": commercial_terms_blocked,
            "local_validation_source": local_validation_source,
            "cargo_version": cargo_version,
            "non_blocking_warnings": non_blocking_warnings,
        },
        "runtime": {
            "uses_bundled_adapter": True,
            "bundle_execution_requires_zephyr_dev_working_tree": False,
            "uses_current_python_environment": True,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "installer_runtime_complete": False,
        },
        "boundary": {
            "boundary_check_pass": boundary_summary["pass"],
            "commercial_terms_blocked": commercial_terms_blocked,
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
            "requires_p45_substrate": bundle_manifest["requires_p45_substrate"],
            "requires_network": bundle_manifest["requires_network"],
        },
        "scope": {
            "production_ui_implemented": False,
            "installer_built": False,
            "release_created": False,
            "cloud_core_used": False,
        },
        "ci": {
            "workflow_updated": True,
            "runs_tauri_command_bridge_check": (
                "check_tauri_command_bridge.py --json" in workflow_text
            ),
            "runs_tauri_build": False,
            "requires_secrets": False,
        },
        "issues": issues,
        "non_blocking_notes": non_blocking_notes,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    tauri_bridge = _as_dict(report["tauri_bridge"])
    s6_seal_update = _as_dict(report["s6_seal_update"])
    runtime = _as_dict(report["runtime"])
    boundary = _as_dict(report["boundary"])
    notes = cast(list[object], report["non_blocking_notes"])
    lines = [
        "# P6-M3-S6 Tauri command bridge handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- status: {summary['m3_s6_status']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        "",
        "## S6 seal update",
        f"- cargo_available: {s6_seal_update['cargo_available']}",
        f"- cargo_check_pass: {s6_seal_update['cargo_check_pass']}",
        f"- static_bridge_check_pass: {s6_seal_update['static_bridge_check_pass']}",
        f"- commercial_terms_blocked: {s6_seal_update['commercial_terms_blocked']}",
        f"- local_validation_source: {s6_seal_update['local_validation_source']}",
        f"- cargo_version: {s6_seal_update['cargo_version']}",
        "",
        "## What S6 completed",
        (
            "- turned `src-tauri` from placeholder-only files into a real Rust "
            "command-bridge first slice"
        ),
        (
            "- added Rust commands for `run_local_file`, `run_local_text`, "
            "`read_run_result`, and `open_output_folder_plan`"
        ),
        (
            "- wired Rust bridge resolution to the bundled Python adapter "
            "rather than a Zephyr-dev root"
        ),
        "- added static Tauri bridge validation and CI coverage",
        "",
        "## Rust/Tauri command bridge",
        f"- commands_exist: {tauri_bridge['commands_exist']}",
        f"- bundled_adapter_invocation: {tauri_bridge['bundled_adapter_invocation']}",
        f"- uses_zephyr_dev_root: {tauri_bridge['uses_zephyr_dev_root']}",
        f"- fixture_fallback_used: {tauri_bridge['fixture_fallback_used']}",
        f"- cargo_check_pass: {tauri_bridge['cargo_check_pass']}",
        f"- static_bridge_check_pass: {tauri_bridge['static_bridge_check_pass']}",
        "",
        "## Bundled adapter invocation",
        f"- uses_bundled_adapter: {runtime['uses_bundled_adapter']}",
        (
            "- bundle_execution_requires_zephyr_dev_working_tree: "
            f"{runtime['bundle_execution_requires_zephyr_dev_working_tree']}"
        ),
        f"- uses_current_python_environment: {runtime['uses_current_python_environment']}",
        f"- installer_runtime_complete: {runtime['installer_runtime_complete']}",
        "",
        "## Runtime limitations",
        "- S6 still depends on the current Python environment.",
        "- S6 is not an embedded-Python or wheelhouse bundle.",
        "- S6 is not a production UI or installer build.",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- commercial_terms_blocked: {boundary['commercial_terms_blocked']}",
        f"- requires_p45_substrate: {boundary['requires_p45_substrate']}",
        f"- requires_network: {boundary['requires_network']}",
        "",
        "## What S6 did not do",
        "- did not implement a production UI shell",
        "- did not build an installer",
        "- did not create a release artifact",
        "- did not add cloud or commercial runtime logic",
        "",
        "## Next step",
        "- P6-M3-S7: Web UI shell consumes command bridge result artifacts.",
        "",
        "## Notes",
    ]
    for note in notes:
        lines.append(f"- {note}")
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
    parser = argparse.ArgumentParser(description="Build the P6-M3-S6 Tauri bridge handoff.")
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
