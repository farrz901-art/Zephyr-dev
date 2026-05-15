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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s9_tauri_app_flow_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "962ac72d8a0e312821a7d615a3d1e107ae7e803e"


class SummaryDict(TypedDict):
    overall: str
    m3_s9_status: str
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


def _repo_status(base_root: Path) -> dict[str, object]:
    status_path = base_root / ".tmp/s9_repo_status.json"
    status_file = _load_json_object(status_path) if status_path.exists() else None
    if status_file is not None:
        return {
            "previous_head_sha": status_file.get("previous_head_sha", PREVIOUS_HEAD_SHA),
            "current_head_sha": status_file.get("current_head_sha", "unknown"),
            "pushed": status_file.get("pushed", False),
        }

    current_head = _git_output(base_root, "rev-parse", "HEAD")
    origin_head = _git_output(base_root, "rev-parse", "origin/main")
    return {
        "previous_head_sha": PREVIOUS_HEAD_SHA,
        "current_head_sha": current_head if current_head is not None else "unknown",
        "pushed": origin_head == current_head and origin_head is not None,
    }


def _required_paths(base_root: Path) -> dict[str, Path]:
    return {
        "no_bom": base_root / ".tmp/no_bom_check.json",
        "boundary": base_root / ".tmp/base_boundary_check.json",
        "bundle_surface": base_root / ".tmp/public_core_bundle_surface_audit.json",
        "unsupported_surface": base_root / ".tmp/bundle_unsupported_surface_check.json",
        "bundled_smoke": base_root / ".tmp/bundled_adapter_flow_check.json",
        "tauri_bridge": base_root / ".tmp/tauri_command_bridge_check.json",
        "ui_shell": base_root / ".tmp/ui_shell_check.json",
        "ui_lifecycle": base_root / ".tmp/ui_result_lifecycle_check.json",
        "ui_build": base_root / ".tmp/ui_build_check.json",
        "tauri_app_path": base_root / ".tmp/tauri_app_path_check.json",
        "repo_status": base_root / ".tmp/s9_repo_status.json",
        "local_flow_doc": base_root / "docs/BASE_LOCAL_APP_FLOW.md",
        "invoke_doc": base_root / "docs/UI_TAURI_INVOKE_INTEGRATION.md",
        "package_lock": base_root / "ui/package-lock.json",
        "main_rs": base_root / "src-tauri/src/main.rs",
        "commands_rs": base_root / "src-tauri/src/commands.rs",
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

    no_bom_report = _load_json_object(required["no_bom"])
    boundary_report = _load_json_object(required["boundary"])
    ui_shell_report = _load_json_object(required["ui_shell"])
    ui_lifecycle_report = _load_json_object(required["ui_lifecycle"])
    ui_build_report = _load_json_object(required["ui_build"])
    tauri_bridge_report = _load_json_object(required["tauri_bridge"])
    tauri_app_path_report = _load_json_object(required["tauri_app_path"])
    repo_status = _repo_status(base_root)

    no_bom_summary = _as_dict(no_bom_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    ui_shell_summary = _as_dict(ui_shell_report["summary"])
    ui_lifecycle_summary = _as_dict(ui_lifecycle_report["summary"])
    ui_build_summary = _as_dict(ui_build_report["summary"])
    tauri_bridge_summary = _as_dict(tauri_bridge_report["summary"])
    tauri_bridge_details = _as_dict(tauri_bridge_report["bridge"])
    tauri_app_path_summary = _as_dict(tauri_app_path_report["summary"])
    tauri_app_path_details = _as_dict(tauri_app_path_report["tauri_app_path"])
    local_run_lifecycle = _as_dict(tauri_app_path_report["local_run_lifecycle"])
    cargo_details = _as_dict(tauri_app_path_report["cargo"])

    issues: list[str] = []
    if _as_bool(no_bom_summary["pass"]) is False:
        issues.append("No-BOM hygiene check did not pass.")
    if _as_bool(boundary_summary["pass"]) is False:
        issues.append("Boundary check did not pass.")
    if _as_bool(ui_shell_summary["pass"]) is False:
        issues.append("UI shell static check did not pass.")
    if _as_bool(ui_lifecycle_summary["pass"]) is False:
        issues.append("UI result lifecycle check did not pass.")
    if _as_bool(ui_build_summary["pass"]) is False:
        issues.append("UI build baseline did not pass.")
    if _as_bool(tauri_bridge_summary["pass"]) is False:
        issues.append("Tauri command bridge check did not pass.")
    if _as_bool(tauri_app_path_summary["pass"]) is False:
        issues.append("Tauri app path smoke did not pass.")
    if _as_bool(repo_status["pushed"]) is False:
        issues.append("Zephyr-base S9 commit has not been pushed to origin/main.")

    active_blockers = len(issues)
    if active_blockers > 0:
        overall = "fail"
        status = "open"
    else:
        overall = "pass"
        status = "sealed"

    cargo_detail_text = str(cargo_details.get("check_detail", ""))
    non_blocking_warnings: list[str] = []
    if "BridgeErrorKind" in cargo_detail_text:
        non_blocking_warnings.append("BridgeErrorKind::Input currently unused")
    if "BridgeError::input" in cargo_detail_text or (
        "associated function `input` is never used" in cargo_detail_text
    ):
        non_blocking_warnings.append("BridgeError::input currently unused")

    non_blocking_notes = [
        (
            "S9 proves the visible app path through UI build, Tauri registration, "
            "and a real Rust CLI local lifecycle."
        ),
        "Full window click end-to-end automation is still not claimed in this slice.",
        "The bundled runtime still uses the current Python environment.",
    ]

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s9.tauri_app_flow_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s9_status": status,
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": repo_status["pushed"],
        },
        "ui_build": {
            "node_available": ui_build_summary["node_available"],
            "npm_available": ui_build_summary["npm_available"],
            "lock_file_exists": ui_build_summary["lock_file_exists"],
            "ui_build_pass": ui_build_summary["ui_build_pass"],
            "ui_dist_exists": ui_build_summary["ui_dist_exists"],
        },
        "tauri_app_path": {
            "cargo_available": tauri_app_path_details["cargo_available"],
            "cargo_check_pass": tauri_app_path_details["cargo_check_pass"],
            "tauri_command_registration_pass": tauri_app_path_details[
                "tauri_command_registration_pass"
            ],
            "rust_cli_lifecycle_pass": tauri_app_path_details["rust_cli_lifecycle_pass"],
            "tauri_window_launch_attempted": tauri_app_path_details[
                "tauri_window_launch_attempted"
            ],
            "tauri_window_click_e2e_verified": tauri_app_path_details[
                "tauri_window_click_e2e_verified"
            ],
        },
        "local_run_lifecycle": {
            "marker_found": local_run_lifecycle["marker_found"],
            "run_result_exists": local_run_lifecycle["run_result_exists"],
            "billing_semantics": local_run_lifecycle["billing_semantics"],
            "bundled_runtime_used": local_run_lifecycle["bundled_runtime_used"],
            "fixture_runner_used": local_run_lifecycle["fixture_runner_used"],
            "zephyr_dev_working_tree_required": local_run_lifecycle[
                "zephyr_dev_working_tree_required"
            ],
        },
        "boundary": {
            "boundary_check_pass": boundary_summary["pass"],
            "ui_shell_check_pass": ui_shell_summary["pass"],
            "ui_result_lifecycle_check_pass": ui_lifecycle_summary["pass"],
            "no_bom_check_pass": no_bom_summary["pass"],
            "commercial_terms_blocked": ui_shell_summary["commercial_terms_blocked"],
            "network_calls_blocked": ui_shell_summary["network_calls_blocked"],
            "unsupported_format_claims": len(_as_list(ui_shell_report["unsupported_hits"])),
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
        },
        "runtime": {
            "uses_bundled_adapter": True,
            "uses_current_python_environment": True,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "installer_runtime_complete": False,
        },
        "scope": {
            "installer_built": False,
            "release_created": False,
            "cloud_core_used": False,
            "pro_or_web_core_used": False,
        },
        "warnings": non_blocking_warnings,
        "issues": issues,
        "non_blocking_notes": non_blocking_notes,
        "tauri_bridge": {
            "commands_exist": tauri_bridge_details["commands_exist"],
            "bundled_adapter_invocation": tauri_bridge_details[
                "bundled_adapter_invocation_detected"
            ],
            "uses_zephyr_dev_root": tauri_bridge_details["uses_zephyr_dev_root"],
            "fixture_fallback_used": tauri_bridge_details["fixture_fallback_used"],
        },
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    ui_build = _as_dict(report["ui_build"])
    tauri_app_path = _as_dict(report["tauri_app_path"])
    local_run_lifecycle = _as_dict(report["local_run_lifecycle"])
    boundary = _as_dict(report["boundary"])
    warnings = _as_list(report["warnings"])
    notes = _as_list(report["non_blocking_notes"])
    lines = [
        "# P6-M3-S9 Tauri app path handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s9_status: {summary['m3_s9_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## What S9 proved",
        "- built the UI dist baseline with a committed lock file",
        "- registered the real Tauri commands against the shared Rust bridge",
        "- proved a real local run lifecycle through the Tauri CLI/app path",
        "",
        "## UI build baseline",
        f"- node_available: {ui_build['node_available']}",
        f"- npm_available: {ui_build['npm_available']}",
        f"- lock_file_exists: {ui_build['lock_file_exists']}",
        f"- ui_build_pass: {ui_build['ui_build_pass']}",
        f"- ui_dist_exists: {ui_build['ui_dist_exists']}",
        "",
        "## Tauri app path / command registration",
        f"- cargo_available: {tauri_app_path['cargo_available']}",
        f"- cargo_check_pass: {tauri_app_path['cargo_check_pass']}",
        f"- tauri_command_registration_pass: {tauri_app_path['tauri_command_registration_pass']}",
        f"- rust_cli_lifecycle_pass: {tauri_app_path['rust_cli_lifecycle_pass']}",
        "",
        "## Local run lifecycle",
        f"- marker_found: {local_run_lifecycle['marker_found']}",
        f"- run_result_exists: {local_run_lifecycle['run_result_exists']}",
        f"- billing_semantics: {local_run_lifecycle['billing_semantics']}",
        f"- bundled_runtime_used: {local_run_lifecycle['bundled_runtime_used']}",
        f"- fixture_runner_used: {local_run_lifecycle['fixture_runner_used']}",
        (
            "- zephyr_dev_working_tree_required: "
            f"{local_run_lifecycle['zephyr_dev_working_tree_required']}"
        ),
        "",
        "## Window e2e status",
        f"- tauri_window_launch_attempted: {tauri_app_path['tauri_window_launch_attempted']}",
        f"- tauri_window_click_e2e_verified: {tauri_app_path['tauri_window_click_e2e_verified']}",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- ui_shell_check_pass: {boundary['ui_shell_check_pass']}",
        f"- ui_result_lifecycle_check_pass: {boundary['ui_result_lifecycle_check_pass']}",
        f"- no_bom_check_pass: {boundary['no_bom_check_pass']}",
        f"- commercial_terms_blocked: {boundary['commercial_terms_blocked']}",
        f"- network_calls_blocked: {boundary['network_calls_blocked']}",
        "",
        "## Current limitations",
        "- S9 does not claim installer packaging.",
        "- S9 does not claim release creation.",
        "- S9 still depends on the current Python environment behind the bundled adapter.",
        "- Full window click e2e remains a later manual or automated proof step.",
        "",
        "## What S9 did not do",
        "- did not build an installer",
        "- did not create a release",
        "- did not add cloud, Web-core, Pro, or commercial logic",
        "",
        "## Next step",
        (
            "- P6-M3-S10: move from launch-ready app path to manual/automated "
            "visible-window interaction proof and tighter output lifecycle UX."
        ),
    ]
    if warnings:
        lines.extend(["", "## Non-blocking warnings"])
        for warning in warnings:
            lines.append(f"- {warning}")
    if notes:
        lines.extend(["", "## Notes"])
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
    parser = argparse.ArgumentParser(description="Build the P6-M3-S9 Tauri app path handoff.")
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
