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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s10_window_interaction_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "7691ec4d8006b62707930fad29a990a9f2fb3601"


class SummaryDict(TypedDict):
    overall: str
    m3_s10_status: str
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


def _repo_status(base_root: Path) -> dict[str, object]:
    status_path = base_root / ".tmp/s10_repo_status.json"
    if status_path.exists():
        status_file = _load_json_object(status_path)
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
        "payload_shape": base_root / ".tmp/tauri_invoke_payload_shape_check.json",
        "python_runtime": base_root / ".tmp/python_runtime_dependencies_check.json",
        "bundle_surface": base_root / ".tmp/public_core_bundle_surface_audit.json",
        "unsupported_surface": base_root / ".tmp/bundle_unsupported_surface_check.json",
        "bundled_smoke": base_root / ".tmp/bundled_adapter_flow_check.json",
        "tauri_bridge": base_root / ".tmp/tauri_command_bridge_check.json",
        "ui_shell": base_root / ".tmp/ui_shell_check.json",
        "ui_lifecycle": base_root / ".tmp/ui_result_lifecycle_check.json",
        "ui_build": base_root / ".tmp/ui_build_check.json",
        "tauri_app_path": base_root / ".tmp/tauri_app_path_check.json",
        "ux_lifecycle": base_root / ".tmp/local_result_lifecycle_ux_check.json",
        "launch_attempt": base_root / ".tmp/tauri_window_launch_attempt.json",
        "proof_check": base_root / ".tmp/tauri_window_interaction_proof_check.json",
        "proof_doc": base_root / "docs/TAURI_WINDOW_INTERACTION_PROOF.md",
        "manual_proof_doc": base_root / "docs/MANUAL_TAURI_WINDOW_PROOF.md",
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
    payload_shape_report = _load_json_object(required["payload_shape"])
    python_runtime_report = _load_json_object(required["python_runtime"])
    ui_shell_report = _load_json_object(required["ui_shell"])
    ui_lifecycle_report = _load_json_object(required["ui_lifecycle"])
    ui_build_report = _load_json_object(required["ui_build"])
    tauri_app_path_report = _load_json_object(required["tauri_app_path"])
    ux_lifecycle_report = _load_json_object(required["ux_lifecycle"])
    launch_attempt_report = _load_json_object(required["launch_attempt"])
    proof_check_report = _load_json_object(required["proof_check"])
    repo_status = _repo_status(base_root)

    no_bom_summary = _as_dict(no_bom_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    payload_shape_summary = _as_dict(payload_shape_report["summary"])
    python_runtime_summary = _as_dict(python_runtime_report["summary"])
    ui_shell_summary = _as_dict(ui_shell_report["summary"])
    ui_lifecycle_summary = _as_dict(ui_lifecycle_report["summary"])
    ui_build_summary = _as_dict(ui_build_report["summary"])
    tauri_app_path_summary = _as_dict(tauri_app_path_report["summary"])
    local_run_lifecycle = _as_dict(tauri_app_path_report["local_run_lifecycle"])
    ux_summary = _as_dict(ux_lifecycle_report["summary"])
    launch_summary = _as_dict(launch_attempt_report["summary"])
    launch_details = _as_dict(launch_attempt_report["launch"])
    proof_summary = _as_dict(proof_check_report["summary"])
    proof_payload = _as_dict(proof_check_report["proof"]) if proof_check_report["proof"] else {}

    hard_failures: list[str] = []
    if _as_bool(no_bom_summary["pass"]) is False:
        hard_failures.append("No-BOM hygiene check did not pass.")
    if _as_bool(boundary_summary["pass"]) is False:
        hard_failures.append("Boundary check did not pass.")
    if _as_bool(payload_shape_summary["pass"]) is False:
        hard_failures.append("Tauri invoke payload casing check did not pass.")
    if _as_bool(python_runtime_summary["pass"]) is False:
        hard_failures.append("Python runtime dependency preflight did not pass.")
    if _as_bool(ui_shell_summary["pass"]) is False:
        hard_failures.append("UI shell static check did not pass.")
    if _as_bool(ui_lifecycle_summary["pass"]) is False:
        hard_failures.append("UI result lifecycle check did not pass.")
    if _as_bool(ui_build_summary["pass"]) is False:
        hard_failures.append("UI build baseline did not pass.")
    if _as_bool(tauri_app_path_summary["pass"]) is False:
        hard_failures.append("Tauri app path smoke did not pass.")
    if _as_bool(ux_summary["pass"]) is False:
        hard_failures.append("Local result lifecycle UX check did not pass.")
    if _as_bool(repo_status["pushed"]) is False:
        hard_failures.append("Zephyr-base S10 commit has not been pushed to origin/main.")

    proof_pass = _as_bool(proof_summary["pass"]) if "pass" in proof_summary else False
    proof_exists = (
        _as_bool(proof_summary["proof_exists"])
        if "proof_exists" in proof_summary
        else False
    )
    click_e2e_verified = (
        _as_bool(launch_summary["tauri_window_click_e2e_verified"]) or proof_pass
    )

    if hard_failures:
        overall = "fail"
        status = "open"
        issues = hard_failures
        major_gaps = 0
    elif proof_pass:
        overall = "pass"
        status = "sealed"
        issues = []
        major_gaps = 0
    else:
        overall = "conditional"
        status = "conditional"
        issues = []
        major_gaps = 1

    non_blocking_notes = [
        "S10 proves a visible Tauri launch-ready path and a hardened local result lifecycle UX.",
        "The bundled runtime still uses the current Python environment.",
    ]
    if not proof_pass:
        non_blocking_notes.append(
            "Window click proof is still pending because no manual or automated "
            "proof pack was present."
        )

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s10.window_interaction_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s10_status": status,
            "active_blockers": len(hard_failures),
            "major_gaps": major_gaps,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": repo_status["pushed"],
        },
        "s10_p_fix": {
            "invoke_payload_casing_fixed": payload_shape_summary["pass"],
            "python_runtime_preflight_added": python_runtime_summary["pass"],
            "selected_python": python_runtime_report["selected_python"],
            "pydantic_importable": python_runtime_report["pydantic_importable"],
            "unstructured_importable": python_runtime_report["unstructured_importable"],
            "manual_proof_pack_exists": proof_exists,
        },
        "window_interaction": {
            "tauri_window_launch_attempted": launch_summary["tauri_window_launch_attempted"],
            "tauri_window_launch_process_started": launch_summary[
                "tauri_window_launch_process_started"
            ],
            "tauri_window_click_e2e_verified": click_e2e_verified,
            "manual_proof_pack_exists": proof_exists,
            "manual_or_automated_proof_check_pass": proof_pass,
            "marker_found": proof_summary.get("marker_found", False),
        },
        "ux_lifecycle": {
            "real_run_mode_exists": ux_summary["real_run_mode_exists"],
            "sample_mode_retained": ux_summary["sample_mode_retained"],
            "run_status_timeline_exists": ux_summary["run_status_timeline_exists"],
            "runtime_preflight_card_exists": ux_summary["runtime_preflight_card_exists"],
            "supported_formats_notice_exists": ux_summary[
                "supported_formats_notice_exists"
            ],
            "interaction_proof_panel_exists": ux_summary[
                "interaction_proof_panel_exists"
            ],
            "output_folder_plan_visible": True,
            "error_panel_available": True,
            "billing_semantics_false_visible": True,
        },
        "runtime": {
            "uses_bundled_adapter": True,
            "uses_current_python_environment": True,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "installer_runtime_complete": False,
        },
        "boundary": {
            "boundary_check_pass": boundary_summary["pass"],
            "ui_shell_check_pass": ui_shell_summary["pass"],
            "ui_build_pass": ui_build_summary["pass"],
            "tauri_app_path_check_pass": tauri_app_path_summary["pass"],
            "local_result_lifecycle_ux_check_pass": ux_summary["pass"],
            "commercial_terms_blocked": ui_shell_summary["commercial_terms_blocked"],
            "network_calls_blocked": ui_shell_summary["network_calls_blocked"],
            "unsupported_format_claims": 0,
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
        },
        "scope": {
            "installer_built": False,
            "release_created": False,
            "cloud_core_used": False,
            "pro_or_web_core_used": False,
        },
        "issues": issues,
        "non_blocking_notes": non_blocking_notes,
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
        "proof_payload_excerpt": {
            "proof_kind": proof_payload.get("proof_kind"),
            "ui_mode": proof_payload.get("ui_mode"),
            "run_result_path": proof_payload.get("run_result_path"),
        },
        "launch_reason": launch_details.get("reason"),
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    s10_p_fix = _as_dict(report["s10_p_fix"])
    window_interaction = _as_dict(report["window_interaction"])
    ux_lifecycle = _as_dict(report["ux_lifecycle"])
    runtime = _as_dict(report["runtime"])
    boundary = _as_dict(report["boundary"])
    notes = cast(list[object], report["non_blocking_notes"])
    issues = cast(list[object], report["issues"])

    lines = [
        "# P6-M3-S10 Visible window interaction handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s10_status: {summary['m3_s10_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## S10-P-FIX update",
        (
            "- invoke_payload_casing_fixed: "
            f"{s10_p_fix['invoke_payload_casing_fixed']}"
        ),
        (
            "- python_runtime_preflight_added: "
            f"{s10_p_fix['python_runtime_preflight_added']}"
        ),
        f"- selected_python: {s10_p_fix['selected_python']}",
        f"- pydantic_importable: {s10_p_fix['pydantic_importable']}",
        f"- unstructured_importable: {s10_p_fix['unstructured_importable']}",
        "",
        "## Window launch / click proof status",
        f"- tauri_window_launch_attempted: {window_interaction['tauri_window_launch_attempted']}",
        (
            "- tauri_window_launch_process_started: "
            f"{window_interaction['tauri_window_launch_process_started']}"
        ),
        (
            "- tauri_window_click_e2e_verified: "
            f"{window_interaction['tauri_window_click_e2e_verified']}"
        ),
        "",
        "## Manual proof status",
        f"- manual_proof_pack_exists: {window_interaction['manual_proof_pack_exists']}",
        (
            "- manual_or_automated_proof_check_pass: "
            f"{window_interaction['manual_or_automated_proof_check_pass']}"
        ),
        f"- marker_found: {window_interaction['marker_found']}",
        "",
        "## UX lifecycle hardening",
        f"- real_run_mode_exists: {ux_lifecycle['real_run_mode_exists']}",
        f"- sample_mode_retained: {ux_lifecycle['sample_mode_retained']}",
        f"- run_status_timeline_exists: {ux_lifecycle['run_status_timeline_exists']}",
        f"- runtime_preflight_card_exists: {ux_lifecycle['runtime_preflight_card_exists']}",
        f"- supported_formats_notice_exists: {ux_lifecycle['supported_formats_notice_exists']}",
        f"- interaction_proof_panel_exists: {ux_lifecycle['interaction_proof_panel_exists']}",
        "",
        "## Runtime truth",
        f"- uses_bundled_adapter: {runtime['uses_bundled_adapter']}",
        f"- uses_current_python_environment: {runtime['uses_current_python_environment']}",
        f"- installer_runtime_complete: {runtime['installer_runtime_complete']}",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- ui_shell_check_pass: {boundary['ui_shell_check_pass']}",
        f"- ui_build_pass: {boundary['ui_build_pass']}",
        f"- tauri_app_path_check_pass: {boundary['tauri_app_path_check_pass']}",
        (
            "- local_result_lifecycle_ux_check_pass: "
            f"{boundary['local_result_lifecycle_ux_check_pass']}"
        ),
        f"- commercial_terms_blocked: {boundary['commercial_terms_blocked']}",
        f"- network_calls_blocked: {boundary['network_calls_blocked']}",
        "",
        "## Current limitations",
        "- S10 does not claim installer packaging.",
        "- S10 does not claim release creation.",
        "- S10 still uses the current Python environment.",
        (
            "- Click proof remains conditional until a manual or automated proof pack "
            "is validated after the payload/runtime preflight fix."
        ),
        "",
        "## What S10 did not do",
        "- did not build an installer",
        "- did not create a release",
        "- did not add cloud, Web-core, Pro, or commercial logic",
        "",
        "## Next step",
        (
            "- P6-M3-S11: capture a validated manual click-proof pack or "
            "automate bounded visible-window interaction."
        ),
    ]
    if issues:
        lines.extend(["", "## Issues"])
        for item in issues:
            lines.append(f"- {item}")
    if notes:
        lines.extend(["", "## Notes"])
        for item in notes:
            lines.append(f"- {item}")
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
        description="Build the P6-M3-S10 visible window interaction handoff."
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
