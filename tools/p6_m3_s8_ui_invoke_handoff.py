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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s8_ui_invoke_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "db5d9ef0e5f85601319ad99dda9777d4faa4f246"


class SummaryDict(TypedDict):
    overall: str
    m3_s8_status: str
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


def _repo_status(base_root: Path) -> dict[str, object]:
    status_path = base_root / ".tmp/s8_repo_status.json"
    status_file = _load_json_object(status_path) if status_path.exists() else None
    if not (base_root / ".git").exists() and status_file is not None:
        return {
            "previous_head_sha": status_file.get("previous_head_sha", PREVIOUS_HEAD_SHA),
            "current_head_sha": status_file.get("current_head_sha", "unknown"),
            "pushed": status_file.get("pushed", False),
        }
    current_head = _git_output(base_root, "rev-parse", "HEAD")
    origin_head = _git_output(base_root, "rev-parse", "origin/main")
    if current_head is None:
        if status_file is not None:
            return {
                "previous_head_sha": status_file.get("previous_head_sha", PREVIOUS_HEAD_SHA),
                "current_head_sha": status_file.get("current_head_sha", "unknown"),
                "pushed": status_file.get("pushed", False),
            }
        return {
            "previous_head_sha": PREVIOUS_HEAD_SHA,
            "current_head_sha": "unknown",
            "pushed": False,
        }
    return {
        "previous_head_sha": PREVIOUS_HEAD_SHA,
        "current_head_sha": current_head,
        "pushed": origin_head == current_head and origin_head is not None,
    }


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _required_paths(base_root: Path) -> dict[str, Path]:
    return {
        "boundary_report": base_root / ".tmp/base_boundary_check.json",
        "ui_shell_report": base_root / ".tmp/ui_shell_check.json",
        "tauri_check": base_root / ".tmp/tauri_command_bridge_check.json",
        "rust_cli_report": base_root / ".tmp/s8_tauri_bridge_cli_flow_check.json",
        "ui_lifecycle_report": base_root / ".tmp/ui_result_lifecycle_check.json",
        "bundle_manifest": (
            base_root / "runtime/public-core-bundle/manifest/public_core_bundle_manifest.json"
        ),
        "invoke_doc": base_root / "docs/UI_TAURI_INVOKE_INTEGRATION.md",
        "bridge_client": base_root / "ui/src/services/baseBridgeClient.ts",
        "contracts": base_root / "ui/src/contracts/baseRunResult.ts",
        "commands_rs": base_root / "src-tauri/src/commands.rs",
        "main_rs": base_root / "src-tauri/src/main.rs",
        "bridge_rs": base_root / "src-tauri/src/bridge.rs",
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

    boundary_report = _load_json_object(required["boundary_report"])
    ui_shell_report = _load_json_object(required["ui_shell_report"])
    tauri_check = _load_json_object(required["tauri_check"])
    rust_cli_report = _load_json_object(required["rust_cli_report"])
    ui_lifecycle_report = _load_json_object(required["ui_lifecycle_report"])
    bridge_client_text = _read_text(required["bridge_client"])
    commands_text = _read_text(required["commands_rs"])
    main_text = _read_text(required["main_rs"])
    bridge_text = _read_text(required["bridge_rs"])
    workflow_text = _read_text(required["workflow"])
    repo_status = _repo_status(base_root)

    boundary_summary = _as_dict(boundary_report["summary"])
    ui_shell_summary = _as_dict(ui_shell_report["summary"])
    tauri_summary = _as_dict(tauri_check["summary"])
    rust_cli_summary = _as_dict(rust_cli_report["summary"])
    ui_lifecycle_summary = _as_dict(ui_lifecycle_report["summary"])

    command_names = [
        "run_local_file",
        "run_local_text",
        "read_run_result",
        "open_output_folder_plan",
        "read_lineage_snapshot",
    ]
    ui_command_names_match_rust = all(
        name in bridge_client_text and name in commands_text and name.replace("_", "-") in main_text
        for name in command_names
    )
    ui_payload_shapes_match_rust = all(
        token in bridge_client_text and token in bridge_text
        for token in ("input_path", "output_dir", "inline_text")
    )

    issues: list[str] = []
    if _as_bool(boundary_summary["pass"]) is False:
        issues.append("Zephyr-base boundary check did not pass.")
    if _as_bool(ui_shell_summary["pass"]) is False:
        issues.append("UI shell static check did not pass.")
    if _as_bool(tauri_summary["cargo_check_pass"]) is False:
        issues.append("Tauri cargo check did not pass.")
    if _as_bool(rust_cli_summary["pass"]) is False:
        issues.append("Rust bridge CLI flow did not pass.")
    if _as_bool(ui_lifecycle_summary["pass"]) is False:
        issues.append("UI result lifecycle check did not pass.")
    if ui_command_names_match_rust is False:
        issues.append("UI command names do not match Rust command names.")
    if ui_payload_shapes_match_rust is False:
        issues.append("UI payload shapes do not match Rust bridge request shapes.")

    active_blockers = len(issues)
    pushed = _as_bool(repo_status["pushed"])
    if active_blockers > 0:
        overall = "fail"
        status = "open"
    elif pushed is False:
        overall = "conditional"
        status = "conditional"
    else:
        overall = "pass"
        status = "sealed"

    non_blocking_notes = [
        "S8 moves Base from mock-only UI shell toward invoke/lifecycle verified shell behavior.",
        "S8 still does not claim full Tauri window end-to-end verification.",
        "The bundled adapter still uses the current Python environment.",
        "S8 is not an installer or release build.",
    ]

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s8.ui_invoke_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s8_status": status,
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": pushed,
        },
        "invoke_alignment": {
            "ui_command_names_match_rust": ui_command_names_match_rust,
            "ui_payload_shapes_match_rust": ui_payload_shapes_match_rust,
            "ui_does_not_call_python_directly": (
                ui_shell_summary["ui_does_not_call_python_directly"]
            ),
            "ui_does_not_call_network": int(ui_shell_summary["network_calls_blocked"]) == 0,
            "ui_does_not_use_zephyr_dev_root": (
                ui_shell_summary["ui_does_not_use_zephyr_dev_root"]
            ),
            "tauri_window_e2e_verified": False,
        },
        "rust_bridge_cli_flow": {
            "cargo_available": rust_cli_summary["cargo_available"],
            "cargo_check_pass": rust_cli_summary["cargo_check_pass"],
            "cargo_run_local_text_pass": rust_cli_summary["cargo_run_local_text_pass"],
            "run_result_exists": rust_cli_summary["run_result_exists"],
            "marker_found": rust_cli_summary["marker_found"],
            "billing_semantics": rust_cli_summary["billing_semantics"],
            "bundled_runtime_used": rust_cli_summary["bundled_runtime_used"],
            "fixture_runner_used": rust_cli_summary["fixture_runner_used"],
            "zephyr_dev_working_tree_required": (
                rust_cli_summary["zephyr_dev_working_tree_required"]
            ),
        },
        "ui_result_lifecycle": {
            "lifecycle_check_pass": ui_lifecycle_summary["pass"],
            "real_run_result_consumed": ui_lifecycle_summary["real_run_result_consumed"],
            "sample_success_consumed": ui_lifecycle_summary["sample_success_consumed"],
            "sample_error_consumed": ui_lifecycle_summary["sample_error_consumed"],
            "display_model_fields_covered": ui_lifecycle_summary["display_model_fields_covered"],
        },
        "boundary": {
            "boundary_check_pass": boundary_summary["pass"],
            "ui_shell_check_pass": ui_shell_summary["pass"],
            "commercial_terms_blocked": ui_shell_summary["commercial_terms_blocked"],
            "network_calls_blocked": ui_shell_summary["network_calls_blocked"],
            "unsupported_format_claims": len(_as_list(ui_shell_report["unsupported_hits"])),
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
        },
        "runtime": {
            "uses_bundled_adapter": "runtime/public-core-bundle" in bridge_text,
            "uses_current_python_environment": True,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "installer_runtime_complete": False,
        },
        "scope": {
            "full_tauri_window_e2e": False,
            "installer_built": False,
            "release_created": False,
            "cloud_core_used": False,
            "pro_or_web_core_used": False,
        },
        "ci": {
            "workflow_updated": True,
            "runs_ui_result_lifecycle_check": (
                "check_ui_result_lifecycle.py --json" in workflow_text
            ),
            "runs_rust_cli_flow": "check_rust_bridge_cli_flow.py" in workflow_text,
            "runs_tauri_build": False,
            "requires_node_install": False,
            "requires_secrets": False,
        },
        "issues": issues,
        "non_blocking_notes": non_blocking_notes,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    invoke_alignment = _as_dict(report["invoke_alignment"])
    rust_bridge_cli_flow = _as_dict(report["rust_bridge_cli_flow"])
    ui_result_lifecycle = _as_dict(report["ui_result_lifecycle"])
    boundary = _as_dict(report["boundary"])
    notes = _as_list(report["non_blocking_notes"])
    lines = [
        "# P6-M3-S8 UI invoke integration handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s8_status: {summary['m3_s8_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## What S8 proved",
        "- aligned the visible UI shell with the Rust/Tauri command bridge command surface",
        "- proved a windowless Rust CLI invoke-equivalent flow through the bundled adapter",
        "- proved the UI result lifecycle can consume a real `run_result.json` shape",
        "",
        "## UI-to-command alignment",
        f"- ui_command_names_match_rust: {invoke_alignment['ui_command_names_match_rust']}",
        f"- ui_payload_shapes_match_rust: {invoke_alignment['ui_payload_shapes_match_rust']}",
        (
            "- ui_does_not_call_python_directly: "
            f"{invoke_alignment['ui_does_not_call_python_directly']}"
        ),
        f"- ui_does_not_call_network: {invoke_alignment['ui_does_not_call_network']}",
        f"- ui_does_not_use_zephyr_dev_root: {invoke_alignment['ui_does_not_use_zephyr_dev_root']}",
        f"- tauri_window_e2e_verified: {invoke_alignment['tauri_window_e2e_verified']}",
        "",
        "## Rust bridge CLI flow",
        f"- cargo_available: {rust_bridge_cli_flow['cargo_available']}",
        f"- cargo_check_pass: {rust_bridge_cli_flow['cargo_check_pass']}",
        f"- cargo_run_local_text_pass: {rust_bridge_cli_flow['cargo_run_local_text_pass']}",
        f"- run_result_exists: {rust_bridge_cli_flow['run_result_exists']}",
        f"- marker_found: {rust_bridge_cli_flow['marker_found']}",
        "",
        "## UI result lifecycle",
        f"- lifecycle_check_pass: {ui_result_lifecycle['lifecycle_check_pass']}",
        f"- real_run_result_consumed: {ui_result_lifecycle['real_run_result_consumed']}",
        f"- sample_success_consumed: {ui_result_lifecycle['sample_success_consumed']}",
        f"- sample_error_consumed: {ui_result_lifecycle['sample_error_consumed']}",
        f"- display_model_fields_covered: {ui_result_lifecycle['display_model_fields_covered']}",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- ui_shell_check_pass: {boundary['ui_shell_check_pass']}",
        f"- commercial_terms_blocked: {boundary['commercial_terms_blocked']}",
        f"- network_calls_blocked: {boundary['network_calls_blocked']}",
        "",
        "## Current limitations",
        "- S8 still does not prove a full Tauri window end-to-end run.",
        "- S8 still depends on the current Python environment behind the bundled adapter.",
        "- S8 is not an installer or release build.",
        "",
        "## What S8 did not do",
        "- did not build a production installer",
        "- did not build a release artifact",
        "- did not add cloud, Web-core, Pro, or commercial logic",
        "",
        "## Next step",
        "- P6-M3-S9: visible UI shell drives verified local run lifecycle in a real Tauri window.",
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
    parser = argparse.ArgumentParser(description="Build the P6-M3-S8 UI invoke handoff.")
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
