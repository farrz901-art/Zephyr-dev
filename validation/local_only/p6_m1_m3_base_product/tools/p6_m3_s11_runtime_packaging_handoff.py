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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s11_runtime_packaging_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "3ff61babac58b15e8d4ba7fc34be2f68a350f909"


class SummaryDict(TypedDict):
    overall: str
    m3_s11_status: str
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
    status_path = base_root / ".tmp/s11_repo_status.json"
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
        "runtime_baseline": base_root / ".tmp/runtime_packaging_baseline_check.json",
        "python_runtime": base_root / ".tmp/python_runtime_dependencies_check.json",
        "bootstrap": base_root / ".tmp/base_runtime_bootstrap.json",
        "managed_flow": base_root / ".tmp/managed_runtime_flow_check.json",
        "payload_shape": base_root / ".tmp/tauri_invoke_payload_shape_check.json",
        "ui_build": base_root / ".tmp/ui_build_check.json",
        "tauri_app_path": base_root / ".tmp/tauri_app_path_check.json",
        "runtime_manifest": base_root / "runtime/python-runtime/runtime_manifest.json",
        "runtime_policy": base_root / "docs/PACKAGED_RUNTIME_BASELINE.md",
        "requirements_in": base_root / "runtime/python-runtime/base-runtime-requirements.in",
        "requirements_txt": base_root / "runtime/python-runtime/base-runtime-requirements.txt",
        "runtime_readme": base_root / "runtime/python-runtime/README.md",
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
    runtime_baseline_report = _load_json_object(required["runtime_baseline"])
    python_runtime_report = _load_json_object(required["python_runtime"])
    bootstrap_report = _load_json_object(required["bootstrap"])
    managed_flow_report = _load_json_object(required["managed_flow"])
    payload_shape_report = _load_json_object(required["payload_shape"])
    ui_build_report = _load_json_object(required["ui_build"])
    tauri_app_path_report = _load_json_object(required["tauri_app_path"])
    runtime_manifest = _load_json_object(required["runtime_manifest"])
    repo_status = _repo_status(base_root)

    no_bom_summary = _as_dict(no_bom_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    runtime_baseline_summary = _as_dict(runtime_baseline_report["summary"])
    python_runtime_summary = _as_dict(python_runtime_report["summary"])
    bootstrap_summary = _as_dict(bootstrap_report["summary"])
    managed_flow_summary = _as_dict(managed_flow_report["summary"])
    payload_shape_summary = _as_dict(payload_shape_report["summary"])
    ui_build_summary = _as_dict(ui_build_report["summary"])
    tauri_app_path_summary = _as_dict(tauri_app_path_report["summary"])
    managed_flow_runtime = _as_dict(managed_flow_report["runtime"])
    managed_flow_evidence = _as_dict(managed_flow_report["evidence"])

    wheelhouse_build_path = base_root / ".tmp/base_runtime_wheelhouse_manifest.json"
    wheelhouse_install_path = base_root / ".tmp/wheelhouse_runtime_install_check.json"
    wheelhouse_build_report = (
        _load_json_object(wheelhouse_build_path) if wheelhouse_build_path.exists() else None
    )
    wheelhouse_install_report = (
        _load_json_object(wheelhouse_install_path) if wheelhouse_install_path.exists() else None
    )

    hard_failures: list[str] = []
    if _as_bool(no_bom_summary["pass"]) is False:
        hard_failures.append("No-BOM hygiene check did not pass.")
    if _as_bool(boundary_summary["pass"]) is False:
        hard_failures.append("Boundary check did not pass.")
    if _as_bool(runtime_baseline_summary["pass"]) is False:
        hard_failures.append("Runtime packaging baseline check did not pass.")
    if _as_bool(python_runtime_summary["pass"]) is False:
        hard_failures.append("Python runtime dependency preflight did not pass.")
    if _as_bool(bootstrap_summary["pass"]) is False:
        hard_failures.append("Managed runtime bootstrap did not pass.")
    if _as_bool(managed_flow_summary["pass"]) is False:
        hard_failures.append("Managed runtime flow did not pass.")
    if _as_bool(payload_shape_summary["pass"]) is False:
        hard_failures.append("Tauri invoke payload shape check did not pass.")
    if _as_bool(ui_build_summary["pass"]) is False:
        hard_failures.append("UI build regression check did not pass.")
    if _as_bool(tauri_app_path_summary["pass"]) is False:
        hard_failures.append("Tauri app path regression check did not pass.")
    if _as_bool(repo_status["pushed"]) is False:
        hard_failures.append("Zephyr-base S11 commit has not been pushed to origin/main.")

    if hard_failures:
        overall = "fail"
        status = "open"
        major_gaps = 0
    else:
        overall = "pass"
        status = "sealed"
        major_gaps = 0

    wheelhouse_build_attempted = wheelhouse_build_report is not None
    wheelhouse_built = False
    if wheelhouse_build_report is not None:
        wheelhouse_built = _as_bool(
            _as_dict(wheelhouse_build_report["summary"])["wheelhouse_built"]
        )

    wheelhouse_install_pass = False
    offline_runtime_install_proven_locally = False
    if wheelhouse_install_report is not None:
        wheelhouse_install_summary = _as_dict(wheelhouse_install_report["summary"])
        wheelhouse_install_pass = _as_bool(wheelhouse_install_summary["pass"])
        offline_runtime_install_proven_locally = _as_bool(
            wheelhouse_install_summary["offline_runtime_install_proven_locally"]
        )

    non_blocking_notes: list[str] = [
        "S11 proves a repo-local managed Python runtime first slice for the Base bundled adapter.",
        (
            "Installer packaging, release creation, and clean-machine runtime proof "
            "remain out of scope."
        ),
    ]
    if wheelhouse_build_attempted and not wheelhouse_built:
        non_blocking_notes.append(
            "Optional wheelhouse preparation was attempted but did not complete locally."
        )

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s11.runtime_packaging_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s11_status": status,
            "active_blockers": len(hard_failures),
            "major_gaps": major_gaps,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": repo_status["pushed"],
        },
        "runtime_packaging": {
            "runtime_manifest_exists": True,
            "requirements_exists": True,
            "bootstrap_script_exists": True,
            "managed_venv_supported": runtime_manifest["managed_venv_supported"],
            "managed_venv_created": bootstrap_summary["managed_venv_created"],
            "managed_runtime_flow_pass": managed_flow_summary["managed_runtime_flow_pass"],
            "selected_python_is_managed_runtime": managed_flow_summary[
                "selected_python_is_managed_runtime"
            ],
            "uses_current_shell_python_for_verified_flow": managed_flow_summary[
                "uses_current_shell_python_for_verified_flow"
            ],
        },
        "runtime_evidence": {
            "marker_found": managed_flow_evidence["marker_found"],
            "run_result_exists": managed_flow_evidence["run_result_exists"],
            "billing_semantics": managed_flow_evidence["billing_semantics"],
            "bundled_runtime_used": managed_flow_evidence["bundled_runtime_used"],
            "fixture_runner_used": managed_flow_evidence["fixture_runner_used"],
            "zephyr_dev_working_tree_required": managed_flow_evidence[
                "zephyr_dev_working_tree_required"
            ],
            "requires_network": managed_flow_evidence["requires_network"],
            "requires_p45_substrate": managed_flow_evidence["requires_p45_substrate"],
        },
        "wheelhouse": {
            "wheelhouse_build_attempted": wheelhouse_build_attempted,
            "wheelhouse_built": wheelhouse_built,
            "wheelhouse_install_pass": wheelhouse_install_pass,
            "offline_runtime_install_proven_locally": offline_runtime_install_proven_locally,
            "wheelhouse_bundled": False,
        },
        "scope": {
            "installer_built": False,
            "release_created": False,
            "embedded_python_runtime": False,
            "installer_runtime_complete": False,
            "clean_machine_runtime_proven": False,
        },
        "boundary": {
            "boundary_check_pass": boundary_summary["pass"],
            "runtime_packaging_baseline_check_pass": runtime_baseline_summary["pass"],
            "commercial_terms_blocked": 0,
            "network_runtime_calls_blocked": 0,
            "venv_committed": runtime_baseline_summary["venv_committed"],
            "wheelhouse_committed": runtime_baseline_summary["wheelhouse_committed"],
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
        },
        "issues": hard_failures,
        "non_blocking_notes": non_blocking_notes,
        "selected_python": {
            "path": python_runtime_report["selected_python"],
            "managed": python_runtime_report["selected_python_is_managed_runtime"],
            "differs_from_current_shell_python": python_runtime_report[
                "selected_python_differs_from_current_shell_python"
            ],
        },
        "regression": {
            "payload_shape_check_pass": payload_shape_summary["pass"],
            "ui_build_check_pass": ui_build_summary["pass"],
            "tauri_app_path_check_pass": tauri_app_path_summary["pass"],
            "selected_python_path": managed_flow_runtime["selected_python_path"],
        },
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    runtime_packaging = _as_dict(report["runtime_packaging"])
    runtime_evidence = _as_dict(report["runtime_evidence"])
    wheelhouse = _as_dict(report["wheelhouse"])
    boundary = _as_dict(report["boundary"])
    scope = _as_dict(report["scope"])
    selected_python = _as_dict(report["selected_python"])
    notes = cast(list[object], report["non_blocking_notes"])
    issues = cast(list[object], report["issues"])

    lines = [
        "# P6-M3-S11 Runtime packaging baseline handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s11_status: {summary['m3_s11_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## What S11 proved",
        "- Zephyr-base can create and verify a repo-local managed Python runtime first slice.",
        "- The Rust/Tauri bridge can invoke the bundled adapter through the managed runtime.",
        "- The verified flow no longer depends on the current shell Python path.",
        "",
        "## Managed Python runtime",
        f"- runtime_manifest_exists: {runtime_packaging['runtime_manifest_exists']}",
        f"- requirements_exists: {runtime_packaging['requirements_exists']}",
        f"- bootstrap_script_exists: {runtime_packaging['bootstrap_script_exists']}",
        f"- managed_venv_created: {runtime_packaging['managed_venv_created']}",
        f"- selected_python: {selected_python['path']}",
        (
            "- selected_python_is_managed_runtime: "
            f"{runtime_packaging['selected_python_is_managed_runtime']}"
        ),
        (
            "- uses_current_shell_python_for_verified_flow: "
            f"{runtime_packaging['uses_current_shell_python_for_verified_flow']}"
        ),
        "",
        "## Runtime dependency manifest",
        "- supported formats remain `.txt`, `.text`, `.log`, `.md`, `.markdown`.",
        "- `installer_runtime_complete = false` remains explicit.",
        "- `embedded_python_runtime = false` remains explicit.",
        "",
        "## Managed runtime flow evidence",
        f"- run_result_exists: {runtime_evidence['run_result_exists']}",
        f"- marker_found: {runtime_evidence['marker_found']}",
        f"- billing_semantics: {runtime_evidence['billing_semantics']}",
        f"- bundled_runtime_used: {runtime_evidence['bundled_runtime_used']}",
        f"- fixture_runner_used: {runtime_evidence['fixture_runner_used']}",
        (
            "- zephyr_dev_working_tree_required: "
            f"{runtime_evidence['zephyr_dev_working_tree_required']}"
        ),
        "",
        "## Wheelhouse status",
        f"- wheelhouse_build_attempted: {wheelhouse['wheelhouse_build_attempted']}",
        f"- wheelhouse_built: {wheelhouse['wheelhouse_built']}",
        f"- wheelhouse_install_pass: {wheelhouse['wheelhouse_install_pass']}",
        (
            "- offline_runtime_install_proven_locally: "
            f"{wheelhouse['offline_runtime_install_proven_locally']}"
        ),
        f"- wheelhouse_bundled: {wheelhouse['wheelhouse_bundled']}",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        (
            "- runtime_packaging_baseline_check_pass: "
            f"{boundary['runtime_packaging_baseline_check_pass']}"
        ),
        f"- venv_committed: {boundary['venv_committed']}",
        f"- wheelhouse_committed: {boundary['wheelhouse_committed']}",
        "",
        "## Current limitations",
        f"- installer_built: {scope['installer_built']}",
        f"- release_created: {scope['release_created']}",
        f"- embedded_python_runtime: {scope['embedded_python_runtime']}",
        f"- installer_runtime_complete: {scope['installer_runtime_complete']}",
        f"- clean_machine_runtime_proven: {scope['clean_machine_runtime_proven']}",
        "",
        "## What S11 did not do",
        "- did not build an installer",
        "- did not create a release",
        "- did not claim a clean-machine runtime",
        "- did not add Web-core, Pro, or commercial logic",
        "",
        "## Next step",
        "- P6-M3-S12: installer/runtime packaging next slice or clean-machine runtime proof path.",
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
        description="Build the P6-M3-S11 runtime packaging baseline handoff."
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
