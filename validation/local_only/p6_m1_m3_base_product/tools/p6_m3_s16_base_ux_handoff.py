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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s16_base_ux_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "e62d301a84d2379ffffd9c1e9a4191ebf274a876"


class SummaryDict(TypedDict):
    overall: str
    m3_s16_status: str
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
    loaded: object = json.loads(_read_text(path))
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return cast(dict[str, object], loaded)


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected dict, got {type(value).__name__}")
    return cast(dict[str, object], value)


def _as_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected bool, got {type(value).__name__}")
    return value


def _git_output(repo: Path, *args: str) -> str | None:
    completed = subprocess.run(
        ["git", "-c", f"safe.directory={repo}", "-C", str(repo), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _repo_status(base_root: Path) -> dict[str, object]:
    status_path = base_root / ".tmp/s16_repo_status.json"
    if status_path.exists():
        return _load_json_object(status_path)
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
        "ux_shell": base_root / ".tmp/base_ux_shell_check.json",
        "ui_shell": base_root / ".tmp/ui_shell_check.json",
        "ui_build": base_root / ".tmp/ui_build_check.json",
        "ui_lifecycle": base_root / ".tmp/ui_result_lifecycle_check.json",
        "payload_shape": base_root / ".tmp/tauri_invoke_payload_shape_check.json",
        "tauri_app_path": base_root / ".tmp/tauri_app_path_check.json",
        "package_report": base_root / ".tmp/windows_installer_package_report.json",
        "package_audit": base_root / ".tmp/windows_installer_package_audit.json",
        "install_smoke": base_root / ".tmp/windows_install_smoke_report.json",
        "installer_baseline": base_root / ".tmp/windows_installer_baseline_check.json",
        "offline_runtime_baseline": base_root / ".tmp/offline_runtime_baseline_check.json",
        "network_guard": base_root / ".tmp/offline_install_network_guard.json",
        "managed_runtime_flow": base_root / ".tmp/managed_runtime_flow_check.json",
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
    ux_shell_report = _load_json_object(required["ux_shell"])
    ui_shell_report = _load_json_object(required["ui_shell"])
    ui_build_report = _load_json_object(required["ui_build"])
    ui_lifecycle_report = _load_json_object(required["ui_lifecycle"])
    payload_shape_report = _load_json_object(required["payload_shape"])
    tauri_app_path_report = _load_json_object(required["tauri_app_path"])
    package_report = _load_json_object(required["package_report"])
    package_audit_report = _load_json_object(required["package_audit"])
    install_smoke_report = _load_json_object(required["install_smoke"])
    installer_baseline_report = _load_json_object(required["installer_baseline"])
    offline_runtime_baseline_report = _load_json_object(required["offline_runtime_baseline"])
    network_guard_report = _load_json_object(required["network_guard"])
    managed_runtime_flow_report = _load_json_object(required["managed_runtime_flow"])
    repo_status = _repo_status(base_root)

    no_bom_summary = _as_dict(no_bom_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    ux_summary = _as_dict(ux_shell_report["summary"])
    ui_shell_summary = _as_dict(ui_shell_report["summary"])
    ui_build_summary = _as_dict(ui_build_report["summary"])
    ui_lifecycle_summary = _as_dict(ui_lifecycle_report["summary"])
    payload_shape_summary = _as_dict(payload_shape_report["summary"])
    tauri_app_path_summary = _as_dict(tauri_app_path_report["summary"])
    package_summary = _as_dict(package_report["summary"])
    package_audit_summary = _as_dict(package_audit_report["summary"])
    install_smoke_summary = _as_dict(install_smoke_report["summary"])
    installer_baseline_summary = _as_dict(installer_baseline_report["summary"])
    offline_runtime_baseline_summary = _as_dict(offline_runtime_baseline_report["summary"])
    network_guard_summary = _as_dict(network_guard_report["summary"])
    managed_runtime_flow_summary = _as_dict(managed_runtime_flow_report["summary"])

    issues: list[str] = []
    if not _as_bool(no_bom_summary["pass"]):
        issues.append("No-BOM check did not pass.")
    if not _as_bool(boundary_summary["pass"]):
        issues.append("Boundary check did not pass.")
    if not _as_bool(ux_summary["pass"]):
        issues.append("Base UX shell check did not pass.")
    if not _as_bool(ui_shell_summary["pass"]):
        issues.append("UI shell check did not pass.")
    if not _as_bool(ui_build_summary["pass"]):
        issues.append("UI build did not pass.")
    if not _as_bool(ui_lifecycle_summary["pass"]):
        issues.append("UI result lifecycle check did not pass.")
    if not _as_bool(payload_shape_summary["pass"]):
        issues.append("Tauri invoke payload shape check did not pass.")
    if not _as_bool(tauri_app_path_summary["pass"]):
        issues.append("Tauri app path regression did not pass.")
    if not _as_bool(package_summary["pass"]):
        issues.append("Windows installer package regression did not pass.")
    if not _as_bool(package_audit_summary["pass"]):
        issues.append("Windows installer package audit did not pass.")
    if not _as_bool(install_smoke_summary["pass"]):
        issues.append("Windows install smoke did not pass.")
    if not _as_bool(installer_baseline_summary["pass"]):
        issues.append("Windows installer baseline check did not pass.")
    if not _as_bool(offline_runtime_baseline_summary["pass"]):
        issues.append("Offline runtime baseline regression did not pass.")
    if not _as_bool(network_guard_summary["pass"]):
        issues.append("Offline install network guard regression did not pass.")
    if not _as_bool(managed_runtime_flow_summary["pass"]):
        issues.append("Managed runtime flow regression did not pass.")
    if _as_bool(repo_status["pushed"]) is False:
        issues.append("Zephyr-base S16 commit has not been pushed to origin/main.")

    overall = "pass" if not issues else "fail"
    status = "sealed" if not issues else "open"

    wheelhouse = _load_json_object(base_root / ".tmp/wheelhouse_wheel_only_readiness.json")
    non_blocking_notes = [
        "S16 upgrades the user-facing shell, but does not change runtime capability.",
    ]
    if wheelhouse.get("wheel_only_ready") is False:
        non_blocking_notes.append(
            "Wheel-only readiness remains incomplete because "
            "langdetect-1.0.9.tar.gz is still present."
        )

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s16.base_ux_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s16_status": status,
            "active_blockers": len(issues),
            "major_gaps": 0,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": repo_status["pushed"],
        },
        "ux": {
            "bilingual_ui": ux_summary["bilingual_ui"],
            "english_supported": ux_summary["english_supported"],
            "chinese_supported": ux_summary["chinese_supported"],
            "language_toggle_present": ux_summary["language_toggle_present"],
            "primary_run_button_present": ux_summary["primary_run_button_present"],
            "advanced_diagnostics_collapsed_by_default": ux_summary[
                "advanced_diagnostics_collapsed_by_default"
            ],
            "sample_mode_demoted": ux_summary["sample_mode_demoted"],
            "proof_export_demoted": ux_summary["proof_export_demoted"],
            "lineage_demoted": ux_summary["lineage_demoted"],
            "user_friendly_runtime_copy": ux_summary["user_friendly_runtime_copy"],
            "supported_formats_truthful": ux_summary["supported_formats_truthful"],
        },
        "runtime_scope": {
            "runtime_contract_changed": False,
            "supported_formats_changed": False,
            "pdf_claimed": False,
            "docx_claimed": False,
            "image_or_ocr_claimed": False,
            "network_or_cloud_claimed": False,
            "commercial_claimed": False,
        },
        "validation": {
            "ux_shell_check_pass": ux_summary["pass"],
            "ui_build_pass": ui_build_summary["pass"],
            "ui_typecheck_pass": ui_build_summary["typecheck_pass"],
            "ui_result_lifecycle_pass": ui_lifecycle_summary["pass"],
            "tauri_app_path_pass": tauri_app_path_summary["pass"],
            "installer_package_regression_pass": package_summary["pass"]
            and package_audit_summary["pass"],
            "install_smoke_pass": install_smoke_summary["install_smoke_pass"],
        },
        "scope": {
            "signed_installer": False,
            "release_created": False,
            "official_release": False,
            "embedded_python_runtime": False,
            "auto_update": False,
        },
        "boundary": {
            "commercial_terms_blocked": 0,
            "network_runtime_calls_blocked": 0,
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
            "secrets_printed": False,
        },
        "issues": issues,
        "non_blocking_notes": non_blocking_notes,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    ux = _as_dict(report["ux"])
    runtime_scope = _as_dict(report["runtime_scope"])
    validation = _as_dict(report["validation"])
    scope = _as_dict(report["scope"])
    boundary = _as_dict(report["boundary"])
    issues = cast(list[object], report["issues"])
    notes = cast(list[object], report["non_blocking_notes"])

    lines = [
        "# P6-M3-S16 Base UX handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s16_status: {summary['m3_s16_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## UX shell",
        f"- bilingual_ui: {ux['bilingual_ui']}",
        f"- english_supported: {ux['english_supported']}",
        f"- chinese_supported: {ux['chinese_supported']}",
        f"- language_toggle_present: {ux['language_toggle_present']}",
        f"- primary_run_button_present: {ux['primary_run_button_present']}",
        (
            "- advanced_diagnostics_collapsed_by_default: "
            f"{ux['advanced_diagnostics_collapsed_by_default']}"
        ),
        f"- sample_mode_demoted: {ux['sample_mode_demoted']}",
        f"- proof_export_demoted: {ux['proof_export_demoted']}",
        f"- lineage_demoted: {ux['lineage_demoted']}",
        f"- user_friendly_runtime_copy: {ux['user_friendly_runtime_copy']}",
        f"- supported_formats_truthful: {ux['supported_formats_truthful']}",
        "",
        "## Runtime scope",
        f"- runtime_contract_changed: {runtime_scope['runtime_contract_changed']}",
        f"- supported_formats_changed: {runtime_scope['supported_formats_changed']}",
        f"- pdf_claimed: {runtime_scope['pdf_claimed']}",
        f"- docx_claimed: {runtime_scope['docx_claimed']}",
        f"- image_or_ocr_claimed: {runtime_scope['image_or_ocr_claimed']}",
        f"- network_or_cloud_claimed: {runtime_scope['network_or_cloud_claimed']}",
        f"- commercial_claimed: {runtime_scope['commercial_claimed']}",
        "",
        "## Validation",
        f"- ux_shell_check_pass: {validation['ux_shell_check_pass']}",
        f"- ui_build_pass: {validation['ui_build_pass']}",
        f"- ui_typecheck_pass: {validation['ui_typecheck_pass']}",
        f"- ui_result_lifecycle_pass: {validation['ui_result_lifecycle_pass']}",
        f"- tauri_app_path_pass: {validation['tauri_app_path_pass']}",
        f"- installer_package_regression_pass: {validation['installer_package_regression_pass']}",
        f"- install_smoke_pass: {validation['install_smoke_pass']}",
        "",
        "## Scope",
        f"- signed_installer: {scope['signed_installer']}",
        f"- release_created: {scope['release_created']}",
        f"- official_release: {scope['official_release']}",
        f"- embedded_python_runtime: {scope['embedded_python_runtime']}",
        f"- auto_update: {scope['auto_update']}",
        "",
        "## Boundary",
        f"- commercial_terms_blocked: {boundary['commercial_terms_blocked']}",
        f"- network_runtime_calls_blocked: {boundary['network_runtime_calls_blocked']}",
        "",
        "## Next step",
        (
            "- Continue toward signed installer and wheel-only runtime hardening "
            "without changing Base capability scope."
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
    parser = argparse.ArgumentParser(description="Build the P6-M3-S16 Base UX handoff.")
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
