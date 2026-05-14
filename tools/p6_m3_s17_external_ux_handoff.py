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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s17_external_ux_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "19387f14b19c3dfaeceeb49268a63dd893dbac0c"


class SummaryDict(TypedDict):
    overall: str
    m3_s17_status: str
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
    status_path = base_root / ".tmp/s17_repo_status.json"
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
        "ux_baseline": base_root / ".tmp/external_package_ux_baseline_check.json",
        "proof_pack": base_root / ".tmp/external_package_ux_proof_pack_report.json",
        "runtime_smoke": base_root / ".tmp/external_package_runtime_smoke_report.json",
        "gui_bootstrap": base_root / ".tmp/external_gui_runtime_bootstrap_check.json",
        "package_report": base_root / ".tmp/windows_installer_package_report.json",
        "package_audit": base_root / ".tmp/windows_installer_package_audit.json",
        "ui_build": base_root / ".tmp/ui_build_check.json",
        "ui_lifecycle": base_root / ".tmp/ui_result_lifecycle_check.json",
        "tauri_app_path": base_root / ".tmp/tauri_app_path_check.json",
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
    ux_baseline_report = _load_json_object(required["ux_baseline"])
    proof_pack_report = _load_json_object(required["proof_pack"])
    runtime_smoke_report = _load_json_object(required["runtime_smoke"])
    gui_bootstrap_report = _load_json_object(required["gui_bootstrap"])
    package_report = _load_json_object(required["package_report"])
    package_audit_report = _load_json_object(required["package_audit"])
    ui_build_report = _load_json_object(required["ui_build"])
    ui_lifecycle_report = _load_json_object(required["ui_lifecycle"])
    tauri_app_path_report = _load_json_object(required["tauri_app_path"])
    repo_status = _repo_status(base_root)

    imported_proof_path = base_root / ".tmp/imported_external_package_ux_proof_report.json"
    imported_proof_report = (
        _load_json_object(imported_proof_path) if imported_proof_path.exists() else {}
    )

    no_bom_summary = _as_dict(no_bom_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    ux_summary = _as_dict(ux_shell_report["summary"])
    ux_baseline_summary = _as_dict(ux_baseline_report["summary"])
    proof_pack_summary = _as_dict(proof_pack_report["summary"])
    runtime_smoke_summary = _as_dict(runtime_smoke_report["summary"])
    gui_bootstrap_summary = _as_dict(gui_bootstrap_report["summary"])
    package_summary = _as_dict(package_report["summary"])
    package_audit_summary = _as_dict(package_audit_report["summary"])
    ui_build_summary = _as_dict(ui_build_report["summary"])
    ui_lifecycle_summary = _as_dict(ui_lifecycle_report["summary"])
    tauri_app_path_summary = _as_dict(tauri_app_path_report["summary"])

    imported_summary = (
        _as_dict(imported_proof_report["summary"])
        if isinstance(imported_proof_report.get("summary"), dict)
        else {}
    )
    validation_report = (
        _as_dict(imported_proof_report["validation_report"])
        if isinstance(imported_proof_report.get("validation_report"), dict)
        else {}
    )
    validation_summary = (
        _as_dict(validation_report["summary"])
        if isinstance(validation_report.get("summary"), dict)
        else {}
    )
    validation_proof = (
        _as_dict(validation_report["proof"])
        if isinstance(validation_report.get("proof"), dict)
        else {}
    )
    validation_ui = (
        _as_dict(validation_proof["ui"])
        if isinstance(validation_proof.get("ui"), dict)
        else {}
    )

    issues: list[str] = []
    if not _as_bool(no_bom_summary["pass"]):
        issues.append("No-BOM check did not pass.")
    if not _as_bool(boundary_summary["pass"]):
        issues.append("Boundary check did not pass.")
    if not _as_bool(ux_summary["pass"]):
        issues.append("Base UX shell regression did not pass.")
    if not _as_bool(ux_baseline_summary["pass"]):
        issues.append("External package UX baseline check did not pass.")
    if not _as_bool(proof_pack_summary["pass"]):
        issues.append("External package UX proof pack build did not pass.")
    if not _as_bool(runtime_smoke_summary["pass"]):
        issues.append("External package runtime smoke did not pass.")
    if not _as_bool(gui_bootstrap_summary["pass"]):
        issues.append("External package GUI runtime bootstrap check did not pass.")
    if not _as_bool(package_summary["pass"]):
        issues.append("Windows installer package regression did not pass.")
    if not _as_bool(package_audit_summary["pass"]):
        issues.append("Windows installer package audit did not pass.")
    if not _as_bool(ui_build_summary["pass"]):
        issues.append("UI build did not pass.")
    if not _as_bool(ui_lifecycle_summary["pass"]):
        issues.append("UI result lifecycle did not pass.")
    if not _as_bool(tauri_app_path_summary["pass"]):
        issues.append("Tauri app path regression did not pass.")
    if _as_bool(repo_status["pushed"]) is False:
        issues.append("Zephyr-base S17 commit has not been pushed to origin/main.")

    manual_gui_proof_imported = imported_summary.get("manual_gui_proof_imported") is True
    manual_gui_proof_pass = imported_summary.get("manual_gui_proof_pass") is True

    if issues:
        overall = "fail"
        status = "open"
    elif manual_gui_proof_pass:
        overall = "pass"
        status = "sealed"
    else:
        overall = "conditional"
        status = "conditional"

    runtime_text_flow = (
        _as_dict(runtime_smoke_report["text_flow"])
        if isinstance(runtime_smoke_report.get("text_flow"), dict)
        else {}
    )
    runtime_file_flow = (
        _as_dict(runtime_smoke_report["file_flow"])
        if isinstance(runtime_smoke_report.get("file_flow"), dict)
        else {}
    )

    non_blocking_notes = []
    if not manual_gui_proof_pass:
        non_blocking_notes.append(
            "Manual GUI proof is still required to seal the external package UX claim."
        )

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s17.external_ux_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s17_status": status,
            "active_blockers": len(issues),
            "major_gaps": 0 if manual_gui_proof_pass else 1,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": repo_status["pushed"],
        },
        "external_package": {
            "package_kind": "portable_zip",
            "package_built": package_summary["package_built"],
            "package_audit_pass": package_audit_summary["pass"],
            "external_runtime_smoke_pass": runtime_smoke_summary["external_runtime_smoke_pass"],
            "external_gui_runtime_bootstrap_pass": gui_bootstrap_summary["pass"],
            "manual_gui_proof_imported": manual_gui_proof_imported,
            "manual_gui_proof_pass": manual_gui_proof_pass,
        },
        "ux": {
            "bilingual_ui": ux_summary["bilingual_ui"],
            "language_toggle_visible": validation_ui.get("language_toggle_visible", False),
            "english_visible": validation_ui.get("english_visible", False),
            "chinese_visible": validation_ui.get("chinese_visible", False),
            "primary_run_button_visible": validation_ui.get("primary_run_button_visible", False),
            "advanced_collapsed_by_default": validation_ui.get(
                "advanced_collapsed_by_default", False
            ),
            "advanced_expanded": validation_ui.get("advanced_expanded", False),
        },
        "runtime_evidence": {
            "text_flow_pass": runtime_text_flow.get("pass") is True,
            "file_flow_pass": runtime_file_flow.get("pass") is True,
            "text_marker_found": runtime_text_flow.get("marker_found") is True,
            "file_marker_found": runtime_file_flow.get("marker_found") is True,
            "billing_semantics": runtime_text_flow.get("billing_semantics"),
            "bundled_runtime_used": runtime_text_flow.get("bundled_runtime_used"),
            "fixture_runner_used": runtime_text_flow.get("fixture_runner_used"),
            "zephyr_dev_working_tree_required": runtime_text_flow.get(
                "zephyr_dev_working_tree_required"
            ),
            "requires_network_for_dependency_install": False,
            "requires_network_at_runtime": runtime_text_flow.get("requires_network"),
            "requires_p45_substrate": runtime_text_flow.get("requires_p45_substrate"),
        },
        "truth_boundary": {
            "pdf_claimed": False,
            "docx_claimed": False,
            "image_or_ocr_claimed": False,
            "cloud_claimed": False,
            "login_claimed": False,
            "license_or_entitlement_claimed": False,
            "payment_or_billing_claimed": False,
        },
        "scope": {
            "signed_installer": False,
            "official_release": False,
            "release_created": False,
            "auto_update": False,
            "runtime_capability_changed": False,
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
        "manual_proof_validation_summary": validation_summary,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    external_package = _as_dict(report["external_package"])
    ux = _as_dict(report["ux"])
    runtime_evidence = _as_dict(report["runtime_evidence"])
    scope = _as_dict(report["scope"])
    issues = cast(list[object], report["issues"])
    notes = cast(list[object], report["non_blocking_notes"])
    if summary["m3_s17_status"] == "sealed":
        next_step = (
            "- S17 external package GUI UX proof is sealed; carry this "
            "evidence forward into M3 closure/readiness."
        )
    else:
        next_step = (
            "- Import a validated manual external GUI proof to seal the "
            "S17 external UX claim."
        )

    lines = [
        "# P6-M3-S17 External package UX handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s17_status: {summary['m3_s17_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## External package",
        f"- package_kind: {external_package['package_kind']}",
        f"- package_built: {external_package['package_built']}",
        f"- package_audit_pass: {external_package['package_audit_pass']}",
        f"- external_runtime_smoke_pass: {external_package['external_runtime_smoke_pass']}",
        "- external_gui_runtime_bootstrap_pass: "
        f"{external_package.get('external_gui_runtime_bootstrap_pass', False)}",
        f"- manual_gui_proof_imported: {external_package['manual_gui_proof_imported']}",
        f"- manual_gui_proof_pass: {external_package['manual_gui_proof_pass']}",
        "",
        "## UX proof status",
        f"- bilingual_ui: {ux['bilingual_ui']}",
        f"- language_toggle_visible: {ux['language_toggle_visible']}",
        f"- english_visible: {ux['english_visible']}",
        f"- chinese_visible: {ux['chinese_visible']}",
        f"- primary_run_button_visible: {ux['primary_run_button_visible']}",
        f"- advanced_collapsed_by_default: {ux['advanced_collapsed_by_default']}",
        f"- advanced_expanded: {ux['advanced_expanded']}",
        "",
        "## Runtime evidence",
        f"- text_flow_pass: {runtime_evidence['text_flow_pass']}",
        f"- file_flow_pass: {runtime_evidence['file_flow_pass']}",
        f"- text_marker_found: {runtime_evidence['text_marker_found']}",
        f"- file_marker_found: {runtime_evidence['file_marker_found']}",
        f"- billing_semantics: {runtime_evidence['billing_semantics']}",
        "",
        "## Scope",
        f"- signed_installer: {scope['signed_installer']}",
        f"- official_release: {scope['official_release']}",
        f"- release_created: {scope['release_created']}",
        f"- auto_update: {scope['auto_update']}",
        "",
        "## Next step",
        next_step,
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
    parser = argparse.ArgumentParser(description="Build the P6-M3-S17 external UX handoff.")
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
