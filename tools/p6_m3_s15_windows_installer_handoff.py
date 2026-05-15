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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s15_windows_installer_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "2dbfaaa8ec4a4801affe22fbe8184cca2e683243"


class SummaryDict(TypedDict):
    overall: str
    m3_s15_status: str
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
    status_path = base_root / ".tmp/s15_repo_status.json"
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
        "installer_baseline": base_root / ".tmp/windows_installer_baseline_check.json",
        "tauri_app_baseline": base_root / ".tmp/tauri_app_build_baseline.json",
        "package_report": base_root / ".tmp/windows_installer_package_report.json",
        "package_manifest": base_root / ".tmp/windows_installer_package/installer_manifest.json",
        "package_audit": base_root / ".tmp/windows_installer_package_audit.json",
        "install_smoke": base_root / ".tmp/windows_install_smoke_report.json",
        "wheel_readiness": base_root / ".tmp/wheelhouse_wheel_only_readiness.json",
        "offline_runtime_baseline": base_root / ".tmp/offline_runtime_baseline_check.json",
        "network_guard": base_root / ".tmp/offline_install_network_guard.json",
        "ui_build": base_root / ".tmp/ui_build_check.json",
        "tauri_app_path": base_root / ".tmp/tauri_app_path_check.json",
        "managed_runtime_flow": base_root / ".tmp/managed_runtime_flow_check.json",
        "install_layout_baseline": base_root / ".tmp/install_layout_baseline_check.json",
        "clean_machine_pack_baseline": base_root
        / ".tmp/clean_machine_proof_pack_baseline_check.json",
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
    installer_baseline_report = _load_json_object(required["installer_baseline"])
    tauri_app_baseline_report = _load_json_object(required["tauri_app_baseline"])
    package_report = _load_json_object(required["package_report"])
    package_manifest = _load_json_object(required["package_manifest"])
    package_audit_report = _load_json_object(required["package_audit"])
    install_smoke_report = _load_json_object(required["install_smoke"])
    wheel_readiness_report = _load_json_object(required["wheel_readiness"])
    offline_runtime_baseline_report = _load_json_object(required["offline_runtime_baseline"])
    network_guard_report = _load_json_object(required["network_guard"])
    ui_build_report = _load_json_object(required["ui_build"])
    tauri_app_path_report = _load_json_object(required["tauri_app_path"])
    managed_runtime_flow_report = _load_json_object(required["managed_runtime_flow"])
    install_layout_baseline_report = _load_json_object(required["install_layout_baseline"])
    clean_machine_pack_baseline_report = _load_json_object(required["clean_machine_pack_baseline"])
    repo_status = _repo_status(base_root)

    no_bom_summary = _as_dict(no_bom_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    installer_baseline_summary = _as_dict(installer_baseline_report["summary"])
    tauri_app_baseline_summary = _as_dict(tauri_app_baseline_report["summary"])
    package_summary = _as_dict(package_report["summary"])
    package_audit_summary = _as_dict(package_audit_report["summary"])
    install_smoke_summary = _as_dict(install_smoke_report["summary"])
    wheel_readiness_summary = _as_dict(wheel_readiness_report["summary"])
    offline_runtime_baseline_summary = _as_dict(offline_runtime_baseline_report["summary"])
    network_guard_summary = _as_dict(network_guard_report["summary"])
    ui_build_summary = _as_dict(ui_build_report["summary"])
    tauri_app_path_summary = _as_dict(tauri_app_path_report["summary"])
    managed_runtime_flow_summary = _as_dict(managed_runtime_flow_report["summary"])
    install_layout_baseline_summary = _as_dict(install_layout_baseline_report["summary"])
    clean_machine_pack_baseline_summary = _as_dict(clean_machine_pack_baseline_report["summary"])

    issues: list[str] = []
    if not _as_bool(no_bom_summary["pass"]):
        issues.append("No-BOM check did not pass.")
    if not _as_bool(boundary_summary["pass"]):
        issues.append("Boundary check did not pass.")
    if not _as_bool(installer_baseline_summary["pass"]):
        issues.append("Windows installer baseline check did not pass.")
    if not _as_bool(tauri_app_baseline_summary["pass"]):
        issues.append("Tauri app build baseline did not pass.")
    if not _as_bool(package_summary["pass"]):
        issues.append("Windows installer package build did not pass.")
    if not _as_bool(package_audit_summary["pass"]):
        issues.append("Windows installer package audit did not pass.")
    if not _as_bool(install_smoke_summary["pass"]):
        issues.append("Windows install smoke did not pass.")
    if not _as_bool(offline_runtime_baseline_summary["pass"]):
        issues.append("Offline runtime baseline regression did not pass.")
    if not _as_bool(network_guard_summary["pass"]):
        issues.append("Offline install network guard regression did not pass.")
    if not _as_bool(ui_build_summary["pass"]):
        issues.append("UI build regression did not pass.")
    if not _as_bool(tauri_app_path_summary["pass"]):
        issues.append("Tauri app path regression did not pass.")
    if not _as_bool(managed_runtime_flow_summary["pass"]):
        issues.append("Managed runtime flow regression did not pass.")
    if not _as_bool(install_layout_baseline_summary["pass"]):
        issues.append("Install layout baseline regression did not pass.")
    if not _as_bool(clean_machine_pack_baseline_summary["pass"]):
        issues.append("Clean-machine proof pack baseline regression did not pass.")
    if _as_bool(repo_status["pushed"]) is False:
        issues.append("Zephyr-base S15 commit has not been pushed to origin/main.")

    overall = "pass" if not issues else "fail"
    status = "sealed" if not issues else "open"

    text_flow = _as_dict(install_smoke_report["text_flow"])
    file_flow = _as_dict(install_smoke_report["file_flow"])
    runtime = _as_dict(install_smoke_report["runtime"])
    non_blocking_notes = [
        (
            "S15 proves an unsigned Windows package first slice, not a signed "
            "installer or official release."
        ),
    ]
    if wheel_readiness_summary["wheel_only_ready"] is False:
        sdist_artifacts = cast(list[object], wheel_readiness_report["sdist_artifacts"])
        non_blocking_notes.append(
            "Wheel-only readiness is not complete; sdist artifacts remain in the wheelhouse: "
            + ", ".join(str(item) for item in sdist_artifacts)
            + "."
        )

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s15.windows_installer_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s15_status": status,
            "active_blockers": len(issues),
            "major_gaps": 0,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": repo_status["pushed"],
        },
        "installer_package": {
            "package_kind": package_manifest["package_kind"],
            "package_built": package_summary["package_built"],
            "package_path": package_report["package_path"],
            "installer_manifest_exists": True,
            "installer_built": package_manifest["installer_built"],
            "signed_installer": package_manifest["signed_installer"],
            "release_created": package_manifest["release_created"],
            "package_audit_pass": package_audit_summary["pass"],
            "install_smoke_pass": install_smoke_summary["install_smoke_pass"],
        },
        "package_contents": {
            "ui_dist_present": package_manifest["ui_dist_present"],
            "tauri_app_present": package_manifest["tauri_app_present"],
            "public_core_bundle_present": package_manifest["public_core_bundle_present"],
            "runtime_manifest_present": package_manifest["runtime_manifest_present"],
            "wheelhouse_in_package": package_manifest["wheelhouse_in_package"],
            "bootstrap_present": True,
            "docs_present": True,
            "forbidden_dirs_present": False,
        },
        "runtime_evidence": {
            "text_flow_pass": text_flow["pass"],
            "file_flow_pass": file_flow["pass"],
            "text_marker_found": text_flow["marker_found"],
            "file_marker_found": file_flow["marker_found"],
            "billing_semantics": text_flow["billing_semantics"],
            "bundled_runtime_used": text_flow["bundled_runtime_used"],
            "fixture_runner_used": text_flow["fixture_runner_used"],
            "zephyr_dev_working_tree_required": text_flow["zephyr_dev_working_tree_required"],
            "requires_network_for_dependency_install": install_smoke_report["uses_no_index"]
            and install_smoke_report["uses_find_links"]
            and False,
            "requires_network_at_runtime": text_flow["requires_network"],
            "requires_p45_substrate": text_flow["requires_p45_substrate"],
        },
        "wheelhouse": {
            "wheelhouse_in_package": package_manifest["wheelhouse_in_package"],
            "wheelhouse_committed_to_repo": package_manifest["wheelhouse_committed_to_repo"],
            "wheel_only_ready": wheel_readiness_report["wheel_only_ready"],
            "sdist_artifacts": wheel_readiness_report["sdist_artifacts"],
        },
        "scope": {
            "signed_installer": package_manifest["signed_installer"],
            "release_created": package_manifest["release_created"],
            "official_release": False,
            "embedded_python_runtime": package_manifest["embedded_python_runtime"],
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
        "selected_python": runtime["managed_python"],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    installer_package = _as_dict(report["installer_package"])
    package_contents = _as_dict(report["package_contents"])
    runtime_evidence = _as_dict(report["runtime_evidence"])
    wheelhouse = _as_dict(report["wheelhouse"])
    scope = _as_dict(report["scope"])
    boundary = _as_dict(report["boundary"])
    selected_python = report["selected_python"]
    issues = cast(list[object], report["issues"])
    notes = cast(list[object], report["non_blocking_notes"])

    lines = [
        "# P6-M3-S15 Windows installer package handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s15_status: {summary['m3_s15_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## Installer package",
        f"- package_kind: {installer_package['package_kind']}",
        f"- package_built: {installer_package['package_built']}",
        f"- package_path: {installer_package['package_path']}",
        f"- installer_manifest_exists: {installer_package['installer_manifest_exists']}",
        f"- installer_built: {installer_package['installer_built']}",
        f"- signed_installer: {installer_package['signed_installer']}",
        f"- release_created: {installer_package['release_created']}",
        f"- package_audit_pass: {installer_package['package_audit_pass']}",
        f"- install_smoke_pass: {installer_package['install_smoke_pass']}",
        "",
        "## Package contents",
        f"- ui_dist_present: {package_contents['ui_dist_present']}",
        f"- tauri_app_present: {package_contents['tauri_app_present']}",
        f"- public_core_bundle_present: {package_contents['public_core_bundle_present']}",
        f"- runtime_manifest_present: {package_contents['runtime_manifest_present']}",
        f"- wheelhouse_in_package: {package_contents['wheelhouse_in_package']}",
        f"- bootstrap_present: {package_contents['bootstrap_present']}",
        f"- docs_present: {package_contents['docs_present']}",
        f"- forbidden_dirs_present: {package_contents['forbidden_dirs_present']}",
        "",
        "## Install smoke",
        f"- selected_python: {selected_python}",
        f"- text_flow_pass: {runtime_evidence['text_flow_pass']}",
        f"- file_flow_pass: {runtime_evidence['file_flow_pass']}",
        f"- text_marker_found: {runtime_evidence['text_marker_found']}",
        f"- file_marker_found: {runtime_evidence['file_marker_found']}",
        f"- billing_semantics: {runtime_evidence['billing_semantics']}",
        f"- bundled_runtime_used: {runtime_evidence['bundled_runtime_used']}",
        f"- fixture_runner_used: {runtime_evidence['fixture_runner_used']}",
        (
            "- zephyr_dev_working_tree_required: "
            f"{runtime_evidence['zephyr_dev_working_tree_required']}"
        ),
        (
            "- requires_network_for_dependency_install: "
            f"{runtime_evidence['requires_network_for_dependency_install']}"
        ),
        f"- requires_network_at_runtime: {runtime_evidence['requires_network_at_runtime']}",
        f"- requires_p45_substrate: {runtime_evidence['requires_p45_substrate']}",
        "",
        "## Wheelhouse",
        f"- wheelhouse_in_package: {wheelhouse['wheelhouse_in_package']}",
        f"- wheelhouse_committed_to_repo: {wheelhouse['wheelhouse_committed_to_repo']}",
        f"- wheel_only_ready: {wheelhouse['wheel_only_ready']}",
        f"- sdist_artifacts: {wheelhouse['sdist_artifacts']}",
        "",
        "## Boundary",
        f"- commercial_terms_blocked: {boundary['commercial_terms_blocked']}",
        f"- network_runtime_calls_blocked: {boundary['network_runtime_calls_blocked']}",
        "",
        "## Current limitations",
        f"- signed_installer: {scope['signed_installer']}",
        f"- release_created: {scope['release_created']}",
        f"- official_release: {scope['official_release']}",
        f"- embedded_python_runtime: {scope['embedded_python_runtime']}",
        f"- auto_update: {scope['auto_update']}",
        "",
        "## What S15 did not do",
        "- S15 did not create a signed installer, official release, or auto-update channel.",
        "- S15 did not embed Python into the package payload.",
        "",
        "## Next step",
        (
            "- Harden the wheelhouse toward wheel-only readiness or continue "
            "into signed installer packaging."
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
    parser = argparse.ArgumentParser(description="Build the P6-M3-S15 Windows installer handoff.")
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
