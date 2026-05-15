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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s14_offline_runtime_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "25c10848606209166f2f39948ff7c864d5085c68"


class SummaryDict(TypedDict):
    overall: str
    m3_s14_status: str
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
    status_path = base_root / ".tmp/s14_repo_status.json"
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
        "offline_runtime_baseline": base_root / ".tmp/offline_runtime_baseline_check.json",
        "wheelhouse_manifest": base_root / ".tmp/base_runtime_wheelhouse_manifest.json",
        "wheelhouse_install": base_root / ".tmp/wheelhouse_runtime_install_check.json",
        "network_guard": base_root / ".tmp/offline_install_network_guard.json",
        "offline_pack_manifest": base_root
        / ".tmp/clean_machine_offline_proof_pack/offline_proof_pack_manifest.json",
        "offline_pack_local_simulation": base_root
        / ".tmp/offline_proof_pack_local_simulation_check.json",
        "runtime_packaging_baseline": base_root / ".tmp/runtime_packaging_baseline_check.json",
        "install_layout_baseline": base_root / ".tmp/install_layout_baseline_check.json",
        "clean_machine_pack_baseline": base_root
        / ".tmp/clean_machine_proof_pack_baseline_check.json",
        "ui_build": base_root / ".tmp/ui_build_check.json",
        "tauri_app_path": base_root / ".tmp/tauri_app_path_check.json",
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
    offline_runtime_baseline_report = _load_json_object(required["offline_runtime_baseline"])
    wheelhouse_manifest = _load_json_object(required["wheelhouse_manifest"])
    wheelhouse_install_report = _load_json_object(required["wheelhouse_install"])
    network_guard_report = _load_json_object(required["network_guard"])
    offline_pack_manifest = _load_json_object(required["offline_pack_manifest"])
    offline_pack_local_simulation_report = _load_json_object(
        required["offline_pack_local_simulation"]
    )
    runtime_packaging_report = _load_json_object(required["runtime_packaging_baseline"])
    install_layout_report = _load_json_object(required["install_layout_baseline"])
    clean_machine_pack_report = _load_json_object(required["clean_machine_pack_baseline"])
    ui_build_report = _load_json_object(required["ui_build"])
    tauri_app_path_report = _load_json_object(required["tauri_app_path"])
    managed_runtime_flow_report = _load_json_object(required["managed_runtime_flow"])
    imported_report_path = base_root / ".tmp/imported_offline_runtime_proof_report.json"
    imported_report = (
        _load_json_object(imported_report_path) if imported_report_path.exists() else None
    )
    repo_status = _repo_status(base_root)

    no_bom_summary = _as_dict(no_bom_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    offline_runtime_baseline_summary = _as_dict(offline_runtime_baseline_report["summary"])
    wheelhouse_install_summary = _as_dict(wheelhouse_install_report["summary"])
    network_guard_summary = _as_dict(network_guard_report["summary"])
    offline_pack_local_simulation_summary = _as_dict(
        offline_pack_local_simulation_report["summary"]
    )
    runtime_packaging_summary = _as_dict(runtime_packaging_report["summary"])
    install_layout_summary = _as_dict(install_layout_report["summary"])
    clean_machine_pack_summary = _as_dict(clean_machine_pack_report["summary"])
    ui_build_summary = _as_dict(ui_build_report["summary"])
    tauri_app_path_summary = _as_dict(tauri_app_path_report["summary"])
    managed_runtime_summary = _as_dict(managed_runtime_flow_report["summary"])

    external_imported = False
    external_pass = False
    if imported_report is not None:
        imported_summary = _as_dict(imported_report["summary"])
        external_imported = _as_bool(imported_summary["external_offline_proof_imported"])
        external_pass = _as_bool(imported_summary["external_offline_proof_pass"])

    issues: list[str] = []
    if not _as_bool(no_bom_summary["pass"]):
        issues.append("No-BOM hygiene check did not pass.")
    if not _as_bool(boundary_summary["pass"]):
        issues.append("Boundary check did not pass.")
    if not _as_bool(offline_runtime_baseline_summary["pass"]):
        issues.append("Offline runtime baseline check did not pass.")
    if wheelhouse_manifest.get("download_pass") is not True:
        issues.append("Wheelhouse build did not pass.")
    if not _as_bool(wheelhouse_install_summary["pass"]):
        issues.append("Offline wheelhouse install check did not pass.")
    if not _as_bool(network_guard_summary["pass"]):
        issues.append("Offline install network guard did not pass.")
    if not _as_bool(offline_pack_local_simulation_summary["pass"]):
        issues.append("Offline proof pack local simulation did not pass.")
    if not _as_bool(runtime_packaging_summary["pass"]):
        issues.append("Runtime packaging baseline regression did not pass.")
    if not _as_bool(install_layout_summary["pass"]):
        issues.append("Install-layout baseline regression did not pass.")
    if not _as_bool(clean_machine_pack_summary["pass"]):
        issues.append("Clean-machine proof-pack baseline regression did not pass.")
    if not _as_bool(ui_build_summary["pass"]):
        issues.append("UI build regression did not pass.")
    if not _as_bool(tauri_app_path_summary["pass"]):
        issues.append("Tauri app path regression did not pass.")
    if not _as_bool(managed_runtime_summary["pass"]):
        issues.append("Managed runtime regression did not pass.")
    if _as_bool(repo_status["pushed"]) is False:
        issues.append("Zephyr-base S14 commit has not been pushed to origin/main.")

    if issues:
        overall = "fail"
        status = "open"
    else:
        overall = "pass"
        status = "sealed"

    text_flow = _as_dict(wheelhouse_install_report["text_flow"])
    file_flow = _as_dict(wheelhouse_install_report["file_flow"])
    non_blocking_notes = [
        "S14 proves a local offline wheelhouse install path, not an installer or embedded Python.",
        (
            "The current offline proof uses --no-index/--find-links and passes, but the "
            "wheelhouse still includes a langdetect sdist build path; future installer/release "
            "work should prefer wheel-only or --only-binary proof."
        ),
    ]
    if not external_pass:
        non_blocking_notes.append(
            "External clean-machine offline proof has not been imported in this slice."
        )

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s14.offline_runtime_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s14_status": status,
            "active_blockers": len(issues),
            "major_gaps": 0,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": repo_status["pushed"],
        },
        "wheelhouse": {
            "wheelhouse_build_attempted": True,
            "wheelhouse_built": wheelhouse_manifest["download_pass"],
            "wheels_count": wheelhouse_manifest["wheels_count"],
            "missing_requirements": wheelhouse_manifest["missing_requirements"],
            "wheelhouse_committed": False,
            "venv_committed": False,
        },
        "offline_install": {
            "offline_install_pass": wheelhouse_install_summary["offline_install_pass"],
            "uses_no_index": wheelhouse_install_summary["uses_no_index"],
            "uses_find_links": wheelhouse_install_summary["uses_find_links"],
            "requires_network_for_dependency_install": wheelhouse_install_summary[
                "requires_network_for_dependency_install"
            ],
            "requires_network_at_runtime": wheelhouse_install_summary[
                "requires_network_at_runtime"
            ],
            "selected_python_is_wheelhouse_venv": wheelhouse_install_report[
                "selected_python_is_wheelhouse_venv"
            ],
        },
        "runtime_evidence": {
            "text_flow_pass": text_flow["pass"],
            "file_flow_pass": file_flow["pass"],
            "text_marker_found": text_flow["marker_found"],
            "file_marker_found": file_flow["marker_found"],
            "billing_semantics": text_flow["billing_semantics"],
            "bundled_runtime_used": text_flow["bundled_runtime_used"],
            "fixture_runner_used": text_flow["fixture_runner_used"],
            "zephyr_dev_working_tree_required": text_flow[
                "zephyr_dev_working_tree_required"
            ],
            "requires_p45_substrate": text_flow["requires_p45_substrate"],
        },
        "offline_proof_pack": {
            "offline_pack_built": True,
            "offline_pack_local_simulation_pass": offline_pack_local_simulation_summary[
                "local_simulation_pass"
            ],
            "external_offline_proof_imported": external_imported,
            "external_offline_proof_pass": external_pass,
        },
        "scope": {
            "installer_built": offline_pack_manifest["installer_built"],
            "release_created": offline_pack_manifest["release_created"],
            "signed_installer": False,
            "embedded_python_runtime": offline_pack_manifest["embedded_python_runtime"],
            "wheelhouse_bundled_in_repo": False,
            "offline_runtime_install_proven_locally": offline_pack_local_simulation_summary[
                "offline_runtime_install_proven_locally"
            ],
            "external_clean_machine_offline_proven": external_pass,
        },
        "boundary": {
            "commercial_terms_blocked": 0,
            "network_runtime_calls_blocked": 0,
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
        },
        "issues": issues,
        "non_blocking_notes": non_blocking_notes,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    wheelhouse = _as_dict(report["wheelhouse"])
    offline_install = _as_dict(report["offline_install"])
    runtime_evidence = _as_dict(report["runtime_evidence"])
    offline_proof_pack = _as_dict(report["offline_proof_pack"])
    scope = _as_dict(report["scope"])
    boundary = _as_dict(report["boundary"])
    issues = cast(list[object], report["issues"])
    notes = cast(list[object], report["non_blocking_notes"])

    lines = [
        "# P6-M3-S14 Offline wheelhouse runtime handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s14_status: {summary['m3_s14_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## What S14 proved",
        (
            "- Zephyr-base can build a local runtime wheelhouse, install from it with "
            "`--no-index --find-links`, and run bundled-adapter local_text/local_file flows."
        ),
        (
            "- Zephyr-base can also package that wheelhouse into an offline proof pack and "
            "replay the same local runtime flows from an unpacked layout."
        ),
        "",
        "## Wheelhouse",
        f"- wheelhouse_build_attempted: {wheelhouse['wheelhouse_build_attempted']}",
        f"- wheelhouse_built: {wheelhouse['wheelhouse_built']}",
        f"- wheels_count: {wheelhouse['wheels_count']}",
        f"- missing_requirements: {wheelhouse['missing_requirements']}",
        f"- wheelhouse_committed: {wheelhouse['wheelhouse_committed']}",
        f"- venv_committed: {wheelhouse['venv_committed']}",
        "",
        "## Offline install",
        f"- offline_install_pass: {offline_install['offline_install_pass']}",
        f"- uses_no_index: {offline_install['uses_no_index']}",
        f"- uses_find_links: {offline_install['uses_find_links']}",
        (
            "- requires_network_for_dependency_install: "
            f"{offline_install['requires_network_for_dependency_install']}"
        ),
        (
            "- requires_network_at_runtime: "
            f"{offline_install['requires_network_at_runtime']}"
        ),
        (
            "- selected_python_is_wheelhouse_venv: "
            f"{offline_install['selected_python_is_wheelhouse_venv']}"
        ),
        "",
        "## Runtime evidence",
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
        f"- requires_p45_substrate: {runtime_evidence['requires_p45_substrate']}",
        "",
        "## Offline proof pack",
        f"- offline_pack_built: {offline_proof_pack['offline_pack_built']}",
        (
            "- offline_pack_local_simulation_pass: "
            f"{offline_proof_pack['offline_pack_local_simulation_pass']}"
        ),
        (
            "- external_offline_proof_imported: "
            f"{offline_proof_pack['external_offline_proof_imported']}"
        ),
        (
            "- external_offline_proof_pass: "
            f"{offline_proof_pack['external_offline_proof_pass']}"
        ),
        "",
        "## Boundary",
        f"- commercial_terms_blocked: {boundary['commercial_terms_blocked']}",
        f"- network_runtime_calls_blocked: {boundary['network_runtime_calls_blocked']}",
        "",
        "## Current limitations",
        f"- installer_built: {scope['installer_built']}",
        f"- release_created: {scope['release_created']}",
        f"- signed_installer: {scope['signed_installer']}",
        f"- embedded_python_runtime: {scope['embedded_python_runtime']}",
        f"- wheelhouse_bundled_in_repo: {scope['wheelhouse_bundled_in_repo']}",
        (
            "- external_clean_machine_offline_proven: "
            f"{scope['external_clean_machine_offline_proven']}"
        ),
        "",
        "## What S14 did not do",
        "- S14 did not build an installer, sign a release, or embed Python into the app payload.",
        (
            "- S14 did not claim clean-machine offline proof "
            "unless an external proof is later imported."
        ),
        "",
        "## Next step",
        "- Capture an external clean-machine offline proof or continue into installer packaging.",
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
        description="Build the P6-M3-S14 offline runtime handoff."
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
