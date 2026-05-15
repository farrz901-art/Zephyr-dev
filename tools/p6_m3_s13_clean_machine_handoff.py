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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s13_clean_machine_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "4b19d216ac76829a333712f74dbcdf285e940ad9"


class SummaryDict(TypedDict):
    overall: str
    m3_s13_status: str
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
    status_path = base_root / ".tmp/s13_repo_status.json"
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
        "install_layout_baseline": base_root / ".tmp/install_layout_baseline_check.json",
        "runtime_packaging_baseline": base_root / ".tmp/runtime_packaging_baseline_check.json",
        "layout_build": base_root / ".tmp/base_install_layout_build.json",
        "layout_audit": base_root / ".tmp/base_install_layout_audit.json",
        "clean_pack_manifest": base_root
        / ".tmp/clean_machine_proof_pack/clean_machine_proof_pack_manifest.json",
        "clean_pack_relocation": base_root / ".tmp/clean_machine_pack_relocation_audit.json",
        "clean_pack_baseline": base_root / ".tmp/clean_machine_proof_pack_baseline_check.json",
        "clean_pack_local_simulation": base_root
        / ".tmp/clean_machine_pack_local_simulation_check.json",
        "ui_build": base_root / ".tmp/ui_build_check.json",
        "tauri_app_path": base_root / ".tmp/tauri_app_path_check.json",
        "managed_runtime_flow": base_root / ".tmp/managed_runtime_flow_check.json",
    }


def _load_optional_json(path: Path) -> dict[str, object] | None:
    return _load_json_object(path) if path.exists() else None


def _proof_runtime_evidence(
    local_simulation_report: dict[str, object],
    imported_report: dict[str, object] | None,
) -> dict[str, object]:
    source_root: Path | None = None
    if imported_report is not None:
        imported_summary = _as_dict(imported_report["summary"])
        if _as_bool(imported_summary["external_clean_machine_proof_pass"]):
            source_root = Path(str(imported_report["imported_proof_root"])).resolve()
    if source_root is None:
        source_root = Path(str(local_simulation_report["simulation_root"])).resolve() / "proof"
    proof_path = source_root / "clean_machine_runtime_proof.json"
    proof = _load_json_object(proof_path)
    text_flow = _as_dict(proof["text_flow"])
    file_flow = _as_dict(proof["file_flow"])
    return {
        "text_flow_pass": text_flow["pass"],
        "file_flow_pass": file_flow["pass"],
        "text_marker_found": text_flow["marker_found"],
        "file_marker_found": file_flow["marker_found"],
        "billing_semantics": text_flow["billing_semantics"],
        "bundled_runtime_used": text_flow["bundled_runtime_used"],
        "fixture_runner_used": text_flow["fixture_runner_used"],
        "zephyr_dev_working_tree_required": text_flow["zephyr_dev_working_tree_required"],
        "requires_network_at_runtime": text_flow["requires_network"],
        "requires_p45_substrate": text_flow["requires_p45_substrate"],
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
    install_layout_baseline_report = _load_json_object(required["install_layout_baseline"])
    runtime_packaging_report = _load_json_object(required["runtime_packaging_baseline"])
    layout_build_report = _load_json_object(required["layout_build"])
    layout_audit_report = _load_json_object(required["layout_audit"])
    clean_pack_manifest = _load_json_object(required["clean_pack_manifest"])
    clean_pack_relocation_report = _load_json_object(required["clean_pack_relocation"])
    clean_pack_baseline_report = _load_json_object(required["clean_pack_baseline"])
    clean_pack_local_simulation_report = _load_json_object(required["clean_pack_local_simulation"])
    ui_build_report = _load_json_object(required["ui_build"])
    tauri_app_path_report = _load_json_object(required["tauri_app_path"])
    managed_runtime_flow_report = _load_json_object(required["managed_runtime_flow"])
    imported_report = _load_optional_json(
        base_root / ".tmp/imported_clean_machine_proof_report.json"
    )
    repo_status = _repo_status(base_root)

    no_bom_summary = _as_dict(no_bom_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    install_layout_summary = _as_dict(install_layout_baseline_report["summary"])
    runtime_packaging_summary = _as_dict(runtime_packaging_report["summary"])
    layout_build_summary = _as_dict(layout_build_report["summary"])
    layout_audit_summary = _as_dict(layout_audit_report["summary"])
    clean_pack_relocation_summary = _as_dict(clean_pack_relocation_report["summary"])
    clean_pack_baseline_summary = _as_dict(clean_pack_baseline_report["summary"])
    clean_pack_local_simulation_summary = _as_dict(clean_pack_local_simulation_report["summary"])
    ui_build_summary = _as_dict(ui_build_report["summary"])
    tauri_app_path_summary = _as_dict(tauri_app_path_report["summary"])
    managed_runtime_summary = _as_dict(managed_runtime_flow_report["summary"])

    external_imported = False
    external_pass = False
    if imported_report is not None:
        imported_summary = _as_dict(imported_report["summary"])
        external_imported = _as_bool(imported_summary["external_clean_machine_proof_imported"])
        external_pass = _as_bool(imported_summary["external_clean_machine_proof_pass"])

    issues: list[str] = []
    if not _as_bool(no_bom_summary["pass"]):
        issues.append("No-BOM hygiene check did not pass.")
    if not _as_bool(boundary_summary["pass"]):
        issues.append("Boundary check did not pass.")
    if not _as_bool(install_layout_summary["pass"]):
        issues.append("Install-layout baseline check did not pass.")
    if not _as_bool(runtime_packaging_summary["pass"]):
        issues.append("Runtime packaging baseline regression did not pass.")
    if not _as_bool(layout_build_summary["pass"]):
        issues.append("Install layout build did not pass.")
    if not _as_bool(layout_audit_summary["pass"]):
        issues.append("Install layout audit did not pass.")
    if not _as_bool(clean_pack_relocation_summary["pass"]):
        issues.append("Clean-machine pack relocation audit did not pass.")
    if not _as_bool(clean_pack_baseline_summary["pass"]):
        issues.append("Clean-machine proof pack baseline did not pass.")
    if not _as_bool(clean_pack_local_simulation_summary["pass"]):
        issues.append("Clean-machine pack local simulation did not pass.")
    if not _as_bool(ui_build_summary["pass"]):
        issues.append("UI build regression did not pass.")
    if not _as_bool(tauri_app_path_summary["pass"]):
        issues.append("Tauri app path regression did not pass.")
    if not _as_bool(managed_runtime_summary["pass"]):
        issues.append("Managed runtime flow regression did not pass.")
    if _as_bool(repo_status["pushed"]) is False:
        issues.append("Zephyr-base S13 commit has not been pushed to origin/main.")

    major_gaps = 0
    if not external_pass:
        major_gaps = 1

    if issues:
        overall = "fail"
        status = "open"
    elif external_pass:
        overall = "pass"
        status = "sealed"
    else:
        overall = "conditional"
        status = "conditional"

    runtime_evidence = _proof_runtime_evidence(clean_pack_local_simulation_report, imported_report)
    non_blocking_notes = [
        "S13 proves a clean-machine proof-pack path, not an installer.",
    ]
    if not external_pass:
        non_blocking_notes.append(
            "Without imported external clean-machine proof, "
            "clean_machine_runtime_proven stays false."
        )

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s13.clean_machine_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s13_status": status,
            "active_blockers": len(issues),
            "major_gaps": major_gaps,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": repo_status["pushed"],
        },
        "proof_pack": {
            "pack_built": True,
            "zip_exists": Path(str(clean_pack_manifest["zip_path"])).exists(),
            "relocation_audit_pass": clean_pack_relocation_summary["pass"],
            "local_unzip_simulation_pass": clean_pack_local_simulation_summary[
                "local_simulation_pass"
            ],
            "external_clean_machine_proof_imported": external_imported,
            "external_clean_machine_proof_pass": external_pass,
        },
        "clean_machine_runtime": {
            "proof_level": clean_pack_manifest["proof_level"],
            "requires_python_on_clean_machine": clean_pack_manifest[
                "requires_python_on_clean_machine"
            ],
            "requires_network_for_bootstrap": clean_pack_manifest[
                "requires_network_for_bootstrap"
            ],
            "requires_git": clean_pack_manifest["requires_git"],
            "requires_node": clean_pack_manifest["requires_node"],
            "requires_rust": clean_pack_manifest["requires_rust"],
            "requires_zephyr_dev": clean_pack_manifest["requires_zephyr_dev"],
            "requires_zephyr_base_repo": clean_pack_manifest["requires_zephyr_base_repo"],
            "clean_machine_runtime_proven": external_pass,
        },
        "runtime_evidence": runtime_evidence,
        "scope": {
            "installer_built": clean_pack_manifest["installer_built"],
            "release_created": clean_pack_manifest["release_created"],
            "signed_installer": False,
            "embedded_python_runtime": clean_pack_manifest["embedded_python_runtime"],
            "wheelhouse_bundled": clean_pack_manifest["wheelhouse_bundled"],
            "offline_runtime_install_proven": False,
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
    proof_pack = _as_dict(report["proof_pack"])
    clean_machine_runtime = _as_dict(report["clean_machine_runtime"])
    runtime_evidence = _as_dict(report["runtime_evidence"])
    scope = _as_dict(report["scope"])
    boundary = _as_dict(report["boundary"])
    issues = cast(list[object], report["issues"])
    notes = cast(list[object], report["non_blocking_notes"])

    lines = [
        "# P6-M3-S13 Clean-machine runtime proof handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s13_status: {summary['m3_s13_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## What S13 proved",
        (
            "- Zephyr-base now has a clean-machine proof pack builder, "
            "relocation audit, local unzip simulation, and proof import/validation path."
        ),
        (
            "- The pack can be unpacked outside the repo root and bootstrap "
            "a layout-local managed runtime for local text and local file proof flows."
        ),
        "",
        "## Proof pack",
        f"- pack_built: {proof_pack['pack_built']}",
        f"- zip_exists: {proof_pack['zip_exists']}",
        f"- relocation_audit_pass: {proof_pack['relocation_audit_pass']}",
        f"- local_unzip_simulation_pass: {proof_pack['local_unzip_simulation_pass']}",
        (
            "- external_clean_machine_proof_imported: "
            f"{proof_pack['external_clean_machine_proof_imported']}"
        ),
        f"- external_clean_machine_proof_pass: {proof_pack['external_clean_machine_proof_pass']}",
        "",
        "## Clean-machine runtime",
        f"- proof_level: {clean_machine_runtime['proof_level']}",
        (
            "- requires_python_on_clean_machine: "
            f"{clean_machine_runtime['requires_python_on_clean_machine']}"
        ),
        (
            "- requires_network_for_bootstrap: "
            f"{clean_machine_runtime['requires_network_for_bootstrap']}"
        ),
        f"- requires_git: {clean_machine_runtime['requires_git']}",
        f"- requires_node: {clean_machine_runtime['requires_node']}",
        f"- requires_rust: {clean_machine_runtime['requires_rust']}",
        f"- requires_zephyr_dev: {clean_machine_runtime['requires_zephyr_dev']}",
        f"- requires_zephyr_base_repo: {clean_machine_runtime['requires_zephyr_base_repo']}",
        f"- clean_machine_runtime_proven: {clean_machine_runtime['clean_machine_runtime_proven']}",
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
        f"- requires_network_at_runtime: {runtime_evidence['requires_network_at_runtime']}",
        f"- requires_p45_substrate: {runtime_evidence['requires_p45_substrate']}",
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
        f"- wheelhouse_bundled: {scope['wheelhouse_bundled']}",
        f"- offline_runtime_install_proven: {scope['offline_runtime_install_proven']}",
        "",
        "## Next step",
        (
            "- Import a real external clean-machine proof to seal S13, "
            "or continue into installer/offline packaging slices."
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
        description="Build the P6-M3-S13 clean-machine runtime handoff."
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
