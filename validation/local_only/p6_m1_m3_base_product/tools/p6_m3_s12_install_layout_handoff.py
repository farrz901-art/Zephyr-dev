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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s12_install_layout_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "c84e68c726f35d3adfbc9c9c575a83a3dcb3557d"


class SummaryDict(TypedDict):
    overall: str
    m3_s12_status: str
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
    status_path = base_root / ".tmp/s12_repo_status.json"
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
        "generated_hygiene": base_root / ".tmp/generated_artifact_hygiene_check.json",
        "install_layout_baseline": base_root / ".tmp/install_layout_baseline_check.json",
        "layout_build": base_root / ".tmp/base_install_layout_build.json",
        "layout_audit": base_root / ".tmp/base_install_layout_audit.json",
        "layout_runtime_smoke": base_root / ".tmp/base_install_layout_runtime_smoke.json",
        "runtime_packaging": base_root / ".tmp/runtime_packaging_baseline_check.json",
        "managed_flow": base_root / ".tmp/managed_runtime_flow_check.json",
        "ui_build": base_root / ".tmp/ui_build_check.json",
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
    generated_hygiene_report = _load_json_object(required["generated_hygiene"])
    install_layout_baseline_report = _load_json_object(required["install_layout_baseline"])
    layout_build_report = _load_json_object(required["layout_build"])
    layout_audit_report = _load_json_object(required["layout_audit"])
    layout_runtime_smoke_report = _load_json_object(required["layout_runtime_smoke"])
    runtime_packaging_report = _load_json_object(required["runtime_packaging"])
    managed_flow_report = _load_json_object(required["managed_flow"])
    ui_build_report = _load_json_object(required["ui_build"])
    tauri_app_path_report = _load_json_object(required["tauri_app_path"])
    repo_status = _repo_status(base_root)

    no_bom_summary = _as_dict(no_bom_report["summary"])
    boundary_summary = _as_dict(boundary_report["summary"])
    generated_summary = _as_dict(generated_hygiene_report["summary"])
    install_layout_baseline_summary = _as_dict(install_layout_baseline_report["summary"])
    layout_build_summary = _as_dict(layout_build_report["summary"])
    layout_build_manifest = _as_dict(layout_build_report["manifest"])
    layout_audit_summary = _as_dict(layout_audit_report["summary"])
    layout_runtime_summary = _as_dict(layout_runtime_smoke_report["summary"])
    layout_runtime_evidence = _as_dict(layout_runtime_smoke_report["evidence"])
    runtime_packaging_summary = _as_dict(runtime_packaging_report["summary"])
    managed_flow_summary = _as_dict(managed_flow_report["summary"])
    ui_build_summary = _as_dict(ui_build_report["summary"])
    tauri_app_path_summary = _as_dict(tauri_app_path_report["summary"])

    issues: list[str] = []
    if not _as_bool(no_bom_summary["pass"]):
        issues.append("No-BOM hygiene check did not pass.")
    if not _as_bool(boundary_summary["pass"]):
        issues.append("Boundary check did not pass.")
    if not _as_bool(generated_summary["generated_artifact_hygiene_pass"]):
        issues.append("Generated artifact hygiene check did not pass.")
    if not _as_bool(install_layout_baseline_summary["pass"]):
        issues.append("Install layout baseline check did not pass.")
    if not _as_bool(layout_build_summary["pass"]):
        issues.append("Install layout build did not pass.")
    if not _as_bool(layout_audit_summary["pass"]):
        issues.append("Install layout audit did not pass.")
    if not _as_bool(layout_runtime_summary["pass"]):
        issues.append("Install layout runtime smoke did not pass.")
    if not _as_bool(runtime_packaging_summary["pass"]):
        issues.append("Runtime packaging baseline regression did not pass.")
    if not _as_bool(managed_flow_summary["pass"]):
        issues.append("Managed runtime flow regression did not pass.")
    if not _as_bool(ui_build_summary["pass"]):
        issues.append("UI build regression did not pass.")
    if not _as_bool(tauri_app_path_summary["pass"]):
        issues.append("Tauri app path regression did not pass.")
    if _as_bool(repo_status["pushed"]) is False:
        issues.append("Zephyr-base S12 commit has not been pushed to origin/main.")

    overall = "pass" if not issues else "fail"
    status = "sealed" if not issues else "open"

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s12.install_layout_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s12_status": status,
            "active_blockers": len(issues),
            "major_gaps": 0,
        },
        "zephyr_base": {
            "previous_head_sha": repo_status["previous_head_sha"],
            "current_head_sha": repo_status["current_head_sha"],
            "pushed": repo_status["pushed"],
        },
        "install_layout": {
            "layout_built": layout_build_summary["pass"],
            "layout_audit_pass": layout_audit_summary["pass"],
            "layout_runtime_smoke_pass": layout_runtime_summary["pass"],
            "ui_dist_present": layout_build_manifest["ui_dist_present"],
            "public_core_bundle_present": layout_build_manifest["public_core_bundle_present"],
            "runtime_manifest_present": runtime_packaging_summary["runtime_manifest_exists"],
            "bootstrap_script_present": layout_build_manifest["runtime_bootstrap_present"],
            "forbidden_dirs_present": layout_audit_summary["forbidden_dirs_present"],
        },
        "generated_artifacts": {
            "src_tauri_gen_classified_generated": generated_summary[
                "src_tauri_gen_classified_generated"
            ],
            "src_tauri_gen_committed": generated_summary["src_tauri_gen_committed"],
            "generated_artifact_hygiene_pass": generated_summary[
                "generated_artifact_hygiene_pass"
            ],
        },
        "runtime_evidence": {
            "marker_found": layout_runtime_evidence["marker_found"],
            "run_result_exists": layout_runtime_evidence["run_result_exists"],
            "billing_semantics": layout_runtime_evidence["billing_semantics"],
            "bundled_runtime_used": layout_runtime_evidence["bundled_runtime_used"],
            "fixture_runner_used": layout_runtime_evidence["fixture_runner_used"],
            "zephyr_dev_working_tree_required": layout_runtime_evidence[
                "zephyr_dev_working_tree_required"
            ],
            "requires_network": layout_runtime_evidence["requires_network"],
            "requires_p45_substrate": layout_runtime_evidence["requires_p45_substrate"],
        },
        "scope": {
            "installer_built": layout_build_manifest["installer_built"],
            "release_created": layout_build_manifest["release_created"],
            "signed_installer": False,
            "embedded_python_runtime": layout_build_manifest["embedded_python_runtime"],
            "wheelhouse_bundled": layout_build_manifest["wheelhouse_bundled"],
            "clean_machine_runtime_proven": layout_build_manifest["clean_machine_runtime_proven"],
        },
        "boundary": {
            "boundary_check_pass": boundary_summary["pass"],
            "install_layout_baseline_check_pass": install_layout_baseline_summary["pass"],
            "commercial_terms_blocked": layout_audit_summary["commercial_terms_blocked"],
            "network_runtime_calls_blocked": 0,
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
        },
        "issues": issues,
        "non_blocking_notes": [
            "S12 proves an installer-precursor install layout, not a signed installer.",
            "Clean-machine runtime proof remains explicitly out of scope for S12.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    install_layout = _as_dict(report["install_layout"])
    generated_artifacts = _as_dict(report["generated_artifacts"])
    runtime_evidence = _as_dict(report["runtime_evidence"])
    scope = _as_dict(report["scope"])
    boundary = _as_dict(report["boundary"])
    issues = cast(list[object], report["issues"])
    notes = cast(list[object], report["non_blocking_notes"])

    lines = [
        "# P6-M3-S12 Install layout first slice handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s12_status: {summary['m3_s12_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## What S12 proved",
        (
            "- Zephyr-base now has an installer-layout precursor that combines UI dist, "
            "bundled public-core runtime, runtime manifests, public-core bridge, checks, "
            "and legal/product-boundary docs."
        ),
        (
            "- The layout-root runtime smoke proves the layout can bootstrap a managed "
            "runtime and run local text without a Zephyr-dev working tree."
        ),
        "",
        "## Generated artifact hygiene",
        (
            "- src_tauri_gen_classified_generated: "
            f"{generated_artifacts['src_tauri_gen_classified_generated']}"
        ),
        f"- src_tauri_gen_committed: {generated_artifacts['src_tauri_gen_committed']}",
        (
            "- generated_artifact_hygiene_pass: "
            f"{generated_artifacts['generated_artifact_hygiene_pass']}"
        ),
        "",
        "## Install layout",
        f"- layout_built: {install_layout['layout_built']}",
        f"- layout_audit_pass: {install_layout['layout_audit_pass']}",
        f"- layout_runtime_smoke_pass: {install_layout['layout_runtime_smoke_pass']}",
        f"- ui_dist_present: {install_layout['ui_dist_present']}",
        f"- public_core_bundle_present: {install_layout['public_core_bundle_present']}",
        f"- runtime_manifest_present: {install_layout['runtime_manifest_present']}",
        f"- bootstrap_script_present: {install_layout['bootstrap_script_present']}",
        "",
        "## Layout runtime smoke",
        f"- run_result_exists: {runtime_evidence['run_result_exists']}",
        f"- marker_found: {runtime_evidence['marker_found']}",
        f"- billing_semantics: {runtime_evidence['billing_semantics']}",
        f"- bundled_runtime_used: {runtime_evidence['bundled_runtime_used']}",
        f"- fixture_runner_used: {runtime_evidence['fixture_runner_used']}",
        (
            "- zephyr_dev_working_tree_required: "
            f"{runtime_evidence['zephyr_dev_working_tree_required']}"
        ),
        f"- requires_network: {runtime_evidence['requires_network']}",
        f"- requires_p45_substrate: {runtime_evidence['requires_p45_substrate']}",
        "",
        "## Boundary",
        f"- boundary_check_pass: {boundary['boundary_check_pass']}",
        f"- install_layout_baseline_check_pass: {boundary['install_layout_baseline_check_pass']}",
        f"- commercial_terms_blocked: {boundary['commercial_terms_blocked']}",
        "",
        "## Current limitations",
        f"- installer_built: {scope['installer_built']}",
        f"- release_created: {scope['release_created']}",
        f"- signed_installer: {scope['signed_installer']}",
        f"- embedded_python_runtime: {scope['embedded_python_runtime']}",
        f"- wheelhouse_bundled: {scope['wheelhouse_bundled']}",
        f"- clean_machine_runtime_proven: {scope['clean_machine_runtime_proven']}",
        "",
        "## What S12 did not do",
        "- did not build a signed installer",
        "- did not create a release",
        "- did not claim clean-machine runtime proof",
        "- did not add Web-core, Pro, or commercial logic",
        "",
        "## Next step",
        "- P6-M3-S13: clean-machine runtime proof slice or installer packaging next slice.",
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
        description="Build the P6-M3-S12 install layout handoff."
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
