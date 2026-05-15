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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s18_base_mvp_closure_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"


class SummaryDict(TypedDict):
    overall: str
    m3_s18_status: str
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


def _required_paths(root: Path, base_root: Path) -> dict[str, Path]:
    return {
        "readiness": base_root / ".tmp/base_m3_readiness/report.json",
        "overclaim": base_root / ".tmp/base_m3_overclaim_check.json",
        "package_report": base_root / ".tmp/windows_installer_package_report.json",
        "package_audit": base_root / ".tmp/windows_installer_package_audit.json",
        "install_smoke": base_root / ".tmp/windows_install_smoke_report.json",
        "external_runtime_smoke": base_root / ".tmp/external_package_runtime_smoke_report.json",
        "gui_bootstrap": base_root / ".tmp/external_gui_runtime_bootstrap_check.json",
        "ux_shell": base_root / ".tmp/base_ux_shell_check.json",
        "boundary": base_root / ".tmp/base_boundary_check.json",
        "s17_handoff": root / ".tmp/p6_m3_s17_external_ux_handoff/report.json",
    }


def build_report(*, root: Path, base_root: Path) -> dict[str, object]:
    required = _required_paths(root, base_root)
    missing = [
        path.as_posix() for path in required.values() if not path.exists()
    ]
    if missing:
        raise FileNotFoundError("; ".join(missing))

    readiness = _load_json_object(required["readiness"])
    overclaim = _load_json_object(required["overclaim"])
    package_report = _load_json_object(required["package_report"])
    package_audit = _load_json_object(required["package_audit"])
    install_smoke = _load_json_object(required["install_smoke"])
    external_runtime_smoke = _load_json_object(required["external_runtime_smoke"])
    gui_bootstrap = _load_json_object(required["gui_bootstrap"])
    ux_shell = _load_json_object(required["ux_shell"])
    boundary = _load_json_object(required["boundary"])
    s17_handoff = _load_json_object(required["s17_handoff"])

    readiness_summary = _as_dict(readiness["summary"])
    readiness_block = _as_dict(readiness["m3_readiness"])
    scope_block = _as_dict(readiness["scope"])
    boundary_block = _as_dict(readiness["boundary"])
    overclaim_summary = _as_dict(overclaim["summary"])
    package_summary = _as_dict(package_report["summary"])
    package_manifest = _as_dict(package_report["manifest"])
    package_audit_summary = _as_dict(package_audit["summary"])
    install_smoke_summary = _as_dict(install_smoke["summary"])
    external_runtime_smoke_summary = _as_dict(external_runtime_smoke["summary"])
    gui_bootstrap_summary = _as_dict(gui_bootstrap["summary"])
    ux_summary = _as_dict(ux_shell["summary"])
    boundary_summary = _as_dict(boundary["summary"])
    s17_summary = _as_dict(s17_handoff["summary"])

    current_head = _git_output(base_root, "rev-parse", "HEAD") or "unknown"
    origin_head = _git_output(base_root, "rev-parse", "origin/main")

    issues: list[str] = []
    if not _as_bool(readiness_summary["pass"]):
        issues.append("Base M3 readiness report did not pass.")
    if not _as_bool(overclaim_summary["pass"]):
        issues.append("Base M3 overclaim check did not pass.")
    if package_summary["pass"] is not True:
        issues.append("Windows package build regression did not pass.")
    if package_audit_summary["pass"] is not True:
        issues.append("Windows package audit regression did not pass.")
    if install_smoke_summary["install_smoke_pass"] is not True:
        issues.append("Windows install smoke regression did not pass.")
    if external_runtime_smoke_summary["external_runtime_smoke_pass"] is not True:
        issues.append("External package runtime smoke regression did not pass.")
    if gui_bootstrap_summary["pass"] is not True:
        issues.append("External GUI bootstrap regression did not pass.")
    if ux_summary["pass"] is not True:
        issues.append("Base UX shell regression did not pass.")
    if boundary_summary["pass"] is not True:
        issues.append("Boundary regression did not pass.")
    if s17_summary.get("m3_s17_status") != "sealed":
        issues.append("S17 sealed external UX evidence is missing.")
    if origin_head != current_head or origin_head is None:
        issues.append("Zephyr-base head is not pushed to origin/main.")

    overall = "pass" if not issues else "fail"
    status = "sealed" if not issues else "open"

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s18.base_mvp_closure_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s18_status": status,
            "active_blockers": len(issues),
            "major_gaps": 0,
        },
        "zephyr_base": {
            "current_head_sha": current_head,
            "pushed": origin_head == current_head and origin_head is not None,
        },
        "m3_readiness": {
            "base_mvp_runnable": readiness_block["base_mvp_runnable"],
            "external_package_ux_proven": readiness_block["external_package_ux_proven"],
            "portable_zip_package_proven": readiness_block["portable_zip_package_proven"],
            "managed_runtime_bootstrap_proven": readiness_block["managed_runtime_bootstrap_proven"],
            "offline_runtime_proven": readiness_block["offline_runtime_proven"],
            "clean_machine_runtime_proven": readiness_block["clean_machine_runtime_proven"],
            "bilingual_ui_proven": readiness_block["bilingual_ui_proven"],
            "m3_distribution_decision": readiness_block["m3_distribution_decision"],
        },
        "scope": {
            "signed_installer": scope_block["signed_installer"],
            "official_release": scope_block["official_release"],
            "release_created": scope_block["release_created"],
            "auto_update": scope_block["auto_update"],
            "embedded_python_runtime": scope_block["embedded_python_runtime"],
            "runtime_capability_changed": scope_block["runtime_capability_changed"],
        },
        "boundary": {
            "pdf_claimed": boundary_block["pdf_claimed"],
            "docx_claimed": boundary_block["docx_claimed"],
            "image_or_ocr_claimed": boundary_block["image_or_ocr_claimed"],
            "html_claimed": boundary_block["html_claimed"],
            "cloud_claimed": boundary_block["cloud_claimed"],
            "pro_claimed": boundary_block["pro_claimed"],
            "license_or_entitlement_claimed": boundary_block["license_or_entitlement_claimed"],
            "payment_or_billing_claimed": boundary_block["payment_or_billing_claimed"],
            "private_core_allowed": boundary_block["private_core_allowed"],
            "web_core_dependency_allowed": boundary_block["web_core_dependency_allowed"],
            "secrets_printed": boundary_block["secrets_printed"],
        },
        "next": {
            "recommended_next_step": "P6-M3-S19 real user UX walkthrough",
            "deferred_to_p6_m8": [
                "signed installer",
                "official release signing pipeline",
                "auto update manifest",
                "controlled download hard gates",
            ],
        },
        "issues": issues,
        "validation": {
            "readiness_pass": readiness_summary["pass"],
            "overclaim_pass": overclaim_summary["pass"],
            "package_kind": package_manifest["package_kind"],
            "package_built": package_summary["package_built"],
            "package_audit_pass": package_audit_summary["pass"],
            "install_smoke_pass": install_smoke_summary["install_smoke_pass"],
            "external_runtime_smoke_pass": external_runtime_smoke_summary[
                "external_runtime_smoke_pass"
            ],
            "external_gui_bootstrap_pass": gui_bootstrap_summary["pass"],
            "s17_status": s17_summary["m3_s17_status"],
        },
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    readiness = _as_dict(report["m3_readiness"])
    scope = _as_dict(report["scope"])
    boundary = _as_dict(report["boundary"])
    next_block = _as_dict(report["next"])
    issues = cast(list[object], report["issues"])

    lines = [
        "# P6-M3-S18 Base MVP closure handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- m3_s18_status: {summary['m3_s18_status']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        "",
        "## M3 readiness",
        f"- base_mvp_runnable: {readiness['base_mvp_runnable']}",
        f"- external_package_ux_proven: {readiness['external_package_ux_proven']}",
        f"- portable_zip_package_proven: {readiness['portable_zip_package_proven']}",
        f"- managed_runtime_bootstrap_proven: {readiness['managed_runtime_bootstrap_proven']}",
        f"- offline_runtime_proven: {readiness['offline_runtime_proven']}",
        f"- clean_machine_runtime_proven: {readiness['clean_machine_runtime_proven']}",
        f"- bilingual_ui_proven: {readiness['bilingual_ui_proven']}",
        f"- m3_distribution_decision: {readiness['m3_distribution_decision']}",
        "",
        "## Scope",
        f"- signed_installer: {scope['signed_installer']}",
        f"- official_release: {scope['official_release']}",
        f"- release_created: {scope['release_created']}",
        f"- auto_update: {scope['auto_update']}",
        f"- embedded_python_runtime: {scope['embedded_python_runtime']}",
        f"- runtime_capability_changed: {scope['runtime_capability_changed']}",
        "",
        "## Boundary",
        f"- pdf_claimed: {boundary['pdf_claimed']}",
        f"- docx_claimed: {boundary['docx_claimed']}",
        f"- image_or_ocr_claimed: {boundary['image_or_ocr_claimed']}",
        f"- html_claimed: {boundary['html_claimed']}",
        f"- cloud_claimed: {boundary['cloud_claimed']}",
        f"- pro_claimed: {boundary['pro_claimed']}",
        f"- license_or_entitlement_claimed: {boundary['license_or_entitlement_claimed']}",
        f"- payment_or_billing_claimed: {boundary['payment_or_billing_claimed']}",
        "",
        "## Next",
        f"- recommended_next_step: {next_block['recommended_next_step']}",
    ]
    if issues:
        lines.extend(["", "## Issues"])
        for item in issues:
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
    parser = argparse.ArgumentParser(description="Build the P6-M3-S18 Base MVP closure handoff.")
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
