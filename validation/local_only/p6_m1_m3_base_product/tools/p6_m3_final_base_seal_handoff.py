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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_final_base_seal_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
PREVIOUS_HEAD_SHA = "cfd9c0b6d87177a4984335f748ede695ac974e81"


class SummaryDict(TypedDict):
    overall: str
    p6_m3_final_status: str
    go: bool
    active_blockers: int
    major_gaps: int
    allowed_next_phase: str


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
        "boundary": base_root / ".tmp/base_boundary_check.json",
        "overclaim": base_root / ".tmp/base_m3_overclaim_check.json",
        "readiness": base_root / ".tmp/base_m3_readiness/report.json",
        "final_seal": base_root / ".tmp/base_m3_final_seal_check.json",
        "package_report": base_root / ".tmp/windows_installer_package_report.json",
        "package_audit": base_root / ".tmp/windows_installer_package_audit.json",
        "install_smoke": base_root / ".tmp/windows_install_smoke_report.json",
        "external_runtime_smoke": base_root / ".tmp/external_package_runtime_smoke_report.json",
        "gui_bootstrap": base_root / ".tmp/external_gui_runtime_bootstrap_check.json",
        "s17_import": base_root / ".tmp/imported_external_package_ux_proof_report.json",
        "s18_handoff": root / ".tmp/p6_m3_s18_base_mvp_closure_handoff/report.json",
        "final_manifest": base_root / "manifests/base_m3_final_seal.json",
    }


def build_report(*, root: Path, base_root: Path) -> dict[str, object]:
    required = _required_paths(root, base_root)
    missing = [path.as_posix() for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError("; ".join(missing))

    boundary = _load_json_object(required["boundary"])
    overclaim = _load_json_object(required["overclaim"])
    readiness = _load_json_object(required["readiness"])
    final_seal = _load_json_object(required["final_seal"])
    package_report = _load_json_object(required["package_report"])
    package_audit = _load_json_object(required["package_audit"])
    install_smoke = _load_json_object(required["install_smoke"])
    external_runtime_smoke = _load_json_object(required["external_runtime_smoke"])
    gui_bootstrap = _load_json_object(required["gui_bootstrap"])
    s17_import = _load_json_object(required["s17_import"])
    s18_handoff = _load_json_object(required["s18_handoff"])
    final_manifest = _load_json_object(required["final_manifest"])

    boundary_summary = _as_dict(boundary["summary"])
    overclaim_summary = _as_dict(overclaim["summary"])
    readiness_summary = _as_dict(readiness["summary"])
    final_seal_summary = _as_dict(final_seal["summary"])
    package_summary = _as_dict(package_report["summary"])
    package_audit_summary = _as_dict(package_audit["summary"])
    install_smoke_summary = _as_dict(install_smoke["summary"])
    external_runtime_smoke_summary = _as_dict(external_runtime_smoke["summary"])
    gui_bootstrap_summary = _as_dict(gui_bootstrap["summary"])
    s17_import_summary = _as_dict(s17_import["summary"])
    s18_summary = _as_dict(s18_handoff["summary"])

    manifest_summary = _as_dict(final_manifest["summary"])
    manifest_proof_reuse = _as_dict(final_manifest["proof_reuse"])
    manifest_delivery = _as_dict(final_manifest["delivery"])
    manifest_scope = _as_dict(final_manifest["scope"])
    manifest_deferred = _as_dict(final_manifest["deferred"])
    manifest_capability = _as_dict(final_manifest["capability"])

    current_head = _git_output(base_root, "rev-parse", "HEAD") or "unknown"
    origin_head = _git_output(base_root, "rev-parse", "origin/main")

    issues: list[str] = []
    if boundary_summary.get("pass") is not True:
        issues.append("Boundary check did not pass.")
    if overclaim_summary.get("pass") is not True:
        issues.append("M3 overclaim check did not pass.")
    if readiness_summary.get("pass") is not True:
        issues.append("M3 readiness report did not pass.")
    if final_seal_summary.get("pass") is not True:
        issues.append("M3 final seal checker did not pass.")
    if package_summary.get("pass") is not True:
        issues.append("Package build regression did not pass.")
    if package_audit_summary.get("pass") is not True:
        issues.append("Package audit regression did not pass.")
    if install_smoke_summary.get("install_smoke_pass") is not True:
        issues.append("Install smoke regression did not pass.")
    if external_runtime_smoke_summary.get("external_runtime_smoke_pass") is not True:
        issues.append("External runtime smoke regression did not pass.")
    if gui_bootstrap_summary.get("pass") is not True:
        issues.append("External GUI bootstrap regression did not pass.")
    if s17_import_summary.get("manual_gui_proof_pass") is not True:
        issues.append("S17 imported external user proof did not pass.")
    if s18_summary.get("m3_s18_status") != "sealed":
        issues.append("S18 closure audit handoff is not sealed.")
    if origin_head != current_head or origin_head is None:
        issues.append("Zephyr-base head is not pushed to origin/main.")

    overall = "pass" if not issues else "fail"
    status = "sealed" if not issues else "open"
    go = not issues and manifest_summary.get("go") is True

    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.final_base_seal_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "p6_m3_final_status": status,
            "go": go,
            "active_blockers": len(issues),
            "major_gaps": 0,
            "allowed_next_phase": manifest_summary["allowed_next_phase"],
        },
        "zephyr_base": {
            "previous_head_sha": PREVIOUS_HEAD_SHA,
            "current_head_sha": current_head,
            "pushed": origin_head == current_head and origin_head is not None,
        },
        "proof_reuse": {
            "s17_external_user_proof_reused": manifest_proof_reuse[
                "s17_external_user_proof_reused"
            ],
            "s17_external_user_proof_pass": manifest_proof_reuse["s17_external_user_proof_pass"],
            "s18_closure_audit_pass": manifest_proof_reuse["s18_closure_audit_pass"],
            "manual_gui_retest_required": manifest_proof_reuse["manual_gui_retest_required"],
        },
        "delivery": {
            "artifact_kind": manifest_delivery["artifact_kind"],
            "recommended_filename": manifest_delivery["recommended_filename"],
            "recommended_hosting": manifest_delivery["recommended_hosting"],
            "website_strategy": manifest_delivery["website_strategy"],
            "paid_cdn_required": manifest_delivery["paid_cdn_required"],
            "controlled_download_required": manifest_delivery["controlled_download_required"],
        },
        "scope": manifest_scope,
        "boundary": {
            "pdf_claimed": manifest_capability["pdf"],
            "docx_claimed": manifest_capability["docx"],
            "image_or_ocr_claimed": manifest_capability["image_or_ocr"],
            "html_claimed": manifest_capability["html"],
            "cloud_claimed": manifest_capability["cloud"],
            "pro_claimed": manifest_capability["pro"],
            "license_or_entitlement_claimed": manifest_capability["license_or_entitlement"],
            "payment_or_billing_claimed": manifest_capability["payment_or_billing"],
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
            "secrets_printed": False,
        },
        "deferred": manifest_deferred,
        "issues": issues,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    proof_reuse = _as_dict(report["proof_reuse"])
    delivery = _as_dict(report["delivery"])
    scope = _as_dict(report["scope"])
    issues = cast(list[object], report["issues"])
    lines = [
        "# P6-M3 Final Base seal handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- p6_m3_final_status: {summary['p6_m3_final_status']}",
        f"- go: {summary['go']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- major_gaps: {summary['major_gaps']}",
        f"- allowed_next_phase: {summary['allowed_next_phase']}",
        "",
        "## Proof reuse",
        f"- s17_external_user_proof_reused: {proof_reuse['s17_external_user_proof_reused']}",
        f"- s17_external_user_proof_pass: {proof_reuse['s17_external_user_proof_pass']}",
        f"- s18_closure_audit_pass: {proof_reuse['s18_closure_audit_pass']}",
        f"- manual_gui_retest_required: {proof_reuse['manual_gui_retest_required']}",
        "",
        "## Delivery",
        f"- artifact_kind: {delivery['artifact_kind']}",
        f"- recommended_filename: {delivery['recommended_filename']}",
        f"- recommended_hosting: {delivery['recommended_hosting']}",
        f"- website_strategy: {delivery['website_strategy']}",
        "",
        "## Scope",
        f"- signed_installer: {scope['signed_installer']}",
        f"- official_release: {scope['official_release']}",
        f"- release_created: {scope['release_created']}",
        f"- auto_update: {scope['auto_update']}",
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
    parser = argparse.ArgumentParser(description="Build the P6-M3 final Base seal handoff.")
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
