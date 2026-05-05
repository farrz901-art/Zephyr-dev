from __future__ import annotations

import argparse
import json
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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_prep_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"


class SummaryDict(TypedDict):
    overall: str
    active_blockers: int
    major_gaps: int
    m3_prep_status: str


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


def _as_int(value: object) -> int:
    if not isinstance(value, int):
        raise ValueError(f"Expected int, got {type(value).__name__}")
    return value


def _required_paths(root: Path) -> dict[str, Path]:
    return {
        "tech_decision": root / "docs/p6/P6_M3_BASE_DESKTOP_TECH_DECISION.md",
        "product_boundary": root / "docs/p6/P6_M3_BASE_PRODUCT_BOUNDARY.md",
        "ui_brief": root / "docs/p6/P6_M3_BASE_UI_DESIGN_BRIEF.md",
        "acceptance_gates": root / "docs/p6/P6_M3_BASE_ACCEPTANCE_GATES.md",
        "capability_manifest": root / "docs/p6/base_product_capability_manifest.json",
        "design_asset_request": root / "docs/p6/base_design_asset_request.json",
        "base_scaffold_tool": root / "tools/p6_m3_base_scaffold.py",
        "pro_scaffold_tool": root / "tools/p6_m3_pro_scaffold.py",
        "export_dry_run_tool": root / "tools/p6_m3_export_dry_run.py",
    }


def build_report(*, root: Path) -> dict[str, object]:
    required_paths = _required_paths(root)
    missing = [
        f"{label}: {path.relative_to(root).as_posix()}"
        for label, path in required_paths.items()
        if not path.exists()
    ]
    capability_manifest = _load_json_object(required_paths["capability_manifest"])
    design_request = _load_json_object(required_paths["design_asset_request"])
    capability_forbidden = cast(list[object], capability_manifest["forbidden_capabilities"])
    active_blockers = len(missing)
    design_task_count = _as_int(design_request["design_tasks_count"])
    overall = "pass" if active_blockers == 0 else "fail"
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.prep_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "active_blockers": active_blockers,
            "major_gaps": 0,
            "m3_prep_status": "sealed" if active_blockers == 0 else "open",
        },
        "decisions": {
            "desktop_stack": "tauri_rust_shell_web_ui",
            "python_to_rust_full_rewrite_required": False,
            "bridge_strategy": "json_artifact_or_process_bridge",
            "base_local_only_default": True,
            "requires_p45_substrate": False,
            "base_license_allowed": False,
            "base_entitlement_allowed": False,
            "base_private_core_allowed": False,
            "zephyr_base_repo_created_this_step": False,
        },
        "ui_design": {
            "ui_brief_exists": True,
            "design_window_input_required": _as_bool(
                design_request["design_window_input_required"]
            ),
            "design_asset_request_exists": True,
            "design_tasks_count": design_task_count,
        },
        "acceptance_gates": {
            "ci_only_sufficient": False,
            "local_file_smoke_required": True,
            "output_filesystem_smoke_required": True,
            "receipt_evidence_display_required": True,
            "commercial_contamination_scan_required": True,
            "source_sha_manifest_hash_required": True,
        },
        "capability_boundary": {
            "requires_p45_substrate": capability_manifest["requires_p45_substrate"],
            "private_core_allowed": capability_manifest["private_core_allowed"],
            "license_allowed": capability_manifest["license_allowed"],
            "entitlement_allowed": capability_manifest["entitlement_allowed"],
            "forbidden_capabilities_count": len(capability_forbidden),
        },
        "issues": missing,
        "non_blocking_notes": [
            "M3-0 freezes route and product boundary only; no real repo is created here.",
            (
                "Design asset request is generated, but final UI and animation "
                "assets remain later work."
            ),
            "Base acceptance requires local smoke and boundary scans; CI-only is insufficient.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    decisions = _as_dict(report["decisions"])
    ui_design = _as_dict(report["ui_design"])
    acceptance = _as_dict(report["acceptance_gates"])
    lines = [
        "# P6-M3-0 Base route decision freeze",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        f"- m3_prep_status: {summary['m3_prep_status']}",
        "",
        "## Decisions",
        f"- desktop_stack: {decisions['desktop_stack']}",
        (
            "- python_to_rust_full_rewrite_required: "
            f"{decisions['python_to_rust_full_rewrite_required']}"
        ),
        f"- bridge_strategy: {decisions['bridge_strategy']}",
        f"- base_local_only_default: {decisions['base_local_only_default']}",
        f"- requires_p45_substrate: {decisions['requires_p45_substrate']}",
        f"- base_license_allowed: {decisions['base_license_allowed']}",
        f"- base_entitlement_allowed: {decisions['base_entitlement_allowed']}",
        f"- base_private_core_allowed: {decisions['base_private_core_allowed']}",
        "",
        "## UI design handoff",
        (
            "- design_window_input_required: "
            f"{ui_design['design_window_input_required']}"
        ),
        f"- design_asset_request_exists: {ui_design['design_asset_request_exists']}",
        f"- design_tasks_count: {ui_design['design_tasks_count']}",
        "",
        "## Acceptance gates",
        f"- ci_only_sufficient: {acceptance['ci_only_sufficient']}",
        f"- local_file_smoke_required: {acceptance['local_file_smoke_required']}",
        (
            "- output_filesystem_smoke_required: "
            f"{acceptance['output_filesystem_smoke_required']}"
        ),
        (
            "- receipt_evidence_display_required: "
            f"{acceptance['receipt_evidence_display_required']}"
        ),
        (
            "- commercial_contamination_scan_required: "
            f"{acceptance['commercial_contamination_scan_required']}"
        ),
        (
            "- source_sha_manifest_hash_required: "
            f"{acceptance['source_sha_manifest_hash_required']}"
        ),
    ]
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
    parser = argparse.ArgumentParser(description="Build the P6-M3-0 prep handoff pack.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--json", action="store_true")
    mode.add_argument("--markdown", action="store_true")
    mode.add_argument("--check-artifacts", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    out_root = args.out_root.resolve()
    if args.check_artifacts:
        required = (out_root / OUTPUT_JSON, out_root / OUTPUT_MD)
        return 0 if all(path.exists() for path in required) else 1
    report = build_report(root=args.root.resolve())
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
