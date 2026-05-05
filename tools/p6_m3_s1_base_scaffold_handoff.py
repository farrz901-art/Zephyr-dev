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
DEFAULT_SCAFFOLD_ROOT = Path(".tmp/p6_m3_base_repo_scaffold")
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s1_base_scaffold_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"


class SummaryDict(TypedDict):
    overall: str
    m3_s1_status: str
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


def _required_paths(root: Path, scaffold_root: Path) -> dict[str, Path]:
    return {
        "scaffold_plan_doc": root / "docs/p6/P6_M3_BASE_REPO_SCAFFOLD_PLAN.md",
        "scaffold_manifest": root / "docs/p6/base_repo_scaffold_manifest.json",
        "consumption_doc": root / "docs/p6/P6_M3_BASE_PUBLIC_EXPORT_CONSUMPTION_PLAN.md",
        "consumption_plan": root / "docs/p6/base_public_export_consumption_plan.json",
        "bridge_doc": root / "docs/p6/P6_M3_BASE_BRIDGE_CONTRACT.md",
        "bridge_contract": root / "docs/p6/base_local_bridge_contract.json",
        "scaffold_tool": root / "tools/p6_m3_base_repo_scaffold_plan.py",
        "lineage_tool": root / "tools/p6_m3_base_lineage_gate.py",
        "boundary_tool": root / "tools/p6_m3_base_boundary_gate.py",
        "scaffold_report": scaffold_root / "report.json",
        "lineage_report": scaffold_root / "base_lineage_gate.json",
        "boundary_report": scaffold_root / "base_boundary_gate.json",
    }


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def build_report(*, root: Path, scaffold_root: Path) -> dict[str, object]:
    required_paths = _required_paths(root, scaffold_root)
    missing = [
        path.relative_to(root).as_posix()
        if path.is_relative_to(root)
        else str(path)
        for path in required_paths.values()
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError("; ".join(missing))
    scaffold_report = _load_json_object(required_paths["scaffold_report"])
    lineage_report = _load_json_object(required_paths["lineage_report"])
    boundary_report = _load_json_object(required_paths["boundary_report"])
    scaffold_summary = _as_dict(scaffold_report["summary"])
    scaffold_route = _as_dict(scaffold_report["base_route"])
    scaffold_boundary = _as_dict(scaffold_report["boundary"])
    lineage = _as_dict(lineage_report["lineage"])
    boundary_summary = _as_dict(boundary_report["summary"])
    active_blockers = cast(int, scaffold_summary["active_blockers"]) + cast(
        int, boundary_summary["active_blockers"]
    )
    overall = "pass" if active_blockers == 0 else "fail"
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s1.base_scaffold_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s1_status": "sealed" if active_blockers == 0 else "open",
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "scope": {
            "actual_repo_created": False,
            "actual_code_migration_performed": False,
            "shadow_scaffold_only": True,
            "production_tauri_app_implemented": False,
        },
        "base_route": {
            "desktop_stack": scaffold_route["desktop_stack"],
            "bridge_strategy": scaffold_route["bridge_strategy"],
            "python_to_rust_full_rewrite_required": scaffold_route[
                "python_to_rust_full_rewrite_required"
            ],
            "requires_p45_substrate": scaffold_route["requires_p45_substrate"],
        },
        "boundary": {
            "license_allowed": scaffold_boundary["license_allowed"],
            "entitlement_allowed": scaffold_boundary["entitlement_allowed"],
            "private_core_allowed": scaffold_boundary["private_core_allowed"],
            "web_core_dependency_allowed": scaffold_boundary["web_core_dependency_allowed"],
            "commercial_logic_allowed": scaffold_boundary["commercial_logic_allowed"],
        },
        "lineage": {
            "source_sha_recorded": _as_bool(lineage["source_sha_recorded"]),
            "public_manifest_hash_recorded": _as_bool(
                lineage["public_manifest_hash_recorded"]
            ),
            "bridge_contract_hash_recorded": _as_bool(
                lineage["bridge_contract_hash_recorded"]
            ),
            "combined_base_scaffold_lineage_sha256_present": bool(
                lineage["combined_base_scaffold_lineage_sha256"]
            ),
        },
        "gates": {
            "commercial_contamination_blockers": boundary_summary[
                "commercial_contamination_blockers"
            ],
            "forbidden_import_blockers": boundary_summary["forbidden_import_blockers"],
            "security_sensitive_runtime_blockers": boundary_summary[
                "security_sensitive_runtime_blockers"
            ],
        },
        "next_step": (
            "P6-M3-S2 can create or initialize the real Zephyr-base repo only after "
            "user authorization."
        ),
        "issues": [],
        "non_blocking_notes": [
            "S1 emits only a shadow scaffold and planning artifacts.",
            "S1 does not migrate runtime code or implement a real Tauri app.",
            "S2 still requires explicit user authorization before real repo creation.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    route = _as_dict(report["base_route"])
    boundary = _as_dict(report["boundary"])
    lineage = _as_dict(report["lineage"])
    gates = _as_dict(report["gates"])
    lines = [
        "# P6-M3-S1 Base repo scaffold handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        f"- M3-S1 status: {summary['m3_s1_status']}",
        "",
        "## What S1 completed",
        "- Base repo scaffold plan",
        "- Base public export consumption plan",
        "- Base local bridge contract",
        "- Base lineage gate",
        "- Base boundary gate",
        "- Base shadow scaffold output",
        "",
        "## What S1 did not do",
        "- did not create the real Zephyr-base repo",
        "- did not migrate runtime code",
        "- did not implement a real Tauri app",
        "- did not implement payment, license, entitlement, or update logic",
        "",
        "## Base scaffold structure",
        f"- desktop_stack: {route['desktop_stack']}",
        f"- bridge_strategy: {route['bridge_strategy']}",
        "- shadow_scaffold_only: true",
        "",
        "## Public export consumption",
        "- public export only",
        "- no private core",
        "- no Web-core dependency",
        "",
        "## Bridge contract",
        "- base_run_request_v1",
        "- base_run_result_v1",
        "- base_receipt_view_v1",
        "- base_error_v1",
        "",
        "## Lineage",
        f"- source_sha_recorded: {lineage['source_sha_recorded']}",
        (
            "- public_manifest_hash_recorded: "
            f"{lineage['public_manifest_hash_recorded']}"
        ),
        (
            "- bridge_contract_hash_recorded: "
            f"{lineage['bridge_contract_hash_recorded']}"
        ),
        (
            "- combined_base_scaffold_lineage_sha256_present: "
            f"{lineage['combined_base_scaffold_lineage_sha256_present']}"
        ),
        "",
        "## Boundary gates",
        (
            "- commercial_contamination_blockers: "
            f"{gates['commercial_contamination_blockers']}"
        ),
        f"- forbidden_import_blockers: {gates['forbidden_import_blockers']}",
        (
            "- security_sensitive_runtime_blockers: "
            f"{gates['security_sensitive_runtime_blockers']}"
        ),
        f"- license_allowed: {boundary['license_allowed']}",
        f"- entitlement_allowed: {boundary['entitlement_allowed']}",
        f"- private_core_allowed: {boundary['private_core_allowed']}",
        "",
        "## Next step",
        (
            "- S2 may create or initialize the real Zephyr-base repo only after "
            "explicit user authorization."
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
    parser = argparse.ArgumentParser(
        description="Build the P6-M3-S1 Base scaffold handoff pack."
    )
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
    root = args.root.resolve()
    out_root = _resolve_from_root(root, args.out_root)
    scaffold_root = root / DEFAULT_SCAFFOLD_ROOT
    if args.check_artifacts:
        required = (
            scaffold_root / "report.json",
            scaffold_root / "base_lineage_gate.json",
            scaffold_root / "base_boundary_gate.json",
            out_root / OUTPUT_JSON,
            out_root / OUTPUT_MD,
        )
        return 0 if all(path.exists() for path in required) else 1
    report = build_report(root=root, scaffold_root=scaffold_root)
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
