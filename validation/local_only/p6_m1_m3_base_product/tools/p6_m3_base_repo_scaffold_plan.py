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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_base_repo_scaffold")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
SHADOW_REPO_NAME = "Zephyr-base-shadow"


class SummaryDict(TypedDict):
    overall: str
    active_blockers: int
    major_gaps: int
    shadow_scaffold_emitted: bool
    actual_repo_created: bool
    actual_code_migration_performed: bool


class ShadowScaffoldDict(TypedDict):
    path: str
    directories_created: int
    files_created: int
    placeholder_only_markers: int


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


def _as_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list, got {type(value).__name__}")
    return cast(list[object], value)


def _as_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected bool, got {type(value).__name__}")
    return value


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value).__name__}")
    return value


def _required_paths(root: Path) -> dict[str, Path]:
    return {
        "tech_decision": root / "docs/p6/P6_M3_BASE_DESKTOP_TECH_DECISION.md",
        "product_boundary": root / "docs/p6/P6_M3_BASE_PRODUCT_BOUNDARY.md",
        "acceptance_gates": root / "docs/p6/P6_M3_BASE_ACCEPTANCE_GATES.md",
        "capability_manifest": root / "docs/p6/base_product_capability_manifest.json",
        "design_asset_request": root / "docs/p6/base_design_asset_request.json",
        "public_export_manifest": root / "docs/p6/public_core_export_manifest.json",
        "six_repo_manifest": root / "docs/p6/six_repo_manifest.json",
        "scaffold_manifest": root / "docs/p6/base_repo_scaffold_manifest.json",
        "consumption_plan": root / "docs/p6/base_public_export_consumption_plan.json",
        "bridge_contract": root / "docs/p6/base_local_bridge_contract.json",
    }


def _placeholder_header(title: str) -> str:
    return (
        f"# {title}\n\n"
        "placeholder only\n"
        "generated for scaffold planning\n"
        "not production implementation\n"
    )


def _shadow_file_map(
    *,
    root: Path,
) -> dict[str, str]:
    capability_manifest = _load_json_object(root / "docs/p6/base_product_capability_manifest.json")
    design_request = _load_json_object(root / "docs/p6/base_design_asset_request.json")
    bridge_contract = _load_json_object(root / "docs/p6/base_local_bridge_contract.json")
    contracts = _as_dict(bridge_contract["contracts"])
    return {
        "README.md": (
            "# Zephyr-base shadow scaffold\n\n"
            "placeholder only\n"
            "generated for scaffold planning\n"
            "not production implementation\n\n"
            "Zephyr-base is a future public free desktop product.\n"
            "Local-only is the default route.\n"
            "No license.\n"
            "No entitlement.\n"
            "No private core.\n"
            "No Web-core dependency.\n"
            "not production-ready yet.\n"
        ),
        "PRODUCT_BOUNDARY.md": _placeholder_header("Product Boundary"),
        "LICENSE": (
            "placeholder only\n"
            "generated for scaffold planning\n"
            "not production implementation\n"
        ),
        "NOTICE.md": _placeholder_header("NOTICE Placeholder"),
        "docs/USER_GUIDE.md": _placeholder_header("User Guide"),
        "docs/LOCAL_ONLY_PRIVACY.md": _placeholder_header("Local Only Privacy"),
        "docs/TROUBLESHOOTING.md": _placeholder_header("Troubleshooting"),
        "docs/SOURCE_LINEAGE.md": _placeholder_header("Source Lineage"),
        "src-tauri/Cargo.toml": (
            "[package]\n"
            'name = "zephyr-base-shadow"\n'
            'version = "0.0.0-shadow"\n'
            "placeholder_only = true\n"
        ),
        "src-tauri/tauri.conf.json": json.dumps(
            {
                "placeholder_only": True,
                "generated_for": "scaffold_planning",
                "not_production_implementation": True,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        "src-tauri/src/main.rs": (
            "// placeholder only\n"
            "// generated for scaffold planning\n"
            "// not production implementation\n"
        ),
        "src-tauri/src/commands.rs": (
            "// placeholder only\n"
            "// generated for scaffold planning\n"
            "// not production implementation\n"
        ),
        "src-tauri/src/bridge.rs": (
            "// placeholder only\n"
            "// generated for scaffold planning\n"
            "// not production implementation\n"
        ),
        "src-tauri/src/errors.rs": (
            "// placeholder only\n"
            "// generated for scaffold planning\n"
            "// not production implementation\n"
        ),
        "src-tauri/src/lineage.rs": (
            "// placeholder only\n"
            "// generated for scaffold planning\n"
            "// not production implementation\n"
        ),
        "ui/package.json": json.dumps(
            {
                "name": "zephyr-base-shadow-ui",
                "private": True,
                "placeholder_only": True,
                "generated_for": "scaffold_planning",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        "ui/index.html": (
            "<!-- placeholder only -->\n"
            "<!-- generated for scaffold planning -->\n"
            "<!-- not production implementation -->\n"
        ),
        "ui/src/App.tsx": (
            "// placeholder only\n"
            "// generated for scaffold planning\n"
            "// not production implementation\n"
        ),
        "ui/src/components/Welcome.tsx": "// placeholder only\n",
        "ui/src/components/FileDropZone.tsx": "// placeholder only\n",
        "ui/src/components/ProgressPanel.tsx": "// placeholder only\n",
        "ui/src/components/ResultSummary.tsx": "// placeholder only\n",
        "ui/src/components/EvidenceCard.tsx": "// placeholder only\n",
        "ui/src/components/ReceiptCard.tsx": "// placeholder only\n",
        "ui/src/components/UsageFactCard.tsx": "// placeholder only\n",
        "ui/src/components/ErrorDiagnosisPanel.tsx": "// placeholder only\n",
        "ui/src/styles/tokens.css": "/* placeholder only */\n",
        "public-core-bridge/bridge_contract.json": json.dumps(
            {
                "placeholder_only": True,
                "generated_for": "scaffold_planning",
                "contracts": list(contracts.keys()),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        "public-core-bridge/sample_input_request.json": json.dumps(
            _as_dict(contracts["base_run_request_v1"]),
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        "public-core-bridge/sample_output_artifact.json": json.dumps(
            _as_dict(contracts["base_run_result_v1"]),
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        "public-core-bridge/run_public_core.placeholder.py": (
            "# placeholder only\n"
            "# generated for scaffold planning\n"
            "# not production implementation\n"
        ),
        "manifests/base_capability_manifest.json": json.dumps(
            capability_manifest, ensure_ascii=False, indent=2
        )
        + "\n",
        "manifests/public_export_lineage.json": json.dumps(
            {
                "placeholder_only": True,
                "generated_for": "scaffold_planning",
                "source_sha": "record-later",
                "public_manifest_hash": "record-later",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        "manifests/design_asset_request.json": json.dumps(
            design_request, ensure_ascii=False, indent=2
        )
        + "\n",
        "tests/smoke/local_file_flow_smoke.md": _placeholder_header(
            "Local File Flow Smoke"
        ),
        "tests/smoke/boundary_scan_smoke.md": _placeholder_header(
            "Boundary Scan Smoke"
        ),
        "packaging/windows/installer_plan.md": _placeholder_header(
            "Windows Installer Plan"
        ),
        "packaging/update/update_manifest_consumer_plan.md": _placeholder_header(
            "Update Manifest Consumer Plan"
        ),
    }


def _display_path(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _emit_shadow_scaffold(*, root: Path, out_root: Path) -> ShadowScaffoldDict:
    shadow_root = out_root / SHADOW_REPO_NAME
    file_map = _shadow_file_map(root=root)
    directories = {Path(rel).parent for rel in file_map}
    for directory in sorted(directories):
        target_dir = shadow_root / directory
        target_dir.mkdir(parents=True, exist_ok=True)
    for relative_path, contents in file_map.items():
        target = shadow_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents, encoding="utf-8")
    placeholder_markers = sum(contents.count("placeholder only") for contents in file_map.values())
    return {
        "path": _display_path(root, shadow_root),
        "directories_created": len(directories),
        "files_created": len(file_map),
        "placeholder_only_markers": placeholder_markers,
    }


def build_report(
    *,
    root: Path,
    out_root: Path,
    emit_shadow_scaffold: bool,
) -> dict[str, object]:
    required_paths = _required_paths(root)
    issues = [
        f"missing required file: {path.relative_to(root).as_posix()}"
        for path in required_paths.values()
        if not path.exists()
    ]
    manifest = _load_json_object(required_paths["scaffold_manifest"])
    capability_manifest = _load_json_object(required_paths["capability_manifest"])
    design_request = _load_json_object(required_paths["design_asset_request"])
    shadow_scaffold: ShadowScaffoldDict = {
        "path": _display_path(root, out_root / SHADOW_REPO_NAME),
        "directories_created": 0,
        "files_created": 0,
        "placeholder_only_markers": 0,
    }
    if not issues and emit_shadow_scaffold:
        shadow_scaffold = _emit_shadow_scaffold(root=root, out_root=out_root)
    overall = "pass" if not issues else "fail"
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.base_repo_scaffold_plan.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "active_blockers": len(issues),
            "major_gaps": 0,
            "shadow_scaffold_emitted": emit_shadow_scaffold and not issues,
            "actual_repo_created": False,
            "actual_code_migration_performed": False,
        },
        "base_route": {
            "desktop_stack": _as_str(manifest["desktop_stack"]),
            "bridge_strategy": _as_str(manifest["bridge_strategy"]),
            "python_to_rust_full_rewrite_required": False,
            "requires_p45_substrate": _as_bool(manifest["requires_p45_substrate"]),
            "local_only_default": _as_bool(capability_manifest["base_local_only_default"]),
        },
        "boundary": {
            "license_allowed": _as_bool(manifest["license_allowed"]),
            "entitlement_allowed": _as_bool(manifest["entitlement_allowed"]),
            "private_core_allowed": _as_bool(manifest["private_core_allowed"]),
            "web_core_dependency_allowed": _as_bool(manifest["web_core_dependency_allowed"]),
            "commercial_logic_allowed": _as_bool(manifest["commercial_logic_allowed"]),
        },
        "shadow_scaffold": shadow_scaffold,
        "design_window": {
            "design_window_input_required": _as_bool(
                design_request["design_window_input_required"]
            ),
            "requested_assets_count": len(_as_list(design_request["requested_assets"])),
        },
        "production_runtime_implemented": False,
        "issues": issues,
        "non_blocking_notes": [
            "Shadow scaffold is placeholder-only and does not create a real repository.",
            "No runtime source is migrated from Zephyr-dev in this step.",
            "No real Tauri build, installer, or packaging artifact is produced.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    base_route = _as_dict(report["base_route"])
    boundary = _as_dict(report["boundary"])
    shadow = cast(ShadowScaffoldDict, report["shadow_scaffold"])
    lines = [
        "# P6-M3-S1 Base repo scaffold plan",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- shadow_scaffold_emitted: {summary['shadow_scaffold_emitted']}",
        f"- actual_repo_created: {summary['actual_repo_created']}",
        f"- actual_code_migration_performed: {summary['actual_code_migration_performed']}",
        "",
        "## Base route",
        f"- desktop_stack: {base_route['desktop_stack']}",
        f"- bridge_strategy: {base_route['bridge_strategy']}",
        (
            "- python_to_rust_full_rewrite_required: "
            f"{base_route['python_to_rust_full_rewrite_required']}"
        ),
        f"- requires_p45_substrate: {base_route['requires_p45_substrate']}",
        f"- local_only_default: {base_route['local_only_default']}",
        "",
        "## Boundary",
        f"- license_allowed: {boundary['license_allowed']}",
        f"- entitlement_allowed: {boundary['entitlement_allowed']}",
        f"- private_core_allowed: {boundary['private_core_allowed']}",
        (
            "- web_core_dependency_allowed: "
            f"{boundary['web_core_dependency_allowed']}"
        ),
        "",
        "## Shadow scaffold",
        f"- path: {shadow['path']}",
        f"- directories_created: {shadow['directories_created']}",
        f"- files_created: {shadow['files_created']}",
        f"- placeholder_only_markers: {shadow['placeholder_only_markers']}",
        "",
        "## Scope guardrails",
        "- shadow scaffold only",
        "- no real repository created",
        "- no runtime code migration performed",
        "- no production Tauri app implemented",
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
        description="Build the P6-M3-S1 Base repo scaffold plan."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--emit-shadow-scaffold", action="store_true")
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
    if args.check_artifacts:
        required = (out_root / OUTPUT_JSON, out_root / OUTPUT_MD)
        return 0 if all(path.exists() for path in required) else 1
    report = build_report(
        root=root,
        out_root=out_root,
        emit_shadow_scaffold=bool(args.emit_shadow_scaffold),
    )
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
