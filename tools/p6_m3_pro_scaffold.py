from __future__ import annotations

import argparse
import importlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, TypedDict, cast


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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_scaffold")
OUTPUT_JSON = "pro_scaffold.json"
OUTPUT_MD = "pro_scaffold.md"

EXTRA_CAPABILITIES = (
    "retained_connector_matrix_private_access",
    "destination_evidence_private_tools",
    "advanced_source_destination_orchestration",
)


class _PrivateExportModule(Protocol):
    def build_report(
        self,
        *,
        root: Path,
        manifest_path: Path,
        six_repo_manifest_path: Path,
    ) -> dict[str, object]: ...


class SummaryDict(TypedDict):
    overall: str
    active_blockers: int
    major_gaps: int
    actual_export_performed: bool
    code_migration_performed: bool
    target_repos_created: bool


class ScaffoldFileDict(TypedDict):
    path: str
    purpose: str
    status: str
    design_window_input_required: bool


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


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value).__name__}")
    return value


def _load_private_export_module() -> _PrivateExportModule:
    import sys

    repo_root = _discover_repo_root(Path(__file__).resolve().parent)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return cast(
        _PrivateExportModule,
        importlib.import_module("tools.p6_private_core_export_dry_run"),
    )


def _scaffold_files() -> list[ScaffoldFileDict]:
    return [
        {
            "path": "derivation/pro/installer/package_manifest.template.json",
            "purpose": "Installer metadata skeleton for the future Pro desktop product.",
            "status": "placeholder_only",
            "design_window_input_required": False,
        },
        {
            "path": "derivation/pro/runtime/cli_hooks.py",
            "purpose": "CLI hook placeholder for Pro dry-run, diagnostics, and smoke commands.",
            "status": "placeholder_only",
            "design_window_input_required": False,
        },
        {
            "path": "derivation/pro/runtime/extra_capabilities.json",
            "purpose": "Extra capability placeholder without entitlement or billing logic.",
            "status": "placeholder_only",
            "design_window_input_required": False,
        },
        {
            "path": "derivation/pro/runtime/private_connector_matrix.json",
            "purpose": "Private retained connector matrix placeholder for later product wiring.",
            "status": "placeholder_only",
            "design_window_input_required": False,
        },
    ]


def build_report(*, root: Path) -> dict[str, object]:
    export_module = _load_private_export_module()
    export_report = export_module.build_report(
        root=root,
        manifest_path=root / "docs/p6/private_core_export_manifest.json",
        six_repo_manifest_path=root / "docs/p6/six_repo_manifest.json",
    )
    export_summary = _as_dict(export_report["summary"])
    export_items = cast(list[dict[str, object]], _as_list(export_report["export_items"]))
    placeholder_items = sum(1 for item in export_items if item["path_status"] == "draft_only")
    issues: list[str] = []
    if export_summary["overall"] != "pass":
        issues.append("Private core export prerequisite is not in pass state for Pro scaffold.")
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.pro_scaffold.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": "pass" if not issues else "fail",
            "active_blockers": len(issues),
            "major_gaps": 0,
            "actual_export_performed": False,
            "code_migration_performed": False,
            "target_repos_created": False,
        },
        "consumer_repo": "Zephyr-Pro",
        "export_scope": "private_core_plus_extra",
        "private_export_report_id": _as_str(export_report["report_id"]),
        "export_items": len(export_items),
        "placeholder_export_items": placeholder_items,
        "extra_capabilities": list(EXTRA_CAPABILITIES),
        "scaffold_files": _scaffold_files(),
        "runtime_cli_hooks": [
            "zephyr-pro run --retained-connector-matrix",
            "zephyr-pro diagnose --delivery-evidence",
            "zephyr-pro smoke --matrix retained-private",
        ],
        "installer_skeleton": {
            "windows": "placeholder_only_private_installer_later",
            "macos": "placeholder_only_private_bundle_later",
            "linux": "placeholder_only_private_package_later",
            "real_installer_created": False,
        },
        "dependency_surface": {
            "python_dependencies": [
                "Python 3.12",
                "uv workspace",
                "zephyr-core contracts",
                "private retained connector surfaces",
            ],
            "os_dependencies": [
                "Git CLI",
                "Private installer tooling later",
                "Signing toolchain later",
            ],
            "manual_uv_add_required_by_user": [],
        },
        "design_window_hooks": [
            "Pro shell UI remains placeholder-only and needs later design-window input.",
            "Entitlement status display is a hook only; no commercial logic is implemented here.",
        ],
        "boundary_checks": {
            "uses_private_core_subset": True,
            "commercial_truth_owned_here": False,
            "entitlement_client_placeholder_only": True,
            "commercial_logic_written_to_zephyr_dev_runtime": False,
            "real_execution_performed": False,
        },
        "issues": issues,
        "non_blocking_notes": [
            "Pro scaffold is dry-run only and does not create a real private repo.",
            (
                "Pro scaffold may consume private core subset later, but commercial "
                "truth stays in Web-core."
            ),
            (
                "Extra capabilities are matrix/audit/orchestration placeholders, "
                "not entitlement logic."
            ),
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    files = cast(list[ScaffoldFileDict], report["scaffold_files"])
    lines = [
        "# P6-M3 Pro scaffold",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- actual_export_performed: {summary['actual_export_performed']}",
        f"- code_migration_performed: {summary['code_migration_performed']}",
        "",
        "## Private subset boundary",
        "- consumer_repo: Zephyr-Pro",
        "- export_scope: private_core_plus_extra",
        "- commercial truth remains outside Pro",
        "- no commercial logic is written back into Zephyr-dev runtime",
        "",
        "## Scaffold files",
    ]
    for item in files:
        lines.append(
            f"- {item['path']} | status={item['status']} | purpose={item['purpose']}"
        )
    lines.extend(
        [
            "",
            "## Dependency surface",
            "- Python 3.12 / uv workspace reused",
            "- no manual `uv add` is required in this dry-run scaffold",
            "- signing, packaging, and entitlement infrastructure stay later-phase placeholders",
        ]
    )
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
    parser = argparse.ArgumentParser(description="Build the P6-M3 Pro scaffold dry-run report.")
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
