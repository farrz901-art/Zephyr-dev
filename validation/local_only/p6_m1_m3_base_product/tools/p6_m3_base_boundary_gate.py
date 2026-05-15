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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_base_repo_scaffold")
DEFAULT_LINEAGE = Path(".tmp/p6_m3_base_repo_scaffold/base_lineage_gate.json")
DEFAULT_COMMERCIAL_SCAN = Path(".tmp/p6_m1_boundary/commercial_contamination_scan.json")
DEFAULT_FORBIDDEN_IMPORT_SCAN = Path(".tmp/p6_m1_boundary/forbidden_import_scan.json")
DEFAULT_SECURITY_SCAN = Path(".tmp/p6_m1_boundary/security_sensitive_path_scan.json")
OUTPUT_JSON = "base_boundary_gate.json"
OUTPUT_MD = "base_boundary_gate.md"


class _CommercialScanModule(Protocol):
    def load_denylist(self, path: Path) -> object: ...

    def scan_repo(self, *, root: Path, denylist: object) -> dict[str, object]: ...


class _RuleScanModule(Protocol):
    def load_rules(self, path: Path) -> object: ...

    def scan_repo(self, *, root: Path, rules: object) -> dict[str, object]: ...


class _LineageModule(Protocol):
    def build_report(self, *, root: Path) -> dict[str, object]: ...


class SummaryDict(TypedDict):
    overall: str
    active_blockers: int
    commercial_contamination_blockers: int
    forbidden_import_blockers: int
    security_sensitive_runtime_blockers: int


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


def _as_int(value: object) -> int:
    if not isinstance(value, int):
        raise ValueError(f"Expected int, got {type(value).__name__}")
    return value


def _as_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected bool, got {type(value).__name__}")
    return value


def _load_modules() -> tuple[
    _CommercialScanModule, _RuleScanModule, _RuleScanModule, _LineageModule
]:
    import sys

    repo_root = _discover_repo_root(Path(__file__).resolve().parent)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return (
        cast(
            _CommercialScanModule,
            importlib.import_module("tools.p6_commercial_contamination_scan"),
        ),
        cast(_RuleScanModule, importlib.import_module("tools.p6_forbidden_import_scan")),
        cast(
            _RuleScanModule,
            importlib.import_module("tools.p6_security_sensitive_path_scan"),
        ),
        cast(_LineageModule, importlib.import_module("tools.p6_m3_base_lineage_gate")),
    )


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _load_or_build_lineage(
    *,
    root: Path,
    lineage_path: Path,
    out_root: Path,
) -> tuple[dict[str, object], list[str]]:
    _, _, _, lineage_tool = _load_modules()
    if lineage_path.exists():
        return _load_json_object(lineage_path), []
    report = lineage_tool.build_report(root=root)
    target = out_root / "base_lineage_gate.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report, ["Lineage artifact was missing and was rebuilt from repo-tracked inputs."]


def _load_or_build_scans(
    *,
    root: Path,
    commercial_scan_path: Path,
    forbidden_import_scan_path: Path,
    security_scan_path: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], list[str]]:
    commercial_tool, forbidden_tool, security_tool, _ = _load_modules()
    notes: list[str] = []
    if commercial_scan_path.exists():
        commercial_scan = _load_json_object(commercial_scan_path)
    else:
        commercial_scan = commercial_tool.scan_repo(
            root=root,
            denylist=commercial_tool.load_denylist(
                root / "docs/p6/commercial_contamination_denylist.json"
            ),
        )
        notes.append("Commercial scan artifact was missing and was rebuilt.")
    if forbidden_import_scan_path.exists():
        forbidden_scan = _load_json_object(forbidden_import_scan_path)
    else:
        forbidden_scan = forbidden_tool.scan_repo(
            root=root,
            rules=forbidden_tool.load_rules(root / "docs/p6/forbidden_import_map.json"),
        )
        notes.append("Forbidden import scan artifact was missing and was rebuilt.")
    if security_scan_path.exists():
        security_scan = _load_json_object(security_scan_path)
    else:
        security_scan = security_tool.scan_repo(
            root=root,
            rules=security_tool.load_rules(root / "docs/p6/security_sensitive_paths.json"),
        )
        notes.append("Security sensitive scan artifact was missing and was rebuilt.")
    return commercial_scan, forbidden_scan, security_scan, notes


def build_report(
    *,
    root: Path,
    out_root: Path,
    lineage_path: Path,
    commercial_scan_path: Path,
    forbidden_import_scan_path: Path,
    security_scan_path: Path,
) -> dict[str, object]:
    lineage_report, lineage_notes = _load_or_build_lineage(
        root=root,
        lineage_path=lineage_path,
        out_root=out_root,
    )
    commercial_scan, forbidden_scan, security_scan, scan_notes = _load_or_build_scans(
        root=root,
        commercial_scan_path=commercial_scan_path,
        forbidden_import_scan_path=forbidden_import_scan_path,
        security_scan_path=security_scan_path,
    )
    scaffold_manifest = _load_json_object(root / "docs/p6/base_repo_scaffold_manifest.json")
    lineage = _as_dict(lineage_report["lineage"])
    commercial_summary = _as_dict(commercial_scan["summary"])
    forbidden_summary = _as_dict(forbidden_scan["summary"])
    security_summary = _as_dict(security_scan["summary"])
    issues: list[str] = []
    if _as_int(commercial_summary["blocker_count"]) != 0:
        issues.append("Commercial contamination blocker_count must be zero.")
    if _as_int(forbidden_summary["blocker_count"]) != 0:
        issues.append("Forbidden import blocker_count must be zero.")
    if _as_int(security_summary["blocked_runtime_paths"]) != 0:
        issues.append("Security sensitive blocked_runtime_paths must be zero.")
    if _as_bool(scaffold_manifest["license_allowed"]):
        issues.append("Base manifest license_allowed must remain false.")
    if _as_bool(scaffold_manifest["entitlement_allowed"]):
        issues.append("Base manifest entitlement_allowed must remain false.")
    if _as_bool(scaffold_manifest["private_core_allowed"]):
        issues.append("Base manifest private_core_allowed must remain false.")
    if _as_bool(scaffold_manifest["web_core_dependency_allowed"]):
        issues.append("Base manifest web_core_dependency_allowed must remain false.")
    if _as_bool(scaffold_manifest["requires_p45_substrate"]):
        issues.append("Base manifest requires_p45_substrate must remain false.")
    if _as_bool(scaffold_manifest["actual_repo_created"]) is not False:
        issues.append("Base manifest actual_repo_created must remain false.")
    if _as_bool(scaffold_manifest["actual_code_migration_performed"]) is not False:
        issues.append("Base manifest actual_code_migration_performed must remain false.")
    if not _as_bool(lineage["public_manifest_hash_recorded"]):
        issues.append("Public manifest hash must be recorded.")
    if not _as_bool(lineage["bridge_contract_hash_recorded"]):
        issues.append("Bridge contract hash must be recorded.")
    source_sha = cast(str, lineage["source_sha"])
    if source_sha == "unknown":
        issues.append("Source SHA must be known for Base lineage gate.")
    active_blockers = len(issues)
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.base_boundary_gate.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": "pass" if active_blockers == 0 else "fail",
            "active_blockers": active_blockers,
            "commercial_contamination_blockers": _as_int(commercial_summary["blocker_count"]),
            "forbidden_import_blockers": _as_int(forbidden_summary["blocker_count"]),
            "security_sensitive_runtime_blockers": _as_int(
                security_summary["blocked_runtime_paths"]
            ),
        },
        "boundary": {
            "license_allowed": _as_bool(scaffold_manifest["license_allowed"]),
            "entitlement_allowed": _as_bool(scaffold_manifest["entitlement_allowed"]),
            "private_core_allowed": _as_bool(scaffold_manifest["private_core_allowed"]),
            "web_core_dependency_allowed": _as_bool(
                scaffold_manifest["web_core_dependency_allowed"]
            ),
            "commercial_logic_allowed": _as_bool(scaffold_manifest["commercial_logic_allowed"]),
            "requires_p45_substrate": _as_bool(scaffold_manifest["requires_p45_substrate"]),
            "actual_repo_created": _as_bool(scaffold_manifest["actual_repo_created"]),
            "actual_code_migration_performed": _as_bool(
                scaffold_manifest["actual_code_migration_performed"]
            ),
        },
        "lineage": {
            "source_sha_recorded": source_sha != "unknown",
            "public_manifest_hash_recorded": _as_bool(
                lineage["public_manifest_hash_recorded"]
            ),
            "bridge_contract_hash_recorded": _as_bool(
                lineage["bridge_contract_hash_recorded"]
            ),
        },
        "issues": issues,
        "non_blocking_notes": [
            *lineage_notes,
            *scan_notes,
            "Boundary gate validates scaffold planning only and does not create a repo.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    boundary = _as_dict(report["boundary"])
    lines = [
        "# P6-M3-S1 Base boundary gate",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active_blockers: {summary['active_blockers']}",
        (
            "- commercial_contamination_blockers: "
            f"{summary['commercial_contamination_blockers']}"
        ),
        f"- forbidden_import_blockers: {summary['forbidden_import_blockers']}",
        (
            "- security_sensitive_runtime_blockers: "
            f"{summary['security_sensitive_runtime_blockers']}"
        ),
        "",
        "## Boundary",
        f"- license_allowed: {boundary['license_allowed']}",
        f"- entitlement_allowed: {boundary['entitlement_allowed']}",
        f"- private_core_allowed: {boundary['private_core_allowed']}",
        (
            "- web_core_dependency_allowed: "
            f"{boundary['web_core_dependency_allowed']}"
        ),
        f"- requires_p45_substrate: {boundary['requires_p45_substrate']}",
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
    parser = argparse.ArgumentParser(description="Build the Base scaffold boundary gate.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--lineage", type=Path, default=DEFAULT_LINEAGE)
    parser.add_argument("--commercial-scan", type=Path, default=DEFAULT_COMMERCIAL_SCAN)
    parser.add_argument("--forbidden-import-scan", type=Path, default=DEFAULT_FORBIDDEN_IMPORT_SCAN)
    parser.add_argument("--security-sensitive-scan", type=Path, default=DEFAULT_SECURITY_SCAN)
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
    if args.check_artifacts:
        required = (out_root / OUTPUT_JSON, out_root / OUTPUT_MD)
        return 0 if all(path.exists() for path in required) else 1
    report = build_report(
        root=root,
        out_root=out_root,
        lineage_path=_resolve_from_root(root, args.lineage),
        commercial_scan_path=_resolve_from_root(root, args.commercial_scan),
        forbidden_import_scan_path=_resolve_from_root(root, args.forbidden_import_scan),
        security_scan_path=_resolve_from_root(root, args.security_sensitive_scan),
    )
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
