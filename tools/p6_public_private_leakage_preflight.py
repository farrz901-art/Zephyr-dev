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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m2_derivation")
DEFAULT_PUBLIC_PLAN = Path(".tmp/p6_m2_derivation/public_export_plan.json")
DEFAULT_PRIVATE_PLAN = Path(".tmp/p6_m2_derivation/private_export_plan.json")
DEFAULT_COMMERCIAL_SCAN = Path(".tmp/p6_m1_boundary/commercial_contamination_scan.json")
DEFAULT_FORBIDDEN_IMPORT_SCAN = Path(".tmp/p6_m1_boundary/forbidden_import_scan.json")
DEFAULT_SECURITY_SCAN = Path(".tmp/p6_m1_boundary/security_sensitive_path_scan.json")
OUTPUT_JSON = "leakage_preflight.json"
OUTPUT_MD = "leakage_preflight.md"


class _CommercialScanModule(Protocol):
    def load_denylist(self, path: Path) -> object: ...

    def scan_repo(self, *, root: Path, denylist: object) -> dict[str, object]: ...


class _RuleScanModule(Protocol):
    def load_rules(self, path: Path) -> object: ...

    def scan_repo(self, *, root: Path, rules: object) -> dict[str, object]: ...


class _PublicPlanModule(Protocol):
    def build_report(
        self,
        *,
        root: Path,
        manifest_path: Path,
        six_repo_manifest_path: Path,
    ) -> dict[str, object]: ...


class _PrivatePlanModule(Protocol):
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
    public_leakage_blockers: int
    private_leakage_blockers: int
    zephyr_dev_contamination_blockers: int
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


def _load_scan_modules() -> tuple[_CommercialScanModule, _RuleScanModule, _RuleScanModule]:
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
    )


def _load_plan_modules() -> tuple[_PublicPlanModule, _PrivatePlanModule]:
    import sys

    repo_root = _discover_repo_root(Path(__file__).resolve().parent)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return (
        cast(
            _PublicPlanModule,
            importlib.import_module("tools.p6_public_core_export_dry_run"),
        ),
        cast(
            _PrivatePlanModule,
            importlib.import_module("tools.p6_private_core_export_dry_run"),
        ),
    )


def _load_or_build_plan(
    *,
    root: Path,
    plan_path: Path,
    plan_kind: str,
) -> tuple[dict[str, object], str]:
    if plan_path.exists():
        return _load_json_object(plan_path), "provided_artifact"
    public_module, private_module = _load_plan_modules()
    if plan_kind == "public":
        report = public_module.build_report(
            root=root,
            manifest_path=root / "docs/p6/public_core_export_manifest.json",
            six_repo_manifest_path=root / "docs/p6/six_repo_manifest.json",
        )
    else:
        report = private_module.build_report(
            root=root,
            manifest_path=root / "docs/p6/private_core_export_manifest.json",
            six_repo_manifest_path=root / "docs/p6/six_repo_manifest.json",
        )
    return report, "rebuilt_from_repo_manifests"


def _load_or_build_scans(
    *,
    root: Path,
    commercial_scan_path: Path,
    forbidden_import_scan_path: Path,
    security_scan_path: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], list[str]]:
    commercial_tool, forbidden_tool, security_tool = _load_scan_modules()
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
        notes.append("Commercial contamination scan artifact was missing and was rebuilt.")
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
        notes.append("Security sensitive path scan artifact was missing and was rebuilt.")
    return commercial_scan, forbidden_scan, security_scan, notes


def build_report(
    *,
    root: Path,
    public_plan_path: Path,
    private_plan_path: Path,
    commercial_scan_path: Path,
    forbidden_import_scan_path: Path,
    security_scan_path: Path,
) -> dict[str, object]:
    public_plan, public_plan_source = _load_or_build_plan(
        root=root,
        plan_path=public_plan_path,
        plan_kind="public",
    )
    private_plan, private_plan_source = _load_or_build_plan(
        root=root,
        plan_path=private_plan_path,
        plan_kind="private",
    )
    commercial_scan, forbidden_scan, security_scan, scan_notes = _load_or_build_scans(
        root=root,
        commercial_scan_path=commercial_scan_path,
        forbidden_import_scan_path=forbidden_import_scan_path,
        security_scan_path=security_scan_path,
    )
    public_forbidden = _as_dict(public_plan["forbidden_export_check"])
    private_forbidden = _as_dict(private_plan["forbidden_export_check"])
    commercial_summary = _as_dict(commercial_scan["summary"])
    forbidden_summary = _as_dict(forbidden_scan["summary"])
    security_summary = _as_dict(security_scan["summary"])
    public_blockers = sum(
        1
        for value in (
            public_forbidden["commercial_logic_in_public_export"],
            public_forbidden["private_core_in_public_export"],
            public_forbidden["secret_risk_in_public_export"],
        )
        if value is True
    )
    private_blockers = sum(
        1
        for value in (
            private_forbidden["commercial_secrets_in_private_export_plan"],
            private_forbidden["commercial_decision_code_back_to_zephyr_dev"],
        )
        if value is True
    )
    contamination_blockers = _as_int(commercial_summary["blocker_count"])
    forbidden_import_blockers = _as_int(forbidden_summary["blocker_count"])
    security_blockers = _as_int(security_summary["blocked_runtime_paths"])
    active_blockers = (
        public_blockers
        + private_blockers
        + contamination_blockers
        + forbidden_import_blockers
        + security_blockers
    )
    overall = "pass" if active_blockers == 0 else "fail"
    issues: list[str] = []
    notes = [
        "Leakage preflight is dry-run only and does not create downstream repos.",
        f"Public plan source: {public_plan_source}.",
        f"Private plan source: {private_plan_source}.",
        *scan_notes,
    ]
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m2.public_private_leakage_preflight.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "active_blockers": active_blockers,
            "public_leakage_blockers": public_blockers,
            "private_leakage_blockers": private_blockers,
            "zephyr_dev_contamination_blockers": contamination_blockers,
            "forbidden_import_blockers": forbidden_import_blockers,
            "security_sensitive_runtime_blockers": security_blockers,
        },
        "actual_export_performed": False,
        "code_migration_performed": False,
        "target_repos_created": False,
        "issues": issues,
        "non_blocking_notes": notes,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    lines = [
        "# P6 public/private leakage preflight",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active_blockers: {summary['active_blockers']}",
        "",
        "## Leakage summary",
        f"- public_leakage_blockers: {summary['public_leakage_blockers']}",
        f"- private_leakage_blockers: {summary['private_leakage_blockers']}",
        (
            f"- zephyr_dev_contamination_blockers: "
            f"{summary['zephyr_dev_contamination_blockers']}"
        ),
        f"- forbidden_import_blockers: {summary['forbidden_import_blockers']}",
        (
            f"- security_sensitive_runtime_blockers: "
            f"{summary['security_sensitive_runtime_blockers']}"
        ),
        "",
        "## Dry-run status",
        f"- actual_export_performed: {report['actual_export_performed']}",
        f"- code_migration_performed: {report['code_migration_performed']}",
        f"- target_repos_created: {report['target_repos_created']}",
        "",
        "## Notes",
    ]
    for note in cast(list[str], report["non_blocking_notes"]):
        lines.append(f"- {note}")
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
    parser = argparse.ArgumentParser(description="Run public/private leakage preflight checks.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--public-plan", type=Path, default=DEFAULT_PUBLIC_PLAN)
    parser.add_argument("--private-plan", type=Path, default=DEFAULT_PRIVATE_PLAN)
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
    out_root = args.out_root.resolve()
    if args.check_artifacts:
        required = (out_root / OUTPUT_JSON, out_root / OUTPUT_MD)
        return 0 if all(path.exists() for path in required) else 1
    report = build_report(
        root=root,
        public_plan_path=args.public_plan.resolve(),
        private_plan_path=args.private_plan.resolve(),
        commercial_scan_path=args.commercial_scan.resolve(),
        forbidden_import_scan_path=args.forbidden_import_scan.resolve(),
        security_scan_path=args.security_sensitive_scan.resolve(),
    )
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
