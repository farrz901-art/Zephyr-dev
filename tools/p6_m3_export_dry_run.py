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

PUBLIC_EXPORT_JSON = "public_core_export.json"
PRIVATE_EXPORT_JSON = "private_core_export.json"
SMOKE_JSON = "representative_nm_smoke.json"
REPORT_JSON = "report.json"
PUBLIC_EXPORT_MD = "public_core_export.md"
PRIVATE_EXPORT_MD = "private_core_export.md"
SMOKE_MD = "representative_nm_smoke.md"
REPORT_MD = "report.md"

SOURCE_IDS = (
    "source.uns.http_document.v1",
    "source.uns.s3_document.v1",
    "source.uns.git_document.v1",
    "source.uns.google_drive_document.v1",
    "source.uns.confluence_document.v1",
    "source.it.http_json_cursor.v1",
    "source.it.postgresql_incremental.v1",
    "source.it.clickhouse_incremental.v1",
    "source.it.kafka_partition_offset.v1",
    "source.it.mongodb_incremental.v1",
)
DESTINATION_IDS = (
    "filesystem",
    "destination.webhook.v1",
    "destination.kafka.v1",
    "destination.weaviate.v1",
    "destination.s3.v1",
    "destination.opensearch.v1",
    "destination.clickhouse.v1",
    "destination.mongodb.v1",
    "destination.loki.v1",
    "destination.sqlite.v1",
)

BASE_REALISH_SOURCES = {
    "source.uns.http_document.v1",
    "source.uns.git_document.v1",
}
BASE_REALISH_DESTINATIONS = {
    "filesystem",
    "destination.webhook.v1",
    "destination.sqlite.v1",
}


class _BaseModule(Protocol):
    def build_report(self, *, root: Path) -> dict[str, object]: ...

    def render_markdown(self, report: dict[str, object]) -> str: ...


class _ProModule(Protocol):
    def build_report(self, *, root: Path) -> dict[str, object]: ...

    def render_markdown(self, report: dict[str, object]) -> str: ...


class _CommercialScanModule(Protocol):
    def load_denylist(self, path: Path) -> object: ...

    def scan_repo(self, *, root: Path, denylist: object) -> dict[str, object]: ...


class SummaryDict(TypedDict):
    overall: str
    active_blockers: int
    review_required_issues: int
    actual_export_performed: bool
    code_migration_performed: bool
    target_repos_created: bool


class SmokeProductSummaryDict(TypedDict):
    product: str
    total_routes: int
    dry_run_contract_ready_routes: int
    placeholder_only_routes: int
    real_execution_routes: int


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


def _load_modules() -> tuple[_BaseModule, _ProModule, _CommercialScanModule]:
    import sys

    repo_root = _discover_repo_root(Path(__file__).resolve().parent)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return (
        cast(_BaseModule, importlib.import_module("tools.p6_m3_base_scaffold")),
        cast(_ProModule, importlib.import_module("tools.p6_m3_pro_scaffold")),
        cast(
            _CommercialScanModule,
            importlib.import_module("tools.p6_commercial_contamination_scan"),
        ),
    )


def _build_route(
    *,
    product: str,
    source_id: str,
    destination_id: str,
) -> dict[str, object]:
    if product == "Zephyr-base":
        dry_run_ready = (
            source_id in BASE_REALISH_SOURCES and destination_id in BASE_REALISH_DESTINATIONS
        )
        execution_status = "dry_run_contract_ready" if dry_run_ready else "placeholder_only"
        notes = (
            [
                "Public subset route aligned with Base local/document-first scaffold boundary.",
            ]
            if dry_run_ready
            else [
                "Route remains placeholder-only for Base until later public product narrowing.",
            ]
        )
    else:
        execution_status = "dry_run_contract_ready"
        notes = [
            "Private retained matrix route is scaffold-ready for Pro dry-run only.",
        ]
    return {
        "product": product,
        "source_id": source_id,
        "destination_id": destination_id,
        "execution_status": execution_status,
        "real_execution_performed": False,
        "path_kind": "simulated_dry_run",
        "notes": notes,
    }


def _build_smoke_report() -> dict[str, object]:
    routes: list[dict[str, object]] = []
    product_summaries: list[SmokeProductSummaryDict] = []
    for product in ("Zephyr-base", "Zephyr-Pro"):
        product_routes: list[dict[str, object]] = []
        for source_id in SOURCE_IDS:
            for destination_id in DESTINATION_IDS:
                route = _build_route(
                    product=product,
                    source_id=source_id,
                    destination_id=destination_id,
                )
                routes.append(route)
                product_routes.append(route)
        dry_run_ready = sum(
            1
            for route in product_routes
            if route["execution_status"] == "dry_run_contract_ready"
        )
        placeholder_only = sum(
            1 for route in product_routes if route["execution_status"] == "placeholder_only"
        )
        product_summaries.append(
            {
                "product": product,
                "total_routes": len(product_routes),
                "dry_run_contract_ready_routes": dry_run_ready,
                "placeholder_only_routes": placeholder_only,
                "real_execution_routes": 0,
            }
        )
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.representative_nm_smoke.v1",
        "generated_at_utc": _generated_at_utc(),
        "source_count": len(SOURCE_IDS),
        "destination_count": len(DESTINATION_IDS),
        "simulation_only": True,
        "product_summaries": product_summaries,
        "routes": routes,
        "non_blocking_notes": [
            "Representative N×M smoke is simulated scaffold routing only.",
            "Routes marked placeholder_only are not product claims or executed deliveries.",
        ],
    }


def build_report(
    *,
    root: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    base_module, pro_module, contamination_module = _load_modules()
    public_export = base_module.build_report(root=root)
    private_export = pro_module.build_report(root=root)
    smoke_report = _build_smoke_report()
    contamination_scan = contamination_module.scan_repo(
        root=root,
        denylist=contamination_module.load_denylist(
            root / "docs/p6/commercial_contamination_denylist.json"
        ),
    )
    contamination_summary = _as_dict(contamination_scan["summary"])
    active_blockers = _as_int(contamination_summary["blocker_count"])
    review_required = _as_int(contamination_summary["review_required_hits"])
    overall = "pass" if active_blockers == 0 else "fail"
    report = {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.export_dry_run.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "active_blockers": active_blockers,
            "review_required_issues": review_required,
            "actual_export_performed": False,
            "code_migration_performed": False,
            "target_repos_created": False,
        },
        "base_public_export_report_id": public_export["report_id"],
        "pro_private_export_report_id": private_export["report_id"],
        "representative_nm_smoke_report_id": smoke_report["report_id"],
        "commercial_contamination": {
            "overall": contamination_summary["overall"],
            "blocked_hits": contamination_summary["blocked_hits"],
            "blocker_count": contamination_summary["blocker_count"],
            "review_required_hits": contamination_summary["review_required_hits"],
        },
        "issues": [],
        "non_blocking_notes": [
            "M3 remains scaffold-only and does not create Base/Pro repos.",
            (
                "Review-required contamination hits remain historical/docs/tooling "
                "context unless promoted later."
            ),
            "No runtime-home modification and no secrets are emitted.",
        ],
    }
    return public_export, private_export, smoke_report, report


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _render_smoke_markdown(report: dict[str, object]) -> str:
    product_summaries = cast(
        list[SmokeProductSummaryDict],
        report["product_summaries"],
    )
    lines = [
        "# P6-M3 representative N×M scaffold smoke",
        "",
        "## Summary",
        f"- source_count: {report['source_count']}",
        f"- destination_count: {report['destination_count']}",
        f"- simulation_only: {report['simulation_only']}",
        "",
        "## Products",
    ]
    for summary in product_summaries:
        lines.append(
            f"- {summary['product']} | total_routes={summary['total_routes']} | "
            f"dry_run_contract_ready={summary['dry_run_contract_ready_routes']} | "
            f"placeholder_only={summary['placeholder_only_routes']}"
        )
    return "\n".join(lines) + "\n"


def _render_report_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    contamination = _as_dict(report["commercial_contamination"])
    lines = [
        "# P6-M3 export dry-run",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- review_required_issues: {summary['review_required_issues']}",
        f"- actual_export_performed: {summary['actual_export_performed']}",
        f"- code_migration_performed: {summary['code_migration_performed']}",
        "",
        "## Contamination guardrail",
        f"- overall: {contamination['overall']}",
        f"- blocker_count: {contamination['blocker_count']}",
        f"- review_required_hits: {contamination['review_required_hits']}",
    ]
    return "\n".join(lines) + "\n"


def emit_outputs(
    *,
    public_export: dict[str, object],
    private_export: dict[str, object],
    smoke_report: dict[str, object],
    report: dict[str, object],
    out_root: Path,
    markdown: bool,
) -> None:
    out_root.mkdir(parents=True, exist_ok=True)
    if markdown:
        base_module, pro_module, _ = _load_modules()
        (out_root / PUBLIC_EXPORT_MD).write_text(
            base_module.render_markdown(public_export),
            encoding="utf-8",
        )
        (out_root / PRIVATE_EXPORT_MD).write_text(
            pro_module.render_markdown(private_export),
            encoding="utf-8",
        )
        (out_root / SMOKE_MD).write_text(
            _render_smoke_markdown(smoke_report),
            encoding="utf-8",
        )
        (out_root / REPORT_MD).write_text(
            _render_report_markdown(report),
            encoding="utf-8",
        )
        return
    _write_json(out_root / PUBLIC_EXPORT_JSON, public_export)
    _write_json(out_root / PRIVATE_EXPORT_JSON, private_export)
    _write_json(out_root / SMOKE_JSON, smoke_report)
    _write_json(out_root / REPORT_JSON, report)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the P6-M3 Base/Pro export dry-run scaffold."
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
    out_root = args.out_root.resolve()
    if args.check_artifacts:
        required = (
            out_root / PUBLIC_EXPORT_JSON,
            out_root / PRIVATE_EXPORT_JSON,
            out_root / SMOKE_JSON,
            out_root / REPORT_JSON,
            out_root / PUBLIC_EXPORT_MD,
            out_root / PRIVATE_EXPORT_MD,
            out_root / SMOKE_MD,
            out_root / REPORT_MD,
        )
        return 0 if all(path.exists() for path in required) else 1
    public_export, private_export, smoke_report, report = build_report(root=args.root.resolve())
    emit_outputs(
        public_export=public_export,
        private_export=private_export,
        smoke_report=smoke_report,
        report=report,
        out_root=out_root,
        markdown=bool(args.markdown),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
