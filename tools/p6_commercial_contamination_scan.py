from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypedDict, cast

DEFAULT_ROOT = Path(".")
DEFAULT_DENYLIST = Path("docs/p6/commercial_contamination_denylist.json")
SKIP_DIRS = {
    ".git",
    ".venv",
    ".tmp",
    ".cache",
    ".mypy_cache",
    ".uv-cache",
    "node_modules",
    "dist",
    "build",
    "__pycache__",
    ".ruff_cache",
    ".pytest_cache",
}
BOUNDARY_DOC_FILENAMES = {
    "PURE_CORE_BOUNDARY.md",
    "P6_SCOPE_BOUNDARY.md",
    "P6_DERIVATION_POLICY.md",
    "P6_MILESTONE_MAP.md",
}
TOOLING_FILENAMES = {
    "p6_commercial_contamination_scan.py",
    "commercial_contamination_denylist.json",
}
NEGATIVE_MARKERS = (
    "not_claimed",
    "intentionally_not_claimed",
    "deferred_or_not_claimed",
    "must_not_include",
    "non_billing",
    "usage_not_billing",
    "governance_not_rbac",
    "technical_capability_domain_not_entitlement",
    "technical domains, not entitlement",
    "not billing",
    "not pricing",
    "not entitlement",
    "not rbac",
    "not cloud",
    "not kubernetes",
    "not helm",
    "not multi-tenant",
    "not saas",
    "not installer",
    "not gui",
    "not full",
    "not exhaustive",
    "must not",
    "do not",
    "forbidden",
    "forbid",
    "excluded",
    "deferred",
)

Classification = Literal[
    "allowed_boundary",
    "allowed_tooling",
    "allowed_fixture",
    "review_required",
    "blocked",
]
ContextKind = Literal[
    "boundary_doc",
    "historical_doc",
    "tooling",
    "test_fixture",
    "runtime",
    "connector",
    "core_contract",
    "other",
]


class DenylistEntryDict(TypedDict):
    term: str
    severity: Literal["blocker", "major", "note"]
    blocked_in: list[str]
    allowed_in: list[str]
    reason: str


class FindingDict(TypedDict):
    path: str
    line: int
    term: str
    category: str
    classification: Classification
    severity: str
    reason: str
    context_kind: ContextKind
    suggested_owner: str
    historical_or_fixture: str
    text: str


class SummaryDict(TypedDict):
    scanned_files: int
    allowed_boundary_hits: int
    allowed_tooling_hits: int
    allowed_fixture_hits: int
    review_required_hits: int
    blocked_hits: int
    blocker_count: int
    overall: Literal["pass", "fail"]


class ReportDict(TypedDict):
    schema_version: int
    report_id: str
    generated_at_utc: str
    root: str
    denylist_path: str
    skipped_directories: list[str]
    summary: SummaryDict
    findings: list[FindingDict]
    issues: list[str]


@dataclass(frozen=True, slots=True)
class DenylistEntry:
    category: str
    term: str
    severity: Literal["blocker", "major", "note"]
    blocked_in: tuple[str, ...]
    allowed_in: tuple[str, ...]
    reason: str


def _generated_at_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig")
    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8", "utf-8-sig", "utf-16"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError as err:
            last_error = err
    assert last_error is not None
    raise last_error


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


def _load_denylist(path: Path) -> list[DenylistEntry]:
    loaded = _load_json_object(path)
    groups = _as_dict(loaded["groups"])
    entries: list[DenylistEntry] = []
    for category, items_obj in groups.items():
        items = _as_list(items_obj)
        for item in items:
            data = cast(DenylistEntryDict, _as_dict(item))
            entries.append(
                DenylistEntry(
                    category=category,
                    term=data["term"],
                    severity=data["severity"],
                    blocked_in=tuple(data["blocked_in"]),
                    allowed_in=tuple(data["allowed_in"]),
                    reason=data["reason"],
                )
            )
    return entries


def load_denylist(path: Path) -> list[DenylistEntry]:
    return _load_denylist(path)


def _has_skipped_relative_part(path: Path) -> bool:
    return any(part in SKIP_DIRS for part in path.parts)


def _iter_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root, topdown=True):
        current_dir = Path(dirpath)
        rel_dir = current_dir.relative_to(root)
        if _has_skipped_relative_part(rel_dir):
            continue
        dirnames[:] = [
            name
            for name in dirnames
            if not _has_skipped_relative_part(rel_dir / name)
        ]
        for filename in filenames:
            path = current_dir / filename
            if _has_skipped_relative_part(path.relative_to(root)):
                continue
            files.append(path)
    return files


def _path_context(root: Path, path: Path) -> ContextKind:
    rel = path.relative_to(root)
    rel_str = rel.as_posix()
    parts = set(rel.parts)
    if rel.name in BOUNDARY_DOC_FILENAMES or rel_str.startswith("docs/p6/"):
        return "boundary_doc"
    if rel_str.startswith("docs/"):
        return "historical_doc"
    if rel.name in TOOLING_FILENAMES or rel_str.startswith("tools/p6_"):
        return "tooling"
    if {"tests", "testing", "fixtures"} & parts:
        return "test_fixture"
    if rel_str.startswith("packages/zephyr-core/src/"):
        return "core_contract"
    if rel_str.startswith("packages/") and "/src/" in rel_str:
        if any(segment in parts for segment in ("sources", "destinations", "connectors")):
            return "connector"
        return "runtime"
    return "other"


def _should_ignore_false_positive(term: str, context_lower: str) -> bool:
    if term == "checkout" and any(
        marker in context_lower
        for marker in ("actions/checkout", "sparse-checkout", "checkout state", "ambient checkout")
    ):
        return True
    return term == "subscription" and "subscription selector" in context_lower


def _has_negative_boundary_context(context_lower: str) -> bool:
    return any(marker in context_lower for marker in NEGATIVE_MARKERS)


def _blocked_contexts_for_path(context_kind: ContextKind) -> set[str]:
    if context_kind == "runtime":
        return {"packages", "runtime"}
    if context_kind == "connector":
        return {"packages", "runtime", "connector"}
    if context_kind == "core_contract":
        return {"packages", "core_contract"}
    return set()


def _suggested_owner(context_kind: ContextKind) -> str:
    if context_kind == "boundary_doc":
        return "p6-boundary-owner"
    if context_kind == "historical_doc":
        return "docs-owner"
    if context_kind == "tooling":
        return "tooling-owner"
    if context_kind == "test_fixture":
        return "test-owner"
    if context_kind == "core_contract":
        return "zephyr-core-owner"
    if context_kind == "connector":
        return "connector-owner"
    if context_kind == "runtime":
        return "zephyr-ingest-owner"
    return "repo-owner"


def classify_hit(
    *,
    root: Path,
    path: Path,
    entry: DenylistEntry,
    line_text: str,
    context_window: str,
) -> Classification | None:
    context_kind = _path_context(root, path)
    context_lower = context_window.lower()
    if _should_ignore_false_positive(entry.term, context_lower):
        return None
    if context_kind == "boundary_doc":
        return "allowed_boundary"
    if context_kind == "tooling":
        return "allowed_tooling"
    if context_kind == "test_fixture":
        return "allowed_fixture"
    if _has_negative_boundary_context(context_lower):
        if context_kind == "historical_doc":
            return "allowed_boundary"
        return "review_required"
    if context_kind == "historical_doc":
        return "review_required"
    blocked_contexts = _blocked_contexts_for_path(context_kind)
    if blocked_contexts and blocked_contexts & set(entry.blocked_in):
        return "blocked"
    return "review_required"


def _historical_or_fixture(context_kind: ContextKind) -> str:
    if context_kind == "test_fixture":
        return "test_fixture"
    if context_kind in {"boundary_doc", "historical_doc"}:
        return "historical_text"
    if context_kind == "tooling":
        return "tooling"
    return "actual_runtime_candidate"


def scan_repo(*, root: Path, denylist: list[DenylistEntry]) -> ReportDict:
    findings: list[FindingDict] = []
    scanned_files = 0
    for path in _iter_files(root):
        try:
            text = _read_text(path)
        except (UnicodeDecodeError, OSError):
            continue
        scanned_files += 1
        lines = text.splitlines()
        for line_number, line_text in enumerate(lines, start=1):
            lowered_line = line_text.lower()
            if lowered_line.strip() == "":
                continue
            for entry in denylist:
                if entry.term.lower() not in lowered_line:
                    continue
                line_index = line_number - 1
                context_slice = lines[max(0, line_index - 5) : min(len(lines), line_index + 3)]
                context_window = "\n".join(context_slice)
                classification = classify_hit(
                    root=root,
                    path=path,
                    entry=entry,
                    line_text=line_text,
                    context_window=context_window,
                )
                if classification is None:
                    continue
                context_kind = _path_context(root, path)
                findings.append(
                    {
                        "path": path.relative_to(root).as_posix(),
                        "line": line_number,
                        "term": entry.term,
                        "category": entry.category,
                        "classification": classification,
                        "severity": entry.severity,
                        "reason": entry.reason,
                        "context_kind": context_kind,
                        "suggested_owner": _suggested_owner(context_kind),
                        "historical_or_fixture": _historical_or_fixture(context_kind),
                        "text": line_text.strip(),
                    }
                )
    findings.sort(key=lambda item: (item["path"], item["line"], item["term"]))
    allowed_boundary_hits = sum(
        1 for item in findings if item["classification"] == "allowed_boundary"
    )
    allowed_tooling_hits = sum(
        1 for item in findings if item["classification"] == "allowed_tooling"
    )
    allowed_fixture_hits = sum(
        1 for item in findings if item["classification"] == "allowed_fixture"
    )
    review_required_hits = sum(
        1 for item in findings if item["classification"] == "review_required"
    )
    blocked_findings = [item for item in findings if item["classification"] == "blocked"]
    blocker_count = sum(1 for item in blocked_findings if item["severity"] == "blocker")
    overall: Literal["pass", "fail"] = "fail" if blocker_count > 0 else "pass"
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.commercial_contamination_scan.v1",
        "generated_at_utc": _generated_at_utc(),
        "root": str(root.resolve()),
        "denylist_path": str(DEFAULT_DENYLIST),
        "skipped_directories": sorted(SKIP_DIRS),
        "summary": {
            "scanned_files": scanned_files,
            "allowed_boundary_hits": allowed_boundary_hits,
            "allowed_tooling_hits": allowed_tooling_hits,
            "allowed_fixture_hits": allowed_fixture_hits,
            "review_required_hits": review_required_hits,
            "blocked_hits": len(blocked_findings),
            "blocker_count": blocker_count,
            "overall": overall,
        },
        "findings": findings,
        "issues": [],
    }


def render_markdown(report: ReportDict) -> str:
    summary = report["summary"]
    blocked = [item for item in report["findings"] if item["classification"] == "blocked"]
    review_required = [
        item for item in report["findings"] if item["classification"] == "review_required"
    ]
    allowed = [
        item
        for item in report["findings"]
        if item["classification"] in {"allowed_boundary", "allowed_tooling", "allowed_fixture"}
    ]
    lines = [
        "# Zephyr-dev commercial contamination scan",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- blocker_count: {summary['blocker_count']}",
        f"- blocked_hits: {summary['blocked_hits']}",
        f"- review_required_hits: {summary['review_required_hits']}",
        "",
        "## Summary",
        f"- scanned_files: {summary['scanned_files']}",
        f"- allowed_boundary_hits: {summary['allowed_boundary_hits']}",
        f"- allowed_tooling_hits: {summary['allowed_tooling_hits']}",
        f"- allowed_fixture_hits: {summary['allowed_fixture_hits']}",
        f"- review_required_hits: {summary['review_required_hits']}",
        f"- blocked_hits: {summary['blocked_hits']}",
        "",
        "## Blocked findings",
    ]
    if not blocked:
        lines.append("- none")
    else:
        for finding in blocked[:100]:
            lines.append(
                f"- {finding['path']}:{finding['line']} | {finding['term']} | "
                f"{finding['reason']} | owner={finding['suggested_owner']}"
            )
    lines.extend(["", "## Review required findings"])
    if not review_required:
        lines.append("- none")
    else:
        for finding in review_required[:100]:
            lines.append(
                f"- {finding['path']}:{finding['line']} | {finding['term']} | "
                f"context={finding['context_kind']} | owner={finding['suggested_owner']}"
            )
    lines.extend(["", "## Allowed boundary findings"])
    if not allowed:
        lines.append("- none")
    else:
        for finding in allowed[:100]:
            lines.append(
                f"- {finding['path']}:{finding['line']} | {finding['term']} | "
                f"classification={finding['classification']}"
            )
    lines.extend(
        [
            "",
            "## How to fix",
            (
                "- Remove commercial judgment terms from Zephyr-dev runtime/core code "
                "when they act as positive product logic."
            ),
            (
                "- Keep boundary explanations in docs/p6 or PURE_CORE_BOUNDARY.md "
                "instead of runtime modules when possible."
            ),
            (
                "- If a hit is only a non-claim, negative boundary, or test fixture, "
                "classify it there instead of normalizing it into production runtime "
                "truth."
            ),
            "",
            "## Why Zephyr-dev must stay pure",
            (
                "- Zephyr-dev is the technical source of truth for contracts, payloads, "
                "readiness, evidence, and bounded diagnostics."
            ),
            (
                "- Commercial decisions must stay in downstream private repos, "
                "especially Zephyr-Web-core."
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def emit_outputs(
    *,
    report: ReportDict,
    out_path: Path | None,
    markdown: bool,
) -> str:
    rendered = (
        render_markdown(report)
        if markdown
        else json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    )
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered, encoding="utf-8")
    return rendered


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan Zephyr-dev for commercial contamination terms."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--denylist", type=Path, default=DEFAULT_DENYLIST)
    parser.add_argument("--out", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--json", action="store_true")
    mode.add_argument("--markdown", action="store_true")
    parser.add_argument("--fail-on-blocker", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    root = args.root.resolve()
    denylist_path = args.denylist.resolve()
    denylist = _load_denylist(denylist_path)
    report = scan_repo(root=root, denylist=denylist)
    report["denylist_path"] = str(denylist_path)
    markdown = bool(args.markdown)
    rendered = emit_outputs(report=report, out_path=args.out, markdown=markdown)
    if args.out is None:
        sys.stdout.write(rendered)
    if args.fail_on_blocker and report["summary"]["blocker_count"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
