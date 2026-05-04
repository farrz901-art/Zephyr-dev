from __future__ import annotations

import argparse
import fnmatch
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypedDict, cast

DEFAULT_ROOT = Path(".")
DEFAULT_MAP = Path("docs/p6/forbidden_import_map.json")
SKIP_DIRS = {
    ".git",
    ".tmp",
    ".venv",
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
BOUNDARY_PATHS = ("docs/p6/",)
TOOL_SUFFIXES = ("tools/",)
TEXT_SUFFIXES = {".py", ".ts", ".tsx", ".js", ".jsx", ".json", ".toml", ".md"}
IMPORT_MARKERS = ("import ", "from ", "require(", "package", "dependency", "module")

Classification = Literal["allowed_boundary", "allowed_fixture", "review_required", "blocked"]


class RuleDict(TypedDict):
    id: str
    source_scope: str
    source_paths: list[str]
    forbidden_patterns: list[str]
    severity: str
    reason: str


class FindingDict(TypedDict):
    path: str
    line: int
    pattern: str
    rule_id: str
    classification: Classification
    severity: str
    reason: str
    text: str


class SummaryDict(TypedDict):
    scanned_files: int
    allowed_boundary_hits: int
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
    map_path: str
    skipped_directories: list[str]
    summary: SummaryDict
    findings: list[FindingDict]


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


def _as_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list, got {type(value).__name__}")
    return cast(list[object], value)


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected dict, got {type(value).__name__}")
    return cast(dict[str, object], value)


def _load_rules(path: Path) -> list[RuleDict]:
    loaded = _load_json_object(path)
    return [cast(RuleDict, _as_dict(item)) for item in _as_list(loaded["rules"])]


def load_rules(path: Path) -> list[RuleDict]:
    return _load_rules(path)


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
            if path.suffix.lower() in TEXT_SUFFIXES:
                files.append(path)
    return files


def _path_kind(root: Path, path: Path) -> str:
    rel = path.relative_to(root).as_posix()
    parts = set(path.relative_to(root).parts)
    if any(rel.startswith(prefix) for prefix in BOUNDARY_PATHS):
        return "boundary"
    if {"tests", "testing", "fixtures"} & parts:
        return "fixture"
    if any(rel.startswith(prefix) for prefix in TOOL_SUFFIXES):
        return "tool"
    if rel.startswith("packages/"):
        return "package"
    return "other"


def _line_has_import_context(line: str) -> bool:
    lowered = line.lower()
    return any(marker in lowered for marker in IMPORT_MARKERS)


def _path_matches(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def _allowed_boundary_context(line: str) -> bool:
    lowered = line.lower()
    negatives = ("must not", "not ", "forbidden", "boundary", "draft", "only", "cannot")
    return any(marker in lowered for marker in negatives)


def scan_repo(*, root: Path, rules: list[RuleDict]) -> ReportDict:
    findings: list[FindingDict] = []
    scanned_files = 0
    for path in _iter_files(root):
        rel = path.relative_to(root).as_posix()
        kind = _path_kind(root, path)
        try:
            text = _read_text(path)
        except (UnicodeDecodeError, OSError):
            continue
        scanned_files += 1
        for line_number, line in enumerate(text.splitlines(), start=1):
            for rule in rules:
                for pattern in rule["forbidden_patterns"]:
                    if pattern.lower() not in line.lower():
                        continue
                    if not _line_has_import_context(line):
                        if kind == "boundary" and _allowed_boundary_context(line):
                            classification: Classification = "allowed_boundary"
                        else:
                            continue
                    elif kind == "boundary":
                        classification = "allowed_boundary"
                    elif kind == "fixture":
                        classification = "allowed_fixture"
                    elif _path_matches(rel, rule["source_paths"]) and kind in {"package", "tool"}:
                        classification = "blocked"
                    else:
                        classification = "review_required"
                    findings.append(
                        {
                            "path": rel,
                            "line": line_number,
                            "pattern": pattern,
                            "rule_id": rule["id"],
                            "classification": classification,
                            "severity": rule["severity"],
                            "reason": rule["reason"],
                            "text": line.strip(),
                        }
                    )
    findings.sort(key=lambda item: (item["path"], item["line"], item["pattern"]))
    blocked_hits = sum(1 for item in findings if item["classification"] == "blocked")
    blocker_count = sum(
        1
        for item in findings
        if item["classification"] == "blocked" and item["severity"] == "blocker"
    )
    overall: Literal["pass", "fail"] = "fail" if blocker_count > 0 else "pass"
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.forbidden_import_scan.v1",
        "generated_at_utc": _generated_at_utc(),
        "root": str(root.resolve()),
        "map_path": str(DEFAULT_MAP),
        "skipped_directories": sorted(SKIP_DIRS),
        "summary": {
            "scanned_files": scanned_files,
            "allowed_boundary_hits": sum(
                1 for item in findings if item["classification"] == "allowed_boundary"
            ),
            "allowed_fixture_hits": sum(
                1 for item in findings if item["classification"] == "allowed_fixture"
            ),
            "review_required_hits": sum(
                1 for item in findings if item["classification"] == "review_required"
            ),
            "blocked_hits": blocked_hits,
            "blocker_count": blocker_count,
            "overall": overall,
        },
        "findings": findings,
    }


def render_markdown(report: ReportDict) -> str:
    summary = report["summary"]
    blocked = [item for item in report["findings"] if item["classification"] == "blocked"]
    review = [item for item in report["findings"] if item["classification"] == "review_required"]
    allowed = [
        item
        for item in report["findings"]
        if item["classification"] in {"allowed_boundary", "allowed_fixture"}
    ]
    lines = [
        "# P6 forbidden import scan",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- blocker_count: {summary['blocker_count']}",
        "",
        "## Summary",
        f"- scanned_files: {summary['scanned_files']}",
        f"- allowed_boundary_hits: {summary['allowed_boundary_hits']}",
        f"- allowed_fixture_hits: {summary['allowed_fixture_hits']}",
        f"- review_required_hits: {summary['review_required_hits']}",
        f"- blocked_hits: {summary['blocked_hits']}",
        "",
        "## Blocked findings",
    ]
    if not blocked:
        lines.append("- none")
    else:
        for item in blocked[:100]:
            lines.append(
                f"- {item['path']}:{item['line']} | {item['pattern']} | {item['rule_id']}"
            )
    lines.extend(["", "## Review required findings"])
    if not review:
        lines.append("- none")
    else:
        for item in review[:100]:
            lines.append(
                f"- {item['path']}:{item['line']} | {item['pattern']} | {item['rule_id']}"
            )
    lines.extend(["", "## Allowed boundary findings"])
    if not allowed:
        lines.append("- none")
    else:
        for item in allowed[:100]:
            lines.append(
                f"- {item['path']}:{item['line']} | {item['pattern']} | {item['classification']}"
            )
    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan Zephyr-dev for forbidden downstream imports."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--map", type=Path, default=DEFAULT_MAP)
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
    map_path = args.map.resolve()
    report = scan_repo(root=root, rules=_load_rules(map_path))
    report["map_path"] = str(map_path)
    rendered = (
        render_markdown(report)
        if args.markdown
        else json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    )
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    if args.fail_on_blocker and report["summary"]["blocker_count"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
