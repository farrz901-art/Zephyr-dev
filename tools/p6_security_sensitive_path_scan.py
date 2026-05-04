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
DEFAULT_POLICY = Path("docs/p6/security_sensitive_paths.json")
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

Classification = Literal["boundary_tooling", "review_required", "blocked_runtime"]


class RuleDict(TypedDict):
    id: str
    path_patterns: list[str]
    required_review: bool
    allowed_in_zephyr_dev: str
    owner_hint: str


class FindingDict(TypedDict):
    path: str
    rule_id: str
    classification: Classification
    required_review: bool
    owner_hint: str
    allowed_in_zephyr_dev: str


class SummaryDict(TypedDict):
    scanned_files: int
    sensitive_boundary_paths: int
    sensitive_runtime_paths: int
    blocked_runtime_paths: int
    review_required_paths: int
    overall: Literal["pass", "fail"]


class ReportDict(TypedDict):
    schema_version: int
    report_id: str
    generated_at_utc: str
    root: str
    policy_path: str
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
    return raw.decode("utf-8")


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


def _iter_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root, topdown=True):
        dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]
        current_dir = Path(dirpath)
        if any(part in SKIP_DIRS for part in current_dir.parts):
            continue
        for filename in filenames:
            files.append(current_dir / filename)
    return files


def _classify(path: str, rule: RuleDict) -> Classification:
    if rule["allowed_in_zephyr_dev"] in {"boundary_docs_only", "tooling_only"}:
        return "boundary_tooling"
    if path.startswith("packages/") or path.startswith("tools/"):
        return "blocked_runtime"
    return "review_required"


def scan_repo(*, root: Path, rules: list[RuleDict]) -> ReportDict:
    findings: list[FindingDict] = []
    files = _iter_files(root)
    for path in files:
        rel = path.relative_to(root).as_posix()
        for rule in rules:
            if any(fnmatch.fnmatch(rel, pattern) for pattern in rule["path_patterns"]):
                findings.append(
                    {
                        "path": rel,
                        "rule_id": rule["id"],
                        "classification": _classify(rel, rule),
                        "required_review": rule["required_review"],
                        "owner_hint": rule["owner_hint"],
                        "allowed_in_zephyr_dev": rule["allowed_in_zephyr_dev"],
                    }
                )
    boundary_hits = sum(1 for item in findings if item["classification"] == "boundary_tooling")
    blocked_runtime = sum(1 for item in findings if item["classification"] == "blocked_runtime")
    review_required = sum(1 for item in findings if item["classification"] == "review_required")
    sensitive_runtime = blocked_runtime + review_required
    overall: Literal["pass", "fail"] = "fail" if blocked_runtime > 0 else "pass"
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.security_sensitive_path_scan.v1",
        "generated_at_utc": _generated_at_utc(),
        "root": str(root.resolve()),
        "policy_path": str(DEFAULT_POLICY),
        "summary": {
            "scanned_files": len(files),
            "sensitive_boundary_paths": boundary_hits,
            "sensitive_runtime_paths": sensitive_runtime,
            "blocked_runtime_paths": blocked_runtime,
            "review_required_paths": review_required,
            "overall": overall,
        },
        "findings": sorted(findings, key=lambda item: item["path"]),
    }


def render_markdown(report: ReportDict) -> str:
    summary = report["summary"]
    lines = [
        "# P6 security-sensitive path scan",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- blocked_runtime_paths: {summary['blocked_runtime_paths']}",
        f"- review_required_paths: {summary['review_required_paths']}",
        "",
        "## Summary",
        f"- scanned_files: {summary['scanned_files']}",
        f"- sensitive_boundary_paths: {summary['sensitive_boundary_paths']}",
        f"- sensitive_runtime_paths: {summary['sensitive_runtime_paths']}",
        f"- blocked_runtime_paths: {summary['blocked_runtime_paths']}",
        f"- review_required_paths: {summary['review_required_paths']}",
    ]
    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan path patterns for future security review gates."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--out", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--json", action="store_true")
    mode.add_argument("--markdown", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    root = args.root.resolve()
    policy_path = args.policy.resolve()
    report = scan_repo(root=root, rules=_load_rules(policy_path))
    report["policy_path"] = str(policy_path)
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
