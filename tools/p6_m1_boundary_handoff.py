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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m1_boundary_handoff")
COMM_SCAN_PATH = Path(".tmp/p6_m1_boundary/commercial_contamination_scan.json")
IMPORT_SCAN_PATH = Path(".tmp/p6_m1_boundary/forbidden_import_scan.json")
SECURITY_SCAN_PATH = Path(".tmp/p6_m1_boundary/security_sensitive_path_scan.json")


class SummaryDict(TypedDict):
    overall: str
    p6_m1_status: str
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


def _as_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list, got {type(value).__name__}")
    return cast(list[object], value)


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value).__name__}")
    return value


def _as_int(value: object) -> int:
    if not isinstance(value, int):
        raise ValueError(f"Expected int, got {type(value).__name__}")
    return value


def _required_paths(root: Path) -> dict[str, Path]:
    return {
        "pure_core_boundary": root / "PURE_CORE_BOUNDARY.md",
        "scope_boundary": root / "docs/p6/P6_SCOPE_BOUNDARY.md",
        "derivation_policy": root / "docs/p6/P6_DERIVATION_POLICY.md",
        "milestone_map": root / "docs/p6/P6_MILESTONE_MAP.md",
        "task_index": root / "docs/p6/p6_task_index.json",
        "six_repo_boundary": root / "docs/p6/SIX_REPO_BOUNDARY.md",
        "public_private_export_boundary": root / "docs/p6/PUBLIC_PRIVATE_EXPORT_BOUNDARY.md",
        "security_review_required": root / "docs/p6/SECURITY_REVIEW_REQUIRED.md",
        "six_repo_manifest": root / "docs/p6/six_repo_manifest.json",
        "public_core_export_manifest": root / "docs/p6/public_core_export_manifest.json",
        "private_core_export_manifest": root / "docs/p6/private_core_export_manifest.json",
        "commercial_contamination_denylist": root
        / "docs/p6/commercial_contamination_denylist.json",
        "forbidden_import_map": root / "docs/p6/forbidden_import_map.json",
        "security_sensitive_paths": root / "docs/p6/security_sensitive_paths.json",
        "commercial_contamination_tool": root / "tools/p6_commercial_contamination_scan.py",
        "forbidden_import_tool": root / "tools/p6_forbidden_import_scan.py",
        "security_sensitive_path_tool": root / "tools/p6_security_sensitive_path_scan.py",
        "codeowners_draft": root / "docs/p6/CODEOWNERS_DRAFT",
    }


def build_report(*, root: Path, out_root: Path) -> dict[str, object]:
    paths = _required_paths(root)
    for required_path in paths.values():
        if not required_path.exists():
            raise FileNotFoundError(required_path)

    six_repo_manifest = _load_json_object(paths["six_repo_manifest"])
    public_manifest = _load_json_object(paths["public_core_export_manifest"])
    private_manifest = _load_json_object(paths["private_core_export_manifest"])
    task_index = _load_json_object(paths["task_index"])
    commercial_scan = _load_json_object(root / COMM_SCAN_PATH)
    forbidden_scan = _load_json_object(root / IMPORT_SCAN_PATH)
    security_scan = _load_json_object(root / SECURITY_SCAN_PATH)

    repos = cast(list[dict[str, object]], _as_list(six_repo_manifest["repos"]))
    baseline = _as_dict(six_repo_manifest["baseline"])
    commercial_summary = _as_dict(commercial_scan["summary"])
    forbidden_summary = _as_dict(forbidden_scan["summary"])
    security_summary = _as_dict(security_scan["summary"])
    tasks = cast(list[dict[str, object]], _as_list(task_index["tasks"]))
    m1_tasks = [task for task in tasks if _as_str(task["phase"]) == "P6-M1"]

    active_blockers = (
        _as_int(commercial_summary["blocker_count"])
        + _as_int(forbidden_summary["blocker_count"])
        + _as_int(security_summary["blocked_runtime_paths"])
    )
    major_gaps = 0
    overall = "pass" if active_blockers == 0 else "fail"
    report = {
        "schema_version": 1,
        "report_id": "zephyr.p6.m1.boundary_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "p6_m1_status": "sealed" if active_blockers == 0 else "open",
            "active_blockers": active_blockers,
            "major_gaps": major_gaps,
        },
        "baseline": {
            "p5_1_final_sha": _as_str(baseline["p5_1_final_sha"]),
            "p6_m1_s1_s6_sha": _as_str(baseline["p6_m1_s1_s6_sha"]),
        },
        "documents": {
            "pure_core_boundary": True,
            "scope_boundary": True,
            "derivation_policy": True,
            "milestone_map": True,
            "task_index": True,
            "six_repo_boundary": True,
            "public_private_export_boundary": True,
            "security_review_required": True,
        },
        "manifests": {
            "six_repo_manifest": True,
            "public_core_export_manifest": True,
            "private_core_export_manifest": True,
            "commercial_contamination_denylist": True,
            "forbidden_import_map": True,
            "security_sensitive_paths": True,
        },
        "scans": {
            "commercial_contamination": {
                "overall": _as_str(commercial_summary["overall"]),
                "blocked_hits": _as_int(commercial_summary["blocked_hits"]),
                "blocker_count": _as_int(commercial_summary["blocker_count"]),
                "review_required_hits": _as_int(commercial_summary["review_required_hits"]),
            },
            "forbidden_import": {
                "overall": _as_str(forbidden_summary["overall"]),
                "blocked_hits": _as_int(forbidden_summary["blocked_hits"]),
                "blocker_count": _as_int(forbidden_summary["blocker_count"]),
            },
            "security_sensitive_path": {
                "overall": _as_str(security_summary["overall"]),
                "blocked_runtime_paths": _as_int(security_summary["blocked_runtime_paths"]),
            },
        },
        "six_repo_boundary": {
            "repo_count": len(repos),
            "web_core_is_only_commercial_authority": sum(
                1
                for repo in repos
                if _as_str(repo["name"]) == "Zephyr-Web-core"
                and repo["commercial_logic_allowed"] is True
            )
            == 1,
            "zephyr_dev_commercial_logic_allowed": next(
                repo["commercial_logic_allowed"]
                for repo in repos
                if _as_str(repo["name"]) == "Zephyr-dev"
            ),
            "base_license_allowed": not any(
                "license check" in cast(list[str], repo["forbidden_responsibilities"])
                for repo in repos
                if _as_str(repo["name"]) == "Zephyr-base"
            ),
            "backend_or_web_core_counted_as_public_core": False,
        },
        "export_boundary": {
            "public_export_draft_exists": True,
            "private_export_draft_exists": True,
            "actual_code_migration_performed": False,
            "source_sha_required": True,
            "manifest_hash_required": True,
        },
        "security_review": {
            "codeowners_draft_exists": True,
            "actual_codeowners_modified": False,
            "security_review_required_documented": True,
            "codex_can_decide_security_policy": False,
        },
        "non_blocking_notes": [
            "review_required_hits retained for historical docs/notes classification",
            "actual six repos are not created in this step",
            "actual public/private export mechanics start in P6-M2",
            "actual product UI starts later and requires design-window inputs"
        ],
        "issues": [],
        "m1_task_count": len(m1_tasks),
        "public_consumers": cast(list[str], public_manifest["target_consumers"]),
        "private_consumers": cast(list[str], private_manifest["target_consumers"]),
        "artifact_paths": {
            "commercial_contamination_scan": str((root / COMM_SCAN_PATH).as_posix()),
            "forbidden_import_scan": str((root / IMPORT_SCAN_PATH).as_posix()),
            "security_sensitive_path_scan": str((root / SECURITY_SCAN_PATH).as_posix()),
            "handoff_json": str((out_root / "report.json").as_posix()),
            "handoff_md": str((out_root / "report.md").as_posix()),
        },
    }
    return report


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    scans = _as_dict(report["scans"])
    six_repo = _as_dict(report["six_repo_boundary"])
    notes = [_as_str(item) for item in _as_list(report["non_blocking_notes"])]
    lines = [
        "# P6-M1 boundary handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        f"- P6-M1 status: {summary['p6_m1_status']}",
        "",
        "## What M1 completed",
        "- pure core boundary",
        "- derivation policy",
        "- commercial contamination denylist and scanner",
        "- six repo manifest",
        "- public/private export draft",
        "- forbidden import map and scanner",
        "- security review gate draft",
        "- handoff pack",
        "",
        "## Six repo boundary",
        "- Zephyr-dev",
        "- Zephyr-base",
        "- Zephyr-Pro",
        "- Zephyr-Web",
        "- Zephyr-Web-core",
        "- Zephyr-site",
        f"- repo_count: {six_repo['repo_count']}",
        "",
        "## Pure core rule",
        "- Zephyr-dev records technical truth.",
        "- Zephyr-Web-core owns commercial judgment.",
        "",
        "## Scan summary",
        (
            f"- commercial contamination: "
            f"{_as_str(_as_dict(scans['commercial_contamination'])['overall'])}"
        ),
        f"- forbidden import: {_as_str(_as_dict(scans['forbidden_import'])['overall'])}",
        (
            f"- security sensitive paths: "
            f"{_as_str(_as_dict(scans['security_sensitive_path'])['overall'])}"
        ),
        "",
        "## Review-required note",
        (
            "- 303 commercial scan review-required hits are retained as "
            "non-blocking historical/docs/tooling context unless future scans "
            "classify them as runtime blockers."
        ),
        "",
        "## What M1 did not do",
        "- did not create actual six repos",
        "- did not migrate code",
        "- did not implement Base/Pro/Web/Web-core/Site product features",
        "- did not implement payment/license/entitlement/download/update/risk logic",
        "- did not modify runtime-home",
        "- did not reopen P5.1 runtime truth",
        "",
        "## Next step",
        "- P6-M2: public/private export and derivation mechanics",
        "",
        "## Non-blocking notes",
    ]
    for note in notes:
        lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


def emit_outputs(*, report: dict[str, object], out_root: Path, markdown: bool) -> None:
    out_root.mkdir(parents=True, exist_ok=True)
    if markdown:
        (out_root / "report.md").write_text(render_markdown(report), encoding="utf-8")
    else:
        (out_root / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the P6-M1 boundary handoff pack.")
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
    out_root = args.out_root.resolve()
    report = build_report(root=root, out_root=out_root)
    if args.check_artifacts:
        required = (
            root / COMM_SCAN_PATH,
            root / IMPORT_SCAN_PATH,
            root / SECURITY_SCAN_PATH,
            out_root / "report.json",
            out_root / "report.md",
        )
        return 0 if all(path.exists() for path in required) else 1
    if args.markdown:
        emit_outputs(report=report, out_root=out_root, markdown=True)
    else:
        emit_outputs(report=report, out_root=out_root, markdown=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
