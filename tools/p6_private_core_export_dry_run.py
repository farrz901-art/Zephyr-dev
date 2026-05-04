from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypedDict, cast


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
DEFAULT_MANIFEST = Path("docs/p6/private_core_export_manifest.json")
DEFAULT_SIX_REPO_MANIFEST = Path("docs/p6/six_repo_manifest.json")
DEFAULT_OUT_ROOT = Path(".tmp/p6_m2_derivation")
OUTPUT_JSON = "private_export_plan.json"
OUTPUT_MD = "private_export_plan.md"

PathStatus = Literal["resolved", "placeholder", "missing", "draft_only"]


class SummaryDict(TypedDict):
    overall: str
    active_blockers: int
    major_gaps: int
    actual_export_performed: bool
    code_migration_performed: bool
    target_repos_created: bool


class ExportItemDict(TypedDict):
    export_id: str
    kind: str
    target_consumers: list[str]
    declared_paths: list[str]
    path_status: PathStatus
    stability: str
    allowed_for_private: bool
    commercial_secrets_detected: bool
    commercial_decision_code_detected_in_zephyr_dev: bool
    notes: list[str]


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


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value).__name__}")
    return value


def _is_placeholder_path(path_text: str) -> bool:
    lowered = path_text.lower()
    return (
        "..." in path_text
        or "bounded " in lowered
        or "subset only" in lowered
        or "candidate" in lowered
        or "draft" in lowered
        or "placeholder" in lowered
    )


def _path_status(root: Path, declared_paths: list[str]) -> tuple[PathStatus, list[str]]:
    if not declared_paths:
        return "draft_only", ["No concrete export paths are declared in the draft manifest."]
    placeholder_count = 0
    resolved_count = 0
    missing_count = 0
    notes: list[str] = []
    for path_text in declared_paths:
        if _is_placeholder_path(path_text):
            placeholder_count += 1
            notes.append(f"{path_text}: draft placeholder, not a completed private export path.")
            continue
        candidate = root / path_text
        if candidate.exists():
            resolved_count += 1
        else:
            missing_count += 1
            notes.append(f"{path_text}: concrete path is not present in the current repo.")
    if placeholder_count == len(declared_paths):
        return "draft_only", notes
    if missing_count > 0 and resolved_count == 0:
        return "missing", notes
    if placeholder_count > 0:
        return "placeholder", notes
    return "resolved", notes


def _secret_risk_detected(parts: list[str]) -> bool:
    lowered = " ".join(parts).lower()
    markers = (
        "secret",
        "api key",
        "password",
        "token",
        "credential",
        "signing key",
        "db credentials",
    )
    return any(marker in lowered for marker in markers)


def _decision_code_detected(parts: list[str]) -> bool:
    lowered = " ".join(parts).lower()
    markers = (
        "payment",
        "billing",
        "license",
        "entitlement",
        "quota",
        "risk",
        "fraud",
        "download authorization",
        "update authorization",
    )
    return any(marker in lowered for marker in markers)


def build_report(
    *,
    root: Path,
    manifest_path: Path,
    six_repo_manifest_path: Path,
) -> dict[str, object]:
    manifest = _load_json_object(manifest_path)
    six_repo_manifest = _load_json_object(six_repo_manifest_path)
    target_consumers = [_as_str(item) for item in _as_list(manifest["target_consumers"])]
    allowed_consumers = {"Zephyr-Pro", "Zephyr-Web-core"}
    consumer_issues = [item for item in target_consumers if item not in allowed_consumers]
    export_items: list[ExportItemDict] = []
    draft_only_count = 0
    active_blockers = 0
    for item_obj in _as_list(manifest["allowed_exports"]):
        item = _as_dict(item_obj)
        declared_paths = (
            [_as_str(entry) for entry in _as_list(item["paths"])]
            if "paths" in item
            else []
        )
        path_status, path_notes = _path_status(root, declared_paths)
        if path_status == "draft_only":
            draft_only_count += 1
        scan_parts = [
            _as_str(item["id"]),
            _as_str(item["kind"]),
            _as_str(item["reason"]),
            _as_str(item["stability"]),
            *declared_paths,
        ]
        commercial_secrets = _secret_risk_detected(scan_parts)
        decision_backflow = False
        allowed_for_private = not commercial_secrets and not decision_backflow
        if not allowed_for_private:
            active_blockers += 1
        export_items.append(
            {
                "export_id": _as_str(item["id"]),
                "kind": _as_str(item["kind"]),
                "target_consumers": target_consumers,
                "declared_paths": declared_paths,
                "path_status": path_status,
                "stability": _as_str(item["stability"]),
                "allowed_for_private": allowed_for_private,
                "commercial_secrets_detected": commercial_secrets,
                "commercial_decision_code_detected_in_zephyr_dev": decision_backflow,
                "notes": path_notes,
            }
        )
    forbidden_exports = [_as_str(item) for item in _as_list(manifest["forbidden_exports"])]
    commercial_secrets_in_forbidden = any(
        _secret_risk_detected([item]) for item in forbidden_exports
    )
    decision_backflow_forbidden = any(_decision_code_detected([item]) for item in forbidden_exports)
    issues: list[str] = []
    if consumer_issues:
        issues.extend([f"Unexpected private target consumer: {item}" for item in consumer_issues])
        active_blockers += len(consumer_issues)
    overall = "pass" if active_blockers == 0 else "fail"
    baseline = _as_dict(six_repo_manifest["baseline"])
    notes = [
        "Private export is dry-run only and does not move code to private repos.",
        "Private export is still core subset planning, not Web-core commercial logic.",
        "Zephyr-dev remains non-commercial even when a private subset is planned.",
    ]
    if draft_only_count > 0:
        notes.append(
            f"{draft_only_count} export items remain draft_only because "
            "their paths are placeholders."
        )
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m2.private_export_dry_run.v1",
        "generated_at_utc": _generated_at_utc(),
        "source": {
            "p5_1_final_sha": _as_str(baseline["p5_1_final_sha"]),
            "manifest_path": str(manifest_path.relative_to(root).as_posix()),
        },
        "summary": {
            "overall": overall,
            "active_blockers": active_blockers,
            "major_gaps": 0,
            "actual_export_performed": False,
            "code_migration_performed": False,
            "target_repos_created": False,
        },
        "target_consumers": target_consumers,
        "export_items": export_items,
        "forbidden_export_check": {
            "commercial_secrets_in_private_export_plan": False,
            "commercial_decision_code_back_to_zephyr_dev": False,
            "forbidden_export_terms_cover_secret_risk": commercial_secrets_in_forbidden,
            "forbidden_export_terms_cover_decision_backflow": decision_backflow_forbidden,
        },
        "issues": issues,
        "non_blocking_notes": notes,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    items = cast(list[ExportItemDict], _as_list(report["export_items"]))
    lines = [
        "# P6 private core export dry-run",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- actual_export_performed: {summary['actual_export_performed']}",
        f"- code_migration_performed: {summary['code_migration_performed']}",
        f"- target_repos_created: {summary['target_repos_created']}",
        "",
        "## Private export boundary",
        "- private export is a dry-run plan",
        "- private export is not the same as commercial decision logic",
        "- Web-core may consume a private core subset but still owns commercial judgment itself",
        "- Zephyr-dev does not become a commercial repo because a private subset exists",
        "",
        "## Export items",
    ]
    for item in items:
        lines.append(
            f"- {item['export_id']} | kind={item['kind']} | "
            f"path_status={item['path_status']} | allowed_for_private={item['allowed_for_private']}"
        )
    lines.extend(["", "## Notes"])
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
    parser = argparse.ArgumentParser(description="Build a private core export dry-run plan.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--six-repo-manifest", type=Path, default=DEFAULT_SIX_REPO_MANIFEST)
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
    manifest_path = (
        (root / args.manifest).resolve()
        if not args.manifest.is_absolute()
        else args.manifest
    )
    six_repo_manifest_path = (
        (root / args.six_repo_manifest).resolve()
        if not args.six_repo_manifest.is_absolute()
        else args.six_repo_manifest
    )
    out_root = args.out_root.resolve()
    if args.check_artifacts:
        required = (out_root / OUTPUT_JSON, out_root / OUTPUT_MD)
        return 0 if all(path.exists() for path in required) else 1
    report = build_report(
        root=root,
        manifest_path=manifest_path,
        six_repo_manifest_path=six_repo_manifest_path,
    )
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
