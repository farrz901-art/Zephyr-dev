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
DEFAULT_DERIVATION_ROOT = Path(".tmp/p6_m2_derivation")
DEFAULT_OUT_ROOT = Path(".tmp/p6_m2_derivation_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"


class SummaryDict(TypedDict):
    overall: str
    p6_m2_status: str
    active_blockers: int
    major_gaps: int


class _PlanModule(Protocol):
    def build_report(self, **kwargs: object) -> dict[str, object]: ...


class _NoticeModule(Protocol):
    def build_report(self, **kwargs: object) -> dict[str, object]: ...


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


def _load_tool(name: str) -> _PlanModule:
    import sys

    repo_root = _discover_repo_root(Path(__file__).resolve().parent)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return cast(_PlanModule, importlib.import_module(name))


def _required_doc_paths(root: Path) -> dict[str, Path]:
    return {
        "public_manifest": root / "docs/p6/public_core_export_manifest.json",
        "private_manifest": root / "docs/p6/private_core_export_manifest.json",
        "public_private_export_boundary": root / "docs/p6/PUBLIC_PRIVATE_EXPORT_BOUNDARY.md",
        "six_repo_manifest": root / "docs/p6/six_repo_manifest.json",
        "six_repo_boundary": root / "docs/p6/SIX_REPO_BOUNDARY.md",
        "release_source_policy": root / "docs/p6/RELEASE_SOURCE_SHA_POLICY.md",
        "notice_sbom_policy": root / "docs/p6/NOTICE_SBOM_PREFLIGHT_POLICY.md",
    }


def _artifact_paths(derivation_root: Path) -> dict[str, Path]:
    return {
        "public_plan": derivation_root / "public_export_plan.json",
        "private_plan": derivation_root / "private_export_plan.json",
        "release_source_manifest": derivation_root / "release_source_manifest.json",
        "leakage_preflight": derivation_root / "leakage_preflight.json",
        "notice_sbom_preflight": derivation_root / "notice_sbom_preflight.json",
    }


def _ensure_derivation_artifacts(root: Path, derivation_root: Path) -> dict[str, dict[str, object]]:
    paths = _artifact_paths(derivation_root)
    results: dict[str, dict[str, object]] = {}
    public_module = _load_tool("tools.p6_public_core_export_dry_run")
    private_module = _load_tool("tools.p6_private_core_export_dry_run")
    source_module = _load_tool("tools.p6_release_source_manifest")
    leakage_module = _load_tool("tools.p6_public_private_leakage_preflight")
    notice_module = cast(_NoticeModule, _load_tool("tools.p6_notice_sbom_preflight"))

    if paths["public_plan"].exists():
        results["public_plan"] = _load_json_object(paths["public_plan"])
    else:
        results["public_plan"] = public_module.build_report(
            root=root,
            manifest_path=root / "docs/p6/public_core_export_manifest.json",
            six_repo_manifest_path=root / "docs/p6/six_repo_manifest.json",
        )
        paths["public_plan"].parent.mkdir(parents=True, exist_ok=True)
        paths["public_plan"].write_text(
            json.dumps(results["public_plan"], ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    if paths["private_plan"].exists():
        results["private_plan"] = _load_json_object(paths["private_plan"])
    else:
        results["private_plan"] = private_module.build_report(
            root=root,
            manifest_path=root / "docs/p6/private_core_export_manifest.json",
            six_repo_manifest_path=root / "docs/p6/six_repo_manifest.json",
        )
        paths["private_plan"].write_text(
            json.dumps(results["private_plan"], ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    if paths["release_source_manifest"].exists():
        results["release_source_manifest"] = _load_json_object(paths["release_source_manifest"])
    else:
        results["release_source_manifest"] = source_module.build_report(
            root=root,
            public_manifest_path=root / "docs/p6/public_core_export_manifest.json",
            private_manifest_path=root / "docs/p6/private_core_export_manifest.json",
            six_repo_manifest_path=root / "docs/p6/six_repo_manifest.json",
        )
        paths["release_source_manifest"].write_text(
            json.dumps(results["release_source_manifest"], ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    if paths["leakage_preflight"].exists():
        results["leakage_preflight"] = _load_json_object(paths["leakage_preflight"])
    else:
        results["leakage_preflight"] = leakage_module.build_report(
            root=root,
            public_plan_path=paths["public_plan"],
            private_plan_path=paths["private_plan"],
            commercial_scan_path=root / ".tmp/p6_m1_boundary/commercial_contamination_scan.json",
            forbidden_import_scan_path=root / ".tmp/p6_m1_boundary/forbidden_import_scan.json",
            security_scan_path=root / ".tmp/p6_m1_boundary/security_sensitive_path_scan.json",
        )
        paths["leakage_preflight"].write_text(
            json.dumps(results["leakage_preflight"], ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    if paths["notice_sbom_preflight"].exists():
        results["notice_sbom_preflight"] = _load_json_object(paths["notice_sbom_preflight"])
    else:
        results["notice_sbom_preflight"] = notice_module.build_report(
            root=root,
            public_plan_path=paths["public_plan"],
            private_plan_path=paths["private_plan"],
        )
        paths["notice_sbom_preflight"].write_text(
            json.dumps(results["notice_sbom_preflight"], ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return results


def build_report(*, root: Path, out_root: Path, derivation_root: Path) -> dict[str, object]:
    for path in _required_doc_paths(root).values():
        if not path.exists():
            raise FileNotFoundError(path)
    artifacts = _ensure_derivation_artifacts(root, derivation_root)
    release_source = artifacts["release_source_manifest"]
    leakage = artifacts["leakage_preflight"]
    notice = artifacts["notice_sbom_preflight"]

    leakage_summary = _as_dict(leakage["summary"])
    notice_summary = _as_dict(notice["summary"])
    release_lineage = _as_dict(release_source["lineage"])
    active_blockers = _as_int(leakage_summary["active_blockers"])
    overall = "pass" if active_blockers == 0 else "fail"
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m2.derivation_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "p6_m2_status": "sealed" if active_blockers == 0 else "open",
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "baseline": {
            "p5_1_final_sha": "b94e662a3792fd1a253d457bc1f73b38aefb33fd",
            "p6_m1_sealed": True,
        },
        "export_mechanics": {
            "public_export_plan": True,
            "private_export_plan": True,
            "actual_export_performed": False,
            "code_migration_performed": False,
            "target_repos_created": False,
        },
        "lineage": {
            "source_sha_required": _as_bool(release_lineage["source_sha_required"]),
            "manifest_hash_required": _as_bool(release_lineage["manifest_hash_required"]),
            "combined_derivation_manifest_sha256_present": bool(
                release_lineage["combined_derivation_manifest_sha256"]
            ),
        },
        "leakage_preflight": {
            "overall": leakage_summary["overall"],
            "active_blockers": leakage_summary["active_blockers"],
            "public_leakage_blockers": leakage_summary["public_leakage_blockers"],
            "private_leakage_blockers": leakage_summary["private_leakage_blockers"],
            "zephyr_dev_contamination_blockers": leakage_summary[
                "zephyr_dev_contamination_blockers"
            ],
        },
        "notice_sbom_preflight": {
            "preflight_only": notice_summary["preflight_only"],
            "public_release_legal_ready": notice_summary["public_release_legal_ready"],
            "private_release_legal_ready": notice_summary["private_release_legal_ready"],
        },
        "non_blocking_notes": [
            "M2 is dry-run derivation mechanics, not actual repo split.",
            "NOTICE/SBOM/legal readiness is preflight only.",
            "Actual Base/Pro/Web/Web-core/Site product work starts later.",
            "No runtime-home modification.",
        ],
        "issues": [],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    leakage = _as_dict(report["leakage_preflight"])
    notice = _as_dict(report["notice_sbom_preflight"])
    lines = [
        "# P6-M2 derivation mechanics handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        f"- P6-M2 status: {summary['p6_m2_status']}",
        "",
        "## What M2 completed",
        "- public export dry-run plan",
        "- private export dry-run plan",
        "- release source SHA and manifest hash lineage",
        "- public/private leakage preflight",
        "- NOTICE/SBOM/license preflight",
        "- derivation mechanics handoff pack",
        "",
        "## What M2 did not do",
        "- did not create actual six repos",
        "- did not migrate code",
        "- did not implement product features",
        "- did not implement commercial runtime logic",
        "- did not perform legal review",
        "- did not produce release artifacts",
        "",
        "## Leakage preflight",
        f"- public leakage: {leakage['public_leakage_blockers']}",
        f"- private leakage: {leakage['private_leakage_blockers']}",
        f"- Zephyr-dev contamination: {leakage['zephyr_dev_contamination_blockers']}",
        "",
        "## NOTICE/SBOM preflight",
        f"- preflight_only: {notice['preflight_only']}",
        f"- public_release_legal_ready: {notice['public_release_legal_ready']}",
        f"- private_release_legal_ready: {notice['private_release_legal_ready']}",
        "",
        "## Next step",
        (
            "- P6-M3 can start Base product scaffold only after M2 sealed. "
            "Do not start Pro/Web-core before Base/public export mechanics are "
            "stable unless user explicitly parallelizes."
        ),
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
    parser = argparse.ArgumentParser(description="Build the P6-M2 derivation handoff pack.")
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
    derivation_root = root / DEFAULT_DERIVATION_ROOT
    if args.check_artifacts:
        required = (
            derivation_root / "public_export_plan.json",
            derivation_root / "private_export_plan.json",
            derivation_root / "release_source_manifest.json",
            derivation_root / "leakage_preflight.json",
            derivation_root / "notice_sbom_preflight.json",
            out_root / OUTPUT_JSON,
            out_root / OUTPUT_MD,
        )
        return 0 if all(path.exists() for path in required) else 1
    report = build_report(root=root, out_root=out_root, derivation_root=derivation_root)
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
