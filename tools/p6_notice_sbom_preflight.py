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
DEFAULT_PUBLIC_PLAN = Path(".tmp/p6_m2_derivation/public_export_plan.json")
DEFAULT_PRIVATE_PLAN = Path(".tmp/p6_m2_derivation/private_export_plan.json")
DEFAULT_OUT_ROOT = Path(".tmp/p6_m2_derivation")
OUTPUT_JSON = "notice_sbom_preflight.json"
OUTPUT_MD = "notice_sbom_preflight.md"


class SummaryDict(TypedDict):
    overall: str
    preflight_only: bool
    public_release_legal_ready: bool
    private_release_legal_ready: bool


def _generated_at_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _find_first_existing(root: Path, candidates: list[str]) -> str | None:
    for item in candidates:
        if (root / item).exists():
            return item
    return None


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected dict, got {type(value).__name__}")
    return cast(dict[str, object], value)


def build_report(
    *,
    root: Path,
    public_plan_path: Path,
    private_plan_path: Path,
) -> dict[str, object]:
    license_file = _find_first_existing(root, ["LICENSE", "LICENSE.txt", "COPYING", "COPYING.txt"])
    notice_file = _find_first_existing(root, ["NOTICE", "NOTICE.txt", "docs/NOTICE.md"])
    sbom_file = _find_first_existing(
        root,
        ["SBOM.json", "SBOM.spdx.json", "sbom.json", "sbom.spdx.json"],
    )
    dependency_manifest = _find_first_existing(
        root,
        [
            "pyproject.toml",
            "uv.lock",
            "poetry.lock",
            "package-lock.json",
            "pnpm-lock.yaml",
            "yarn.lock",
            "requirements.txt",
        ],
    )
    public_ready = all(
        item is not None for item in (license_file, notice_file, sbom_file, dependency_manifest)
    )
    private_ready = public_ready
    notes = [
        "M2 is preflight only and does not complete legal review.",
        "Public release cannot proceed without license, NOTICE, and SBOM review.",
        "Private release also requires dependency attribution review.",
    ]
    if not public_plan_path.exists():
        notes.append("Public export plan artifact was not present during preflight.")
    if not private_plan_path.exists():
        notes.append("Private export plan artifact was not present during preflight.")
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m2.notice_sbom_preflight.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": "pass",
            "preflight_only": True,
            "public_release_legal_ready": public_ready,
            "private_release_legal_ready": private_ready,
        },
        "license_file_found": license_file is not None,
        "notice_file_found": notice_file is not None,
        "sbom_file_found": sbom_file is not None,
        "dependency_manifest_found": dependency_manifest is not None,
        "public_release_legal_ready": public_ready,
        "private_release_legal_ready": private_ready,
        "preflight_only": True,
        "discovered_files": {
            "license": license_file,
            "notice": notice_file,
            "sbom": sbom_file,
            "dependency_manifest": dependency_manifest,
        },
        "issues": [],
        "non_blocking_notes": notes,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    lines = [
        "# P6 NOTICE SBOM preflight",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- preflight_only: {summary['preflight_only']}",
        f"- public_release_legal_ready: {summary['public_release_legal_ready']}",
        f"- private_release_legal_ready: {summary['private_release_legal_ready']}",
        "",
        "## Discovery",
        f"- license_file_found: {report['license_file_found']}",
        f"- notice_file_found: {report['notice_file_found']}",
        f"- sbom_file_found: {report['sbom_file_found']}",
        f"- dependency_manifest_found: {report['dependency_manifest_found']}",
        "",
        "## Policy note",
        "- M2 is preflight only.",
        "- Full legal/license/NOTICE/SBOM finalization belongs to later P6 release gates.",
        "- Public Base release cannot happen without license/NOTICE/SBOM review.",
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
    parser = argparse.ArgumentParser(description="Run NOTICE/SBOM/license preflight checks.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--public-plan", type=Path, default=DEFAULT_PUBLIC_PLAN)
    parser.add_argument("--private-plan", type=Path, default=DEFAULT_PRIVATE_PLAN)
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
    )
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
