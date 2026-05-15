from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_base_repo_scaffold")
OUTPUT_JSON = "base_lineage_gate.json"
OUTPUT_MD = "base_lineage_gate.md"


class SummaryDict(TypedDict):
    overall: str
    active_blockers: int
    source_sha_recorded: bool
    public_manifest_hash_recorded: bool


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


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _best_effort_git_sha(root: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return "unknown"
    return completed.stdout.strip() if completed.returncode == 0 else "unknown"


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def build_report(*, root: Path) -> dict[str, object]:
    public_manifest = root / "docs/p6/public_core_export_manifest.json"
    scaffold_manifest = root / "docs/p6/base_repo_scaffold_manifest.json"
    consumption_plan = root / "docs/p6/base_public_export_consumption_plan.json"
    bridge_contract = root / "docs/p6/base_local_bridge_contract.json"
    required = (
        public_manifest,
        scaffold_manifest,
        consumption_plan,
        bridge_contract,
    )
    issues = [
        f"missing required file: {path.relative_to(root).as_posix()}"
        for path in required
        if not path.exists()
    ]
    source_sha = _best_effort_git_sha(root)
    public_sha = _sha256_file(public_manifest)
    scaffold_sha = _sha256_file(scaffold_manifest)
    consumption_sha = _sha256_file(consumption_plan)
    bridge_sha = _sha256_file(bridge_contract)
    combined_sha = hashlib.sha256(
        "\n".join([public_sha, scaffold_sha, consumption_sha, bridge_sha]).encode("utf-8")
    ).hexdigest()
    source_known = source_sha != "unknown"
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.base_lineage_gate.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": "pass" if not issues else "fail",
            "active_blockers": len(issues),
            "source_sha_recorded": source_known,
            "public_manifest_hash_recorded": True,
        },
        "lineage": {
            "source_sha": source_sha,
            "public_manifest_sha256": public_sha,
            "base_repo_scaffold_manifest_sha256": scaffold_sha,
            "base_public_export_consumption_plan_sha256": consumption_sha,
            "base_local_bridge_contract_sha256": bridge_sha,
            "combined_base_scaffold_lineage_sha256": combined_sha,
            "source_sha_recorded": source_known,
            "public_manifest_hash_recorded": True,
            "scaffold_manifest_hash_recorded": True,
            "bridge_contract_hash_recorded": True,
        },
        "issues": issues,
        "non_blocking_notes": [
            "Lineage gate records scaffold hashes only and does not create release artifacts.",
            "No signing or upload is performed in this step.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    lineage = _as_dict(report["lineage"])
    lines = [
        "# P6-M3-S1 Base lineage gate",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active_blockers: {summary['active_blockers']}",
        f"- source_sha_recorded: {summary['source_sha_recorded']}",
        f"- public_manifest_hash_recorded: {summary['public_manifest_hash_recorded']}",
        "",
        "## Lineage",
        f"- source_sha: {lineage['source_sha']}",
        f"- public_manifest_sha256: {lineage['public_manifest_sha256']}",
        (
            "- base_repo_scaffold_manifest_sha256: "
            f"{lineage['base_repo_scaffold_manifest_sha256']}"
        ),
        (
            "- base_public_export_consumption_plan_sha256: "
            f"{lineage['base_public_export_consumption_plan_sha256']}"
        ),
        (
            "- base_local_bridge_contract_sha256: "
            f"{lineage['base_local_bridge_contract_sha256']}"
        ),
        (
            "- combined_base_scaffold_lineage_sha256: "
            f"{lineage['combined_base_scaffold_lineage_sha256']}"
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
    parser = argparse.ArgumentParser(description="Build the Base scaffold lineage gate.")
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
    out_root = _resolve_from_root(root, args.out_root)
    if args.check_artifacts:
        required = (out_root / OUTPUT_JSON, out_root / OUTPUT_MD)
        return 0 if all(path.exists() for path in required) else 1
    report = build_report(root=root)
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
