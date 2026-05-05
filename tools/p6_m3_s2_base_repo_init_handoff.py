from __future__ import annotations

import argparse
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
DEFAULT_BASE_ROOT = Path("E:/Github_Projects/Zephyr-base")
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_s2_base_repo_init_handoff")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
EXPECTED_SOURCE_SHA = "780690a21ddc60bb5a01529d3d35358f08903685"
P5_FINAL_SHA = "b94e662a3792fd1a253d457bc1f73b38aefb33fd"


class SummaryDict(TypedDict):
    overall: str
    m3_s2_status: str
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


def _as_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected bool, got {type(value).__name__}")
    return value


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value).__name__}")
    return value


def _resolve_from_root(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (root / path).resolve()


def _git_output(repo: Path, *args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _git_first_line(repo: Path, *args: str) -> str | None:
    output = _git_output(repo, *args)
    if not output:
        return None
    return output.splitlines()[0].strip()


def _required_paths(base_root: Path) -> dict[str, Path]:
    return {
        "readme": base_root / "README.md",
        "product_boundary": base_root / "PRODUCT_BOUNDARY.md",
        "source_lineage_md": base_root / "docs/SOURCE_LINEAGE.md",
        "source_lineage_json": base_root / "manifests/public_export_lineage.json",
        "boundary_script": base_root / "scripts/check_boundary.py",
        "boundary_ci": base_root / ".github/workflows/boundary.yml",
        "boundary_report": base_root / ".tmp/base_boundary_check.json",
    }


def build_report(*, root: Path, base_root: Path) -> dict[str, object]:
    required_paths = _required_paths(base_root)
    missing = [
        path.relative_to(base_root).as_posix()
        for path in required_paths.values()
        if not path.exists()
    ]
    repo_exists = base_root.exists()
    repo_created = repo_exists and (base_root / ".git").exists()
    remote_url = _git_output(base_root, "remote", "get-url", "origin") if repo_created else None
    current_head_sha = _git_output(base_root, "rev-parse", "HEAD") if repo_created else None
    initial_scaffold_commit_sha = (
        _git_first_line(base_root, "rev-list", "--max-parents=0", "HEAD")
        if repo_created
        else None
    )
    default_branch = (
        _git_output(base_root, "rev-parse", "--abbrev-ref", "HEAD") if repo_created else None
    )
    pushed = bool(remote_url and current_head_sha)
    lineage_json = (
        _load_json_object(required_paths["source_lineage_json"])
        if required_paths["source_lineage_json"].exists()
        else {}
    )
    boundary_json = (
        _load_json_object(required_paths["boundary_report"])
        if required_paths["boundary_report"].exists()
        else {}
    )
    lineage = _as_dict(lineage_json) if lineage_json else {}
    boundary_check = _as_dict(boundary_json) if boundary_json else {}
    boundary_summary = _as_dict(boundary_check["summary"]) if boundary_check else {}
    issues = [
        f"missing required base repo file: {path}"
        for path in missing
    ]
    if lineage:
        if _as_str(lineage["zephyr_dev_source_sha"]) != EXPECTED_SOURCE_SHA:
            issues.append("Base repo lineage does not record the expected Zephyr-dev source SHA.")
        if _as_bool(lineage["actual_runtime_source_copied"]):
            issues.append("Base repo must not claim copied Zephyr-dev runtime source.")
        if _as_bool(lineage["actual_product_runtime_implemented"]):
            issues.append("Base repo must not claim implemented product runtime.")
    if boundary_summary and _as_bool(boundary_summary["pass"]) is False:
        issues.append("Base repo boundary check did not pass.")
    active_blockers = len(issues)
    if active_blockers == 0 and pushed:
        overall = "pass"
    elif active_blockers == 0:
        overall = "conditional"
    else:
        overall = "fail"
    if overall == "pass":
        status = "sealed"
    elif overall == "conditional":
        status = "conditional"
    else:
        status = "open"
    manual_commands: list[str] = []
    if not pushed:
        manual_commands.extend(
            [
                (
                    "gh repo create farrz901-art/Zephyr-base --public "
                    "--source E:\\Github_Projects\\Zephyr-base "
                    "--remote origin --push"
                ),
                "git -C E:\\Github_Projects\\Zephyr-base remote -v",
                "git -C E:\\Github_Projects\\Zephyr-base rev-parse HEAD",
            ]
        )
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s2.base_repo_init_handoff.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": overall,
            "m3_s2_status": status,
            "active_blockers": active_blockers,
            "major_gaps": 0,
        },
        "zephyr_base": {
            "local_workspace": str(base_root),
            "repo_created": repo_created,
            "repo_url": remote_url,
            "pushed": pushed,
            "initial_scaffold_commit_sha": initial_scaffold_commit_sha,
            "current_head_sha": current_head_sha,
            "initial_commit_sha": initial_scaffold_commit_sha,
            "default_branch": default_branch,
            "public_visibility": True if remote_url else None,
        },
        "scope": {
            "shadow_scaffold_used": True,
            "actual_runtime_source_copied": False,
            "actual_product_runtime_implemented": False,
            "production_tauri_app_implemented": False,
            "installer_built": False,
            "release_created": False,
        },
        "lineage": {
            "zephyr_dev_source_sha": EXPECTED_SOURCE_SHA,
            "p5_1_final_sha": P5_FINAL_SHA,
            "public_manifest_hash_recorded": bool(lineage.get("public_manifest_sha256")),
            "bridge_contract_hash_recorded": bool(
                lineage.get("base_local_bridge_contract_sha256")
            ),
            "combined_base_scaffold_lineage_sha256_present": bool(
                lineage.get("combined_base_scaffold_lineage_sha256")
            ),
        },
        "boundary": {
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
            "base_boundary_check_pass": (
                _as_bool(boundary_summary["pass"]) if boundary_summary else False
            ),
        },
        "ci": {
            "workflow_added": required_paths["boundary_ci"].exists(),
            "ci_runs_tauri_build": False,
            "ci_requires_secrets": False,
        },
        "issues": issues,
        "manual_commands_if_needed": manual_commands,
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    repo = _as_dict(report["zephyr_base"])
    lineage = _as_dict(report["lineage"])
    boundary = _as_dict(report["boundary"])
    ci = _as_dict(report["ci"])
    lines = [
        "# P6-M3-S2 Base repo initialization handoff",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- major gaps: {summary['major_gaps']}",
        f"- status: {summary['m3_s2_status']}",
        "",
        "## What S2 completed",
        "- regenerated S1 scaffold lineage and boundary gates",
        "- initialized local Zephyr-base scaffold workspace",
        "- wrote source lineage and manifest lineage records",
        "- added local boundary check and minimal CI workflow",
        "",
        "## What S2 did not do",
        "- did not copy Zephyr-dev runtime source",
        "- did not implement a production Tauri app",
        "- did not build installers or releases",
        "- did not implement payment, license, entitlement, or update logic",
        "",
        "## Repo status",
        f"- local workspace: {repo['local_workspace']}",
        f"- repo_created: {repo['repo_created']}",
        f"- repo_url: {repo['repo_url']}",
        f"- pushed: {repo['pushed']}",
        (
            "- initial_scaffold_commit_sha: "
            f"{repo['initial_scaffold_commit_sha']}"
        ),
        f"- current_head_sha: {repo['current_head_sha']}",
        f"- default_branch: {repo['default_branch']}",
        "",
        "## Source lineage",
        f"- zephyr_dev_source_sha: {lineage['zephyr_dev_source_sha']}",
        (
            "- public_manifest_hash_recorded: "
            f"{lineage['public_manifest_hash_recorded']}"
        ),
        (
            "- bridge_contract_hash_recorded: "
            f"{lineage['bridge_contract_hash_recorded']}"
        ),
        (
            "- combined_base_scaffold_lineage_sha256_present: "
            f"{lineage['combined_base_scaffold_lineage_sha256_present']}"
        ),
        "",
        "## Boundary checks",
        f"- base_boundary_check_pass: {boundary['base_boundary_check_pass']}",
        f"- license_allowed: {boundary['license_allowed']}",
        f"- entitlement_allowed: {boundary['entitlement_allowed']}",
        f"- private_core_allowed: {boundary['private_core_allowed']}",
        "",
        "## CI",
        f"- workflow_added: {ci['workflow_added']}",
        f"- ci_runs_tauri_build: {ci['ci_runs_tauri_build']}",
        f"- ci_requires_secrets: {ci['ci_requires_secrets']}",
        "",
        "## Manual next steps if repo push was not possible",
    ]
    manual_commands = cast(list[str], report["manual_commands_if_needed"])
    if not manual_commands:
        lines.append("- none")
    else:
        for command in manual_commands:
            lines.append(f"- `{command}`")
    lines.extend(
        [
            "",
            "## Next step",
            "- P6-M3-S3: Base public-core bridge sample runner / local artifact fixture flow",
        ]
    )
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
    parser = argparse.ArgumentParser(
        description="Build the P6-M3-S2 Base repo initialization handoff."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--base-root", type=Path, default=DEFAULT_BASE_ROOT)
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
    base_root = args.base_root.resolve()
    report = build_report(root=root, base_root=base_root)
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
