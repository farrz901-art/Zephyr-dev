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
DEFAULT_PUBLIC_MANIFEST = Path("docs/p6/public_core_export_manifest.json")
DEFAULT_PRIVATE_MANIFEST = Path("docs/p6/private_core_export_manifest.json")
DEFAULT_SIX_REPO_MANIFEST = Path("docs/p6/six_repo_manifest.json")
DEFAULT_OUT_ROOT = Path(".tmp/p6_m2_derivation")
OUTPUT_JSON = "release_source_manifest.json"
OUTPUT_MD = "release_source_manifest.md"
P5_FINAL_SHA = "b94e662a3792fd1a253d457bc1f73b38aefb33fd"


class SummaryDict(TypedDict):
    overall: str
    source_sha_required: bool
    manifest_hash_required: bool


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


def _as_str(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected str, got {type(value).__name__}")
    return value


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


def build_report(
    *,
    root: Path,
    public_manifest_path: Path,
    private_manifest_path: Path,
    six_repo_manifest_path: Path,
) -> dict[str, object]:
    six_repo_manifest = _load_json_object(six_repo_manifest_path)
    baseline = _as_dict(six_repo_manifest["baseline"])
    public_sha = _sha256_file(public_manifest_path)
    private_sha = _sha256_file(private_manifest_path)
    six_repo_sha = _sha256_file(six_repo_manifest_path)
    combined_sha = hashlib.sha256(
        "\n".join([public_sha, private_sha, six_repo_sha]).encode("utf-8")
    ).hexdigest()
    current_sha = _best_effort_git_sha(root)
    derivation_plan_id = f"zephyr.p6.m2.derivation.{combined_sha[:16]}"
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.release_source_manifest.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": "pass",
            "source_sha_required": True,
            "manifest_hash_required": True,
        },
        "lineage": {
            "p5_1_final_sha": P5_FINAL_SHA,
            "p6_m1_boundary_sha": _as_str(baseline["p6_m1_s1_s6_sha"]),
            "zephyr_dev_current_sha": current_sha,
            "public_core_export_manifest_sha256": public_sha,
            "private_core_export_manifest_sha256": private_sha,
            "six_repo_manifest_sha256": six_repo_sha,
            "combined_derivation_manifest_sha256": combined_sha,
            "derivation_plan_id": derivation_plan_id,
            "source_sha_required": True,
            "manifest_hash_required": True,
        },
        "policy": {
            "every_future_release_must_trace_source_sha": True,
            "every_future_release_must_trace_export_manifest_hash": True,
            "release_artifact_generated": False,
            "signature_generated": False,
        },
        "issues": [],
        "non_blocking_notes": [
            "M2 computes lineage facts only and does not publish any release artifact.",
            "Future Base/Pro/Web/Web-core/Site releases must carry source SHA and manifest hash.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, _as_dict(report["summary"]))
    lineage = _as_dict(report["lineage"])
    lines = [
        "# P6 release source manifest",
        "",
        "## Final judgment",
        f"- overall: {summary['overall']}",
        f"- source_sha_required: {summary['source_sha_required']}",
        f"- manifest_hash_required: {summary['manifest_hash_required']}",
        "",
        "## Lineage",
        f"- p5_1_final_sha: {_as_str(lineage['p5_1_final_sha'])}",
        f"- p6_m1_boundary_sha: {_as_str(lineage['p6_m1_boundary_sha'])}",
        f"- zephyr_dev_current_sha: {_as_str(lineage['zephyr_dev_current_sha'])}",
        (
            "- public_core_export_manifest_sha256: "
            f"{_as_str(lineage['public_core_export_manifest_sha256'])}"
        ),
        (
            "- private_core_export_manifest_sha256: "
            f"{_as_str(lineage['private_core_export_manifest_sha256'])}"
        ),
        f"- six_repo_manifest_sha256: {_as_str(lineage['six_repo_manifest_sha256'])}",
        (
            "- combined_derivation_manifest_sha256: "
            f"{_as_str(lineage['combined_derivation_manifest_sha256'])}"
        ),
        "",
        "## Policy",
        "- every future derived release must record source SHA",
        "- every future derived release must record export manifest hash",
        "- M2 does not create release artifacts or signatures",
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
    parser = argparse.ArgumentParser(description="Build M2 release source lineage metadata.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--public-manifest", type=Path, default=DEFAULT_PUBLIC_MANIFEST)
    parser.add_argument("--private-manifest", type=Path, default=DEFAULT_PRIVATE_MANIFEST)
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
    public_manifest_path = (
        (root / args.public_manifest).resolve()
        if not args.public_manifest.is_absolute()
        else args.public_manifest
    )
    private_manifest_path = (
        (root / args.private_manifest).resolve()
        if not args.private_manifest.is_absolute()
        else args.private_manifest
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
        public_manifest_path=public_manifest_path,
        private_manifest_path=private_manifest_path,
        six_repo_manifest_path=six_repo_manifest_path,
    )
    emit_outputs(report=report, out_root=out_root, markdown=bool(args.markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
