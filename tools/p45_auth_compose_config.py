from __future__ import annotations

import argparse
import json
import platform
from typing import Any

from zephyr_ingest.testing.p45 import resolve_p45_runtime_paths


def _build_report() -> dict[str, Any]:
    runtime_paths = resolve_p45_runtime_paths()
    env_file = runtime_paths.env_dir / ".env.p45.local"
    return {
        "runtime_home": str(runtime_paths.home),
        "compose_path": str(runtime_paths.compose_path),
        "env_file_path": str(env_file),
        "runtime_home_exists": runtime_paths.home.exists(),
        "compose_path_exists": runtime_paths.compose_path.exists(),
        "env_file_exists": env_file.exists(),
        "repo_compose_example": str(runtime_paths.repo_compose_example),
        "repo_compose_example_exists": runtime_paths.repo_compose_example.exists(),
        "platform": platform.system(),
        "platform_path_note": (
            "Legacy p45-substrate-up/down targets still compose the --env-file path with a "
            "Windows-oriented string join. Use this inspection target before manual substrate "
            "operations on non-Windows shells."
        ),
    }


def _format_lines(report: dict[str, Any]) -> str:
    ordered_keys = (
        "runtime_home",
        "runtime_home_exists",
        "compose_path",
        "compose_path_exists",
        "env_file_path",
        "env_file_exists",
        "repo_compose_example",
        "repo_compose_example_exists",
        "platform",
        "platform_path_note",
    )
    lines = ["P4.5 authenticity compose configuration"]
    for key in ordered_keys:
        lines.append(f"{key}: {report[key]}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print the resolved P4.5 authenticity compose/runtime configuration."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the compose configuration as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = _build_report()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(_format_lines(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
