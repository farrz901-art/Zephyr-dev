from __future__ import annotations

from pathlib import Path

from tools.pytest_target import ensure_basetemp_dirs, extract_basetemp_paths


def test_extract_basetemp_paths_supports_equals_and_split_forms() -> None:
    observed = extract_basetemp_paths(
        [
            "--collect-only",
            "--basetemp=.pytest_tmp_artifacts/core",
            "--basetemp",
            ".pytest_tmp_artifacts/contracts",
        ]
    )

    assert observed == (
        Path(".pytest_tmp_artifacts/core"),
        Path(".pytest_tmp_artifacts/contracts"),
    )


def test_ensure_basetemp_dirs_creates_requested_directory(tmp_path: Path) -> None:
    basetemp = tmp_path / "pytest" / "fixtures"

    observed = ensure_basetemp_dirs(["--basetemp", str(basetemp)])

    assert observed == (basetemp,)
    assert basetemp.exists()
    assert basetemp.is_dir()
