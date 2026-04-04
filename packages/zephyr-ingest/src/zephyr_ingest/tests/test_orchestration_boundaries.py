from __future__ import annotations

import ast
from pathlib import Path


def _read_imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)

    return modules


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def test_runner_does_not_import_uns_stream_directly() -> None:
    runner_path = (
        _repo_root() / "packages" / "zephyr-ingest" / "src" / "zephyr_ingest" / "runner.py"
    )

    imports = _read_imported_modules(runner_path)

    assert not any(module == "uns_stream" or module.startswith("uns_stream.") for module in imports)


def test_filesystem_destination_does_not_import_uns_internal_artifacts() -> None:
    filesystem_path = (
        _repo_root()
        / "packages"
        / "zephyr-ingest"
        / "src"
        / "zephyr_ingest"
        / "destinations"
        / "filesystem.py"
    )

    imports = _read_imported_modules(filesystem_path)

    assert "uns_stream._internal.artifacts" not in imports
