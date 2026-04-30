from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Protocol, cast

from zephyr_ingest.testing.p45 import repo_root


class _PreparedCase(Protocol):
    user_seeded_marker_proof: bool
    out_root: Path


class _PreparedRouteCase(Protocol):
    token_strategy: str
    case: _PreparedCase


class _PrepareUnsGitCase(Protocol):
    def __call__(
        self,
        *,
        source_id: str,
        destination_id: str,
        mode: str,
        case_root: Path,
    ) -> _PreparedRouteCase: ...


class _SmokeModule(Protocol):
    def _prepare_uns_git_case(
        self,
        *,
        source_id: str,
        destination_id: str,
        mode: str,
        case_root: Path,
    ) -> _PreparedRouteCase: ...


def _load_smoke_module() -> _SmokeModule:
    tools_dir = repo_root() / "tools"
    module_path = tools_dir / "p5_nm_representative_smoke.py"
    sys.path.insert(0, str(tools_dir))
    try:
        spec = importlib.util.spec_from_file_location(
            "p5_nm_representative_smoke_test",
            module_path,
        )
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    finally:
        del sys.path[0]
    return cast(_SmokeModule, module)


def test_prepare_uns_git_case_uses_absolute_repo_root() -> None:
    smoke = _load_smoke_module()
    prepare_uns_git_case = cast(
        "_PrepareUnsGitCase",
        getattr(smoke, "_prepare_uns_git_case"),
    )
    repo_tmp = repo_root() / ".tmp"
    repo_tmp.mkdir(parents=True, exist_ok=True)
    case_root = Path(tempfile.mkdtemp(prefix="p51_nm_git_sqlite_", dir=str(repo_tmp)))
    try:
        prepared = prepare_uns_git_case(
            source_id="source.uns.git_document.v1",
            destination_id="destination.sqlite.v1",
            mode="direct",
            case_root=case_root,
        )

        spec = json.loads((case_root / "git_document.json").read_text(encoding="utf-8"))
        source = cast(dict[str, object], spec["source"])
        assert isinstance(source, dict)
        repo_root_value = source["repo_root"]
        assert isinstance(repo_root_value, str)
        assert Path(repo_root_value).is_absolute()
        assert Path(repo_root_value) == (case_root / "git_repo").resolve()
        assert prepared.token_strategy == "seeded_source_marker"
        assert prepared.case.user_seeded_marker_proof is True
        assert prepared.case.out_root.exists()
    finally:
        shutil.rmtree(case_root, ignore_errors=True)
