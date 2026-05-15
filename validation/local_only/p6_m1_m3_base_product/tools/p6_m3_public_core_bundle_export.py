from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, TypedDict, cast


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
DEFAULT_OUT_ROOT = Path(".tmp/p6_m3_public_core_bundle")
OUTPUT_JSON = "report.json"
OUTPUT_MD = "report.md"
_TXT_MD_KIND_BY_EXT: Final[dict[str, str]] = {
    ".txt": "text",
    ".text": "text",
    ".log": "text",
    ".md": "md",
    ".markdown": "md",
}
_ALLOWED_PARTITION_FILES: Final[set[str]] = {
    "__init__.py",
    "auto.py",
    "text.py",
    "md.py",
    "strategies.py",
    "text_type.py",
    "model_init.py",
}
_ALLOWED_DESTINATION_FILES: Final[set[str]] = {"__init__.py", "base.py", "filesystem.py"}
_ALLOWED_SOURCE_FILES: Final[set[str]] = {"__init__.py", "local_file.py"}
_FORBIDDEN_INGEST_FILES: Final[tuple[str, ...]] = (
    "queue_backend.py",
    "queue_backend_factory.py",
    "queue_inspect.py",
    "queue_recover.py",
    "sqlite_queue.py",
    "worker_runtime.py",
    "health_server.py",
    "governance_action.py",
    "replay_delivery.py",
    "dlq_prune.py",
    "lock_provider.py",
    "lock_provider_factory.py",
    "sqlite_lock_provider.py",
)
_FORBIDDEN_INGEST_DIRS: Final[tuple[str, ...]] = ("testing",)
_FORBIDDEN_INGEST_OBS_FILES: Final[tuple[str, ...]] = ("prom_export.py",)
_TRIMMED_BUNDLE_README = """# Zephyr Base Public Core Bundle

This is the P6-M3-S5-R first-slice bundled public-core runtime for Zephyr-base.

- public subset only
- trimmed to `.txt` / `.md` local file processing
- local filesystem output only
- not a full installer runtime yet
- uses the current Python environment
- does not require the Zephyr-dev working tree at execution
- does not include cloud, Pro, Web-core, remote sources, remote destinations, or governance helpers

Generated for Base bundled runtime packaging hardening.
"""
_TRIMMED_BUNDLE_RUNNER = """from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict, cast

from uns_stream._internal.utils import sha256_file
from uns_stream.partition.auto import partition as partition_auto
from zephyr_core import PartitionStrategy

SUPPORTED_INPUT_EXTENSIONS = frozenset({".txt", ".md"})
OUTPUT_FILES = [
    "normalized_text.txt",
    "content_evidence.json",
    "receipt.json",
    "usage_fact.json",
    "run_result.json",
]


class BaseErrorDict(TypedDict):
    schema_version: int
    error_code: str
    category: str
    user_message: str
    technical_detail_safe: str
    secret_safe: bool


class RequestDict(TypedDict, total=False):
    schema_version: int
    request_id: str
    input_kind: str
    input_path: str
    inline_text: str
    output_dir: str
    requested_outputs: list[str]


@dataclass(frozen=True, slots=True)
class PreparedInput:
    input_path: Path
    input_kind: str
    request_id: str
    bridge_mode: str | None
    input_bytes: int


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\\xff\\xfe") or raw.startswith(b"\\xfe\\xff"):
        return raw.decode("utf-16")
    if raw.startswith(b"\\xef\\xbb\\xbf"):
        return raw.decode("utf-8-sig")
    return raw.decode("utf-8")


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(_read_text(path))
    if not isinstance(loaded_obj, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return cast(dict[str, object], loaded_obj)


def _resolve_from_root(root: Path, path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else (root / path).resolve()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\\n", encoding="utf-8")


def _build_error(*, code: str, category: str, message: str, detail: str) -> BaseErrorDict:
    return {
        "schema_version": 1,
        "error_code": code,
        "category": category,
        "user_message": message,
        "technical_detail_safe": detail,
        "secret_safe": True,
    }


def _normalize_text_preview(text: str) -> str:
    normalized = text.replace("\\r\\n", "\\n").replace("\\r", "\\n").strip()
    return normalized[:160]


def _write_failure_outputs(
    *,
    out_dir: Path,
    request_id: str,
    error: BaseErrorDict,
    input_bytes: int,
    bridge_mode: str | None,
) -> dict[str, object]:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "normalized_text.txt").write_text("", encoding="utf-8")
    content_evidence: dict[str, object] = {
        "schema_version": 1,
        "evidence_kind": "artifact_reference_only_v1",
        "normalized_text_status": "missing",
        "elements_count": 0,
        "production_runtime": False,
        "adapter_runtime": "zephyr_bundle_public_core_local_runner_v1",
        "fixture_runner_used": False,
        "bundled_runtime_used": True,
        "zephyr_dev_public_core_invoked": False,
        "zephyr_dev_working_tree_required": False,
        "installer_runtime_complete": False,
        "requires_network": False,
        "requires_p45_substrate": False,
    }
    receipt: dict[str, object] = {
        "schema_version": 1,
        "run_id": f"failed-{request_id}",
        "request_id": request_id,
        "status": "failed",
        "delivery_outcome": "failed",
        "output_root": str(out_dir),
        "artifacts": OUTPUT_FILES,
        "created_by": "Zephyr bundled public core runner",
        "production_runtime": False,
        "adapter_runtime": "zephyr_bundle_public_core_local_runner_v1",
        "fixture_runner_used": False,
        "bundled_runtime_used": True,
        "zephyr_dev_public_core_invoked": False,
        "zephyr_dev_working_tree_required": False,
        "installer_runtime_complete": False,
        "requires_network": False,
        "requires_p45_substrate": False,
    }
    usage_fact: dict[str, object] = {
        "schema_version": 1,
        "fact_kind": "technical_usage_fact",
        "billing_semantics": False,
        "input_bytes": input_bytes,
        "output_files_count": len(OUTPUT_FILES),
        "adapter_runtime": "zephyr_bundle_public_core_local_runner_v1",
        "production_runtime": False,
        "fixture_runner_used": False,
        "bundled_runtime_used": True,
        "zephyr_dev_public_core_invoked": False,
        "zephyr_dev_working_tree_required": False,
        "installer_runtime_complete": False,
        "requires_network": False,
        "requires_p45_substrate": False,
    }
    run_result: dict[str, object] = {
        "schema_version": 1,
        "request_id": request_id,
        "status": "failed",
        "normalized_text_preview": "",
        "content_evidence_summary": {
            "elements_count": 0,
            "has_normalized_text": False,
            "evidence_kind": "artifact_reference_only_v1",
        },
        "receipt": receipt,
        "usage_fact": usage_fact,
        "output_files": OUTPUT_FILES,
        "error": error,
        "adapter_runtime": "zephyr_bundle_public_core_local_runner_v1",
        "production_runtime": False,
        "fixture_runner_used": False,
        "bundled_runtime_used": True,
        "zephyr_dev_public_core_invoked": False,
        "zephyr_dev_working_tree_required": False,
        "installer_runtime_complete": False,
        "requires_network": False,
        "requires_p45_substrate": False,
    }
    if bridge_mode is not None:
        content_evidence["local_text_bridge_mode"] = bridge_mode
        receipt["local_text_bridge_mode"] = bridge_mode
        usage_fact["local_text_bridge_mode"] = bridge_mode
        run_result["local_text_bridge_mode"] = bridge_mode
    _write_json(out_dir / "content_evidence.json", content_evidence)
    _write_json(out_dir / "receipt.json", receipt)
    _write_json(out_dir / "usage_fact.json", usage_fact)
    _write_json(out_dir / "run_result.json", run_result)
    return run_result


def _prepare_input(*, root: Path, request: RequestDict, temp_root: Path) -> PreparedInput:
    request_id = str(request.get("request_id", "unknown-request"))
    input_kind = str(request.get("input_kind", "unknown"))
    if input_kind == "local_file":
        input_path_value = str(request.get("input_path", ""))
        input_path = _resolve_from_root(root, input_path_value)
        if not input_path.exists():
            raise FileNotFoundError(str(input_path))
        ext = input_path.suffix.lower()
        if ext not in SUPPORTED_INPUT_EXTENSIONS:
            raise ValueError(f"unsupported_extension:{ext}")
        return PreparedInput(
            input_path=input_path,
            input_kind=input_kind,
            request_id=request_id,
            bridge_mode=None,
            input_bytes=input_path.stat().st_size,
        )
    if input_kind == "local_text":
        inline_text = str(request.get("inline_text", ""))
        temp_root.mkdir(parents=True, exist_ok=True)
        temp_path = temp_root / f"{request_id}.txt"
        temp_path.write_text(inline_text, encoding="utf-8")
        return PreparedInput(
            input_path=temp_path,
            input_kind=input_kind,
            request_id=request_id,
            bridge_mode="temp_file_to_public_core",
            input_bytes=len(inline_text.encode("utf-8")),
        )
    raise ValueError(f"unsupported_input_kind:{input_kind}")


def _build_success_outputs(
    *,
    prepared: PreparedInput,
    request: RequestDict,
    out_dir: Path,
) -> dict[str, object]:
    result = partition_auto(filename=str(prepared.input_path), strategy=PartitionStrategy.AUTO)
    normalized_text = result.normalized_text
    normalized_preview = _normalize_text_preview(normalized_text)
    content_evidence: dict[str, object] = {
        "schema_version": 1,
        "evidence_kind": "public_core_content_evidence_v1",
        "normalized_text_status": "available",
        "normalized_text_preview": normalized_preview,
        "normalized_text_len": len(normalized_text),
        "elements_count": len(result.elements),
        "source_kind": prepared.input_kind,
        "input_bytes": prepared.input_bytes,
        "production_runtime": True,
        "adapter_runtime": "zephyr_bundle_public_core_local_runner_v1",
        "fixture_runner_used": False,
        "bundled_runtime_used": True,
        "zephyr_dev_public_core_invoked": False,
        "zephyr_dev_working_tree_required": False,
        "installer_runtime_complete": False,
        "requires_network": False,
        "requires_p45_substrate": False,
    }
    receipt: dict[str, object] = {
        "schema_version": 1,
        "run_id": sha256_file(prepared.input_path),
        "request_id": prepared.request_id,
        "status": "success",
        "delivery_outcome": "success",
        "output_root": str(out_dir),
        "artifacts": OUTPUT_FILES,
        "created_by": "Zephyr bundled public core runner",
        "production_runtime": True,
        "adapter_runtime": "zephyr_bundle_public_core_local_runner_v1",
        "fixture_runner_used": False,
        "bundled_runtime_used": True,
        "zephyr_dev_public_core_invoked": False,
        "zephyr_dev_working_tree_required": False,
        "installer_runtime_complete": False,
        "requires_network": False,
        "requires_p45_substrate": False,
    }
    usage_fact: dict[str, object] = {
        "schema_version": 1,
        "fact_kind": "technical_usage_fact",
        "billing_semantics": False,
        "input_bytes": prepared.input_bytes,
        "output_files_count": len(OUTPUT_FILES),
        "adapter_runtime": "zephyr_bundle_public_core_local_runner_v1",
        "production_runtime": True,
        "fixture_runner_used": False,
        "bundled_runtime_used": True,
        "zephyr_dev_public_core_invoked": False,
        "zephyr_dev_working_tree_required": False,
        "installer_runtime_complete": False,
        "requires_network": False,
        "requires_p45_substrate": False,
    }
    run_result: dict[str, object] = {
        "schema_version": 1,
        "request_id": prepared.request_id,
        "status": "success",
        "normalized_text_preview": normalized_preview,
        "content_evidence_summary": {
            "elements_count": len(result.elements),
            "has_normalized_text": True,
            "evidence_kind": "public_core_content_evidence_v1",
        },
        "receipt": receipt,
        "usage_fact": usage_fact,
        "output_files": OUTPUT_FILES,
        "error": None,
        "adapter_runtime": "zephyr_bundle_public_core_local_runner_v1",
        "production_runtime": True,
        "fixture_runner_used": False,
        "bundled_runtime_used": True,
        "zephyr_dev_public_core_invoked": False,
        "zephyr_dev_working_tree_required": False,
        "installer_runtime_complete": False,
        "requires_network": False,
        "requires_p45_substrate": False,
        "requested_outputs": request.get("requested_outputs", []),
        "supported_input_extensions": sorted(SUPPORTED_INPUT_EXTENSIONS),
        "content_evidence_kind": "public_core_content_evidence_v1",
    }
    if prepared.bridge_mode is not None:
        content_evidence["local_text_bridge_mode"] = prepared.bridge_mode
        receipt["local_text_bridge_mode"] = prepared.bridge_mode
        usage_fact["local_text_bridge_mode"] = prepared.bridge_mode
        run_result["local_text_bridge_mode"] = prepared.bridge_mode

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "normalized_text.txt").write_text(normalized_text, encoding="utf-8")
    _write_json(out_dir / "content_evidence.json", content_evidence)
    _write_json(out_dir / "receipt.json", receipt)
    _write_json(out_dir / "usage_fact.json", usage_fact)
    _write_json(out_dir / "run_result.json", run_result)
    return run_result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the bundled public core local runner.")
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    root = args.root.resolve()
    request_path = args.request if args.request.is_absolute() else (root / args.request).resolve()
    out_dir = args.out_dir if args.out_dir.is_absolute() else (root / args.out_dir).resolve()
    request = cast(RequestDict, _load_json_object(request_path))
    temp_root = out_dir / "_temp_inputs"
    exit_code = 0
    try:
        prepared = _prepare_input(root=root, request=request, temp_root=temp_root)
        run_result = _build_success_outputs(prepared=prepared, request=request, out_dir=out_dir)
    except FileNotFoundError as exc:
        run_result = _write_failure_outputs(
            out_dir=out_dir,
            request_id=str(request.get("request_id", "unknown-request")),
            error=_build_error(
                code="base_input_missing",
                category="input",
                message="Input file was not found.",
                detail=str(exc),
            ),
            input_bytes=0,
            bridge_mode=None,
        )
        exit_code = 1
    except ValueError as exc:
        detail = str(exc)
        category = "input" if detail.startswith("unsupported_") else "processing"
        run_result = _write_failure_outputs(
            out_dir=out_dir,
            request_id=str(request.get("request_id", "unknown-request")),
            error=_build_error(
                code="base_request_invalid",
                category=category,
                message="Bundled public-core request could not be processed.",
                detail=detail,
            ),
            input_bytes=0,
            bridge_mode=None,
        )
        exit_code = 1
    except Exception as exc:
        run_result = _write_failure_outputs(
            out_dir=out_dir,
            request_id=str(request.get("request_id", "unknown-request")),
            error=_build_error(
                code="base_processing_failed",
                category="processing",
                message="Bundled public-core processing failed.",
                detail=f"{type(exc).__name__}: {exc}",
            ),
            input_bytes=0,
            bridge_mode=None,
        )
        exit_code = 1
    if args.json:
        print(json.dumps(run_result, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
"""
_TRIMMED_ZEPHYR_CORE_INIT = """from zephyr_core.contracts import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
)
from zephyr_core.errors import ErrorCode, ZephyrError

__all__ = [
    "PartitionStrategy",
    "DocumentMetadata",
    "EngineInfo",
    "PartitionResult",
    "ZephyrElement",
    "ErrorCode",
    "ZephyrError",
]
"""
_TRIMMED_ZEPHYR_CORE_CONTRACTS_INIT = """from zephyr_core.contracts.v1 import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
)

__all__ = [
    "PartitionStrategy",
    "DocumentMetadata",
    "EngineInfo",
    "PartitionResult",
    "ZephyrElement",
]
"""
_TRIMMED_ZEPHYR_CORE_V1_INIT = """from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_core.contracts.v1.models import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    ZephyrElement,
)

__all__ = [
    "PartitionStrategy",
    "DocumentMetadata",
    "EngineInfo",
    "PartitionResult",
    "ZephyrElement",
]
"""
_TRIMMED_ZEPHYR_CORE_ERROR_CODES = """from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    IO_READ_FAILED = "ZE-IO-READ-FAILED"
    UNS_EXTRA_MISSING = "ZE-UNS-EXTRA-MISSING"
    UNS_PARTITION_FAILED = "ZE-UNS-PARTITION-FAILED"
    UNS_UNSUPPORTED_TYPE = "ZE-UNS-UNSUPPORTED-TYPE"
"""
_TRIMMED_ZEPHYR_INGEST_INIT = (
    '"""Trimmed Zephyr-ingest placeholder surface for Base bundled mode."""\n'
)
_TRIMMED_ZEPHYR_INGEST_SOURCES_INIT = (
    '"""Local-file-only source placeholder surface for Base bundled mode."""\n'
)
_TRIMMED_ZEPHYR_INGEST_LOCAL_FILE = """from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class LocalFileSource:
    path: Path
"""
_TRIMMED_ZEPHYR_INGEST_DESTINATIONS_INIT = (
    '"""Filesystem-only destination placeholder surface for Base bundled mode."""\n'
)
_TRIMMED_ZEPHYR_INGEST_DEST_BASE = """from __future__ import annotations

DeliveryReceipt = dict[str, object]
"""
_TRIMMED_ZEPHYR_INGEST_DEST_FILESYSTEM = (
    '"""Filesystem destination placeholder surface for Base bundled mode."""\n'
)
_TRIMMED_UNS_SOURCES_INIT = """from __future__ import annotations

from uns_stream.backends.base import PartitionBackend
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import PartitionResult, PartitionStrategy


def normalize_uns_input_identity_sha(*, filename: str, default_sha: str) -> str:
    del filename
    return default_sha


def process_file(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
    sha256: str | None = None,
    size_bytes: int | None = None,
) -> PartitionResult:
    return auto_partition(
        filename=filename,
        strategy=strategy,
        unique_element_ids=unique_element_ids,
        backend=backend,
        run_id=run_id,
        pipeline_version=pipeline_version,
        sha256=sha256,
        size_bytes=size_bytes,
    )
"""
_TRIMMED_PARTITION_AUTO = """from __future__ import annotations

from pathlib import Path

from uns_stream.backends.base import PartitionBackend
from uns_stream.service import partition_file
from zephyr_core import ErrorCode, PartitionResult, PartitionStrategy, ZephyrError

_KIND_BY_EXT: dict[str, str] = {
    ".txt": "text",
    ".text": "text",
    ".log": "text",
    ".md": "md",
    ".markdown": "md",
}


def partition(
    *,
    filename: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
    run_id: str | None = None,
    pipeline_version: str | None = None,
    sha256: str | None = None,
    size_bytes: int | None = None,
) -> PartitionResult:
    ext = Path(filename).suffix.lower()
    kind = _KIND_BY_EXT.get(ext)
    if kind is None:
        raise ZephyrError(
            code=ErrorCode.UNS_UNSUPPORTED_TYPE,
            message=f"Unsupported file extension: {ext}",
            details={"filename": filename, "ext": ext},
        )

    return partition_file(
        filename=filename,
        kind=kind,
        strategy=strategy,
        unique_element_ids=unique_element_ids,
        backend=backend,
        run_id=run_id,
        pipeline_version=pipeline_version,
        sha256=sha256,
        size_bytes=size_bytes,
    )
"""
_TRIMMED_LOCAL_UNSTRUCTURED = """from __future__ import annotations

from typing import Any, Callable

from uns_stream._internal.errors import missing_extra
from uns_stream._internal.serde import to_zephyr_elements
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrElement, ZephyrError

_EXTRA_BY_KIND: dict[str, str] = {
    "text": "text",
    "md": "md",
}


def _load_partition_fn(kind: str) -> Callable[..., Any]:
    fn_name = f"partition_{kind}"

    try:
        mod = __import__(f"unstructured.partition.{kind}", fromlist=[fn_name])
        fn = getattr(mod, fn_name)
        return fn
    except ModuleNotFoundError as e:
        extra = _EXTRA_BY_KIND.get(kind, "text")
        raise missing_extra(extra=extra, detail=str(e)) from e
    except AttributeError as e:
        raise ZephyrError(
            code=ErrorCode.UNS_UNSUPPORTED_TYPE,
            message=(
                f"Unsupported kind '{kind}': missing {fn_name} in unstructured.partition.{kind}"
            ),
            details={"kind": kind, "fn_name": fn_name},
        ) from e


class LocalUnstructuredBackend:
    name = "unstructured"
    backend = "local"

    def __init__(self) -> None:
        try:
            from importlib.metadata import version

            self.version = version("unstructured")
        except Exception:
            from unstructured import __version__ as v

            self.version = getattr(v, "__version__", str(v))

    def partition_elements(
        self,
        *,
        filename: str,
        kind: str,
        strategy: PartitionStrategy,
        unique_element_ids: bool = True,
        **kwargs: Any,
    ) -> list[ZephyrElement]:
        fn = _load_partition_fn(kind)

        call_kwargs: dict[str, Any] = {
            "filename": filename,
            "unique_element_ids": unique_element_ids,
            **kwargs,
        }
        elements = fn(**call_kwargs)
        return to_zephyr_elements(elements)
"""


class SummaryDict(TypedDict):
    overall: str
    active_blockers: int
    bundle_exported: bool
    bundle_execution_requires_zephyr_dev_working_tree: bool
    bundle_generation_requires_zephyr_dev_working_tree: bool
    installer_runtime_complete: bool


def _generated_at_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig")
    return raw.decode("utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(_read_text(path))
    if not isinstance(loaded_obj, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return cast(dict[str, object], loaded_obj)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _git_head_sha(root: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return "unknown"
    if completed.returncode != 0:
        return "unknown"
    return completed.stdout.strip() or "unknown"


def _launcher_text() -> str:
    return """from __future__ import annotations

import sys
from pathlib import Path


def _bundle_root() -> Path:
    return Path(__file__).resolve().parent


def _prepend_bundle_paths(bundle_root: Path) -> None:
    package_roots = [
        bundle_root / "packages" / "zephyr-core" / "src",
        bundle_root / "packages" / "zephyr-ingest" / "src",
        bundle_root / "packages" / "uns-stream" / "src",
        bundle_root / "runner",
    ]
    for package_root in reversed(package_roots):
        sys.path.insert(0, str(package_root))


def main(argv: list[str] | None = None) -> int:
    bundle_root = _bundle_root()
    _prepend_bundle_paths(bundle_root)
    import p6_m3_public_core_local_runner as runner_module

    args = list(sys.argv[1:] if argv is None else argv)
    if "--root" not in args:
        args = ["--root", str(bundle_root), *args]
    return runner_module.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
"""


def _copy_tree(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(
        src,
        dst,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("tests", "__pycache__", ".tmp"),
    )


def _remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
        return
    path.unlink()


def _write_trimmed_bundle_files(bundle_root: Path) -> None:
    _write_text(
        bundle_root / "runner" / "p6_m3_public_core_local_runner.py",
        _TRIMMED_BUNDLE_RUNNER,
    )
    uns_root = bundle_root / "packages" / "uns-stream" / "src" / "uns_stream"
    partition_root = uns_root / "partition"
    for path in partition_root.iterdir():
        if path.is_dir():
            if path.name == "__pycache__":
                shutil.rmtree(path)
            continue
        if path.name not in _ALLOWED_PARTITION_FILES:
            path.unlink()
    _write_text(partition_root / "auto.py", _TRIMMED_PARTITION_AUTO)
    _write_text(
        uns_root / "backends" / "local_unstructured.py",
        _TRIMMED_LOCAL_UNSTRUCTURED,
    )
    sources_root = uns_root / "sources"
    for path in sorted(sources_root.iterdir()):
        if path.is_dir():
            if path.name == "__pycache__":
                shutil.rmtree(path)
            continue
        if path.name != "__init__.py":
            path.unlink()
    _write_text(sources_root / "__init__.py", _TRIMMED_UNS_SOURCES_INIT)

    ingest_root = bundle_root / "packages" / "zephyr-ingest" / "src" / "zephyr_ingest"
    destinations_root = ingest_root / "destinations"
    for path in sorted(destinations_root.iterdir()):
        if path.is_dir():
            if path.name == "__pycache__":
                shutil.rmtree(path)
            continue
        if path.name not in _ALLOWED_DESTINATION_FILES:
            path.unlink()
    sources_root = ingest_root / "sources"
    for path in sorted(sources_root.iterdir()):
        if path.is_dir():
            if path.name == "__pycache__":
                shutil.rmtree(path)
            continue
        if path.name not in _ALLOWED_SOURCE_FILES:
            path.unlink()
    for dirname in _FORBIDDEN_INGEST_DIRS:
        _remove_path(ingest_root / dirname)
    for filename in _FORBIDDEN_INGEST_FILES:
        _remove_path(ingest_root / filename)
    for filename in _FORBIDDEN_INGEST_OBS_FILES:
        _remove_path(ingest_root / "obs" / filename)
    for path in sorted(ingest_root.iterdir()):
        if path.name in {"__init__.py", "py.typed", "sources", "destinations"}:
            continue
        _remove_path(path)
    _write_text(ingest_root / "__init__.py", _TRIMMED_ZEPHYR_INGEST_INIT)
    _write_text(ingest_root / "sources" / "__init__.py", _TRIMMED_ZEPHYR_INGEST_SOURCES_INIT)
    _write_text(ingest_root / "sources" / "local_file.py", _TRIMMED_ZEPHYR_INGEST_LOCAL_FILE)
    _write_text(
        ingest_root / "destinations" / "__init__.py",
        _TRIMMED_ZEPHYR_INGEST_DESTINATIONS_INIT,
    )
    _write_text(ingest_root / "destinations" / "base.py", _TRIMMED_ZEPHYR_INGEST_DEST_BASE)
    _write_text(
        ingest_root / "destinations" / "filesystem.py",
        _TRIMMED_ZEPHYR_INGEST_DEST_FILESYSTEM,
    )

    core_root = bundle_root / "packages" / "zephyr-core" / "src" / "zephyr_core"
    for path in sorted(core_root.iterdir()):
        if path.name in {"__init__.py", "contracts", "errors"}:
            continue
        _remove_path(path)
    contracts_root = core_root / "contracts"
    for path in sorted(contracts_root.iterdir()):
        if path.name in {"__init__.py", "v1"}:
            continue
        _remove_path(path)
    v1_root = contracts_root / "v1"
    for path in sorted(v1_root.iterdir()):
        if path.name in {"__init__.py", "enums.py", "models.py"}:
            continue
        _remove_path(path)
    errors_root = core_root / "errors"
    for path in sorted(errors_root.iterdir()):
        if path.name in {"__init__.py", "codes.py", "exceptions.py"}:
            continue
        _remove_path(path)
    _write_text(core_root / "__init__.py", _TRIMMED_ZEPHYR_CORE_INIT)
    _write_text(contracts_root / "__init__.py", _TRIMMED_ZEPHYR_CORE_CONTRACTS_INIT)
    _write_text(v1_root / "__init__.py", _TRIMMED_ZEPHYR_CORE_V1_INIT)
    _write_text(errors_root / "codes.py", _TRIMMED_ZEPHYR_CORE_ERROR_CODES)


def _augment_bundle_manifest(bundle_manifest: dict[str, object]) -> dict[str, object]:
    payload = dict(bundle_manifest)
    payload["bundle_surface_status"] = "trimmed_base_txt_md_first_slice"
    payload["allowed_partition_kinds"] = ["text", "md"]
    payload["allowed_sources"] = ["local_file"]
    payload["allowed_destinations"] = ["filesystem"]
    payload["removed_surface"] = {
        "non_txt_md_partition_modules_removed": True,
        "remote_sources_removed": True,
        "remote_destinations_removed": True,
        "testing_helpers_removed": True,
        "queue_worker_observability_governance_removed": True,
    }
    return payload


def _emit_bundle(
    *,
    root: Path,
    out_root: Path,
    public_manifest: dict[str, object],
    bundle_manifest: dict[str, object],
) -> tuple[Path, dict[str, object]]:
    bundle_root = out_root / "bundle"
    if bundle_root.exists():
        shutil.rmtree(bundle_root)
    bundle_root.mkdir(parents=True, exist_ok=True)

    runner_src = root / "tools/p6_m3_public_core_local_runner.py"
    runner_dst = bundle_root / "runner/p6_m3_public_core_local_runner.py"
    runner_dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(runner_src, runner_dst)

    package_mappings = {
        "zephyr-core": root / "packages/zephyr-core/src/zephyr_core",
        "zephyr-ingest": root / "packages/zephyr-ingest/src/zephyr_ingest",
        "uns-stream": root / "packages/uns-stream/src/uns_stream",
    }
    for package_name, src in package_mappings.items():
        dst = bundle_root / "packages" / package_name / "src" / src.name
        _copy_tree(src, dst)

    _write_trimmed_bundle_files(bundle_root)
    bundle_manifest_payload = _augment_bundle_manifest(bundle_manifest)
    bundle_manifest_path = bundle_root / "manifest/public_core_bundle_manifest.json"
    _write_json(bundle_manifest_path, bundle_manifest_payload)

    public_manifest_sha = _sha256_file(root / "docs/p6/public_core_export_manifest.json")
    bundle_manifest_sha = _sha256_file(root / "docs/p6/p6_m3_s5_public_core_bundle_manifest.json")
    launcher = _launcher_text()
    launcher_path = bundle_root / "run_bundle_public_core.py"
    _write_text(launcher_path, launcher)
    _write_text(bundle_root / "README.md", _TRIMMED_BUNDLE_README)

    lineage_payload: dict[str, object] = {
        "schema_version": 1,
        "report_id": "zephyr.base.public_core_bundle_lineage.v1",
        "zephyr_dev_adapter_commit_sha": bundle_manifest["zephyr_dev_adapter_commit_sha"],
        "zephyr_dev_current_sha": _git_head_sha(root),
        "p5_1_final_sha": bundle_manifest["p5_1_final_sha"],
        "public_manifest_sha256": public_manifest_sha,
        "public_core_bundle_manifest_sha256": bundle_manifest_sha,
        "runner_sha256": _sha256_file(runner_src),
        "launcher_sha256": _sha256_text(launcher),
        "bundle_generation_requires_zephyr_dev_working_tree": True,
        "bundle_execution_requires_zephyr_dev_working_tree": False,
        "uses_current_python_environment": True,
        "embedded_python_runtime": False,
        "wheelhouse_bundled": False,
        "installer_runtime_complete": False,
    }
    _write_json(bundle_root / "manifest/public_export_lineage.json", lineage_payload)

    file_hashes: dict[str, object] = {
        "schema_version": 1,
        "report_id": "zephyr.base.public_core_bundle_file_hashes.v1",
        "recorded_files_excluding_self": [],
    }
    recorded_files = cast(list[object], file_hashes["recorded_files_excluding_self"])
    for path in sorted(bundle_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(bundle_root).as_posix()
        if rel == "manifest/bundle_file_hashes.json":
            continue
        recorded_files.append({"path": rel, "sha256": _sha256_file(path)})
    _write_json(bundle_root / "manifest/bundle_file_hashes.json", file_hashes)
    return bundle_root, lineage_payload


def _validate_bundle_execution(
    bundle_root: Path,
    out_root: Path,
) -> tuple[bool, dict[str, object], str]:
    validation_root = out_root / "_validation"
    if validation_root.exists():
        shutil.rmtree(validation_root)
    validation_root.mkdir(parents=True, exist_ok=True)
    sample_input = validation_root / "sample_input.txt"
    sample_input.write_text("ZEPHYR_BASE_BUNDLE_MARKER_M3_S5\n", encoding="utf-8")
    request_path = validation_root / "request.json"
    _write_json(
        request_path,
        {
            "schema_version": 1,
            "request_id": "bundle-smoke",
            "input_kind": "local_file",
            "input_path": str(sample_input),
            "output_dir": str(validation_root / "out"),
            "requested_outputs": [
                "normalized_text",
                "content_evidence",
                "receipt",
                "filesystem_output",
            ],
        },
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(bundle_root / "run_bundle_public_core.py"),
            "--request",
            str(request_path),
            "--out-dir",
            str(validation_root / "out"),
            "--json",
        ],
        cwd=bundle_root,
        check=False,
        capture_output=True,
        text=True,
    )
    stdout = completed.stdout.strip()
    details = (
        json.loads(stdout)
        if stdout and stdout.startswith("{")
        else _load_json_object(validation_root / "out/run_result.json")
    )
    return completed.returncode == 0, details, completed.stderr.strip()


def build_report(*, root: Path, out_root: Path) -> dict[str, object]:
    public_manifest = _load_json_object(root / "docs/p6/public_core_export_manifest.json")
    bundle_manifest = _load_json_object(root / "docs/p6/p6_m3_s5_public_core_bundle_manifest.json")
    bundle_root, _ = _emit_bundle(
        root=root,
        out_root=out_root,
        public_manifest=public_manifest,
        bundle_manifest=bundle_manifest,
    )
    bundle_success, run_result, stderr = _validate_bundle_execution(bundle_root, out_root)
    issues: list[str] = []
    if not bundle_success:
        issues.append("Bundle execution failed without the Zephyr-dev working tree.")
    if bool(run_result.get("fixture_runner_used")):
        issues.append("Bundle execution incorrectly used fixture mode.")
    if bool(run_result.get("billing_semantics")):
        issues.append("Bundle execution emitted billing semantics.")
    active_blockers = len(issues)
    return {
        "schema_version": 1,
        "report_id": "zephyr.p6.m3.s5.public_core_bundle_export.v1",
        "generated_at_utc": _generated_at_utc(),
        "summary": {
            "overall": "pass" if active_blockers == 0 else "fail",
            "active_blockers": active_blockers,
            "bundle_exported": True,
            "bundle_execution_requires_zephyr_dev_working_tree": not bundle_success,
            "bundle_generation_requires_zephyr_dev_working_tree": True,
            "installer_runtime_complete": False,
        },
        "bundle": {
            "path": out_root.joinpath("bundle").relative_to(root).as_posix(),
            "runner_exists": (bundle_root / "runner/p6_m3_public_core_local_runner.py").exists(),
            "entrypoint_exists": (bundle_root / "run_bundle_public_core.py").exists(),
            "manifest_exists": (bundle_root / "manifest/public_core_bundle_manifest.json").exists(),
            "file_hashes_recorded": (bundle_root / "manifest/bundle_file_hashes.json").exists(),
        },
        "runtime": {
            "uses_current_python_environment": True,
            "embedded_python_runtime": False,
            "wheelhouse_bundled": False,
            "requires_p45_substrate": False,
            "requires_network": False,
        },
        "boundary": {
            "commercial_logic_allowed": False,
            "license_allowed": False,
            "entitlement_allowed": False,
            "private_core_allowed": False,
            "web_core_dependency_allowed": False,
        },
        "validation": {
            "bundle_smoke_pass": bundle_success,
            "fixture_runner_used": run_result.get("fixture_runner_used"),
            "billing_semantics": _load_json_object(
                out_root / "_validation/out/usage_fact.json"
            ).get("billing_semantics"),
            "stderr": stderr,
        },
        "issues": issues,
        "non_blocking_notes": [
            "Bundle execution uses the current Python environment in the S5 first slice.",
            "S5 does not embed a Python runtime or wheelhouse yet.",
        ],
    }


def render_markdown(report: dict[str, object]) -> str:
    summary = cast(SummaryDict, cast(dict[str, object], report["summary"]))
    bundle = cast(dict[str, object], report["bundle"])
    runtime = cast(dict[str, object], report["runtime"])
    lines = [
        "# P6-M3-S5 Public Core Bundle Export",
        "",
        f"- overall: {summary['overall']}",
        f"- active blockers: {summary['active_blockers']}",
        f"- bundle_exported: {summary['bundle_exported']}",
        (
            "- bundle_execution_requires_zephyr_dev_working_tree: "
            f"{summary['bundle_execution_requires_zephyr_dev_working_tree']}"
        ),
        "",
        "## Bundle",
        f"- path: {bundle['path']}",
        f"- runner_exists: {bundle['runner_exists']}",
        f"- entrypoint_exists: {bundle['entrypoint_exists']}",
        f"- manifest_exists: {bundle['manifest_exists']}",
        f"- file_hashes_recorded: {bundle['file_hashes_recorded']}",
        "",
        "## Runtime",
        f"- uses_current_python_environment: {runtime['uses_current_python_environment']}",
        f"- embedded_python_runtime: {runtime['embedded_python_runtime']}",
        f"- wheelhouse_bundled: {runtime['wheelhouse_bundled']}",
        f"- requires_p45_substrate: {runtime['requires_p45_substrate']}",
        f"- requires_network: {runtime['requires_network']}",
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
    parser = argparse.ArgumentParser(description="Export the P6-M3-S5 public core bundle.")
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
    out_root = args.out_root if args.out_root.is_absolute() else (root / args.out_root).resolve()
    if args.check_artifacts:
        required = [
            out_root / OUTPUT_JSON,
            out_root / OUTPUT_MD,
            out_root / "bundle/run_bundle_public_core.py",
            out_root / "bundle/manifest/public_core_bundle_manifest.json",
            out_root / "bundle/manifest/public_export_lineage.json",
            out_root / "bundle/manifest/bundle_file_hashes.json",
        ]
        missing = [path.name for path in required if not path.exists()]
        if missing:
            raise FileNotFoundError("; ".join(missing))
        return 0
    report = build_report(root=root, out_root=out_root)
    emit_outputs(report=report, out_root=out_root, markdown=args.markdown)
    return 0 if cast(dict[str, object], report["summary"])["overall"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
