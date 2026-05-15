from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict, cast

from zephyr_core import PartitionStrategy, RunContext
from zephyr_core.versioning import PIPELINE_VERSION
from zephyr_ingest._internal.delivery_payload import (
    build_artifacts_paths_for_run_meta_v1,
    build_delivery_content_evidence_v1,
)
from zephyr_ingest._internal.utils import sha256_file
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.flow_processor import DEFAULT_FLOW_KIND, normalize_flow_input_identity_sha
from zephyr_ingest.runner import RetryConfig, RunnerConfig, run_documents
from zephyr_ingest.sources.local_file import LocalFileSource


def _discover_repo_root(start: Path) -> Path:
    current = start.resolve()
    for candidate in (current, *current.parents):
        is_repo_root = (
            ((candidate / "pyproject.toml").exists() or (candidate / ".git").exists())
            and (candidate / "docs/p6").exists()
            and (candidate / "packages/zephyr-ingest").exists()
        )
        is_bundle_root = (
            (candidate / "manifest/public_core_bundle_manifest.json").exists()
            and (candidate / "packages/zephyr-ingest/src").exists()
            and (candidate / "runner").exists()
        )
        if is_repo_root or is_bundle_root:
            return candidate
    raise RuntimeError("Could not locate repository root from tool path")


DEFAULT_ROOT = _discover_repo_root(Path(__file__).resolve().parent)
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


def _as_list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list, got {type(value).__name__}")
    return cast(list[object], value)


def _resolve_from_root(root: Path, path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else (root / path).resolve()


def _build_error(
    *,
    code: str,
    category: str,
    message: str,
    detail: str,
) -> BaseErrorDict:
    return {
        "schema_version": 1,
        "error_code": code,
        "category": category,
        "user_message": message,
        "technical_detail_safe": detail,
        "secret_safe": True,
    }


def _normalize_text_preview(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    return normalized[:160]


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


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
        "adapter_runtime": "zephyr_dev_public_core_local_runner_v1",
    }
    receipt: dict[str, object] = {
        "schema_version": 1,
        "run_id": f"failed-{request_id}",
        "request_id": request_id,
        "status": "failed",
        "delivery_outcome": "failed",
        "output_root": str(out_dir),
        "artifacts": OUTPUT_FILES,
        "created_by": "Zephyr-dev public core local runner",
        "production_runtime": False,
        "adapter_runtime": "zephyr_dev_public_core_local_runner_v1",
        "fixture_runner_used": False,
        "zephyr_dev_public_core_invoked": False,
    }
    usage_fact: dict[str, object] = {
        "schema_version": 1,
        "fact_kind": "technical_usage_fact",
        "billing_semantics": False,
        "input_bytes": input_bytes,
        "output_files_count": len(OUTPUT_FILES),
        "adapter_runtime": "zephyr_dev_public_core_local_runner_v1",
        "production_runtime": False,
        "fixture_runner_used": False,
        "zephyr_dev_public_core_invoked": False,
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
        "adapter_runtime": "zephyr_dev_public_core_local_runner_v1",
        "production_runtime": False,
        "fixture_runner_used": False,
        "zephyr_dev_public_core_invoked": False,
    }
    if bridge_mode is not None:
        run_result["local_text_bridge_mode"] = bridge_mode
    _write_json(out_dir / "content_evidence.json", content_evidence)
    _write_json(out_dir / "receipt.json", receipt)
    _write_json(out_dir / "usage_fact.json", usage_fact)
    _write_json(out_dir / "run_result.json", run_result)
    return run_result


def _prepare_input(
    *,
    root: Path,
    request: RequestDict,
    temp_root: Path,
) -> PreparedInput:
    request_id = str(request.get("request_id", "unknown-request"))
    input_kind = str(request.get("input_kind", "unknown"))
    if input_kind == "local_file":
        input_path_value = str(request.get("input_path", ""))
        input_path = _resolve_from_root(root, input_path_value)
        if not input_path.exists():
            raise FileNotFoundError(str(input_path))
        if input_path.suffix.lower() not in SUPPORTED_INPUT_EXTENSIONS:
            raise ValueError(f"unsupported_extension:{input_path.suffix.lower()}")
        input_bytes = input_path.stat().st_size
        return PreparedInput(
            input_path=input_path,
            input_kind=input_kind,
            request_id=request_id,
            bridge_mode=None,
            input_bytes=input_bytes,
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


def _invoke_real_public_core(
    *,
    input_path: Path,
    work_root: Path,
) -> Path:
    ctx = RunContext.new(pipeline_version=PIPELINE_VERSION)
    cfg = RunnerConfig(
        out_root=work_root,
        strategy=PartitionStrategy.AUTO,
        unique_element_ids=True,
        skip_unsupported=False,
        skip_existing=False,
        force=True,
        retry=RetryConfig(enabled=False),
        workers=1,
        destination=FilesystemDestination(),
        backend=None,
    )
    docs = list(LocalFileSource(path=input_path).iter_documents())
    run_documents(
        docs=docs,
        cfg=cfg,
        ctx=ctx,
        destination=FilesystemDestination(),
    )
    default_sha = sha256_file(input_path)
    normalized_sha = normalize_flow_input_identity_sha(
        flow_kind=DEFAULT_FLOW_KIND,
        filename=str(input_path),
        default_sha=default_sha,
    )
    return work_root / normalized_sha


def _build_success_outputs(
    *,
    request_id: str,
    request: RequestDict,
    out_dir: Path,
    processing_out_dir: Path,
    bridge_mode: str | None,
    input_bytes: int,
) -> dict[str, object]:
    normalized_text = _read_text(processing_out_dir / "normalized.txt")
    run_meta = _load_json_object(processing_out_dir / "run_meta.json")
    delivery_receipt = _load_json_object(processing_out_dir / "delivery_receipt.json")
    usage_record = _load_json_object(processing_out_dir / "usage_record.json")
    artifacts = build_artifacts_paths_for_run_meta_v1(
        out_root=processing_out_dir.parent,
        sha256=processing_out_dir.name,
        run_meta=run_meta,
    )
    content_evidence = cast(
        dict[str, object],
        build_delivery_content_evidence_v1(artifacts=artifacts),
    )
    content_evidence["adapter_runtime"] = "zephyr_dev_public_core_local_runner_v1"
    content_evidence["production_runtime"] = True
    if bridge_mode is not None:
        content_evidence["local_text_bridge_mode"] = bridge_mode

    receipt_summary = _as_dict(delivery_receipt["summary"])
    run_meta_metrics = _as_dict(run_meta["metrics"])
    receipt: dict[str, object] = {
        "schema_version": 1,
        "run_id": run_meta["run_id"],
        "request_id": request_id,
        "status": "success",
        "delivery_outcome": receipt_summary["delivery_outcome"],
        "output_root": str(out_dir),
        "artifacts": OUTPUT_FILES,
        "created_by": "Zephyr-dev public core local runner",
        "production_runtime": True,
        "adapter_runtime": "zephyr_dev_public_core_local_runner_v1",
        "fixture_runner_used": False,
        "zephyr_dev_public_core_invoked": True,
        "upstream_delivery_receipt": delivery_receipt,
    }
    if bridge_mode is not None:
        receipt["local_text_bridge_mode"] = bridge_mode

    usage_fact: dict[str, object] = {
        "schema_version": 1,
        "fact_kind": "technical_usage_fact",
        "billing_semantics": False,
        "input_bytes": input_bytes,
        "output_files_count": len(OUTPUT_FILES),
        "adapter_runtime": "zephyr_dev_public_core_local_runner_v1",
        "production_runtime": True,
        "fixture_runner_used": False,
        "zephyr_dev_public_core_invoked": True,
        "raw_usage_record": usage_record,
    }
    if bridge_mode is not None:
        usage_fact["local_text_bridge_mode"] = bridge_mode

    normalized_preview = _normalize_text_preview(normalized_text)
    run_result: dict[str, object] = {
        "schema_version": 1,
        "request_id": request_id,
        "status": "success",
        "normalized_text_preview": normalized_preview,
        "content_evidence_summary": {
            "elements_count": content_evidence.get("elements_count", 0),
            "has_normalized_text": True,
            "evidence_kind": content_evidence.get("evidence_kind", "unknown"),
        },
        "receipt": receipt,
        "usage_fact": usage_fact,
        "output_files": OUTPUT_FILES,
        "error": None,
        "adapter_runtime": "zephyr_dev_public_core_local_runner_v1",
        "production_runtime": True,
        "fixture_runner_used": False,
        "zephyr_dev_public_core_invoked": True,
        "requested_outputs": request.get("requested_outputs", []),
        "supported_input_extensions": sorted(SUPPORTED_INPUT_EXTENSIONS),
        "content_evidence_kind": content_evidence.get("evidence_kind", "unknown"),
        "normalized_text_len": run_meta_metrics.get("normalized_text_len"),
    }
    if bridge_mode is not None:
        run_result["local_text_bridge_mode"] = bridge_mode

    out_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(processing_out_dir / "normalized.txt", out_dir / "normalized_text.txt")
    _write_json(out_dir / "content_evidence.json", content_evidence)
    _write_json(out_dir / "receipt.json", receipt)
    _write_json(out_dir / "usage_fact.json", usage_fact)
    _write_json(out_dir / "run_result.json", run_result)
    return run_result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the real Zephyr-dev public core local adapter."
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
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
    work_root = out_dir / "_zephyr_dev_public_core_work"
    temp_root = out_dir / "_temp_inputs"
    run_result: dict[str, object]
    exit_code = 0
    try:
        prepared = _prepare_input(root=root, request=request, temp_root=temp_root)
        processing_out_dir = _invoke_real_public_core(
            input_path=prepared.input_path,
            work_root=work_root,
        )
        run_result = _build_success_outputs(
            request_id=prepared.request_id,
            request=request,
            out_dir=out_dir,
            processing_out_dir=processing_out_dir,
            bridge_mode=prepared.bridge_mode,
            input_bytes=prepared.input_bytes,
        )
    except FileNotFoundError as exc:
        error = _build_error(
            code="base_input_missing",
            category="input",
            message="Input file was not found.",
            detail=f"Missing local input file: {Path(str(exc)).as_posix()}",
        )
        run_result = _write_failure_outputs(
            out_dir=out_dir,
            request_id=str(request.get("request_id", "unknown-request")),
            error=error,
            input_bytes=0,
            bridge_mode=None,
        )
        exit_code = 1
    except ValueError as exc:
        message = str(exc)
        if message.startswith("unsupported_extension:"):
            extension = message.split(":", maxsplit=1)[1]
            error = _build_error(
                code="base_input_extension_unsupported",
                category="input",
                message="Input file extension is not supported by the first public-core slice.",
                detail=(
                    f"Supported input extensions: {sorted(SUPPORTED_INPUT_EXTENSIONS)}; "
                    f"got {extension}"
                ),
            )
            bridge_mode = None
        elif message.startswith("unsupported_input_kind:"):
            input_kind = message.split(":", maxsplit=1)[1]
            error = _build_error(
                code="base_input_kind_unsupported",
                category="input",
                message="Input kind is not supported by the public-core local runner.",
                detail=f"Unsupported input kind: {input_kind}",
            )
            bridge_mode = None
        else:
            error = _build_error(
                code="base_processing_error",
                category="processing",
                message="Public core local runner failed before processing completed.",
                detail=message,
            )
            bridge_mode = None
        run_result = _write_failure_outputs(
            out_dir=out_dir,
            request_id=str(request.get("request_id", "unknown-request")),
            error=error,
            input_bytes=0,
            bridge_mode=bridge_mode,
        )
        exit_code = 1
    except Exception as exc:  # pragma: no cover - defensive contract boundary
        error = _build_error(
            code="base_processing_error",
            category="processing",
            message="Public core local runner failed during processing.",
            detail=f"{type(exc).__name__}: {exc}",
        )
        run_result = _write_failure_outputs(
            out_dir=out_dir,
            request_id=str(request.get("request_id", "unknown-request")),
            error=error,
            input_bytes=0,
            bridge_mode=None,
        )
        exit_code = 1
    if args.json:
        print(json.dumps(run_result, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
