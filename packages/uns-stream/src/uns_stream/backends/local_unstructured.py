from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any, Callable

from uns_stream._internal.errors import missing_extra
from uns_stream._internal.ocr_agents import (
    OCR_AGENT_PADDLE_QNAME,
    OCR_AGENT_TESSERACT_QNAME,
)
from uns_stream._internal.paddleocr_runtime import (
    apply_paddle_language_normalization,
    build_tesseract_fallback_kwargs,
    ensure_paddleocr_base_dir,
    format_traceback_tail,
    is_known_paddle_runtime_failure,
    partition_call_uses_paddle_ocr,
    preload_torch_for_paddleocr,
)
from uns_stream._internal.serde import to_zephyr_elements
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrElement, ZephyrError

_EXTRA_BY_KIND: dict[str, str] = {
    "pdf": "pdf",
    # doc/ppt 统一指向 docx/pptx（按官方 full installation 文档）
    "doc": "docx",
    "docx": "docx",
    "ppt": "pptx",
    "pptx": "pptx",
    "xlsx": "xlsx",
    "csv": "csv",
    "tsv": "tsv",
    "md": "md",
    "rtf": "rtf",
    "rst": "rst",
    "epub": "epub",
    "odt": "odt",
    "org": "org",
    "image": "image",
    "email": "email",
    "html": "html",
    "xml": "xml",
    # ndjson 本质属于 json 能力（extra 列表里通常给的是 json）
    "ndjson": "json",
    "json": "json",
    "msg": "msg",
    "text": "all-docs",
}


def _load_partition_fn(kind: str) -> Callable[..., Any]:
    """Load unstructured partition function by kind.

    We avoid unstructured.partition.auto in Phase1 to reduce Windows/libmagic variance.
    """
    fn_name = f"partition_{kind}"

    try:
        mod = __import__(f"unstructured.partition.{kind}", fromlist=[fn_name])
        fn = getattr(mod, fn_name)
        return fn
    except ModuleNotFoundError as e:
        extra = _EXTRA_BY_KIND.get(kind, "all-docs")
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
        # from unstructured import __version__ as v  # local import
        #
        # self.version: str = str(v)
        try:
            from importlib.metadata import version

            self.version = version("unstructured")
        except Exception:
            # 兜底方案
            from unstructured import __version__ as v

            # 如果 v 是模块，尝试取其属性，否则转字符串
            self.version = getattr(v, "__version__", str(v))
        self._runtime_notes: list[dict[str, object]] = []

    def consume_runtime_notes(self) -> list[dict[str, object]]:
        notes = list(self._runtime_notes)
        self._runtime_notes.clear()
        return notes

    def _record_runtime_note(self, note: Mapping[str, object]) -> None:
        self._runtime_notes.append(dict(note))

    def partition_elements(
        self,
        *,
        filename: str,
        kind: str,
        strategy: PartitionStrategy,
        unique_element_ids: bool = True,
        **kwargs: Any,
    ) -> list[ZephyrElement]:
        """
        根据文件类型安全地调用 unstructured 的对应 partition 函数。

        策略：
        - pdf：支持 fast / hi_res / ocr_only / auto
        - image：支持 auto / hi_res / ocr_only（FAST 自动降级为 auto）
        - 其他格式：不传递 strategy 参数，避免 TypeError
        """
        self._runtime_notes.clear()
        fn = _load_partition_fn(kind)
        fn_signature = inspect.signature(fn)
        accepted_params = set(fn_signature.parameters.keys())
        accepts_var_kwargs = any(
            param.kind is inspect.Parameter.VAR_KEYWORD
            for param in fn_signature.parameters.values()
        )

        # 构造调用参数
        call_kwargs: dict[str, Any] = {
            "filename": filename,
            "unique_element_ids": unique_element_ids,
        }

        # 只对 pdf 和 image 传递 strategy 参数
        if kind in {"pdf", "image"}:
            if kind == "image" and strategy == PartitionStrategy.FAST:
                # Image 不支持 fast，官方推荐使用 auto
                call_kwargs["strategy"] = "auto"
            else:
                call_kwargs["strategy"] = strategy.value
            if accepts_var_kwargs or "ocr_agent" in accepted_params:
                call_kwargs.setdefault("ocr_agent", OCR_AGENT_PADDLE_QNAME)
            if accepts_var_kwargs or "table_ocr_agent" in accepted_params:
                call_kwargs.setdefault("table_ocr_agent", OCR_AGENT_PADDLE_QNAME)

        unsupported_keys = [
            key for key in kwargs if not accepts_var_kwargs and key not in accepted_params
        ]
        if unsupported_keys:
            raise ZephyrError(
                code=ErrorCode.UNS_PARTITION_FAILED,
                message=(
                    f"Partition parameters {unsupported_keys!r} are not supported "
                    f"for kind '{kind}' by installed unstructured {self.version}"
                ),
                details={
                    "retryable": False,
                    "kind": kind,
                    "unsupported_partition_kwargs": unsupported_keys,
                    "engine_version": self.version,
                },
            )

        call_kwargs.update(kwargs)
        original_call_kwargs = dict(call_kwargs)
        paddle_path = kind in {"pdf", "image"} and partition_call_uses_paddle_ocr(call_kwargs)
        if paddle_path:
            self._record_runtime_note(preload_torch_for_paddleocr())
            self._record_runtime_note(
                {
                    "event": "paddleocr_base_dir",
                    "paddle_ocr_base_dir": ensure_paddleocr_base_dir(),
                }
            )
            call_kwargs, normalization_note = apply_paddle_language_normalization(
                kind=kind,
                call_kwargs=call_kwargs,
            )
            if normalization_note is not None:
                self._record_runtime_note(normalization_note)

        try:
            elements = fn(**call_kwargs)
        except Exception as exc:
            if paddle_path and is_known_paddle_runtime_failure(exc):
                fallback_kwargs = build_tesseract_fallback_kwargs(original_call_kwargs)
                fallback_note: dict[str, object] = {
                    "event": "paddle_fallback",
                    "paddle_fallback_applied": True,
                    "paddle_fallback_reason": "known_paddle_runtime_failure",
                    "paddle_status": "error",
                    "paddle_original_error_type": type(exc).__name__,
                    "paddle_original_error_message": str(exc),
                    "paddle_traceback_tail": format_traceback_tail(exc),
                    "fallback_ocr_agent": OCR_AGENT_TESSERACT_QNAME,
                    "fallback_table_ocr_agent": OCR_AGENT_TESSERACT_QNAME,
                }
                try:
                    elements = fn(**fallback_kwargs)
                except Exception as fallback_exc:
                    fallback_note["fallback_status"] = "error"
                    fallback_note["fallback_exception_type"] = type(fallback_exc).__name__
                    fallback_note["fallback_exception_message"] = str(fallback_exc)
                    fallback_note["fallback_traceback_tail"] = format_traceback_tail(fallback_exc)
                    self._record_runtime_note(fallback_note)
                    raise ZephyrError(
                        code=ErrorCode.UNS_PARTITION_FAILED,
                        message="PaddleOCR partition failed and Tesseract fallback also failed",
                        details={
                            "retryable": False,
                            "kind": kind,
                            "engine_version": self.version,
                            "paddle_original_error_type": type(exc).__name__,
                            "paddle_original_error_message": str(exc),
                            "paddle_traceback_tail": format_traceback_tail(exc),
                            "fallback_error_type": type(fallback_exc).__name__,
                            "fallback_error_message": str(fallback_exc),
                            "fallback_traceback_tail": format_traceback_tail(fallback_exc),
                        },
                    ) from fallback_exc

                fallback_note["fallback_status"] = "ok"
                self._record_runtime_note(fallback_note)
            else:
                raise

        return to_zephyr_elements(elements)

    # def partition_elements(
    #     self,
    #     *,
    #     filename: str,
    #     kind: str,
    #     strategy: PartitionStrategy,
    #     unique_element_ids: bool = True,
    #     **kwargs: Any,
    # ) -> list[ZephyrElement]:
    #     fn = _load_partition_fn(kind)
    #
    #     # Phase1: keep call minimal and safe across kinds.
    #     # pdf/image strategy will be wired in P1-05 at the module level.
    #     elements = fn(filename=filename, unique_element_ids=unique_element_ids, **kwargs)
    #     return to_zephyr_elements(elements)
