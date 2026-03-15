from __future__ import annotations

from typing import Any, Callable

from uns_stream._internal.errors import missing_extra
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
        from unstructured import __version__ as v  # local import

        self.version: str = str(v)

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
        fn = _load_partition_fn(kind)

        # 构造调用参数
        call_kwargs: dict[str, Any] = {
            "filename": filename,
            "unique_element_ids": unique_element_ids,
            **kwargs,
        }

        # 只对 pdf 和 image 传递 strategy 参数
        if kind in {"pdf", "image"}:
            if kind == "image" and strategy == PartitionStrategy.FAST:
                # Image 不支持 fast，官方推荐使用 auto
                call_kwargs["strategy"] = "auto"
            else:
                call_kwargs["strategy"] = strategy.value

        # 执行调用
        elements = fn(**call_kwargs)

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
