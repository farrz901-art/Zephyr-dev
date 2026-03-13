from __future__ import annotations

from typing import Any, Callable

from uns_stream._internal.errors import missing_extra
from uns_stream._internal.serde import to_zephyr_elements
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrElement, ZephyrError

_EXTRA_BY_KIND: dict[str, str] = {
    # 大多与 kind 同名；先列常见，后续逐步补齐即可
    "pdf": "pdf",
    "docx": "docx",
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
    "json": "json",
    "text": "all-docs",  # text 通常不需要 extra，这里给个宽松兜底
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
        fn = _load_partition_fn(kind)

        # Phase1: keep call minimal and safe across kinds.
        # pdf/image strategy will be wired in P1-05 at the module level.
        elements = fn(filename=filename, unique_element_ids=unique_element_ids, **kwargs)
        return to_zephyr_elements(elements)
