from __future__ import annotations

from typing import Any, Iterable

from uns_stream._internal.normalize import normalize_unstructured_metadata
from zephyr_core import ZephyrElement


def to_zephyr_elements(unstructured_elements: Iterable[Any]) -> list[ZephyrElement]:
    """Convert unstructured elements to Zephyr contract elements.

    We intentionally keep Any here to avoid leaking unstructured types outward.
    """
    out: list[ZephyrElement] = []
    for el in unstructured_elements:
        # Unstructured elements support .to_dict()
        d = el.to_dict()
        raw_meta = dict(d.get("metadata") or {})
        norm_meta, _ = normalize_unstructured_metadata(raw_meta)

        out.append(
            ZephyrElement(
                element_id=str(d.get("element_id") or ""),
                type=str(d.get("type") or ""),
                text=str(d.get("text") or ""),
                metadata=norm_meta,
            )
        )
    return out
