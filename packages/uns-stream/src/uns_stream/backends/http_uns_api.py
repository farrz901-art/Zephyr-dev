from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import httpx

from uns_stream._internal.normalize import normalize_unstructured_metadata
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrElement, ZephyrError


def _bool_str(v: bool) -> str:
    return "true" if v else "false"


def _should_retry_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _strategy_for_kind(kind: str, strategy: PartitionStrategy) -> str:
    # Keep ZephyrStrategy uniform; images do not support "fast" in Unstructured docs,
    # so normalize FAST -> auto for images (same as local backend behavior).
    if kind == "image" and strategy == PartitionStrategy.FAST:
        return "auto"
    return str(strategy)


def _element_dict_to_zephyr(el: dict[str, Any]) -> ZephyrElement:
    raw_meta = dict(el.get("metadata") or {})
    norm_meta, _warnings = normalize_unstructured_metadata(raw_meta)

    return ZephyrElement(
        element_id=str(el.get("element_id") or ""),
        type=str(el.get("type") or ""),
        text=str(el.get("text") or ""),
        metadata=norm_meta,
    )


@dataclass(slots=True)
class HttpUnsApiBackend:
    """
    HTTP backend that calls Unstructured Partition Endpoint (general/v0/general).

    Naming: we call it "uns-api" internally, but it's the Unstructured API endpoint.
    """

    url: str  # e.g. http://localhost:8001/general/v0/general
    api_key: str | None = None
    timeout_s: float = 60.0
    transport: httpx.BaseTransport | None = None

    # PartitionBackend Protocol attrs
    name: str = "unstructured"
    backend: str = "uns_api"
    version: str = "uns_api"

    def partition_elements(
        self,
        *,
        filename: str,
        kind: str,
        strategy: PartitionStrategy,
        unique_element_ids: bool = True,
        **kwargs: Any,
    ) -> list[ZephyrElement]:
        p = Path(filename)

        headers: dict[str, str] = {"accept": "application/json"}
        if self.api_key:
            headers["unstructured-api-key"] = (
                self.api_key
            )  # per Unstructured API docs <!--citation:6-->

        # Build multipart form fields (list-of-tuples to support repeated fields)
        data: dict[str, Any] = {
            "output_format": "application/json",
            "unique_element_ids": _bool_str(bool(unique_element_ids)),
            "coordinates": _bool_str(bool(kwargs.pop("coordinates", False))),
            "strategy": _strategy_for_kind(kind, strategy),
        }

        # Map our local-style kwargs to API param names.
        # Your code uses infer_table_structure for local pdf; API expects
        # pdf_infer_table_structure. <!--citation:6-->
        infer_table_structure = kwargs.pop("infer_table_structure", None)
        if infer_table_structure is not None:
            data["pdf_infer_table_structure"] = _bool_str(bool(infer_table_structure))

        # include_page_breaks is supported by API. <!--citation:6-->
        include_page_breaks = kwargs.pop("include_page_breaks", None)
        if include_page_breaks is not None:
            data["include_page_breaks"] = _bool_str(bool(include_page_breaks))

        # languages: API accepts string[]; safest is repeated fields. <!--citation:7-->
        languages_obj = kwargs.pop("languages", None)
        if isinstance(languages_obj, list):
            safe_languages = cast(list[object], languages_obj)
            languages_list: list[str] = []
            for lang_obj in safe_languages:
                if isinstance(lang_obj, str):
                    languages_list.append(lang_obj)
            if languages_list:
                data["languages"] = languages_list

        # Pass through remaining kwargs if they are simple scalars.
        # (Keeps this backend generic without guessing every API option.)
        for k, v in list(kwargs.items()):
            if v is None:
                continue
            if isinstance(v, bool):
                data[k] = _bool_str(v)
            elif isinstance(v, (int, float, str)):
                data[k] = str(v)
            # ignore complex types by default (dict/list) to avoid sending invalid form encoding

        with p.open("rb") as f:
            files = {
                "files": (p.name, f, "application/octet-stream")
            }  # form field name "files" <!--citation:6-->

            client = httpx.Client(timeout=self.timeout_s, transport=self.transport)
            try:
                resp = client.post(self.url, headers=headers, data=data, files=files)
            finally:
                client.close()

        if not (200 <= resp.status_code < 300):
            retryable = _should_retry_status(resp.status_code)
            raise ZephyrError(
                code=ErrorCode.UNS_PARTITION_FAILED,
                message="uns-api partition failed",
                details={
                    "status_code": resp.status_code,
                    "retryable": retryable,
                    "response_text": resp.text[:800],
                },
            )

        payload = resp.json()
        if not isinstance(payload, list):
            raise ZephyrError(
                code=ErrorCode.UNS_PARTITION_FAILED,
                message="uns-api returned non-list JSON payload",
                details={"retryable": False, "payload_type": type(payload).__name__},
            )

        payload_list = cast(list[object], payload)

        out: list[ZephyrElement] = []
        for item_obj in payload_list:
            if isinstance(item_obj, dict):
                item = cast(dict[str, Any], item_obj)
                out.append(_element_dict_to_zephyr(item))
        return out
