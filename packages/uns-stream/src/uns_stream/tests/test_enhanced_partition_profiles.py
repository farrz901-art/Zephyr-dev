from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from uns_stream._internal.enhanced_partition import (
    direct_known_partition_fields,
    resolve_partition_options,
    supported_partition_profiles,
)
from uns_stream._internal.ocr_agents import (
    OCR_AGENT_PADDLE_QNAME,
    OCR_AGENT_TESSERACT_QNAME,
)
from uns_stream.partition.auto import partition as auto_partition
from uns_stream.service import partition_file
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrElement, ZephyrError

pytestmark = [pytest.mark.uns]


class RecordingBackend:
    name = "dummy"
    backend = "test"
    version = "0.0.0"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def partition_elements(
        self,
        *,
        filename: str,
        kind: str,
        strategy: PartitionStrategy,
        unique_element_ids: bool = True,
        **kwargs: Any,
    ) -> list[ZephyrElement]:
        self.calls.append(
            {
                "filename": filename,
                "kind": kind,
                "strategy": strategy,
                "unique_element_ids": unique_element_ids,
                "kwargs": dict(kwargs),
            }
        )
        return [
            ZephyrElement(
                element_id="e1",
                type="NarrativeText",
                text="hello",
                metadata={"kind": kind},
            )
        ]


def test_supported_partition_profiles_are_stable() -> None:
    assert supported_partition_profiles() == (
        "default",
        "zh",
        "html_heavy",
        "invoice",
        "contract",
        "zh_paddle",
        "invoice_paddle",
    )
    assert "profile" in direct_known_partition_fields()
    assert "extract_image_block_to_payload" in direct_known_partition_fields()
    assert "ocr_agent" in direct_known_partition_fields()
    assert "table_ocr_agent" in direct_known_partition_fields()


def test_default_profile_preserves_lightweight_behavior() -> None:
    resolved = resolve_partition_options(profile=None, strategy=None)

    assert resolved.profile == "default"
    assert resolved.strategy is None
    assert resolved.merged_backend_kwargs() == {}


@pytest.mark.parametrize(
    ("profile", "expected_strategy", "expected_kwargs"),
    [
        (
            "zh",
            None,
            {
                "languages": ["zho", "eng"],
                "detect_language_per_element": True,
            },
        ),
        (
            "html_heavy",
            PartitionStrategy.HI_RES,
            {
                "skip_infer_table_types": [],
                "extract_image_block_types": ["Image", "Table"],
                "extract_image_block_to_payload": True,
            },
        ),
        (
            "invoice",
            PartitionStrategy.HI_RES,
            {
                "languages": ["zho", "eng"],
                "skip_infer_table_types": [],
                "extract_image_block_types": ["Image", "Table"],
                "extract_image_block_to_payload": True,
            },
        ),
        (
            "contract",
            PartitionStrategy.AUTO,
            {
                "languages": ["zho", "eng"],
                "detect_language_per_element": True,
                "skip_infer_table_types": [],
                "extract_image_block_types": ["Image", "Table"],
                "extract_image_block_to_payload": True,
            },
        ),
        (
            "zh_paddle",
            PartitionStrategy.HI_RES,
            {
                "languages": ["zho", "eng"],
                "ocr_agent": OCR_AGENT_PADDLE_QNAME,
                "table_ocr_agent": OCR_AGENT_PADDLE_QNAME,
            },
        ),
        (
            "invoice_paddle",
            PartitionStrategy.HI_RES,
            {
                "languages": ["zho", "eng"],
                "skip_infer_table_types": [],
                "extract_image_block_types": ["Image", "Table"],
                "extract_image_block_to_payload": True,
                "ocr_agent": OCR_AGENT_PADDLE_QNAME,
                "table_ocr_agent": OCR_AGENT_PADDLE_QNAME,
            },
        ),
        (
            "zh-paddle",
            PartitionStrategy.HI_RES,
            {
                "languages": ["zho", "eng"],
                "ocr_agent": OCR_AGENT_PADDLE_QNAME,
                "table_ocr_agent": OCR_AGENT_PADDLE_QNAME,
            },
        ),
        (
            "invoice-paddle",
            PartitionStrategy.HI_RES,
            {
                "languages": ["zho", "eng"],
                "skip_infer_table_types": [],
                "extract_image_block_types": ["Image", "Table"],
                "extract_image_block_to_payload": True,
                "ocr_agent": OCR_AGENT_PADDLE_QNAME,
                "table_ocr_agent": OCR_AGENT_PADDLE_QNAME,
            },
        ),
    ],
)
def test_profiles_apply_expected_defaults(
    profile: str,
    expected_strategy: PartitionStrategy | None,
    expected_kwargs: dict[str, object],
) -> None:
    resolved = resolve_partition_options(profile=profile, strategy=None)

    if profile == "zh-paddle":
        assert resolved.profile == "zh_paddle"
    elif profile == "invoice-paddle":
        assert resolved.profile == "invoice_paddle"
    else:
        assert resolved.profile == profile
    assert resolved.strategy == expected_strategy
    assert resolved.merged_backend_kwargs() == expected_kwargs


def test_explicit_args_override_profile_defaults() -> None:
    resolved = resolve_partition_options(
        profile="invoice",
        strategy=PartitionStrategy.AUTO,
        languages=["eng"],
        detect_language_per_element=False,
        extract_image_block_to_payload=False,
        skip_infer_table_types=["pdf"],
        starting_page_number=3,
        ocr_agent="tesseract",
        table_ocr_agent="tesseract",
    )

    assert resolved.profile == "invoice"
    assert resolved.strategy == PartitionStrategy.AUTO
    assert resolved.merged_backend_kwargs()["languages"] == ["eng"]
    assert resolved.merged_backend_kwargs()["detect_language_per_element"] is False
    assert resolved.merged_backend_kwargs()["extract_image_block_to_payload"] is False
    assert resolved.merged_backend_kwargs()["skip_infer_table_types"] == ["pdf"]
    assert resolved.merged_backend_kwargs()["starting_page_number"] == 3
    assert resolved.merged_backend_kwargs()["ocr_agent"] == OCR_AGENT_TESSERACT_QNAME
    assert resolved.merged_backend_kwargs()["table_ocr_agent"] == OCR_AGENT_TESSERACT_QNAME


def test_invalid_ocr_alias_is_clear() -> None:
    with pytest.raises(ZephyrError) as excinfo:
        resolve_partition_options(
            profile="default",
            strategy=None,
            ocr_agent="made-up-ocr",
        )

    assert excinfo.value.code == ErrorCode.UNS_PARTITION_FAILED
    assert excinfo.value.details is not None
    assert excinfo.value.details["ocr_agent"] == "made-up-ocr"


def test_infer_table_structure_alias_conflict_is_clear() -> None:
    with pytest.raises(ZephyrError) as excinfo:
        resolve_partition_options(
            profile="default",
            strategy=None,
            infer_table_structure=True,
            pdf_infer_table_structure=False,
        )

    assert excinfo.value.code == ErrorCode.UNS_PARTITION_FAILED
    assert excinfo.value.details is not None
    assert excinfo.value.details["infer_table_structure"] is True
    assert excinfo.value.details["pdf_infer_table_structure"] is False


def test_extra_partition_kwargs_cannot_shadow_known_fields() -> None:
    with pytest.raises(ZephyrError) as excinfo:
        resolve_partition_options(
            profile="default",
            strategy=None,
            extra_partition_kwargs={"languages": ["eng"]},
        )

    assert excinfo.value.code == ErrorCode.UNS_PARTITION_FAILED
    assert excinfo.value.details is not None
    assert excinfo.value.details["field"] == "languages"


def test_service_rejects_unknown_partition_kwargs(tmp_path: Path) -> None:
    file_path = tmp_path / "a.txt"
    file_path.write_text("hello", encoding="utf-8")

    with pytest.raises(ZephyrError) as excinfo:
        partition_file(
            filename=str(file_path),
            kind="text",
            backend=RecordingBackend(),
            made_up_flag=True,
        )

    assert excinfo.value.code == ErrorCode.UNS_PARTITION_FAILED
    assert excinfo.value.details is not None
    assert excinfo.value.details["unknown_partition_kwargs"] == ["made_up_flag"]


def test_auto_partition_forwards_enhanced_profile_options(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.pdf"
    file_path.write_bytes(b"%PDF-1.4 fake")
    backend = RecordingBackend()

    auto_partition(
        filename=str(file_path),
        backend=backend,
        profile="invoice",
        languages=["eng"],
        extract_image_block_to_payload=False,
        starting_page_number=5,
    )

    call = backend.calls[-1]
    assert call["kind"] == "pdf"
    assert call["strategy"] == PartitionStrategy.HI_RES
    assert call["kwargs"]["languages"] == ["eng"]
    assert call["kwargs"]["skip_infer_table_types"] == []
    assert call["kwargs"]["extract_image_block_types"] == ["Image", "Table"]
    assert call["kwargs"]["extract_image_block_to_payload"] is False
    assert call["kwargs"]["starting_page_number"] == 5


def test_auto_partition_forwards_explicit_ocr_overrides(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.pdf"
    file_path.write_bytes(b"%PDF-1.4 fake")
    backend = RecordingBackend()

    auto_partition(
        filename=str(file_path),
        backend=backend,
        profile="invoice_paddle",
        ocr_agent="tesseract",
        table_ocr_agent="tesseract",
    )

    call = backend.calls[-1]
    assert call["kwargs"]["ocr_agent"] == OCR_AGENT_TESSERACT_QNAME
    assert call["kwargs"]["table_ocr_agent"] == OCR_AGENT_TESSERACT_QNAME
