from __future__ import annotations

import json
from typing import Any

import pytest

from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.models import DocumentMetadata
from zephyr_core.contracts.v1.run_meta import EngineMetaV1, MetricsV1, RunMetaV1
from zephyr_core.contracts.v2.delivery_payload import (
    DELIVERY_PAYLOAD_SCHEMA_VERSION,
    DeliveryPayloadV1,
)
from zephyr_core.contracts.v2.delivery_receipt import DeliveryReceiptV1

pytestmark = [pytest.mark.contract, pytest.mark.fixture]


def _build_run_meta_dict() -> dict[str, Any]:
    return RunMetaV1(
        run_id="run-qa0",
        pipeline_version="qa0",
        timestamp_utc="2026-05-16T00:00:00Z",
        document=DocumentMetadata(
            filename="invoice.txt",
            mime_type="text/plain",
            sha256="abc123",
            size_bytes=42,
            created_at_utc="2026-05-16T00:00:00Z",
        ),
        outcome=RunOutcome.SUCCESS,
        engine=EngineMetaV1(
            name="unstructured",
            backend="local",
            version="0.22.28",
            strategy="auto",
        ),
        metrics=MetricsV1(duration_ms=17, elements_count=3, normalized_text_len=19),
        warnings=["preview-truncated"],
    ).to_dict()


def _build_delivery_payload() -> DeliveryPayloadV1:
    return {
        "schema_version": DELIVERY_PAYLOAD_SCHEMA_VERSION,
        "sha256": "abc123",
        "run_meta": _build_run_meta_dict(),
        "artifacts": {
            "out_dir": "/tmp/out",
            "run_meta_path": "/tmp/out/run_meta.json",
            "elements_path": "/tmp/out/elements.json",
            "normalized_path": "/tmp/out/normalized.txt",
        },
        "content_evidence": {
            "evidence_kind": "normalized_text_v1",
            "normalized_text_preview": "hello",
            "normalized_text_len": 5,
            "normalized_text_sha256": "def456",
            "normalized_text_truncated": False,
            "normalized_text_status": "present",
        },
    }


def _build_delivery_receipt() -> DeliveryReceiptV1:
    return {
        "destination": "webhook",
        "ok": False,
        "details": {
            "stage": "delivery",
            "error_code": "ZE-DELIVERY-FAILED",
            "retryable": True,
            "evidence_id": "receipt-1",
        },
    }


def _assert_delivery_payload_shape(payload: dict[str, Any]) -> None:
    required_top_level = {"schema_version", "sha256", "run_meta", "artifacts"}
    missing = required_top_level.difference(payload)
    if missing:
        raise AssertionError(f"missing payload keys: {sorted(missing)}")
    assert payload["schema_version"] == DELIVERY_PAYLOAD_SCHEMA_VERSION
    assert isinstance(payload["artifacts"], dict)
    assert isinstance(payload["run_meta"], dict)


def _assert_delivery_receipt_shape(receipt: dict[str, Any]) -> None:
    required_top_level = {"destination", "ok", "details"}
    missing = required_top_level.difference(receipt)
    if missing:
        raise AssertionError(f"missing receipt keys: {sorted(missing)}")
    assert isinstance(receipt["details"], dict)


def test_delivery_payload_fixture_round_trips_without_losing_evidence_metadata() -> None:
    payload = _build_delivery_payload()

    encoded = json.dumps(payload, ensure_ascii=False)
    restored = json.loads(encoded)

    _assert_delivery_payload_shape(restored)
    assert restored["content_evidence"]["evidence_kind"] == "normalized_text_v1"
    assert restored["content_evidence"]["normalized_text_preview"] == "hello"
    assert restored["run_meta"]["document"]["filename"] == "invoice.txt"
    assert restored["run_meta"]["warnings"] == ["preview-truncated"]


def test_delivery_payload_invalid_fixture_is_detected() -> None:
    payload = dict(_build_delivery_payload())
    del payload["schema_version"]

    with pytest.raises(AssertionError, match="missing payload keys"):
        _assert_delivery_payload_shape(payload)


def test_delivery_receipt_fixture_round_trips_with_retryability_details() -> None:
    receipt = _build_delivery_receipt()

    encoded = json.dumps(receipt, ensure_ascii=False)
    restored = json.loads(encoded)

    _assert_delivery_receipt_shape(restored)
    assert restored["destination"] == "webhook"
    assert restored["details"]["stage"] == "delivery"
    assert restored["details"]["error_code"] == "ZE-DELIVERY-FAILED"
    assert restored["details"]["retryable"] is True
    assert restored["details"]["evidence_id"] == "receipt-1"
