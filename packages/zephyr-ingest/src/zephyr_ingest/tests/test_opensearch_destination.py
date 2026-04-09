from __future__ import annotations

import json
from pathlib import Path

import httpx

from zephyr_core import RunContext, RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_ingest.destinations.opensearch import OpenSearchDestination


def _build_meta() -> RunMetaV1:
    ctx = RunContext.new()
    return RunMetaV1(
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        timestamp_utc=ctx.timestamp_utc,
        schema_version=ctx.run_meta_schema_version,
        outcome=RunOutcome.SUCCESS,
        engine=None,
        document=None,
        error=None,
        warnings=[],
    )


def test_opensearch_destination_indexes_one_document(tmp_path: Path) -> None:
    meta = _build_meta()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert request.url.path == f"/zephyr-docs/_doc/abc123:{meta.run_id}"
        body = json.loads(request.content.decode("utf-8"))
        assert body["delivery_identity"] == f"abc123:{meta.run_id}"
        assert body["payload"]["sha256"] == "abc123"
        return httpx.Response(201, json={"result": "created", "_version": 1})

    dest = OpenSearchDestination(
        url="https://search.example.test",
        index="zephyr-docs",
        transport=httpx.MockTransport(handler),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=meta, result=None)

    assert receipt.ok is True
    assert receipt.destination == "opensearch"
    assert receipt.failure_retryability == "not_failed"
    assert receipt.details is not None
    assert receipt.details["attempted_count"] == 1
    assert receipt.details["accepted_count"] == 1
    assert receipt.details["rejected_count"] == 0
    assert receipt.details["result"] == "created"


def test_opensearch_destination_maps_rate_limits_into_shared_failure_semantics(
    tmp_path: Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            429,
            json={"error": {"type": "rejected_execution_exception", "reason": "busy"}},
        )

    dest = OpenSearchDestination(
        url="https://search.example.test",
        index="zephyr-docs",
        transport=httpx.MockTransport(handler),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=_build_meta(), result=None)

    assert receipt.ok is False
    assert receipt.failure_retryability == "retryable"
    assert receipt.shared_failure_kind == "rate_limited"
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert receipt.details is not None
    assert receipt.details["attempted_count"] == 1
    assert receipt.details["accepted_count"] == 0
    assert receipt.details["rejected_count"] == 1
    assert receipt.details["backend_error_type"] == "rejected_execution_exception"
