from __future__ import annotations

import json
from pathlib import Path

import httpx

from zephyr_core import RunContext, RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_ingest.destinations.loki import LokiDestination


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


def test_loki_destination_pushes_one_delivery_payload_entry(tmp_path: Path) -> None:
    meta = _build_meta()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/loki/api/v1/push"
        assert request.headers["X-Scope-OrgID"] == "tenant-a"
        body = json.loads(request.content.decode("utf-8"))
        stream = body["streams"][0]["stream"]
        assert stream["zephyr_stream"] == "delivery"
        assert stream["zephyr_delivery_identity"] == f"abc123:{meta.run_id}"
        assert stream["zephyr_sha256"] == "abc123"
        values = body["streams"][0]["values"]
        assert len(values) == 1
        payload = json.loads(values[0][1])
        assert payload["sha256"] == "abc123"
        assert payload["run_meta"]["run_id"] == meta.run_id
        return httpx.Response(204, text="")

    dest = LokiDestination(
        url="https://logs.example.test",
        stream="delivery",
        tenant_id="tenant-a",
        transport=httpx.MockTransport(handler),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=meta, result=None)

    assert receipt.ok is True
    assert receipt.destination == "loki"
    assert receipt.failure_retryability == "not_failed"
    assert receipt.details is not None
    assert receipt.details["stream"] == "delivery"
    assert receipt.details["tenant_id"] == "tenant-a"
    assert receipt.details["write_mode"] == "append"
    assert receipt.details["accepted_count"] == 1
    assert receipt.details["rejected_count"] == 0
    assert receipt.details["line_count"] == 1


def test_loki_destination_maps_rate_limits_to_retryable_rate_limited(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(429, text="ingester rate limit")

    dest = LokiDestination(
        url="https://logs.example.test",
        stream="delivery",
        transport=httpx.MockTransport(handler),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=_build_meta(), result=None)

    assert receipt.ok is False
    assert receipt.failure_retryability == "retryable"
    assert receipt.shared_failure_kind == "rate_limited"
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert receipt.details is not None
    assert receipt.details["retryable"] is True
    assert receipt.details["accepted_count"] == 0
    assert receipt.details["rejected_count"] == 1


def test_loki_destination_maps_validation_errors_to_constraint(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(400, text="invalid labels")

    dest = LokiDestination(
        url="https://logs.example.test",
        stream="delivery",
        transport=httpx.MockTransport(handler),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=_build_meta(), result=None)

    assert receipt.ok is False
    assert receipt.failure_retryability == "non_retryable"
    assert receipt.shared_failure_kind == "constraint"
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert receipt.details is not None
    assert receipt.details["retryable"] is False
