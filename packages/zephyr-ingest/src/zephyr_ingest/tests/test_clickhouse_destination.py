from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from zephyr_core import RunContext, RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_ingest.destinations.clickhouse import ClickHouseDestination


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


def test_clickhouse_destination_posts_one_json_each_row_insert(tmp_path: Path) -> None:
    meta = _build_meta()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert (
            request.url.params["query"] == "INSERT INTO analytics.delivery_rows FORMAT JSONEachRow"
        )
        row = json.loads(request.content.decode("utf-8").strip())
        assert row["identity_key"] == f"abc123:{meta.run_id}"
        assert row["payload_json"] != ""
        return httpx.Response(200, text="")

    dest = ClickHouseDestination(
        url="https://clickhouse.example.test",
        database="analytics",
        table="delivery_rows",
        transport=httpx.MockTransport(handler),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=meta, result=None)

    assert receipt.ok is True
    assert receipt.destination == "clickhouse"
    assert receipt.failure_retryability == "not_failed"
    assert receipt.details is not None
    assert receipt.details["table"] == "delivery_rows"
    assert receipt.details["database"] == "analytics"
    assert receipt.details["write_mode"] == "append"
    assert receipt.details["row_count"] == 1


def test_clickhouse_destination_maps_schema_errors_to_constraint(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(400, text="DB::Exception: Type mismatch in VALUES section")

    dest = ClickHouseDestination(
        url="https://clickhouse.example.test",
        table="delivery_rows",
        transport=httpx.MockTransport(handler),
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=_build_meta(), result=None)

    assert receipt.ok is False
    assert receipt.failure_retryability == "non_retryable"
    assert receipt.shared_failure_kind == "constraint"
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert receipt.details is not None
    assert receipt.details["retryable"] is False


def test_clickhouse_destination_rejects_invalid_table_identifier(tmp_path: Path) -> None:
    dest = ClickHouseDestination(url="https://clickhouse.example.test", table="bad-name")

    with pytest.raises(ValueError, match="table"):
        dest(out_root=tmp_path / "out", sha256="abc123", meta=_build_meta(), result=None)
