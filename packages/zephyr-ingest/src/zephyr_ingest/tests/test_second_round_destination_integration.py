from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping, cast

import httpx
import pytest

from zephyr_core import DocumentRef, ErrorCode, RunContext
from zephyr_core.contracts.v1.enums import PartitionStrategy
from zephyr_core.contracts.v1.models import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    ZephyrElement,
)
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
)
from zephyr_ingest.destinations.base import Destination
from zephyr_ingest.destinations.clickhouse import ClickHouseDestination
from zephyr_ingest.destinations.filesystem import FilesystemDestination
from zephyr_ingest.destinations.loki import LokiDestination
from zephyr_ingest.destinations.mongodb import MongoDBDestination, MongoReplaceResultProtocol
from zephyr_ingest.destinations.opensearch import OpenSearchDestination
from zephyr_ingest.destinations.s3 import S3Destination
from zephyr_ingest.destinations.webhook import DeliveryRetryConfig, WebhookDestination
from zephyr_ingest.runner import RunnerConfig, run_documents

DestinationName = Literal["s3", "opensearch", "clickhouse"]
RemainingDestinationName = Literal["mongodb", "loki"]
FullSecondRoundDestinationName = Literal["s3", "opensearch", "clickhouse", "mongodb", "loki"]

_BATCH_DESTINATIONS: tuple[DestinationName, ...] = ("s3", "opensearch", "clickhouse")
_REMAINING_BATCH_DESTINATIONS: tuple[RemainingDestinationName, ...] = ("mongodb", "loki")
_FULL_SECOND_ROUND_DESTINATIONS: tuple[FullSecondRoundDestinationName, ...] = (
    "s3",
    "opensearch",
    "clickhouse",
    "mongodb",
    "loki",
)
_FULL_SECOND_ROUND_COUNT_DETAIL_DESTINATIONS: tuple[FullSecondRoundDestinationName, ...] = (
    "opensearch",
    "mongodb",
    "loki",
)


@dataclass
class FakeS3Client:
    calls: list[dict[str, object]]
    exc: Exception | None = None

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> dict[str, object]:
        raw_payload = json.loads(Body.decode("utf-8"))
        assert isinstance(raw_payload, dict)
        self.calls.append(
            {
                "bucket": Bucket,
                "key": Key,
                "content_type": ContentType,
                "payload": cast(dict[str, object], raw_payload),
            }
        )
        if self.exc is not None:
            raise self.exc
        return {
            "ETag": '"etag-1"',
            "VersionId": "v1",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }


class FakeS3ServiceError(Exception):
    def __init__(self) -> None:
        super().__init__("service unavailable")
        self.response = {
            "ResponseMetadata": {"HTTPStatusCode": 503},
            "Error": {"Code": "SlowDown", "Message": "service unavailable"},
        }


@dataclass(frozen=True)
class FakeMongoReplaceResult:
    acknowledged: bool = True
    matched_count: int = 0
    modified_count: int = 0
    upserted_id: object | None = "abc:r1"


@dataclass
class FakeMongoCollection:
    calls: list[dict[str, object]]
    exc: Exception | None = None

    def replace_one(
        self,
        filter: Mapping[str, object],
        replacement: Mapping[str, object],
        *,
        upsert: bool,
    ) -> MongoReplaceResultProtocol:
        if self.exc is not None:
            raise self.exc
        self.calls.append(
            {
                "filter": dict(filter),
                "replacement": dict(replacement),
                "upsert": upsert,
            }
        )
        return FakeMongoReplaceResult()


class DuplicateKeyError(Exception):
    code = 11000


def _doc(tmp_path: Path, *, name: str = "a.txt", content: str = "hello") -> DocumentRef:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return DocumentRef(
        uri=str(path),
        source="local_file",
        discovered_at_utc="2026-04-09T00:00:00Z",
        filename=name,
        extension=Path(name).suffix.lower(),
        size_bytes=path.stat().st_size,
    )


def _ok_partition(**kwargs: Any) -> PartitionResult:
    del kwargs
    return PartitionResult(
        document=DocumentMetadata(
            filename="a.txt",
            mime_type="text/plain",
            sha256="dummy",
            size_bytes=1,
            created_at_utc="2026-04-09T00:00:00Z",
        ),
        engine=EngineInfo(
            name="unstructured",
            backend="local",
            version="0.0.0",
            strategy=PartitionStrategy.AUTO,
        ),
        elements=[ZephyrElement(element_id="1", type="Title", text="Hello", metadata={})],
        normalized_text="Hello",
        warnings=[],
    )


def _single_output_dir(*, out_root: Path) -> Path:
    out_dirs = [p for p in out_root.iterdir() if p.is_dir() and not p.name.startswith("_dlq")]
    assert len(out_dirs) == 1
    return out_dirs[0]


def _read_json(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    return cast(dict[str, object], raw)


def _identity_key(*, sha256: str, run_id: str) -> str:
    return normalize_delivery_idempotency_key(
        identity=DeliveryIdentityV1(sha256=sha256, run_id=run_id)
    )


def _assert_primary_provenance(*, payload: dict[str, object], sha256: str) -> None:
    run_meta_obj = payload.get("run_meta")
    assert isinstance(run_meta_obj, dict)
    run_meta = cast(dict[str, object], run_meta_obj)
    provenance_obj = run_meta.get("provenance")
    assert isinstance(provenance_obj, dict)
    provenance = cast(dict[str, object], provenance_obj)
    assert provenance["delivery_origin"] == "primary"
    assert provenance["run_origin"] == "intake"
    assert provenance["execution_mode"] == "batch"
    assert provenance["task_id"] == sha256
    task_identity_key = provenance.get("task_identity_key")
    assert isinstance(task_identity_key, str)


def _run_second_round_delivery_case(
    *,
    destination_name: FullSecondRoundDestinationName,
    tmp_path: Path,
    failure: bool,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    case_root = tmp_path / destination_name / ("failure" if failure else "success")
    case_root.mkdir(parents=True, exist_ok=True)
    doc = _doc(case_root)
    out_root = case_root / "out"
    ctx = RunContext.new(
        pipeline_version="p4-m15",
        run_id=f"{destination_name}-batch-{'failure' if failure else 'success'}",
        timestamp_utc="2026-04-09T00:00:00Z",
    )

    destination: Destination
    if destination_name == "s3":
        destination = S3Destination(
            bucket="archive",
            region="us-east-1",
            access_key="ak",
            secret_key="sk",
            prefix="deliveries",
            client=FakeS3Client(calls=[], exc=FakeS3ServiceError() if failure else None),
        )
    elif destination_name == "opensearch":

        def handler(request: httpx.Request) -> httpx.Response:
            del request
            if failure:
                return httpx.Response(
                    429,
                    json={"error": {"type": "rejected_execution_exception", "reason": "busy"}},
                )
            return httpx.Response(201, json={"result": "created", "_version": 1})

        destination = OpenSearchDestination(
            url="https://search.example.test",
            index="zephyr-docs",
            transport=httpx.MockTransport(handler),
        )
    elif destination_name == "clickhouse":

        def handler(request: httpx.Request) -> httpx.Response:
            del request
            if failure:
                return httpx.Response(400, text="DB::Exception: Type mismatch in VALUES section")
            return httpx.Response(200, text="")

        destination = ClickHouseDestination(
            url="https://clickhouse.example.test",
            table="delivery_rows",
            transport=httpx.MockTransport(handler),
        )
    elif destination_name == "mongodb":
        destination = MongoDBDestination(
            uri="mongodb://db.example.test",
            database="zephyr",
            collection="delivery_records",
            collection_obj=FakeMongoCollection(
                calls=[],
                exc=DuplicateKeyError("E11000 duplicate key error") if failure else None,
            ),
        )
    else:

        def handler(request: httpx.Request) -> httpx.Response:
            del request
            if failure:
                return httpx.Response(429, text="ingester rate limit")
            return httpx.Response(204, text="")

        destination = LokiDestination(
            url="https://logs.example.test",
            stream="delivery",
            tenant_id="tenant-a",
            transport=httpx.MockTransport(handler),
        )

    stats = run_documents(
        docs=[doc],
        cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
        ctx=ctx,
        partition_fn=_ok_partition,
        destination=destination,
    )
    assert stats.total == 1
    assert stats.success == 1
    assert stats.failed == 0

    out_dir = _single_output_dir(out_root=out_root)
    receipt = _read_json(out_dir / "delivery_receipt.json")
    summary_obj = receipt.get("summary")
    assert isinstance(summary_obj, dict)
    summary = cast(dict[str, object], summary_obj)
    details_obj = receipt.get("details")
    assert isinstance(details_obj, dict)
    details = cast(dict[str, object], details_obj)
    report = _read_json(out_root / "batch_report.json")
    delivery_obj = report.get("delivery")
    assert isinstance(delivery_obj, dict)
    delivery = cast(dict[str, object], delivery_obj)
    return summary, details, delivery, report


def _run_filesystem_success_baseline(
    *,
    tmp_path: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    case_root = tmp_path / "baseline" / "filesystem" / "success"
    case_root.mkdir(parents=True, exist_ok=True)
    doc = _doc(case_root)
    out_root = case_root / "out"
    destination = FilesystemDestination()
    ctx = RunContext.new(
        pipeline_version="p4-m15",
        run_id="filesystem-baseline-success",
        timestamp_utc="2026-04-09T00:00:00Z",
    )

    stats = run_documents(
        docs=[doc],
        cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
        ctx=ctx,
        partition_fn=_ok_partition,
        destination=destination,
    )
    assert stats.total == 1
    assert stats.success == 1
    assert stats.failed == 0

    out_dir = _single_output_dir(out_root=out_root)
    receipt = _read_json(out_dir / "delivery_receipt.json")
    summary_obj = receipt.get("summary")
    assert isinstance(summary_obj, dict)
    summary = cast(dict[str, object], summary_obj)
    details_obj = receipt.get("details")
    assert isinstance(details_obj, dict)
    details = cast(dict[str, object], details_obj)
    report = _read_json(out_root / "batch_report.json")
    delivery_obj = report.get("delivery")
    assert isinstance(delivery_obj, dict)
    delivery = cast(dict[str, object], delivery_obj)
    return summary, details, delivery, report


def _run_webhook_failure_baseline(
    *,
    tmp_path: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    case_root = tmp_path / "baseline" / "webhook" / "failure"
    case_root.mkdir(parents=True, exist_ok=True)
    doc = _doc(case_root)
    out_root = case_root / "out"

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(429, text="retry later")

    destination = WebhookDestination(
        url="https://hooks.example.test",
        transport=httpx.MockTransport(handler),
        retry=DeliveryRetryConfig(enabled=False),
    )
    ctx = RunContext.new(
        pipeline_version="p4-m15",
        run_id="webhook-baseline-failure",
        timestamp_utc="2026-04-09T00:00:00Z",
    )

    stats = run_documents(
        docs=[doc],
        cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
        ctx=ctx,
        partition_fn=_ok_partition,
        destination=destination,
    )
    assert stats.total == 1
    assert stats.success == 1
    assert stats.failed == 0

    out_dir = _single_output_dir(out_root=out_root)
    receipt = _read_json(out_dir / "delivery_receipt.json")
    summary_obj = receipt.get("summary")
    assert isinstance(summary_obj, dict)
    summary = cast(dict[str, object], summary_obj)
    details_obj = receipt.get("details")
    assert isinstance(details_obj, dict)
    details = cast(dict[str, object], details_obj)
    report = _read_json(out_root / "batch_report.json")
    delivery_obj = report.get("delivery")
    assert isinstance(delivery_obj, dict)
    delivery = cast(dict[str, object], delivery_obj)
    return summary, details, delivery, report


@pytest.mark.parametrize("destination_name", _BATCH_DESTINATIONS)
def test_second_round_destinations_use_shared_runner_delivery_path(
    destination_name: DestinationName,
    tmp_path: Path,
) -> None:
    doc = _doc(tmp_path)
    out_root = tmp_path / "out"
    ctx = RunContext.new(
        pipeline_version="p4-m13",
        run_id=f"{destination_name}-runner-success",
        timestamp_utc="2026-04-09T00:00:00Z",
    )

    destination: Destination
    captured_payload: dict[str, object]
    if destination_name == "s3":
        fake = FakeS3Client(calls=[])
        destination = S3Destination(
            bucket="archive",
            region="us-east-1",
            access_key="ak",
            secret_key="sk",
            prefix="deliveries",
            client=fake,
        )
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        assert len(fake.calls) == 1
        captured_payload = cast(dict[str, object], fake.calls[0]["payload"])
    elif destination_name == "opensearch":
        opensearch_captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            assert isinstance(body, dict)
            opensearch_captured["path"] = request.url.path
            opensearch_captured["body"] = cast(dict[str, object], body)
            return httpx.Response(201, json={"result": "created", "_version": 1})

        destination = OpenSearchDestination(
            url="https://search.example.test",
            index="zephyr-docs",
            transport=httpx.MockTransport(handler),
        )
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        body = cast(dict[str, object], opensearch_captured["body"])
        payload_obj = body.get("payload")
        assert isinstance(payload_obj, dict)
        captured_payload = cast(dict[str, object], payload_obj)
    else:
        clickhouse_captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            row = json.loads(request.content.decode("utf-8").strip())
            assert isinstance(row, dict)
            clickhouse_captured["row"] = cast(dict[str, object], row)
            return httpx.Response(200, text="")

        destination = ClickHouseDestination(
            url="https://clickhouse.example.test",
            database="analytics",
            table="delivery_rows",
            transport=httpx.MockTransport(handler),
        )
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        row = cast(dict[str, object], clickhouse_captured["row"])
        payload_json = row.get("payload_json")
        assert isinstance(payload_json, str)
        raw_payload = json.loads(payload_json)
        assert isinstance(raw_payload, dict)
        captured_payload = cast(dict[str, object], raw_payload)

    assert stats.total == 1
    assert stats.success == 1
    assert stats.failed == 0

    out_dir = _single_output_dir(out_root=out_root)
    sha256 = out_dir.name
    identity_key = _identity_key(sha256=sha256, run_id=ctx.run_id)

    receipt = _read_json(out_dir / "delivery_receipt.json")
    summary_obj = receipt.get("summary")
    assert isinstance(summary_obj, dict)
    summary = cast(dict[str, object], summary_obj)
    assert receipt["destination"] == destination_name
    assert receipt["ok"] is True
    assert summary == {
        "delivery_outcome": "delivered",
        "failure_retryability": "not_failed",
        "failure_kind": "not_failed",
        "error_code": "not_failed",
        "attempt_count": 1,
        "payload_count": 1,
    }

    details_obj = receipt.get("details")
    assert isinstance(details_obj, dict)
    details = cast(dict[str, object], details_obj)

    report = _read_json(out_root / "batch_report.json")
    delivery_obj = report.get("delivery")
    assert isinstance(delivery_obj, dict)
    delivery = cast(dict[str, object], delivery_obj)
    by_destination_obj = delivery.get("by_destination")
    assert isinstance(by_destination_obj, dict)
    by_destination = cast(dict[str, object], by_destination_obj)
    destination_bucket_obj = by_destination.get(destination_name)
    assert isinstance(destination_bucket_obj, dict)
    destination_bucket = cast(dict[str, object], destination_bucket_obj)

    assert delivery["total"] == 1
    assert delivery["ok"] == 1
    assert delivery["failed"] == 0
    assert delivery["failed_retryable"] == 0
    assert delivery["failed_non_retryable"] == 0
    assert delivery["failed_unknown"] == 0
    assert destination_bucket == {"total": 1, "ok": 1, "failed": 0}
    assert report["counts_by_error_code"] == {}

    assert captured_payload["sha256"] == sha256
    _assert_primary_provenance(payload=captured_payload, sha256=sha256)

    if destination_name == "s3":
        assert details["object_key"] == f"deliveries/{identity_key}.json"
        assert details["identity_key"] == identity_key
        assert "object_key" not in summary
    elif destination_name == "opensearch":
        assert details["document_id"] == identity_key
        assert details["attempted_count"] == 1
        assert details["accepted_count"] == 1
        assert details["rejected_count"] == 0
        assert "document_id" not in summary
        assert "attempted_count" not in summary
    else:
        assert details["identity_key"] == identity_key
        assert details["row_count"] == 1
        assert details["table"] == "delivery_rows"
        assert "table" not in summary
        assert "row_count" not in summary


@pytest.mark.parametrize("destination_name", _BATCH_DESTINATIONS)
def test_second_round_destinations_integrate_runner_failure_reporting_and_dlq(
    destination_name: DestinationName,
    tmp_path: Path,
) -> None:
    doc = _doc(tmp_path)
    out_root = tmp_path / "out"
    ctx = RunContext.new(
        pipeline_version="p4-m13",
        run_id=f"{destination_name}-runner-failure",
        timestamp_utc="2026-04-09T00:00:00Z",
    )

    destination: Destination
    captured_payload: dict[str, object]
    expected_retryability: str
    expected_failure_kind: str
    if destination_name == "s3":
        fake = FakeS3Client(calls=[], exc=FakeS3ServiceError())
        destination = S3Destination(
            bucket="archive",
            region="us-east-1",
            access_key="ak",
            secret_key="sk",
            prefix="deliveries",
            client=fake,
        )
        expected_retryability = "retryable"
        expected_failure_kind = "server_error"
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        assert len(fake.calls) == 1
        captured_payload = cast(dict[str, object], fake.calls[0]["payload"])
    elif destination_name == "opensearch":
        opensearch_captured: dict[str, object] = {}
        expected_retryability = "retryable"
        expected_failure_kind = "rate_limited"

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            assert isinstance(body, dict)
            opensearch_captured["body"] = cast(dict[str, object], body)
            return httpx.Response(
                429,
                json={"error": {"type": "rejected_execution_exception", "reason": "busy"}},
            )

        destination = OpenSearchDestination(
            url="https://search.example.test",
            index="zephyr-docs",
            transport=httpx.MockTransport(handler),
        )
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        body = cast(dict[str, object], opensearch_captured["body"])
        payload_obj = body.get("payload")
        assert isinstance(payload_obj, dict)
        captured_payload = cast(dict[str, object], payload_obj)
    else:
        clickhouse_captured: dict[str, object] = {}
        expected_retryability = "non_retryable"
        expected_failure_kind = "constraint"

        def handler(request: httpx.Request) -> httpx.Response:
            row = json.loads(request.content.decode("utf-8").strip())
            assert isinstance(row, dict)
            clickhouse_captured["row"] = cast(dict[str, object], row)
            return httpx.Response(400, text="DB::Exception: Type mismatch in VALUES section")

        destination = ClickHouseDestination(
            url="https://clickhouse.example.test",
            table="delivery_rows",
            transport=httpx.MockTransport(handler),
        )
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        row = cast(dict[str, object], clickhouse_captured["row"])
        payload_json = row.get("payload_json")
        assert isinstance(payload_json, str)
        raw_payload = json.loads(payload_json)
        assert isinstance(raw_payload, dict)
        captured_payload = cast(dict[str, object], raw_payload)

    assert stats.total == 1
    assert stats.success == 1
    assert stats.failed == 0

    out_dir = _single_output_dir(out_root=out_root)
    sha256 = out_dir.name
    identity_key = _identity_key(sha256=sha256, run_id=ctx.run_id)

    receipt = _read_json(out_dir / "delivery_receipt.json")
    summary_obj = receipt.get("summary")
    assert isinstance(summary_obj, dict)
    summary = cast(dict[str, object], summary_obj)
    assert receipt["destination"] == destination_name
    assert receipt["ok"] is False
    assert summary == {
        "delivery_outcome": "failed",
        "failure_retryability": expected_retryability,
        "failure_kind": expected_failure_kind,
        "error_code": str(ErrorCode.DELIVERY_FAILED),
        "attempt_count": 1,
        "payload_count": 1,
    }

    report = _read_json(out_root / "batch_report.json")
    counts_obj = report.get("counts")
    assert isinstance(counts_obj, dict)
    counts = cast(dict[str, object], counts_obj)
    delivery_obj = report.get("delivery")
    assert isinstance(delivery_obj, dict)
    delivery = cast(dict[str, object], delivery_obj)
    by_destination_obj = delivery.get("by_destination")
    assert isinstance(by_destination_obj, dict)
    by_destination = cast(dict[str, object], by_destination_obj)
    destination_bucket_obj = by_destination.get(destination_name)
    assert isinstance(destination_bucket_obj, dict)
    destination_bucket = cast(dict[str, object], destination_bucket_obj)
    failure_kinds_obj = delivery.get("failure_kinds_by_destination")
    assert isinstance(failure_kinds_obj, dict)
    failure_kinds = cast(dict[str, object], failure_kinds_obj)

    assert counts["success"] == 1
    assert counts["failed"] == 0
    assert delivery["total"] == 1
    assert delivery["ok"] == 0
    assert delivery["failed"] == 1
    assert delivery["dlq_written_total"] == 1
    assert destination_bucket == {"total": 1, "ok": 0, "failed": 1}
    assert failure_kinds[destination_name] == {expected_failure_kind: 1}
    assert report["counts_by_error_code"] == {str(ErrorCode.DELIVERY_FAILED): 1}
    if expected_retryability == "retryable":
        assert delivery["failed_retryable"] == 1
        assert delivery["failed_non_retryable"] == 0
        assert delivery["failed_unknown"] == 0
    else:
        assert delivery["failed_retryable"] == 0
        assert delivery["failed_non_retryable"] == 1
        assert delivery["failed_unknown"] == 0

    dlq_files = list((out_root / "_dlq" / "delivery").glob("*.json"))
    assert len(dlq_files) == 1
    dlq = _read_json(dlq_files[0])
    assert dlq["sha256"] == sha256
    assert dlq["run_id"] == ctx.run_id
    destination_receipt_obj = dlq.get("destination_receipt")
    assert isinstance(destination_receipt_obj, dict)
    destination_receipt = cast(dict[str, object], destination_receipt_obj)
    assert destination_receipt["destination"] == destination_name
    assert destination_receipt["ok"] is False

    run_meta_obj = dlq.get("run_meta")
    assert isinstance(run_meta_obj, dict)
    run_meta = cast(dict[str, object], run_meta_obj)
    provenance_obj = run_meta.get("provenance")
    assert isinstance(provenance_obj, dict)
    provenance = cast(dict[str, object], provenance_obj)
    assert provenance["delivery_origin"] == "primary"
    assert provenance["run_origin"] == "intake"
    assert provenance["execution_mode"] == "batch"
    assert provenance["task_id"] == sha256

    assert captured_payload["sha256"] == sha256
    _assert_primary_provenance(payload=captured_payload, sha256=sha256)

    details_obj = receipt.get("details")
    assert isinstance(details_obj, dict)
    details = cast(dict[str, object], details_obj)
    if destination_name == "s3":
        assert details["identity_key"] == identity_key
        assert details["backend_error_code"] == "SlowDown"
    elif destination_name == "opensearch":
        assert details["document_id"] == identity_key
        assert details["attempted_count"] == 1
        assert details["accepted_count"] == 0
        assert details["rejected_count"] == 1
    else:
        assert details["identity_key"] == identity_key
        assert details["table"] == "delivery_rows"
        assert details["row_count"] == 1


@pytest.mark.parametrize("destination_name", _REMAINING_BATCH_DESTINATIONS)
def test_remaining_second_round_destinations_use_shared_runner_delivery_path(
    destination_name: RemainingDestinationName,
    tmp_path: Path,
) -> None:
    doc = _doc(tmp_path)
    out_root = tmp_path / "out"
    ctx = RunContext.new(
        pipeline_version="p4-m14",
        run_id=f"{destination_name}-runner-success",
        timestamp_utc="2026-04-09T00:00:00Z",
    )

    destination: Destination
    captured_payload: dict[str, object]
    if destination_name == "mongodb":
        fake = FakeMongoCollection(calls=[])
        destination = MongoDBDestination(
            uri="mongodb://db.example.test",
            database="zephyr",
            collection="delivery_records",
            collection_obj=fake,
        )
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        assert len(fake.calls) == 1
        replacement = cast(dict[str, object], fake.calls[0]["replacement"])
        payload_obj = replacement.get("payload")
        assert isinstance(payload_obj, dict)
        captured_payload = cast(dict[str, object], payload_obj)
    else:
        loki_captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            assert isinstance(body, dict)
            loki_captured["body"] = cast(dict[str, object], body)
            return httpx.Response(204, text="")

        destination = LokiDestination(
            url="https://logs.example.test",
            stream="delivery",
            tenant_id="tenant-a",
            transport=httpx.MockTransport(handler),
        )
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        body = cast(dict[str, object], loki_captured["body"])
        streams = cast(list[dict[str, object]], body["streams"])
        values = cast(list[list[str]], streams[0]["values"])
        payload_obj = json.loads(values[0][1])
        assert isinstance(payload_obj, dict)
        captured_payload = cast(dict[str, object], payload_obj)

    assert stats.total == 1
    assert stats.success == 1
    assert stats.failed == 0

    out_dir = _single_output_dir(out_root=out_root)
    sha256 = out_dir.name
    identity_key = _identity_key(sha256=sha256, run_id=ctx.run_id)

    receipt = _read_json(out_dir / "delivery_receipt.json")
    summary_obj = receipt.get("summary")
    assert isinstance(summary_obj, dict)
    summary = cast(dict[str, object], summary_obj)
    assert receipt["destination"] == destination_name
    assert receipt["ok"] is True
    assert summary == {
        "delivery_outcome": "delivered",
        "failure_retryability": "not_failed",
        "failure_kind": "not_failed",
        "error_code": "not_failed",
        "attempt_count": 1,
        "payload_count": 1,
    }

    details_obj = receipt.get("details")
    assert isinstance(details_obj, dict)
    details = cast(dict[str, object], details_obj)

    report = _read_json(out_root / "batch_report.json")
    delivery_obj = report.get("delivery")
    assert isinstance(delivery_obj, dict)
    delivery = cast(dict[str, object], delivery_obj)
    by_destination_obj = delivery.get("by_destination")
    assert isinstance(by_destination_obj, dict)
    by_destination = cast(dict[str, object], by_destination_obj)
    destination_bucket_obj = by_destination.get(destination_name)
    assert isinstance(destination_bucket_obj, dict)
    destination_bucket = cast(dict[str, object], destination_bucket_obj)

    assert delivery["total"] == 1
    assert delivery["ok"] == 1
    assert delivery["failed"] == 0
    assert destination_bucket == {"total": 1, "ok": 1, "failed": 0}
    assert report["counts_by_error_code"] == {}

    assert captured_payload["sha256"] == sha256
    _assert_primary_provenance(payload=captured_payload, sha256=sha256)

    if destination_name == "mongodb":
        assert details["document_id"] == identity_key
        assert details["attempted_count"] == 1
        assert details["accepted_count"] == 1
        assert details["rejected_count"] == 0
        assert "document_id" not in summary
    else:
        assert details["stream"] == "delivery"
        assert details["tenant_id"] == "tenant-a"
        assert details["line_count"] == 1
        assert details["accepted_count"] == 1
        assert details["rejected_count"] == 0
        assert "stream" not in summary
        assert "line_count" not in summary


@pytest.mark.parametrize("destination_name", _REMAINING_BATCH_DESTINATIONS)
def test_remaining_second_round_destinations_integrate_runner_failure_reporting_and_dlq(
    destination_name: RemainingDestinationName,
    tmp_path: Path,
) -> None:
    doc = _doc(tmp_path)
    out_root = tmp_path / "out"
    ctx = RunContext.new(
        pipeline_version="p4-m14",
        run_id=f"{destination_name}-runner-failure",
        timestamp_utc="2026-04-09T00:00:00Z",
    )

    destination: Destination
    captured_payload: dict[str, object]
    expected_retryability: str
    expected_failure_kind: str
    if destination_name == "mongodb":
        expected_retryability = "non_retryable"
        expected_failure_kind = "constraint"
        fake = FakeMongoCollection(calls=[], exc=DuplicateKeyError("E11000 duplicate key error"))
        destination = MongoDBDestination(
            uri="mongodb://db.example.test",
            database="zephyr",
            collection="delivery_records",
            collection_obj=fake,
        )
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        assert len(fake.calls) == 0
        dlq_payload_path = list((out_root / "_dlq" / "delivery").glob("*.json"))
        assert len(dlq_payload_path) == 1
        destination_receipt_source = _read_json(dlq_payload_path[0])
        run_meta_obj = destination_receipt_source.get("run_meta")
        assert isinstance(run_meta_obj, dict)
        captured_payload = {
            "sha256": cast(str, destination_receipt_source["sha256"]),
            "run_meta": cast(dict[str, object], run_meta_obj),
        }
    else:
        loki_captured: dict[str, object] = {}
        expected_retryability = "retryable"
        expected_failure_kind = "rate_limited"

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            assert isinstance(body, dict)
            loki_captured["body"] = cast(dict[str, object], body)
            return httpx.Response(429, text="ingester rate limit")

        destination = LokiDestination(
            url="https://logs.example.test",
            stream="delivery",
            transport=httpx.MockTransport(handler),
        )
        stats = run_documents(
            docs=[doc],
            cfg=RunnerConfig(out_root=out_root, workers=1, destination=destination),
            ctx=ctx,
            partition_fn=_ok_partition,
            destination=destination,
        )
        body = cast(dict[str, object], loki_captured["body"])
        streams = cast(list[dict[str, object]], body["streams"])
        values = cast(list[list[str]], streams[0]["values"])
        payload_obj = json.loads(values[0][1])
        assert isinstance(payload_obj, dict)
        captured_payload = cast(dict[str, object], payload_obj)

    assert stats.total == 1
    assert stats.success == 1
    assert stats.failed == 0

    out_dir = _single_output_dir(out_root=out_root)
    sha256 = out_dir.name
    identity_key = _identity_key(sha256=sha256, run_id=ctx.run_id)

    receipt = _read_json(out_dir / "delivery_receipt.json")
    summary_obj = receipt.get("summary")
    assert isinstance(summary_obj, dict)
    summary = cast(dict[str, object], summary_obj)
    assert receipt["destination"] == destination_name
    assert receipt["ok"] is False
    assert summary == {
        "delivery_outcome": "failed",
        "failure_retryability": expected_retryability,
        "failure_kind": expected_failure_kind,
        "error_code": str(ErrorCode.DELIVERY_FAILED),
        "attempt_count": 1,
        "payload_count": 1,
    }

    report = _read_json(out_root / "batch_report.json")
    counts_obj = report.get("counts")
    assert isinstance(counts_obj, dict)
    counts = cast(dict[str, object], counts_obj)
    delivery_obj = report.get("delivery")
    assert isinstance(delivery_obj, dict)
    delivery = cast(dict[str, object], delivery_obj)
    by_destination_obj = delivery.get("by_destination")
    assert isinstance(by_destination_obj, dict)
    by_destination = cast(dict[str, object], by_destination_obj)
    destination_bucket_obj = by_destination.get(destination_name)
    assert isinstance(destination_bucket_obj, dict)
    destination_bucket = cast(dict[str, object], destination_bucket_obj)
    failure_kinds_obj = delivery.get("failure_kinds_by_destination")
    assert isinstance(failure_kinds_obj, dict)
    failure_kinds = cast(dict[str, object], failure_kinds_obj)

    assert counts["success"] == 1
    assert counts["failed"] == 0
    assert delivery["total"] == 1
    assert delivery["ok"] == 0
    assert delivery["failed"] == 1
    assert delivery["dlq_written_total"] == 1
    assert destination_bucket == {"total": 1, "ok": 0, "failed": 1}
    assert failure_kinds[destination_name] == {expected_failure_kind: 1}
    assert report["counts_by_error_code"] == {str(ErrorCode.DELIVERY_FAILED): 1}

    dlq_files = list((out_root / "_dlq" / "delivery").glob("*.json"))
    assert len(dlq_files) == 1
    dlq = _read_json(dlq_files[0])
    assert dlq["sha256"] == sha256
    assert dlq["run_id"] == ctx.run_id
    destination_receipt_obj = dlq.get("destination_receipt")
    assert isinstance(destination_receipt_obj, dict)
    destination_receipt = cast(dict[str, object], destination_receipt_obj)
    assert destination_receipt["destination"] == destination_name
    assert destination_receipt["ok"] is False

    run_meta_obj = dlq.get("run_meta")
    assert isinstance(run_meta_obj, dict)
    run_meta = cast(dict[str, object], run_meta_obj)
    provenance_obj = run_meta.get("provenance")
    assert isinstance(provenance_obj, dict)
    provenance = cast(dict[str, object], provenance_obj)
    assert provenance["delivery_origin"] == "primary"
    assert provenance["run_origin"] == "intake"
    assert provenance["execution_mode"] == "batch"
    assert provenance["task_id"] == sha256

    assert captured_payload["sha256"] == sha256
    _assert_primary_provenance(payload=captured_payload, sha256=sha256)

    details_obj = receipt.get("details")
    assert isinstance(details_obj, dict)
    details = cast(dict[str, object], details_obj)
    if destination_name == "mongodb":
        assert details["document_id"] == identity_key
        assert details["backend_error_code"] == 11000
        assert details["accepted_count"] == 0
        assert details["rejected_count"] == 1
    else:
        assert details["stream"] == "delivery"
        assert details["line_count"] == 1
        assert details["accepted_count"] == 0
        assert details["rejected_count"] == 1


def test_full_second_round_destination_batch_preserves_operator_facing_summary_boundary(
    tmp_path: Path,
) -> None:
    for destination_name in _FULL_SECOND_ROUND_DESTINATIONS:
        (
            success_summary,
            success_details,
            success_delivery,
            success_report,
        ) = _run_second_round_delivery_case(
            destination_name=destination_name,
            tmp_path=tmp_path,
            failure=False,
        )
        (
            failure_summary,
            failure_details,
            failure_delivery,
            failure_report,
        ) = _run_second_round_delivery_case(
            destination_name=destination_name,
            tmp_path=tmp_path,
            failure=True,
        )

        assert success_summary == {
            "delivery_outcome": "delivered",
            "failure_retryability": "not_failed",
            "failure_kind": "not_failed",
            "error_code": "not_failed",
            "attempt_count": 1,
            "payload_count": 1,
        }
        assert success_delivery["total"] == 1
        assert success_delivery["ok"] == 1
        assert success_delivery["failed"] == 0
        assert success_delivery["failed_retryable"] == 0
        assert success_delivery["failed_non_retryable"] == 0
        assert success_delivery["failed_unknown"] == 0
        success_by_destination_obj = success_delivery.get("by_destination")
        assert isinstance(success_by_destination_obj, dict)
        success_by_destination = cast(dict[str, object], success_by_destination_obj)
        assert success_by_destination[destination_name] == {"total": 1, "ok": 1, "failed": 0}
        assert success_report["counts_by_error_code"] == {}

        assert set(failure_summary) == {
            "delivery_outcome",
            "failure_retryability",
            "failure_kind",
            "error_code",
            "attempt_count",
            "payload_count",
        }
        assert failure_summary["delivery_outcome"] == "failed"
        assert failure_summary["attempt_count"] == 1
        assert failure_summary["payload_count"] == 1
        assert failure_summary["error_code"] == str(ErrorCode.DELIVERY_FAILED)
        assert failure_delivery["total"] == 1
        assert failure_delivery["ok"] == 0
        assert failure_delivery["failed"] == 1
        failure_by_destination_obj = failure_delivery.get("by_destination")
        assert isinstance(failure_by_destination_obj, dict)
        failure_by_destination = cast(dict[str, object], failure_by_destination_obj)
        assert failure_by_destination[destination_name] == {"total": 1, "ok": 0, "failed": 1}
        failure_kinds_obj = failure_delivery.get("failure_kinds_by_destination")
        assert isinstance(failure_kinds_obj, dict)
        failure_kinds = cast(dict[str, object], failure_kinds_obj)
        assert failure_kinds[destination_name] == {cast(str, failure_summary["failure_kind"]): 1}
        assert failure_report["counts_by_error_code"] == {str(ErrorCode.DELIVERY_FAILED): 1}
        if failure_summary["failure_retryability"] == "retryable":
            assert failure_delivery["failed_retryable"] == 1
            assert failure_delivery["failed_non_retryable"] == 0
            assert failure_delivery["failed_unknown"] == 0
        else:
            assert failure_delivery["failed_retryable"] == 0
            assert failure_delivery["failed_non_retryable"] == 1
            assert failure_delivery["failed_unknown"] == 0

        if destination_name in _FULL_SECOND_ROUND_COUNT_DETAIL_DESTINATIONS:
            assert success_details["accepted_count"] == 1
            assert success_details["rejected_count"] == 0
            assert failure_details["accepted_count"] == 0
            assert failure_details["rejected_count"] == 1
        else:
            assert "accepted_count" not in success_details
            assert "rejected_count" not in success_details
            assert "accepted_count" not in failure_details
            assert "rejected_count" not in failure_details


def test_full_second_round_batch_stays_within_established_shared_delivery_model(
    tmp_path: Path,
) -> None:
    (
        filesystem_summary,
        filesystem_details,
        filesystem_delivery,
        filesystem_report,
    ) = _run_filesystem_success_baseline(tmp_path=tmp_path)
    (
        webhook_summary,
        webhook_details,
        webhook_delivery,
        webhook_report,
    ) = _run_webhook_failure_baseline(tmp_path=tmp_path)

    assert filesystem_summary == {
        "delivery_outcome": "delivered",
        "failure_retryability": "not_failed",
        "failure_kind": "not_failed",
        "error_code": "not_failed",
        "attempt_count": 1,
        "payload_count": 1,
    }
    assert "out_dir" in filesystem_details
    filesystem_by_destination_obj = filesystem_delivery.get("by_destination")
    assert isinstance(filesystem_by_destination_obj, dict)
    filesystem_by_destination = cast(dict[str, object], filesystem_by_destination_obj)
    assert filesystem_by_destination["filesystem"] == {"total": 1, "ok": 1, "failed": 0}
    assert filesystem_report["counts_by_error_code"] == {}

    assert set(webhook_summary) == set(filesystem_summary)
    assert webhook_summary["delivery_outcome"] == "failed"
    assert webhook_summary["failure_retryability"] == "retryable"
    assert webhook_summary["failure_kind"] == "rate_limited"
    assert webhook_summary["error_code"] == str(ErrorCode.DELIVERY_HTTP_FAILED)
    assert webhook_details["retryable"] is True
    assert webhook_details["failure_kind"] == "rate_limited"
    webhook_by_destination_obj = webhook_delivery.get("by_destination")
    assert isinstance(webhook_by_destination_obj, dict)
    webhook_by_destination = cast(dict[str, object], webhook_by_destination_obj)
    assert webhook_by_destination["webhook"] == {"total": 1, "ok": 0, "failed": 1}
    assert webhook_report["counts_by_error_code"] == {str(ErrorCode.DELIVERY_HTTP_FAILED): 1}

    for destination_name in _FULL_SECOND_ROUND_DESTINATIONS:
        (
            success_summary,
            _success_details,
            success_delivery,
            success_report,
        ) = _run_second_round_delivery_case(
            destination_name=destination_name,
            tmp_path=tmp_path / "second_round",
            failure=False,
        )
        (
            failure_summary,
            failure_details,
            failure_delivery,
            failure_report,
        ) = _run_second_round_delivery_case(
            destination_name=destination_name,
            tmp_path=tmp_path / "second_round",
            failure=True,
        )

        assert success_summary == filesystem_summary
        assert set(failure_summary) == set(webhook_summary)
        assert failure_summary["error_code"] == str(ErrorCode.DELIVERY_FAILED)
        assert failure_summary["error_code"] != webhook_summary["error_code"]

        success_by_destination_obj = success_delivery.get("by_destination")
        assert isinstance(success_by_destination_obj, dict)
        success_by_destination = cast(dict[str, object], success_by_destination_obj)
        assert success_by_destination[destination_name] == {
            "total": 1,
            "ok": 1,
            "failed": 0,
        }
        failure_by_destination_obj = failure_delivery.get("by_destination")
        assert isinstance(failure_by_destination_obj, dict)
        failure_by_destination = cast(dict[str, object], failure_by_destination_obj)
        assert failure_by_destination[destination_name] == {
            "total": 1,
            "ok": 0,
            "failed": 1,
        }
        assert success_report["counts_by_error_code"] == filesystem_report["counts_by_error_code"]
        assert failure_report["counts_by_error_code"] == {str(ErrorCode.DELIVERY_FAILED): 1}

        assert failure_details["retryable"] == (
            failure_summary["failure_retryability"] == "retryable"
        )
