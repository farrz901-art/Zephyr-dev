from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping, cast

import httpx
import pytest

from zephyr_core import RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.run_meta import RunProvenanceV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
)
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.destinations.clickhouse import ClickHouseDestination
from zephyr_ingest.destinations.loki import LokiDestination
from zephyr_ingest.destinations.mongodb import MongoDBDestination, MongoReplaceResultProtocol
from zephyr_ingest.destinations.opensearch import OpenSearchDestination
from zephyr_ingest.destinations.s3 import S3Destination
from zephyr_ingest.replay_delivery import (
    ClickHouseReplaySink,
    LokiReplaySink,
    MongoDBReplaySink,
    OpenSearchReplaySink,
    ReplaySink,
    S3ReplaySink,
    replay_delivery_dlq,
)

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
_FULL_SECOND_ROUND_FAILURE_BOUNDARY = {
    ("retryable", "server_error", "ZE-DELIVERY-FAILED"),
    ("retryable", "rate_limited", "ZE-DELIVERY-FAILED"),
    ("non_retryable", "constraint", "ZE-DELIVERY-FAILED"),
}
_SHARED_SUMMARY_KEYS = {
    "delivery_outcome",
    "failure_retryability",
    "failure_kind",
    "error_code",
    "attempt_count",
    "payload_count",
}


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
    upserted_id: object | None = "abc123:r1"


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


def _build_meta(*, run_id: str) -> RunMetaV1:
    return RunMetaV1(
        run_id=run_id,
        pipeline_version="p4-m13",
        timestamp_utc="2026-04-09T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=RunOutcome.SUCCESS,
        engine=None,
        document=None,
        error=None,
        warnings=[],
    )


def _identity_key(*, sha256: str, run_id: str) -> str:
    return normalize_delivery_idempotency_key(
        identity=DeliveryIdentityV1(sha256=sha256, run_id=run_id)
    )


def _shared_summary(
    *,
    delivery_outcome: Literal["delivered", "failed"],
    failure_retryability: str,
    failure_kind: str,
    error_code: str,
) -> dict[str, object]:
    return {
        "delivery_outcome": delivery_outcome,
        "failure_retryability": failure_retryability,
        "failure_kind": failure_kind,
        "error_code": error_code,
        "attempt_count": 1,
        "payload_count": 1,
    }


def _assert_shared_boundary(receipt: DeliveryReceipt) -> None:
    assert set(receipt.shared_summary) == _SHARED_SUMMARY_KEYS
    assert "backend_error_code" not in receipt.shared_summary
    assert "identity_key" not in receipt.shared_summary
    assert "bucket" not in receipt.shared_summary
    assert "object_key" not in receipt.shared_summary
    assert "index" not in receipt.shared_summary
    assert "database" not in receipt.shared_summary
    assert "collection" not in receipt.shared_summary
    assert "document_id" not in receipt.shared_summary
    assert "write_mode" not in receipt.shared_summary
    assert "operation" not in receipt.shared_summary
    assert "table" not in receipt.shared_summary
    assert "row_count" not in receipt.shared_summary
    assert "attempted_count" not in receipt.shared_summary
    assert "accepted_count" not in receipt.shared_summary
    assert "rejected_count" not in receipt.shared_summary


def _assert_remaining_shared_boundary(receipt: DeliveryReceipt) -> None:
    _assert_shared_boundary(receipt)
    assert "stream" not in receipt.shared_summary
    assert "tenant_id" not in receipt.shared_summary
    assert "line_count" not in receipt.shared_summary


def _write_replay_record(*, out_root: Path, sha256: str, run_id: str) -> None:
    dlq_dir = out_root / "_dlq" / "delivery"
    dlq_dir.mkdir(parents=True, exist_ok=True)
    run_meta = _build_meta(run_id=run_id).to_dict()
    run_meta["provenance"] = RunProvenanceV1(
        run_origin="intake",
        delivery_origin="primary",
        execution_mode="worker",
        task_id="task-1",
        checkpoint_identity_key="cp-1",
        task_identity_key="task-key-1",
    ).to_dict()
    (dlq_dir / "one.json").write_text(
        json.dumps({"sha256": sha256, "run_id": run_id, "run_meta": run_meta}),
        encoding="utf-8",
    )


def _build_second_round_success_receipt(
    *,
    destination_name: FullSecondRoundDestinationName,
    tmp_path: Path,
) -> DeliveryReceipt:
    sha256 = "abc123"
    meta = _build_meta(run_id=f"{destination_name}-batch-success")
    identity_key = _identity_key(sha256=sha256, run_id=meta.run_id)

    if destination_name == "s3":
        receipt = S3Destination(
            bucket="archive",
            region="us-east-1",
            access_key="ak",
            secret_key="sk",
            prefix="deliveries",
            client=FakeS3Client(calls=[]),
        )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["identity_key"] == identity_key
        assert receipt.details["object_key"] == f"deliveries/{identity_key}.json"
        _assert_shared_boundary(receipt)
        return receipt

    if destination_name == "opensearch":

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/zephyr-docs/_doc/{identity_key}"
            return httpx.Response(201, json={"result": "created", "_version": 1})

        receipt = OpenSearchDestination(
            url="https://search.example.test",
            index="zephyr-docs",
            transport=httpx.MockTransport(handler),
        )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["document_id"] == identity_key
        assert receipt.details["attempted_count"] == 1
        assert receipt.details["accepted_count"] == 1
        assert receipt.details["rejected_count"] == 0
        _assert_shared_boundary(receipt)
        return receipt

    if destination_name == "clickhouse":

        def handler(request: httpx.Request) -> httpx.Response:
            row = json.loads(request.content.decode("utf-8").strip())
            assert isinstance(row, dict)
            typed_row = cast(dict[str, object], row)
            assert typed_row["identity_key"] == identity_key
            return httpx.Response(200, text="")

        receipt = ClickHouseDestination(
            url="https://clickhouse.example.test",
            database="analytics",
            table="delivery_rows",
            transport=httpx.MockTransport(handler),
        )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["identity_key"] == identity_key
        assert receipt.details["row_count"] == 1
        _assert_shared_boundary(receipt)
        return receipt

    if destination_name == "mongodb":
        fake = FakeMongoCollection(calls=[])
        receipt = MongoDBDestination(
            uri="mongodb://db.example.test",
            database="zephyr",
            collection="delivery_records",
            collection_obj=fake,
        )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
        assert len(fake.calls) == 1
        assert receipt.details is not None
        assert receipt.details["document_id"] == identity_key
        assert receipt.details["attempted_count"] == 1
        assert receipt.details["accepted_count"] == 1
        assert receipt.details["rejected_count"] == 0
        _assert_remaining_shared_boundary(receipt)
        return receipt

    def success_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        stream = body["streams"][0]["stream"]
        assert stream["zephyr_stream"] == "delivery"
        assert stream["zephyr_delivery_identity"] == identity_key
        return httpx.Response(204, text="")

    receipt = LokiDestination(
        url="https://logs.example.test",
        stream="delivery",
        tenant_id="tenant-a",
        transport=httpx.MockTransport(success_handler),
    )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
    assert receipt.details is not None
    assert receipt.details["stream"] == "delivery"
    assert receipt.details["tenant_id"] == "tenant-a"
    assert receipt.details["line_count"] == 1
    assert receipt.details["accepted_count"] == 1
    assert receipt.details["rejected_count"] == 0
    _assert_remaining_shared_boundary(receipt)
    return receipt


def _build_second_round_failure_receipt(
    *,
    destination_name: FullSecondRoundDestinationName,
    tmp_path: Path,
) -> DeliveryReceipt:
    sha256 = "abc123"
    meta = _build_meta(run_id=f"{destination_name}-batch-failure")
    identity_key = _identity_key(sha256=sha256, run_id=meta.run_id)

    if destination_name == "s3":
        receipt = S3Destination(
            bucket="archive",
            region="us-east-1",
            access_key="ak",
            secret_key="sk",
            prefix="deliveries",
            client=FakeS3Client(calls=[], exc=FakeS3ServiceError()),
        )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["identity_key"] == identity_key
        assert receipt.details["backend_error_code"] == "SlowDown"
        _assert_shared_boundary(receipt)
        return receipt

    if destination_name == "opensearch":

        def handler(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(
                429,
                json={"error": {"type": "rejected_execution_exception", "reason": "busy"}},
            )

        receipt = OpenSearchDestination(
            url="https://search.example.test",
            index="zephyr-docs",
            transport=httpx.MockTransport(handler),
        )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["document_id"] == identity_key
        assert receipt.details["attempted_count"] == 1
        assert receipt.details["accepted_count"] == 0
        assert receipt.details["rejected_count"] == 1
        _assert_shared_boundary(receipt)
        return receipt

    if destination_name == "clickhouse":

        def handler(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(400, text="DB::Exception: Type mismatch in VALUES section")

        receipt = ClickHouseDestination(
            url="https://clickhouse.example.test",
            table="delivery_rows",
            transport=httpx.MockTransport(handler),
        )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["identity_key"] == identity_key
        assert receipt.details["row_count"] == 1
        _assert_shared_boundary(receipt)
        return receipt

    if destination_name == "mongodb":
        receipt = MongoDBDestination(
            uri="mongodb://db.example.test",
            database="zephyr",
            collection="delivery_records",
            collection_obj=FakeMongoCollection(
                calls=[],
                exc=DuplicateKeyError("E11000 duplicate key error"),
            ),
        )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["document_id"] == identity_key
        assert receipt.details["accepted_count"] == 0
        assert receipt.details["rejected_count"] == 1
        assert receipt.details["backend_error_code"] == 11000
        _assert_remaining_shared_boundary(receipt)
        return receipt

    def failure_handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(429, text="ingester rate limit")

    receipt = LokiDestination(
        url="https://logs.example.test",
        stream="delivery",
        transport=httpx.MockTransport(failure_handler),
    )(out_root=tmp_path / destination_name, sha256=sha256, meta=meta, result=None)
    assert receipt.details is not None
    assert receipt.details["accepted_count"] == 0
    assert receipt.details["rejected_count"] == 1
    _assert_remaining_shared_boundary(receipt)
    return receipt


def _replay_second_round_destination_record(
    *,
    destination_name: FullSecondRoundDestinationName,
    tmp_path: Path,
) -> tuple[str, dict[str, object]]:
    sha256 = "abc123"
    run_id = f"{destination_name}-batch-replay"
    identity_key = _identity_key(sha256=sha256, run_id=run_id)
    out_root = tmp_path / destination_name
    _write_replay_record(out_root=out_root, sha256=sha256, run_id=run_id)

    captured_payload: dict[str, object]
    sink: ReplaySink
    if destination_name == "s3":
        fake_s3 = FakeS3Client(calls=[])
        sink = S3ReplaySink(bucket="archive", prefix="replay", client=fake_s3)
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        assert len(fake_s3.calls) == 1
        assert fake_s3.calls[0]["key"] == f"replay/{identity_key}.json"
        captured_payload = cast(dict[str, object], fake_s3.calls[0]["payload"])
    elif destination_name == "opensearch":
        opensearch_captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            opensearch_captured["path"] = request.url.path
            body = json.loads(request.content.decode("utf-8"))
            assert isinstance(body, dict)
            opensearch_captured["body"] = cast(dict[str, object], body)
            return httpx.Response(201, json={"result": "created"})

        sink = OpenSearchReplaySink(
            url="https://search.example.test",
            index="docs",
            transport=httpx.MockTransport(handler),
        )
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        assert opensearch_captured["path"] == f"/docs/_doc/{identity_key}"
        body = cast(dict[str, object], opensearch_captured["body"])
        assert body["delivery_identity"] == identity_key
        payload_obj = body.get("payload")
        assert isinstance(payload_obj, dict)
        captured_payload = cast(dict[str, object], payload_obj)
    elif destination_name == "clickhouse":
        clickhouse_captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            row = json.loads(request.content.decode("utf-8").strip())
            assert isinstance(row, dict)
            clickhouse_captured["row"] = cast(dict[str, object], row)
            return httpx.Response(200, text="")

        sink = ClickHouseReplaySink(
            url="https://clickhouse.example.test",
            table="delivery_rows",
            transport=httpx.MockTransport(handler),
        )
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        row = cast(dict[str, object], clickhouse_captured["row"])
        assert row["identity_key"] == identity_key
        payload_json = row.get("payload_json")
        assert isinstance(payload_json, str)
        raw_payload = json.loads(payload_json)
        assert isinstance(raw_payload, dict)
        captured_payload = cast(dict[str, object], raw_payload)
    elif destination_name == "mongodb":
        fake_mongo = FakeMongoCollection(calls=[])
        sink = MongoDBReplaySink(
            database="zephyr",
            collection="delivery_records",
            collection_obj=fake_mongo,
        )
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        assert len(fake_mongo.calls) == 1
        assert fake_mongo.calls[0]["filter"] == {"_id": identity_key}
        replacement = cast(dict[str, object], fake_mongo.calls[0]["replacement"])
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

        sink = LokiReplaySink(
            url="https://logs.example.test",
            stream="delivery",
            transport=httpx.MockTransport(handler),
        )
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        body = cast(dict[str, object], loki_captured["body"])
        streams = cast(list[dict[str, object]], body["streams"])
        stream_obj = cast(dict[str, object], streams[0]["stream"])
        assert stream_obj["zephyr_delivery_identity"] == identity_key
        values = cast(list[list[str]], streams[0]["values"])
        payload_obj = json.loads(values[0][1])
        assert isinstance(payload_obj, dict)
        captured_payload = cast(dict[str, object], payload_obj)

    assert stats.total == 1
    assert stats.succeeded == 1
    assert stats.failed == 0
    return run_id, captured_payload


@pytest.mark.parametrize("destination_name", _BATCH_DESTINATIONS)
def test_second_round_destinations_share_success_summary_boundary(
    destination_name: DestinationName,
    tmp_path: Path,
) -> None:
    sha256 = "abc123"
    meta = _build_meta(run_id=f"{destination_name}-success")
    identity_key = _identity_key(sha256=sha256, run_id=meta.run_id)

    if destination_name == "s3":
        receipt = S3Destination(
            bucket="archive",
            region="us-east-1",
            access_key="ak",
            secret_key="sk",
            prefix="deliveries",
            client=FakeS3Client(calls=[]),
        )(out_root=tmp_path / "out", sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["identity_key"] == identity_key
        assert receipt.details["object_key"] == f"deliveries/{identity_key}.json"
    elif destination_name == "opensearch":

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/zephyr-docs/_doc/{identity_key}"
            return httpx.Response(201, json={"result": "created", "_version": 1})

        receipt = OpenSearchDestination(
            url="https://search.example.test",
            index="zephyr-docs",
            transport=httpx.MockTransport(handler),
        )(out_root=tmp_path / "out", sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["document_id"] == identity_key
        assert receipt.details["attempted_count"] == 1
        assert receipt.details["accepted_count"] == 1
        assert receipt.details["rejected_count"] == 0
    else:

        def handler(request: httpx.Request) -> httpx.Response:
            row = json.loads(request.content.decode("utf-8").strip())
            assert isinstance(row, dict)
            typed_row = cast(dict[str, object], row)
            assert typed_row["identity_key"] == identity_key
            return httpx.Response(200, text="")

        receipt = ClickHouseDestination(
            url="https://clickhouse.example.test",
            database="analytics",
            table="delivery_rows",
            transport=httpx.MockTransport(handler),
        )(out_root=tmp_path / "out", sha256=sha256, meta=meta, result=None)
        assert receipt.details is not None
        assert receipt.details["identity_key"] == identity_key
        assert receipt.details["row_count"] == 1

    assert receipt.ok is True
    assert receipt.failure_retryability == "not_failed"
    assert receipt.shared_failure_kind == "not_failed"
    assert receipt.shared_error_code == "not_failed"
    assert receipt.shared_summary == _shared_summary(
        delivery_outcome="delivered",
        failure_retryability="not_failed",
        failure_kind="not_failed",
        error_code="not_failed",
    )
    _assert_shared_boundary(receipt)


@pytest.mark.parametrize("destination_name", _BATCH_DESTINATIONS)
def test_second_round_destinations_share_failure_vocabulary_boundary(
    destination_name: DestinationName,
    tmp_path: Path,
) -> None:
    sha256 = "abc123"
    meta = _build_meta(run_id=f"{destination_name}-failure")
    identity_key = _identity_key(sha256=sha256, run_id=meta.run_id)

    if destination_name == "s3":
        receipt = S3Destination(
            bucket="archive",
            region="us-east-1",
            access_key="ak",
            secret_key="sk",
            prefix="deliveries",
            client=FakeS3Client(calls=[], exc=FakeS3ServiceError()),
        )(out_root=tmp_path / "out", sha256=sha256, meta=meta, result=None)
        expected_retryability = "retryable"
        expected_failure_kind = "server_error"
        assert receipt.details is not None
        assert receipt.details["identity_key"] == identity_key
        assert receipt.details["backend_error_code"] == "SlowDown"
    elif destination_name == "opensearch":

        def handler(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(
                429,
                json={"error": {"type": "rejected_execution_exception", "reason": "busy"}},
            )

        receipt = OpenSearchDestination(
            url="https://search.example.test",
            index="zephyr-docs",
            transport=httpx.MockTransport(handler),
        )(out_root=tmp_path / "out", sha256=sha256, meta=meta, result=None)
        expected_retryability = "retryable"
        expected_failure_kind = "rate_limited"
        assert receipt.details is not None
        assert receipt.details["document_id"] == identity_key
        assert receipt.details["attempted_count"] == 1
        assert receipt.details["accepted_count"] == 0
        assert receipt.details["rejected_count"] == 1
    else:

        def handler(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(400, text="DB::Exception: Type mismatch in VALUES section")

        receipt = ClickHouseDestination(
            url="https://clickhouse.example.test",
            table="delivery_rows",
            transport=httpx.MockTransport(handler),
        )(out_root=tmp_path / "out", sha256=sha256, meta=meta, result=None)
        expected_retryability = "non_retryable"
        expected_failure_kind = "constraint"
        assert receipt.details is not None
        assert receipt.details["identity_key"] == identity_key
        assert receipt.details["row_count"] == 1

    assert receipt.ok is False
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert receipt.failure_retryability == expected_retryability
    assert receipt.shared_failure_kind == expected_failure_kind
    assert receipt.shared_summary == _shared_summary(
        delivery_outcome="failed",
        failure_retryability=expected_retryability,
        failure_kind=expected_failure_kind,
        error_code="ZE-DELIVERY-FAILED",
    )
    _assert_shared_boundary(receipt)


@pytest.mark.parametrize("destination_name", _BATCH_DESTINATIONS)
def test_second_round_replay_sinks_preserve_identity_and_replay_provenance(
    destination_name: DestinationName,
    tmp_path: Path,
) -> None:
    sha256 = "abc123"
    run_id = f"{destination_name}-replay"
    identity_key = _identity_key(sha256=sha256, run_id=run_id)
    out_root = tmp_path / destination_name
    _write_replay_record(out_root=out_root, sha256=sha256, run_id=run_id)

    captured_payload: dict[str, object]
    sink: ReplaySink
    if destination_name == "s3":
        fake = FakeS3Client(calls=[])
        sink = S3ReplaySink(bucket="archive", prefix="replay", client=fake)
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        assert len(fake.calls) == 1
        assert fake.calls[0]["key"] == f"replay/{identity_key}.json"
        captured_payload = cast(dict[str, object], fake.calls[0]["payload"])
    elif destination_name == "opensearch":
        opensearch_captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            opensearch_captured["path"] = request.url.path
            body = json.loads(request.content.decode("utf-8"))
            assert isinstance(body, dict)
            opensearch_captured["body"] = cast(dict[str, object], body)
            return httpx.Response(201, json={"result": "created"})

        sink = OpenSearchReplaySink(
            url="https://search.example.test",
            index="docs",
            transport=httpx.MockTransport(handler),
        )
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        assert opensearch_captured["path"] == f"/docs/_doc/{identity_key}"
        body = cast(dict[str, object], opensearch_captured["body"])
        assert body["delivery_identity"] == identity_key
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

        sink = ClickHouseReplaySink(
            url="https://clickhouse.example.test",
            table="delivery_rows",
            transport=httpx.MockTransport(handler),
        )
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        row = cast(dict[str, object], clickhouse_captured["row"])
        assert row["identity_key"] == identity_key
        payload_json = row.get("payload_json")
        assert isinstance(payload_json, str)
        raw_payload = json.loads(payload_json)
        assert isinstance(raw_payload, dict)
        captured_payload = cast(dict[str, object], raw_payload)

    assert stats.total == 1
    assert stats.succeeded == 1
    assert stats.failed == 0
    assert captured_payload["sha256"] == sha256
    run_meta_obj = captured_payload.get("run_meta")
    assert isinstance(run_meta_obj, dict)
    typed_run_meta = cast(dict[str, object], run_meta_obj)
    provenance_obj = typed_run_meta.get("provenance")
    assert isinstance(provenance_obj, dict)
    provenance = cast(dict[str, object], provenance_obj)
    assert typed_run_meta["run_id"] == run_id
    assert provenance["delivery_origin"] == "replay"
    assert provenance["run_origin"] == "intake"
    assert provenance["execution_mode"] == "worker"
    assert provenance["checkpoint_identity_key"] == "cp-1"
    assert provenance["task_identity_key"] == "task-key-1"


@pytest.mark.parametrize("destination_name", _REMAINING_BATCH_DESTINATIONS)
def test_remaining_second_round_destinations_share_contract_boundary(
    destination_name: RemainingDestinationName,
    tmp_path: Path,
) -> None:
    sha256 = "abc123"
    meta = _build_meta(run_id=f"{destination_name}-success")
    identity_key = _identity_key(sha256=sha256, run_id=meta.run_id)

    if destination_name == "mongodb":
        fake = FakeMongoCollection(calls=[])
        success_receipt = MongoDBDestination(
            uri="mongodb://db.example.test",
            database="zephyr",
            collection="delivery_records",
            collection_obj=fake,
        )(out_root=tmp_path / "out", sha256=sha256, meta=meta, result=None)
        assert len(fake.calls) == 1
        assert fake.calls[0]["filter"] == {"_id": identity_key}
        assert success_receipt.details is not None
        assert success_receipt.details["document_id"] == identity_key
        assert success_receipt.details["attempted_count"] == 1
        assert success_receipt.details["accepted_count"] == 1
        assert success_receipt.details["rejected_count"] == 0

        failure_receipt = MongoDBDestination(
            uri="mongodb://db.example.test",
            database="zephyr",
            collection="delivery_records",
            collection_obj=FakeMongoCollection(
                calls=[],
                exc=DuplicateKeyError("E11000 duplicate key error"),
            ),
        )(
            out_root=tmp_path / "out",
            sha256=sha256,
            meta=_build_meta(run_id="mongodb-failure"),
            result=None,
        )
        expected_retryability = "non_retryable"
        expected_failure_kind = "constraint"
    else:

        def success_handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            stream = body["streams"][0]["stream"]
            assert stream["zephyr_stream"] == "delivery"
            assert stream["zephyr_delivery_identity"] == identity_key
            return httpx.Response(204, text="")

        success_receipt = LokiDestination(
            url="https://logs.example.test",
            stream="delivery",
            tenant_id="tenant-a",
            transport=httpx.MockTransport(success_handler),
        )(out_root=tmp_path / "out", sha256=sha256, meta=meta, result=None)
        assert success_receipt.details is not None
        assert success_receipt.details["stream"] == "delivery"
        assert success_receipt.details["tenant_id"] == "tenant-a"
        assert success_receipt.details["line_count"] == 1
        assert success_receipt.details["accepted_count"] == 1
        assert success_receipt.details["rejected_count"] == 0

        def failure_handler(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(429, text="ingester rate limit")

        failure_receipt = LokiDestination(
            url="https://logs.example.test",
            stream="delivery",
            transport=httpx.MockTransport(failure_handler),
        )(
            out_root=tmp_path / "out",
            sha256=sha256,
            meta=_build_meta(run_id="loki-failure"),
            result=None,
        )
        expected_retryability = "retryable"
        expected_failure_kind = "rate_limited"

    assert success_receipt.ok is True
    assert success_receipt.shared_summary == _shared_summary(
        delivery_outcome="delivered",
        failure_retryability="not_failed",
        failure_kind="not_failed",
        error_code="not_failed",
    )
    _assert_remaining_shared_boundary(success_receipt)

    assert failure_receipt.ok is False
    assert failure_receipt.failure_retryability == expected_retryability
    assert failure_receipt.shared_failure_kind == expected_failure_kind
    assert failure_receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert failure_receipt.shared_summary == _shared_summary(
        delivery_outcome="failed",
        failure_retryability=expected_retryability,
        failure_kind=expected_failure_kind,
        error_code="ZE-DELIVERY-FAILED",
    )
    _assert_remaining_shared_boundary(failure_receipt)


def test_full_second_round_destination_batch_shares_stable_failure_boundary(
    tmp_path: Path,
) -> None:
    observed_boundary: dict[FullSecondRoundDestinationName, tuple[str, str, str]] = {}

    for destination_name in _FULL_SECOND_ROUND_DESTINATIONS:
        receipt = _build_second_round_failure_receipt(
            destination_name=destination_name,
            tmp_path=tmp_path,
        )
        assert receipt.ok is False
        observed_boundary[destination_name] = (
            receipt.failure_retryability,
            receipt.shared_failure_kind,
            receipt.shared_error_code,
        )

    assert {boundary[0] for boundary in observed_boundary.values()} == {
        "retryable",
        "non_retryable",
    }
    assert {boundary[1] for boundary in observed_boundary.values()} == {
        "constraint",
        "rate_limited",
        "server_error",
    }
    assert {boundary[2] for boundary in observed_boundary.values()} == {"ZE-DELIVERY-FAILED"}
    assert set(observed_boundary.values()) == _FULL_SECOND_ROUND_FAILURE_BOUNDARY
    assert observed_boundary["s3"] == ("retryable", "server_error", "ZE-DELIVERY-FAILED")
    assert observed_boundary["opensearch"] == ("retryable", "rate_limited", "ZE-DELIVERY-FAILED")
    assert observed_boundary["clickhouse"] == (
        "non_retryable",
        "constraint",
        "ZE-DELIVERY-FAILED",
    )
    assert observed_boundary["mongodb"] == ("non_retryable", "constraint", "ZE-DELIVERY-FAILED")
    assert observed_boundary["loki"] == ("retryable", "rate_limited", "ZE-DELIVERY-FAILED")


def test_full_second_round_destination_batch_shares_stable_details_and_summary_boundary(
    tmp_path: Path,
) -> None:
    for destination_name in _FULL_SECOND_ROUND_DESTINATIONS:
        success_receipt = _build_second_round_success_receipt(
            destination_name=destination_name,
            tmp_path=tmp_path,
        )
        failure_receipt = _build_second_round_failure_receipt(
            destination_name=destination_name,
            tmp_path=tmp_path,
        )

        assert success_receipt.ok is True
        assert success_receipt.shared_summary == _shared_summary(
            delivery_outcome="delivered",
            failure_retryability="not_failed",
            failure_kind="not_failed",
            error_code="not_failed",
        )
        assert failure_receipt.ok is False
        assert set(failure_receipt.shared_summary) == _SHARED_SUMMARY_KEYS
        assert failure_receipt.shared_summary["delivery_outcome"] == "failed"
        assert failure_receipt.shared_summary["attempt_count"] == 1
        assert failure_receipt.shared_summary["payload_count"] == 1
        assert failure_receipt.shared_summary["error_code"] == "ZE-DELIVERY-FAILED"

        assert success_receipt.details is not None
        assert failure_receipt.details is not None
        if destination_name in _FULL_SECOND_ROUND_COUNT_DETAIL_DESTINATIONS:
            assert success_receipt.details["accepted_count"] == 1
            assert success_receipt.details["rejected_count"] == 0
            assert failure_receipt.details["accepted_count"] == 0
            assert failure_receipt.details["rejected_count"] == 1
            assert "accepted_count" not in success_receipt.shared_summary
            assert "rejected_count" not in failure_receipt.shared_summary
        else:
            assert "accepted_count" not in success_receipt.details
            assert "rejected_count" not in success_receipt.details
            assert "accepted_count" not in failure_receipt.details
            assert "rejected_count" not in failure_receipt.details


def test_full_second_round_batch_shares_stable_replay_idempotency_and_provenance_boundary(
    tmp_path: Path,
) -> None:
    for destination_name in _FULL_SECOND_ROUND_DESTINATIONS:
        run_id, captured_payload = _replay_second_round_destination_record(
            destination_name=destination_name,
            tmp_path=tmp_path,
        )

        assert captured_payload["sha256"] == "abc123"
        run_meta_obj = captured_payload.get("run_meta")
        assert isinstance(run_meta_obj, dict)
        typed_run_meta = cast(dict[str, object], run_meta_obj)
        provenance_obj = typed_run_meta.get("provenance")
        assert isinstance(provenance_obj, dict)
        provenance = cast(dict[str, object], provenance_obj)
        assert typed_run_meta["run_id"] == run_id
        assert provenance["delivery_origin"] == "replay"
        assert provenance["run_origin"] == "intake"
        assert provenance["execution_mode"] == "worker"
        assert provenance["checkpoint_identity_key"] == "cp-1"
        assert provenance["task_identity_key"] == "task-key-1"


@pytest.mark.parametrize("destination_name", _REMAINING_BATCH_DESTINATIONS)
def test_remaining_second_round_replay_sinks_preserve_identity_and_replay_provenance(
    destination_name: RemainingDestinationName,
    tmp_path: Path,
) -> None:
    sha256 = "abc123"
    run_id = f"{destination_name}-replay"
    identity_key = _identity_key(sha256=sha256, run_id=run_id)
    out_root = tmp_path / destination_name
    _write_replay_record(out_root=out_root, sha256=sha256, run_id=run_id)

    captured_payload: dict[str, object]
    sink: ReplaySink
    if destination_name == "mongodb":
        fake = FakeMongoCollection(calls=[])
        sink = MongoDBReplaySink(
            database="zephyr",
            collection="delivery_records",
            collection_obj=fake,
        )
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        assert len(fake.calls) == 1
        assert fake.calls[0]["filter"] == {"_id": identity_key}
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

        sink = LokiReplaySink(
            url="https://logs.example.test",
            stream="delivery",
            transport=httpx.MockTransport(handler),
        )
        stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)
        body = cast(dict[str, object], loki_captured["body"])
        streams = cast(list[dict[str, object]], body["streams"])
        stream_obj = cast(dict[str, object], streams[0]["stream"])
        assert stream_obj["zephyr_delivery_identity"] == identity_key
        values = cast(list[list[str]], streams[0]["values"])
        payload_obj = json.loads(values[0][1])
        assert isinstance(payload_obj, dict)
        captured_payload = cast(dict[str, object], payload_obj)

    assert stats.total == 1
    assert stats.succeeded == 1
    assert stats.failed == 0
    assert captured_payload["sha256"] == sha256
    run_meta_obj = captured_payload.get("run_meta")
    assert isinstance(run_meta_obj, dict)
    typed_run_meta = cast(dict[str, object], run_meta_obj)
    provenance_obj = typed_run_meta.get("provenance")
    assert isinstance(provenance_obj, dict)
    provenance = cast(dict[str, object], provenance_obj)
    assert typed_run_meta["run_id"] == run_id
    assert provenance["delivery_origin"] == "replay"
    assert provenance["run_origin"] == "intake"
    assert provenance["execution_mode"] == "worker"
    assert provenance["checkpoint_identity_key"] == "cp-1"
    assert provenance["task_identity_key"] == "task-key-1"
