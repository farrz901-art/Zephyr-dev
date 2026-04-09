from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import httpx

from zephyr_ingest.destinations.mongodb import MongoReplaceResultProtocol
from zephyr_ingest.replay_delivery import (
    ClickHouseReplaySink,
    LokiReplaySink,
    MongoDBReplaySink,
    OpenSearchReplaySink,
    S3ReplaySink,
    replay_delivery_dlq,
)


@dataclass
class FakeS3Client:
    calls: list[dict[str, Any]]

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "Bucket": Bucket,
                "Key": Key,
                "Body": Body,
                "ContentType": ContentType,
            }
        )
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


@dataclass(frozen=True)
class FakeMongoReplaceResult:
    acknowledged: bool = True
    matched_count: int = 0
    modified_count: int = 0
    upserted_id: object | None = "abc:r1"


@dataclass
class FakeMongoCollection:
    calls: list[dict[str, Any]]

    def replace_one(
        self,
        filter: Mapping[str, object],
        replacement: Mapping[str, object],
        *,
        upsert: bool,
    ) -> MongoReplaceResultProtocol:
        self.calls.append(
            {
                "filter": dict(filter),
                "replacement": dict(replacement),
                "upsert": upsert,
            }
        )
        return FakeMongoReplaceResult()


def _write_dlq_record(*, out_root: Path, name: str) -> None:
    dlq_dir = out_root / "_dlq" / "delivery"
    dlq_dir.mkdir(parents=True, exist_ok=True)
    (dlq_dir / name).write_text(
        json.dumps({"sha256": "abc", "run_id": "r1", "run_meta": {"schema_version": 3}}),
        encoding="utf-8",
    )


def test_replay_to_s3_sink(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    _write_dlq_record(out_root=out_root, name="one.json")

    fake = FakeS3Client(calls=[])
    sink = S3ReplaySink(bucket="archive", prefix="replay", client=fake)
    stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)

    assert stats.total == 1
    assert stats.succeeded == 1
    assert len(fake.calls) == 1
    assert fake.calls[0]["Key"] == "replay/abc:r1.json"


def test_replay_to_opensearch_sink(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    _write_dlq_record(out_root=out_root, name="one.json")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/docs/_doc/abc:r1"
        return httpx.Response(201, json={"result": "created"})

    sink = OpenSearchReplaySink(
        url="https://search.example.test",
        index="docs",
        transport=httpx.MockTransport(handler),
    )
    stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)

    assert stats.total == 1
    assert stats.succeeded == 1


def test_replay_to_clickhouse_sink(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    _write_dlq_record(out_root=out_root, name="one.json")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["query"] == "INSERT INTO delivery_rows FORMAT JSONEachRow"
        return httpx.Response(200, text="")

    sink = ClickHouseReplaySink(
        url="https://clickhouse.example.test",
        table="delivery_rows",
        transport=httpx.MockTransport(handler),
    )
    stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)

    assert stats.total == 1
    assert stats.succeeded == 1


def test_replay_to_mongodb_sink(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    _write_dlq_record(out_root=out_root, name="one.json")

    fake = FakeMongoCollection(calls=[])
    sink = MongoDBReplaySink(
        database="zephyr",
        collection="delivery_records",
        collection_obj=fake,
    )
    stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)

    assert stats.total == 1
    assert stats.succeeded == 1
    assert len(fake.calls) == 1
    assert fake.calls[0]["filter"] == {"_id": "abc:r1"}


def test_replay_to_loki_sink(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    _write_dlq_record(out_root=out_root, name="one.json")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/loki/api/v1/push"
        payload = json.loads(request.content.decode("utf-8"))
        stream = payload["streams"][0]["stream"]
        assert stream["zephyr_delivery_identity"] == "abc:r1"
        assert stream["zephyr_stream"] == "delivery"
        return httpx.Response(204, text="")

    sink = LokiReplaySink(
        url="https://logs.example.test",
        stream="delivery",
        transport=httpx.MockTransport(handler),
    )
    stats = replay_delivery_dlq(out_root=out_root, sink=sink, dry_run=False, move_done=False)

    assert stats.total == 1
    assert stats.succeeded == 1
