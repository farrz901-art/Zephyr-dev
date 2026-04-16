from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, cast

from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    RunMetaV1,
    ZephyrElement,
)
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_ingest.delivery_idempotency import normalize_weaviate_delivery_object_id
from zephyr_ingest.destinations.weaviate import WeaviateDestination


@dataclass
class FakeBatch:
    number_errors: int = 0
    added: list[dict[str, Any]] = field(default_factory=lambda: cast(list[dict[str, Any]], []))

    def add_object(self, properties: dict[str, Any], uuid: str | None = None) -> str:
        self.added.append({"properties": properties, "uuid": uuid})
        return uuid or "generated"


@dataclass
class FakeBatchManager:
    batch: FakeBatch
    failed_objects: list[Any] = field(default_factory=lambda: cast(list[dict[str, Any]], []))

    @contextmanager
    def dynamic(self) -> Iterator[FakeBatch]:
        yield self.batch


@dataclass
class FakeCollection:
    batch: FakeBatchManager


def _make_meta() -> RunMetaV1:
    ctx = RunContext.new()
    return RunMetaV1(
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        timestamp_utc=ctx.timestamp_utc,
        schema_version=ctx.run_meta_schema_version,
        outcome=RunOutcome.SUCCESS,
        document=None,
        engine=None,
        error=None,
        warnings=[],
    )


def _make_result(*, sha256: str) -> PartitionResult:
    doc = DocumentMetadata(
        filename="x.txt",
        mime_type="text/plain",
        sha256=sha256,
        size_bytes=3,
        created_at_utc="2026-03-27T00:00:00Z",
    )
    engine = EngineInfo(
        name="unstructured",
        backend="local",
        version="0.21.5",
        strategy=PartitionStrategy.AUTO,
    )
    elements = [ZephyrElement(element_id="e1", type="Text", text="hi", metadata={})]
    return PartitionResult(
        document=doc,
        engine=engine,
        elements=elements,
        normalized_text="hi",
        warnings=[],
    )


def test_weaviate_destination_skips_when_no_result(tmp_path: Path) -> None:
    fake_batch = FakeBatch()
    coll = FakeCollection(batch=FakeBatchManager(batch=fake_batch))
    dest = WeaviateDestination(collection_name="ZephyrDoc", collection=coll)

    receipt = dest(out_root=tmp_path, sha256="abc", meta=_make_meta(), result=None)
    assert receipt.ok is True
    assert receipt.details is not None
    assert receipt.details["skipped"] is True
    assert fake_batch.added == []


def test_weaviate_destination_inserts_one_object(tmp_path: Path) -> None:
    sha = "abc123"
    fake_batch = FakeBatch()
    coll = FakeCollection(batch=FakeBatchManager(batch=fake_batch))
    dest = WeaviateDestination(collection_name="ZephyrDoc", collection=coll)

    receipt = dest(
        out_root=tmp_path, sha256=sha, meta=_make_meta(), result=_make_result(sha256=sha)
    )
    assert receipt.ok is True
    assert receipt.details is not None
    assert receipt.details["retryable"] is False
    assert receipt.details["batch_status"] == "ok"
    assert receipt.details["object_identity"] == normalize_weaviate_delivery_object_id(sha256=sha)
    assert len(fake_batch.added) == 1

    call = fake_batch.added[0]
    assert call["uuid"] == normalize_weaviate_delivery_object_id(sha256=sha)
    assert call["properties"]["sha256"] == sha
    assert call["properties"]["normalized_text"] == "hi"


def test_weaviate_destination_reports_failed_objects(tmp_path: Path) -> None:
    fake_batch = FakeBatch(number_errors=0)
    mgr = FakeBatchManager(batch=fake_batch, failed_objects=[{"status": 422, "err": "x"}])
    coll = FakeCollection(batch=mgr)
    dest = WeaviateDestination(collection_name="ZephyrDoc", collection=coll)

    receipt = dest(
        out_root=tmp_path, sha256="abc", meta=_make_meta(), result=_make_result(sha256="abc")
    )
    assert receipt.ok is False
    assert receipt.details is not None
    assert receipt.details["retryable"] is False
    assert receipt.details["batch_status"] == "partial"
    assert receipt.details["failure_kind"] == "constraint"
    assert receipt.details["max_batch_errors"] == 0


def test_weaviate_destination_tolerates_batch_errors_within_threshold(tmp_path: Path) -> None:
    fake_batch = FakeBatch(number_errors=1)
    coll = FakeCollection(batch=FakeBatchManager(batch=fake_batch, failed_objects=[]))
    dest = WeaviateDestination(collection_name="ZephyrDoc", collection=coll, max_batch_errors=1)

    receipt = dest(
        out_root=tmp_path, sha256="abc", meta=_make_meta(), result=_make_result(sha256="abc")
    )

    assert receipt.ok is True
    assert receipt.details is not None
    assert receipt.details["retryable"] is False
    assert receipt.details["batch_errors"] == 1
    assert receipt.details["max_batch_errors"] == 1
    assert receipt.details["batch_status"] == "partial"


def test_weaviate_destination_fails_when_batch_errors_exceed_threshold(tmp_path: Path) -> None:
    fake_batch = FakeBatch(number_errors=2)
    coll = FakeCollection(batch=FakeBatchManager(batch=fake_batch, failed_objects=[]))
    dest = WeaviateDestination(collection_name="ZephyrDoc", collection=coll, max_batch_errors=1)

    receipt = dest(
        out_root=tmp_path, sha256="abc", meta=_make_meta(), result=_make_result(sha256="abc")
    )

    assert receipt.ok is False
    assert receipt.details is not None
    assert receipt.details["retryable"] is True
    assert receipt.details["batch_errors"] == 2
    assert receipt.details["max_batch_errors"] == 1
    assert receipt.details["failure_kind"] == "operational"
    assert receipt.details["batch_status"] == "partial"
