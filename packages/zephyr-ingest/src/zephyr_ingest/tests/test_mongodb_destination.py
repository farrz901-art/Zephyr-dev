from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, cast

from zephyr_core import RunContext, RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_ingest.destinations.mongodb import (
    MongoDBDestination,
    MongoReplaceResultProtocol,
)


@dataclass(frozen=True)
class FakeReplaceResult:
    acknowledged: bool
    matched_count: int
    modified_count: int
    upserted_id: object | None


@dataclass
class FakeMongoCollection:
    calls: list[dict[str, object]]
    exc: Exception | None = None
    result: FakeReplaceResult = FakeReplaceResult(
        acknowledged=True,
        matched_count=0,
        modified_count=0,
        upserted_id="abc123:r1",
    )

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
        return self.result


class DuplicateKeyError(Exception):
    code = 11000


class ServerSelectionTimeoutError(Exception):
    pass


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


def test_mongodb_destination_replace_upserts_one_delivery_payload(tmp_path: Path) -> None:
    meta = _build_meta()
    fake = FakeMongoCollection(calls=[])
    dest = MongoDBDestination(
        uri="mongodb://db.example.test",
        database="zephyr",
        collection="delivery_records",
        collection_obj=fake,
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=meta, result=None)

    assert receipt.ok is True
    assert receipt.destination == "mongodb"
    assert receipt.failure_retryability == "not_failed"
    assert len(fake.calls) == 1

    call = fake.calls[0]
    assert call["filter"] == {"_id": f"abc123:{meta.run_id}"}
    assert call["upsert"] is True
    replacement = cast(dict[str, object], call["replacement"])
    assert replacement["_id"] == f"abc123:{meta.run_id}"
    assert replacement["delivery_identity"] == f"abc123:{meta.run_id}"
    payload = cast(dict[str, object], replacement["payload"])
    assert payload["sha256"] == "abc123"
    run_meta = cast(dict[str, object], payload["run_meta"])
    assert run_meta["run_id"] == meta.run_id

    assert receipt.details is not None
    assert receipt.details["database"] == "zephyr"
    assert receipt.details["collection"] == "delivery_records"
    assert receipt.details["document_id"] == f"abc123:{meta.run_id}"
    assert receipt.details["write_mode"] == "replace_upsert"
    assert receipt.details["attempted_count"] == 1
    assert receipt.details["accepted_count"] == 1
    assert receipt.details["rejected_count"] == 0
    assert receipt.details["operation"] == "upsert_inserted"


def test_mongodb_destination_maps_duplicate_key_to_constraint(tmp_path: Path) -> None:
    fake = FakeMongoCollection(calls=[], exc=DuplicateKeyError("E11000 duplicate key error"))
    dest = MongoDBDestination(
        uri="mongodb://db.example.test",
        database="zephyr",
        collection="delivery_records",
        collection_obj=fake,
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=_build_meta(), result=None)

    assert receipt.ok is False
    assert receipt.failure_retryability == "non_retryable"
    assert receipt.shared_failure_kind == "constraint"
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert receipt.details is not None
    assert receipt.details["accepted_count"] == 0
    assert receipt.details["rejected_count"] == 1
    assert receipt.details["backend_error_code"] == 11000
    assert receipt.details["retryable"] is False


def test_mongodb_destination_maps_server_selection_timeout_to_retryable_connection(
    tmp_path: Path,
) -> None:
    fake = FakeMongoCollection(
        calls=[],
        exc=ServerSelectionTimeoutError("server selection timeout after 1000 ms"),
    )
    dest = MongoDBDestination(
        uri="mongodb://db.example.test",
        database="zephyr",
        collection="delivery_records",
        collection_obj=fake,
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=_build_meta(), result=None)

    assert receipt.ok is False
    assert receipt.failure_retryability == "retryable"
    assert receipt.shared_failure_kind == "timeout"
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert receipt.details is not None
    assert receipt.details["retryable"] is True
    assert receipt.details["attempted_count"] == 1
    assert receipt.details["accepted_count"] == 0
    assert receipt.details["rejected_count"] == 1
