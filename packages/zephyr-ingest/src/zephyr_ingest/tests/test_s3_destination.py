from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from zephyr_core import RunContext, RunMetaV1
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_ingest.destinations.s3 import S3Destination


@dataclass
class FakeS3Client:
    calls: list[dict[str, Any]]
    exc: Exception | None = None

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> dict[str, object]:
        if self.exc is not None:
            raise self.exc
        self.calls.append(
            {
                "Bucket": Bucket,
                "Key": Key,
                "Body": Body,
                "ContentType": ContentType,
            }
        )
        return {
            "ETag": '"etag-1"',
            "VersionId": "v1",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }


class FakeS3Error(Exception):
    def __init__(self) -> None:
        super().__init__("service unavailable")
        self.response = {
            "ResponseMetadata": {"HTTPStatusCode": 503},
            "Error": {"Code": "SlowDown", "Message": "service unavailable"},
        }


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


def test_s3_destination_writes_shared_payload_to_one_object(tmp_path: Path) -> None:
    fake = FakeS3Client(calls=[])
    dest = S3Destination(
        bucket="zephyr-bucket",
        region="us-east-1",
        access_key="ak",
        secret_key="sk",
        prefix="deliveries",
        client=fake,
    )
    meta = _build_meta()

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=meta, result=None)

    assert receipt.ok is True
    assert receipt.destination == "s3"
    assert receipt.failure_retryability == "not_failed"
    assert receipt.details is not None
    assert receipt.details["bucket"] == "zephyr-bucket"
    assert receipt.details["write_mode"] == "overwrite"
    assert receipt.details["object_key"] == f"deliveries/abc123:{meta.run_id}.json"
    assert receipt.details["etag"] == '"etag-1"'
    assert receipt.details["version_id"] == "v1"
    assert len(fake.calls) == 1

    payload = json.loads(fake.calls[0]["Body"].decode("utf-8"))
    assert payload["schema_version"] == 1
    assert payload["sha256"] == "abc123"
    assert payload["run_meta"]["run_id"] == meta.run_id


def test_s3_destination_maps_service_errors_into_shared_failure_semantics(tmp_path: Path) -> None:
    fake = FakeS3Client(calls=[], exc=FakeS3Error())
    dest = S3Destination(
        bucket="zephyr-bucket",
        region="us-east-1",
        access_key="ak",
        secret_key="sk",
        client=fake,
    )

    receipt = dest(out_root=tmp_path / "out", sha256="abc123", meta=_build_meta(), result=None)

    assert receipt.ok is False
    assert receipt.failure_retryability == "retryable"
    assert receipt.shared_failure_kind == "server_error"
    assert receipt.shared_error_code == "ZE-DELIVERY-FAILED"
    assert receipt.details is not None
    assert receipt.details["backend_error_code"] == "SlowDown"
    assert receipt.details["retryable"] is True
