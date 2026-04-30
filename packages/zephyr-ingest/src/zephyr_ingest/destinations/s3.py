from __future__ import annotations

import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol, cast

from zephyr_core import ErrorCode, PartitionResult, RunMetaV1
from zephyr_core.contracts.v2.delivery_payload import DeliveryPayloadV1
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
)
from zephyr_ingest.destinations.base import DeliveryReceipt


class S3ObjectWriterProtocol(Protocol):
    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> Mapping[str, object]: ...


def make_s3_object_writer(
    *,
    region: str,
    access_key: str,
    secret_key: str,
    endpoint_url: str | None = None,
    session_token: str | None = None,
) -> S3ObjectWriterProtocol:
    boto3 = importlib.import_module("boto3")
    session_module = getattr(boto3, "session")
    session_cls = getattr(session_module, "Session")
    session = session_cls(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region,
    )
    client = session.client("s3", endpoint_url=endpoint_url, region_name=region)
    return cast(S3ObjectWriterProtocol, client)


def _normalize_prefix(*, prefix: str) -> str:
    stripped = prefix.strip().strip("/")
    if not stripped:
        return ""
    return f"{stripped}/"


def build_s3_delivery_object_key(*, prefix: str, idempotency_key: str) -> str:
    return f"{_normalize_prefix(prefix=prefix)}{idempotency_key}.json"


def _status_failure_kind(*, status_code: int) -> str:
    if status_code == 429:
        return "rate_limited"
    if 500 <= status_code <= 599:
        return "server_error"
    return "client_error"


def _status_retryable(*, status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _exception_failure_kind(*, exc: Exception) -> str:
    text = f"{type(exc).__name__} {exc}".lower()
    if "timeout" in text:
        return "timeout"
    if "connection" in text or "connect" in text or "endpoint" in text:
        return "connection"
    return "operational"


def _exception_retryable(*, exc: Exception) -> bool:
    return _exception_failure_kind(exc=exc) in {"timeout", "connection"}


def send_delivery_payload_v1_to_s3(
    *,
    client: S3ObjectWriterProtocol,
    bucket: str,
    prefix: str,
    payload: DeliveryPayloadV1,
    idempotency_key: str,
    write_mode: str,
) -> DeliveryReceipt:
    object_key = build_s3_delivery_object_key(prefix=prefix, idempotency_key=idempotency_key)
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    details: dict[str, object] = {
        "bucket": bucket,
        "object_key": object_key,
        "identity_key": idempotency_key,
        "write_mode": write_mode,
    }

    try:
        response = client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=body,
            ContentType="application/json",
        )
    except Exception as exc:
        details["exc_type"] = type(exc).__name__
        details["exc"] = str(exc)
        details["retryable"] = _exception_retryable(exc=exc)
        details["failure_kind"] = _exception_failure_kind(exc=exc)
        details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
        response_obj = getattr(exc, "response", None)
        if isinstance(response_obj, dict):
            response = cast(dict[str, object], response_obj)
            metadata_obj = response.get("ResponseMetadata")
            if isinstance(metadata_obj, dict):
                metadata = cast(dict[str, object], metadata_obj)
                status_code = metadata.get("HTTPStatusCode")
                if isinstance(status_code, int):
                    details["status_code"] = status_code
                    details["retryable"] = _status_retryable(status_code=status_code)
                    details["failure_kind"] = _status_failure_kind(status_code=status_code)
            error_obj = response.get("Error")
            if isinstance(error_obj, dict):
                error = cast(dict[str, object], error_obj)
                code = error.get("Code")
                message = error.get("Message")
                if isinstance(code, str):
                    details["backend_error_code"] = code
                if isinstance(message, str):
                    details["backend_error_message"] = message
        return DeliveryReceipt(destination="s3", ok=False, details=details)

    metadata_obj = response.get("ResponseMetadata")
    if isinstance(metadata_obj, dict):
        metadata = cast(dict[str, object], metadata_obj)
        status_code = metadata.get("HTTPStatusCode")
        if isinstance(status_code, int):
            details["status_code"] = status_code
    etag = response.get("ETag")
    if isinstance(etag, str):
        details["etag"] = etag
    version_id = response.get("VersionId")
    if isinstance(version_id, str):
        details["version_id"] = version_id
    details["retryable"] = False
    return DeliveryReceipt(destination="s3", ok=True, details=details)


@dataclass(frozen=True, slots=True)
class S3Destination:
    bucket: str
    region: str
    access_key: str
    secret_key: str
    endpoint_url: str | None = None
    session_token: str | None = None
    prefix: str = ""
    write_mode: str = "overwrite"
    client: S3ObjectWriterProtocol | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @property
    def name(self) -> str:
        return "s3"

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        payload = build_delivery_payload_v1(
            out_root=out_root,
            sha256=sha256,
            meta=meta,
            result=result,
        )
        idempotency_key = normalize_delivery_idempotency_key(
            identity=DeliveryIdentityV1(sha256=sha256, run_id=meta.run_id)
        )
        client = self.client
        if client is None:
            client = make_s3_object_writer(
                region=self.region,
                access_key=self.access_key,
                secret_key=self.secret_key,
                endpoint_url=self.endpoint_url,
                session_token=self.session_token,
            )
        return send_delivery_payload_v1_to_s3(
            client=client,
            bucket=self.bucket,
            prefix=self.prefix,
            payload=payload,
            idempotency_key=idempotency_key,
            write_mode=self.write_mode,
        )
