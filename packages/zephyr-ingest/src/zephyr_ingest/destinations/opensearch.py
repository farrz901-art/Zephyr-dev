from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import httpx

from zephyr_core import ErrorCode, PartitionResult, RunMetaV1
from zephyr_core.contracts.v2.delivery_payload import DeliveryPayloadV1
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
)
from zephyr_ingest.destinations.base import DeliveryReceipt


def _should_retry_status(*, status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _status_failure_kind(*, status_code: int) -> str:
    if status_code == 429:
        return "rate_limited"
    if 500 <= status_code <= 599:
        return "server_error"
    return "client_error"


def _http_error_failure_kind(*, exc: httpx.HTTPError) -> str:
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.NetworkError):
        return "connection"
    if isinstance(exc, httpx.RemoteProtocolError):
        return "protocol"
    return "operational"


def _http_error_retryable(*, exc: httpx.HTTPError) -> bool:
    return isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError))


def _build_document(*, payload: DeliveryPayloadV1, idempotency_key: str) -> dict[str, object]:
    return {
        "delivery_identity": idempotency_key,
        "payload": payload,
    }


def send_delivery_payload_v1_to_opensearch(
    *,
    url: str,
    index: str,
    payload: DeliveryPayloadV1,
    idempotency_key: str,
    timeout_s: float,
    verify_tls: bool,
    username: str | None = None,
    password: str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> DeliveryReceipt:
    document = _build_document(payload=payload, idempotency_key=idempotency_key)
    details: dict[str, object] = {
        "index": index,
        "document_id": idempotency_key,
        "write_mode": "index",
        "attempted_count": 1,
    }
    auth: tuple[str, str] | None = None
    if username is not None and password is not None:
        auth = (username, password)
    client = httpx.Client(timeout=timeout_s, verify=verify_tls, auth=auth, transport=transport)
    try:
        response = client.put(
            f"{url.rstrip('/')}/{index}/_doc/{idempotency_key}",
            json=document,
            headers={"content-type": "application/json"},
        )
    except httpx.HTTPError as exc:
        details["exc_type"] = type(exc).__name__
        details["exc"] = str(exc)
        details["retryable"] = _http_error_retryable(exc=exc)
        details["failure_kind"] = _http_error_failure_kind(exc=exc)
        details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
        return DeliveryReceipt(destination="opensearch", ok=False, details=details)
    finally:
        client.close()

    details["status_code"] = response.status_code
    if 200 <= response.status_code < 300:
        try:
            body = response.json()
        except json.JSONDecodeError:
            body = None
        if isinstance(body, dict):
            typed_body = cast(dict[str, object], body)
            result = typed_body.get("result")
            version = typed_body.get("_version")
            shard_obj = typed_body.get("_shards")
            if isinstance(result, str):
                details["result"] = result
            if isinstance(version, int):
                details["version"] = version
            if isinstance(shard_obj, dict):
                details["shards"] = shard_obj
        details["accepted_count"] = 1
        details["rejected_count"] = 0
        details["retryable"] = False
        return DeliveryReceipt(destination="opensearch", ok=True, details=details)

    details["accepted_count"] = 0
    details["rejected_count"] = 1
    details["retryable"] = _should_retry_status(status_code=response.status_code)
    details["failure_kind"] = _status_failure_kind(status_code=response.status_code)
    details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
    try:
        body = response.json()
    except json.JSONDecodeError:
        details["response_text"] = response.text[:500]
    else:
        if isinstance(body, dict):
            typed_body = cast(dict[str, object], body)
            error_obj = typed_body.get("error")
            if isinstance(error_obj, dict):
                error = cast(dict[str, object], error_obj)
                error_type = error.get("type")
                error_reason = error.get("reason")
                if isinstance(error_type, str):
                    details["backend_error_type"] = error_type
                if isinstance(error_reason, str):
                    details["backend_error_reason"] = error_reason
            else:
                details["response_body"] = body
    return DeliveryReceipt(destination="opensearch", ok=False, details=details)


@dataclass(frozen=True, slots=True)
class OpenSearchDestination:
    url: str
    index: str
    timeout_s: float = 10.0
    verify_tls: bool = True
    username: str | None = None
    password: str | None = None
    transport: httpx.BaseTransport | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @property
    def name(self) -> str:
        return "opensearch"

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        del result
        payload = build_delivery_payload_v1(out_root=out_root, sha256=sha256, meta=meta)
        idempotency_key = normalize_delivery_idempotency_key(
            identity=DeliveryIdentityV1(sha256=sha256, run_id=meta.run_id)
        )
        return send_delivery_payload_v1_to_opensearch(
            url=self.url,
            index=self.index,
            payload=payload,
            idempotency_key=idempotency_key,
            timeout_s=self.timeout_s,
            verify_tls=self.verify_tls,
            username=self.username,
            password=self.password,
            transport=self.transport,
        )
