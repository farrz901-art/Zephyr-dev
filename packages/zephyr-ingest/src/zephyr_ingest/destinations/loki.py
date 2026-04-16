from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx

from zephyr_core import ErrorCode, PartitionResult, RunMetaV1
from zephyr_core.contracts.v2.delivery_payload import DeliveryPayloadV1
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
)
from zephyr_ingest.destinations.base import DeliveryReceipt


def _push_url(*, url: str) -> str:
    parts = urlsplit(url)
    if parts.path.rstrip("/") == "/loki/api/v1/push":
        return url
    if parts.path in ("", "/"):
        return urlunsplit(
            (parts.scheme, parts.netloc, "/loki/api/v1/push", parts.query, parts.fragment)
        )
    return url


def _timestamp_ns(*, payload: DeliveryPayloadV1) -> str:
    run_meta = payload.get("run_meta", {})
    timestamp_obj = run_meta.get("timestamp_utc")
    if isinstance(timestamp_obj, str):
        normalized = timestamp_obj.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return str(int(parsed.timestamp() * 1_000_000_000))
        except ValueError:
            pass
    return str(int(datetime.now(timezone.utc).timestamp() * 1_000_000_000))


def _stream_labels(
    *,
    stream: str,
    payload: DeliveryPayloadV1,
    idempotency_key: str,
) -> dict[str, str]:
    run_meta = payload.get("run_meta", {})
    run_id_obj = run_meta.get("run_id")
    labels = {
        "app": "zephyr",
        "zephyr_stream": stream,
        "zephyr_delivery_identity": idempotency_key,
        "zephyr_sha256": payload["sha256"],
    }
    if isinstance(run_id_obj, str):
        labels["zephyr_run_id"] = run_id_obj
    return labels


def _status_retryable(*, status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _status_failure_kind(*, status_code: int) -> str:
    if status_code == 429:
        return "rate_limited"
    if 500 <= status_code <= 599:
        return "server_error"
    if status_code in {400, 413, 422}:
        return "constraint"
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


def send_delivery_payload_v1_to_loki(
    *,
    url: str,
    stream: str,
    payload: DeliveryPayloadV1,
    idempotency_key: str,
    timeout_s: float,
    tenant_id: str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> DeliveryReceipt:
    stream_labels = _stream_labels(stream=stream, payload=payload, idempotency_key=idempotency_key)
    push_body = {
        "streams": [
            {
                "stream": stream_labels,
                "values": [
                    [_timestamp_ns(payload=payload), json.dumps(payload, ensure_ascii=False)]
                ],
            }
        ]
    }
    details: dict[str, object] = {
        "stream": stream,
        "tenant_id": tenant_id,
        "write_mode": "append",
        "attempted_count": 1,
        "line_count": 1,
        "accepted_count": 0,
        "rejected_count": 0,
        "label_keys": sorted(stream_labels.keys()),
    }
    headers = {"content-type": "application/json"}
    if tenant_id is not None:
        headers["X-Scope-OrgID"] = tenant_id
    client = httpx.Client(timeout=timeout_s, transport=transport, trust_env=False)
    try:
        response = client.post(_push_url(url=url), headers=headers, json=push_body)
    except httpx.HTTPError as exc:
        details["exc_type"] = type(exc).__name__
        details["exc"] = str(exc)
        details["retryable"] = _http_error_retryable(exc=exc)
        details["failure_kind"] = _http_error_failure_kind(exc=exc)
        details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
        details["rejected_count"] = 1
        return DeliveryReceipt(destination="loki", ok=False, details=details)
    finally:
        client.close()

    details["status_code"] = response.status_code
    if 200 <= response.status_code < 300:
        details["retryable"] = False
        details["accepted_count"] = 1
        return DeliveryReceipt(destination="loki", ok=True, details=details)

    details["response_text"] = response.text[:500]
    details["retryable"] = _status_retryable(status_code=response.status_code)
    details["failure_kind"] = _status_failure_kind(status_code=response.status_code)
    details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
    details["rejected_count"] = 1
    return DeliveryReceipt(destination="loki", ok=False, details=details)


@dataclass(frozen=True, slots=True)
class LokiDestination:
    url: str
    stream: str
    timeout_s: float = 10.0
    tenant_id: str | None = None
    transport: httpx.BaseTransport | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @property
    def name(self) -> str:
        return "loki"

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
        return send_delivery_payload_v1_to_loki(
            url=self.url,
            stream=self.stream,
            payload=payload,
            idempotency_key=idempotency_key,
            timeout_s=self.timeout_s,
            tenant_id=self.tenant_id,
            transport=self.transport,
        )
