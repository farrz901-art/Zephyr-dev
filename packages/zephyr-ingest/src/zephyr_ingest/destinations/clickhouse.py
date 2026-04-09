from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

from zephyr_core import ErrorCode, PartitionResult, RunMetaV1
from zephyr_core.contracts.v2.delivery_payload import DeliveryPayloadV1
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
)
from zephyr_ingest.destinations.base import DeliveryReceipt

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(*, value: str, field_name: str) -> None:
    if not value or not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"clickhouse destination {field_name} must be a non-empty SQL identifier")


def _qualified_table(*, database: str | None, table: str) -> str:
    _validate_identifier(value=table, field_name="table")
    if database is None:
        return table
    _validate_identifier(value=database, field_name="database")
    return f"{database}.{table}"


def _should_retry_status(*, status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _status_failure_kind(*, status_code: int, response_text: str) -> str:
    if status_code == 429:
        return "rate_limited"
    if 500 <= status_code <= 599:
        return "server_error"
    lowered = response_text.lower()
    if any(
        marker in lowered
        for marker in (
            "type mismatch",
            "cannot parse",
            "unknown table",
            "unknown column",
            "unknown identifier",
        )
    ):
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


def send_delivery_payload_v1_to_clickhouse(
    *,
    url: str,
    table: str,
    payload: DeliveryPayloadV1,
    idempotency_key: str,
    timeout_s: float,
    database: str | None = None,
    username: str | None = None,
    password: str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> DeliveryReceipt:
    qualified_table = _qualified_table(database=database, table=table)
    run_meta = payload.get("run_meta", {})
    run_id_obj = run_meta.get("run_id")
    run_id = run_id_obj if isinstance(run_id_obj, str) else ""
    payload_json = json.dumps(payload, ensure_ascii=False)
    row = {
        "identity_key": idempotency_key,
        "sha256": payload["sha256"],
        "run_id": run_id,
        "schema_version": payload["schema_version"],
        "payload_json": payload_json,
        "delivered_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    query = f"INSERT INTO {qualified_table} FORMAT JSONEachRow"
    details: dict[str, object] = {
        "table": table,
        "database": database,
        "write_mode": "append",
        "row_count": 1,
        "identity_key": idempotency_key,
        "query_mode": "json_each_row",
    }
    auth: tuple[str, str] | None = None
    if username is not None and password is not None:
        auth = (username, password)
    client = httpx.Client(timeout=timeout_s, auth=auth, transport=transport)
    try:
        response = client.post(url, params={"query": query}, content=json.dumps(row) + "\n")
    except httpx.HTTPError as exc:
        details["exc_type"] = type(exc).__name__
        details["exc"] = str(exc)
        details["retryable"] = _http_error_retryable(exc=exc)
        details["failure_kind"] = _http_error_failure_kind(exc=exc)
        details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
        return DeliveryReceipt(destination="clickhouse", ok=False, details=details)
    finally:
        client.close()

    details["status_code"] = response.status_code
    if 200 <= response.status_code < 300:
        details["retryable"] = False
        return DeliveryReceipt(destination="clickhouse", ok=True, details=details)

    details["response_text"] = response.text[:500]
    details["retryable"] = _should_retry_status(status_code=response.status_code)
    details["failure_kind"] = _status_failure_kind(
        status_code=response.status_code,
        response_text=response.text,
    )
    details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
    return DeliveryReceipt(destination="clickhouse", ok=False, details=details)


@dataclass(frozen=True, slots=True)
class ClickHouseDestination:
    url: str
    table: str
    database: str | None = None
    timeout_s: float = 10.0
    username: str | None = None
    password: str | None = None
    transport: httpx.BaseTransport | None = None
    max_inflight: int | None = None
    rate_limit: float | None = None

    @property
    def name(self) -> str:
        return "clickhouse"

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
        return send_delivery_payload_v1_to_clickhouse(
            url=self.url,
            table=self.table,
            database=self.database,
            payload=payload,
            idempotency_key=idempotency_key,
            timeout_s=self.timeout_s,
            username=self.username,
            password=self.password,
            transport=self.transport,
        )
