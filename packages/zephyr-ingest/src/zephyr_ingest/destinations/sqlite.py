from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from zephyr_core import ErrorCode, PartitionResult, RunMetaV1
from zephyr_core.contracts.v2.delivery_payload import DeliveryPayloadV1
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
)
from zephyr_ingest.destinations.base import DeliveryReceipt

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_table_name(*, table_name: str) -> None:
    if not table_name or not _IDENTIFIER_RE.fullmatch(table_name):
        raise ValueError("sqlite destination table_name must be a non-empty SQL identifier")


def _failure_kind_for_sqlite_exception(exc: sqlite3.Error) -> str:
    if isinstance(exc, sqlite3.IntegrityError):
        return "constraint"
    if isinstance(exc, sqlite3.OperationalError):
        message = str(exc).lower()
        if "locked" in message or "busy" in message:
            return "locked"
        return "operational"
    return "database_error"


def _is_retryable_sqlite_exception(exc: sqlite3.Error) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return "locked" in message or "busy" in message


def send_delivery_payload_v1_to_sqlite(
    *,
    db_path: Path,
    table_name: str,
    payload: DeliveryPayloadV1,
    idempotency_key: str,
    timeout_s: float,
) -> DeliveryReceipt:
    _validate_table_name(table_name=table_name)

    resolved_db_path = db_path.expanduser().resolve()
    payload_json = json.dumps(payload, ensure_ascii=False)
    run_meta = payload.get("run_meta", {})
    run_id = run_meta.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        run_id = ""

    details: dict[str, object] = {
        "db_path": str(resolved_db_path),
        "table": table_name,
        "identity_key": idempotency_key,
    }

    try:
        resolved_db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(resolved_db_path, timeout=timeout_s)
        try:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} ("
                "identity_key TEXT PRIMARY KEY, "
                "sha256 TEXT NOT NULL, "
                "run_id TEXT NOT NULL, "
                "schema_version INTEGER NOT NULL, "
                "payload_json TEXT NOT NULL, "
                "delivered_at_utc TEXT NOT NULL)"
            )
            existing = conn.execute(
                f"SELECT 1 FROM {table_name} WHERE identity_key = ?",
                (idempotency_key,),
            ).fetchone()
            conn.execute(
                (
                    f"INSERT INTO {table_name} "
                    "("
                    "identity_key, sha256, run_id, schema_version, payload_json, delivered_at_utc"
                    ") "
                    "VALUES (?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(identity_key) DO UPDATE SET "
                    "sha256 = excluded.sha256, "
                    "run_id = excluded.run_id, "
                    "schema_version = excluded.schema_version, "
                    "payload_json = excluded.payload_json, "
                    "delivered_at_utc = excluded.delivered_at_utc"
                ),
                (
                    idempotency_key,
                    payload["sha256"],
                    run_id,
                    payload["schema_version"],
                    payload_json,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        details["exc_type"] = type(exc).__name__
        details["exc"] = str(exc)
        details["retryable"] = _is_retryable_sqlite_exception(exc)
        details["failure_kind"] = _failure_kind_for_sqlite_exception(exc)
        details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
        return DeliveryReceipt(destination="sqlite", ok=False, details=details)
    except OSError as exc:
        details["exc_type"] = type(exc).__name__
        details["exc"] = str(exc)
        details["retryable"] = False
        details["failure_kind"] = "operational"
        details["error_code"] = str(ErrorCode.DELIVERY_FAILED)
        return DeliveryReceipt(destination="sqlite", ok=False, details=details)

    details["operation"] = "upsert_replaced" if existing is not None else "upsert_inserted"
    details["row_count"] = 1
    details["retryable"] = False
    return DeliveryReceipt(destination="sqlite", ok=True, details=details)


@dataclass(frozen=True, slots=True)
class SqliteDestination:
    db_path: Path
    table_name: str = "zephyr_delivery_records"
    timeout_s: float = 5.0

    @property
    def name(self) -> str:
        return "sqlite"

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
        return send_delivery_payload_v1_to_sqlite(
            db_path=self.db_path,
            table_name=self.table_name,
            payload=payload,
            idempotency_key=idempotency_key,
            timeout_s=self.timeout_s,
        )
