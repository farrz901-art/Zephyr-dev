from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import httpx

from zephyr_core import ErrorCode, PartitionResult, RunMetaV1
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1
from zephyr_ingest.delivery_idempotency import (
    DeliveryIdentityV1,
    normalize_delivery_idempotency_key,
)
from zephyr_ingest.destinations.base import DeliveryReceipt


def _default_headers() -> dict[str, str]:
    return {"content-type": "application/json"}


@dataclass(frozen=True, slots=True)
class DeliveryRetryConfig:
    enabled: bool = True
    max_attempts: int = 3
    base_backoff_ms: int = 200
    max_backoff_ms: int = 5_000


def _default_retry() -> DeliveryRetryConfig:
    return DeliveryRetryConfig()


def _should_retry_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _status_failure_kind(status_code: int) -> str:
    if status_code == 429:
        return "rate_limited"
    if 500 <= status_code <= 599:
        return "server_error"
    return "client_error"


def _parse_retry_after_ms(*, headers: httpx.Headers) -> int | None:
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        retry_after_s = int(raw.strip())
    except ValueError:
        return None
    if retry_after_s < 0:
        return None
    return retry_after_s * 1000


def _rate_limit_delay_ms(*, rate_limit: float | None) -> int | None:
    if rate_limit is None or rate_limit <= 0:
        return None
    return max(1, math.ceil(1000.0 / rate_limit))


def _backoff_ms(cfg: DeliveryRetryConfig, attempt: int) -> int:
    # attempt is 1-based
    ms = cfg.base_backoff_ms * (2 ** (attempt - 1))
    return min(cfg.max_backoff_ms, ms)


def _retry_delay_ms(
    *,
    cfg: DeliveryRetryConfig,
    attempt: int,
    response: httpx.Response | None,
    rate_limit: float | None,
) -> int:
    delay_ms = _backoff_ms(cfg, attempt)
    if response is None or response.status_code != 429:
        return delay_ms

    retry_after_ms = _parse_retry_after_ms(headers=response.headers)
    if retry_after_ms is not None:
        delay_ms = max(delay_ms, retry_after_ms)

    rate_limit_ms = _rate_limit_delay_ms(rate_limit=rate_limit)
    if rate_limit_ms is not None:
        delay_ms = max(delay_ms, rate_limit_ms)

    return delay_ms


def _is_retryable_http_error(exc: httpx.HTTPError) -> bool:
    return isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError))


def _http_error_failure_kind(exc: httpx.HTTPError) -> str:
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.NetworkError):
        return "connection"
    if isinstance(exc, httpx.RemoteProtocolError):
        return "protocol"
    return "http_error"


@dataclass(frozen=True, slots=True)
class WebhookDestination:
    url: str
    timeout_s: float = 10.0
    max_inflight: int | None = None
    rate_limit: float | None = None
    headers: dict[str, str] = field(default_factory=_default_headers)

    retry: DeliveryRetryConfig = field(default_factory=_default_retry)

    # 允许测试注入 MockTransport（不需要 monkeypatch 全局 httpx.Client）
    transport: httpx.BaseTransport | None = None

    send_idempotency_key: bool = True
    # name: str = "webhook"
    name: str = field(default="webhook", init=False)

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        # payload: dict[str, Any] = {
        #     "sha256": sha256,
        #     "run_meta": meta.to_dict(),
        #     # 给下游一个“拉取本地产物”的提示（下游可忽略）
        #     "artifacts": {
        #         "out_dir": str((out_root / sha256).resolve()),
        #         "run_meta_path": str((out_root / sha256 / "run_meta.json").resolve()),
        #         "elements_path": str((out_root / sha256 / "elements.json").resolve()),
        #         "normalized_path": str((out_root / sha256 / "normalized.txt").resolve()),
        #     },
        # }
        payload = build_delivery_payload_v1(out_root=out_root, sha256=sha256, meta=meta)

        # 2. 构造幂等键
        idempotency_key = normalize_delivery_idempotency_key(
            identity=DeliveryIdentityV1(sha256=sha256, run_id=meta.run_id)
        )

        # 3. 委派给通用的 post_payload 方法
        return self.post_payload(payload=payload, idempotency_key=idempotency_key)

    def post_payload(
        self,
        *,
        payload: Mapping[str, Any],
        idempotency_key: str | None = None,
    ) -> DeliveryReceipt:
        """支持自定义 Payload 的投递方法，复用重试机制。"""
        headers = dict(self.headers)
        if self.send_idempotency_key and idempotency_key:
            headers.setdefault("Idempotency-Key", idempotency_key)

        content = json.dumps(payload, ensure_ascii=False)
        return self._post_with_retry(headers=headers, content=content)

    def _post_with_retry(
        self,
        *,
        headers: dict[str, str],
        content: str,
    ) -> DeliveryReceipt:

        attempts = 0
        last_status: int | None = None
        last_text: str | None = None
        last_exc: str | None = None
        last_exc_type: str | None = None

        client = httpx.Client(
            timeout=self.timeout_s,
            transport=self.transport,
            trust_env=False,
        )
        try:
            while True:
                attempts += 1
                try:
                    resp = client.post(
                        self.url,
                        headers=headers,
                        content=content,
                    )
                    last_status = resp.status_code
                    last_text = resp.text[:500]

                    ok = 200 <= resp.status_code < 300
                    if ok:
                        return DeliveryReceipt(
                            destination=self.name,
                            ok=True,
                            details={
                                "attempts": attempts,
                                "status_code": last_status,
                                "retryable": False,
                            },
                        )

                    retryable = _should_retry_status(resp.status_code)
                    if self.retry.enabled and retryable and attempts < self.retry.max_attempts:
                        ms = _retry_delay_ms(
                            cfg=self.retry,
                            attempt=attempts,
                            response=resp,
                            rate_limit=self.rate_limit,
                        )
                        if ms > 0:
                            time.sleep(ms / 1000.0)
                        continue

                    retry_after_ms = _parse_retry_after_ms(headers=resp.headers)
                    return DeliveryReceipt(
                        destination=self.name,
                        ok=False,
                        details={
                            "attempts": attempts,
                            "status_code": resp.status_code,
                            "response_text": last_text,
                            "retryable": retryable,
                            "failure_kind": _status_failure_kind(resp.status_code),
                            "retry_after_s": (
                                None if retry_after_ms is None else retry_after_ms / 1000.0
                            ),
                            "error_code": str(ErrorCode.DELIVERY_HTTP_FAILED),
                        },
                    )

                except httpx.HTTPError as e:
                    last_exc = str(e)
                    last_exc_type = type(e).__name__
                    retryable = _is_retryable_http_error(e)

                    if self.retry.enabled and retryable and attempts < self.retry.max_attempts:
                        ms = _backoff_ms(self.retry, attempts)
                        if ms > 0:
                            time.sleep(ms / 1000.0)
                        continue

                    return DeliveryReceipt(
                        destination=self.name,
                        ok=False,
                        details={
                            "attempts": attempts,
                            "exc_type": last_exc_type,
                            "exc": last_exc,
                            "retryable": retryable,
                            "failure_kind": _http_error_failure_kind(e),
                            "error_code": str(ErrorCode.DELIVERY_HTTP_FAILED),
                        },
                    )
        finally:
            client.close()
