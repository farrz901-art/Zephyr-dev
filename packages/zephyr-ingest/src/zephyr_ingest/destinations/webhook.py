from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from zephyr_core import PartitionResult, RunMetaV1
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


def _backoff_ms(cfg: DeliveryRetryConfig, attempt: int) -> int:
    # attempt is 1-based
    ms = cfg.base_backoff_ms * (2 ** (attempt - 1))
    return min(cfg.max_backoff_ms, ms)


@dataclass(frozen=True, slots=True)
class WebhookDestination:
    url: str
    timeout_s: float = 10.0
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
        payload: dict[str, Any] = {
            "sha256": sha256,
            "run_meta": meta.to_dict(),
            # 给下游一个“拉取本地产物”的提示（下游可忽略）
            "artifacts": {
                "out_dir": str((out_root / sha256).resolve()),
                "run_meta_path": str((out_root / sha256 / "run_meta.json").resolve()),
                "elements_path": str((out_root / sha256 / "elements.json").resolve()),
                "normalized_path": str((out_root / sha256 / "normalized.txt").resolve()),
            },
        }

        headers = dict(self.headers)
        if self.send_idempotency_key:
            headers.setdefault("Idempotency-Key", f"{sha256}:{meta.run_id}")

        attempts = 0
        last_status: int | None = None
        last_text: str | None = None
        last_exc: str | None = None
        last_exc_type: str | None = None

        client = httpx.Client(timeout=self.timeout_s, transport=self.transport)
        try:
            while True:
                attempts += 1
                try:
                    resp = client.post(
                        self.url,
                        headers=headers,
                        content=json.dumps(payload, ensure_ascii=False),
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
                        ms = _backoff_ms(self.retry, attempts)
                        if ms > 0:
                            time.sleep(ms / 1000.0)
                        continue

                    return DeliveryReceipt(
                        destination=self.name,
                        ok=False,
                        details={
                            "attempts": attempts,
                            "status_code": resp.status_code,
                            "response_text": last_text,
                            "retryable": retryable,
                        },
                    )

                except httpx.HTTPError as e:
                    last_exc = str(e)
                    last_exc_type = type(e).__name__

                    retryable = self.retry.enabled and attempts < self.retry.max_attempts
                    if self.retry.enabled and attempts < self.retry.max_attempts:
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
                            "retryable": False,
                        },
                    )
        finally:
            client.close()
