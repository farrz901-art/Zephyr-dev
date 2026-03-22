from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from zephyr_core import PartitionResult, RunMetaV1
from zephyr_ingest.destinations.base import DeliveryReceipt


def _default_headers() -> dict[str, str]:
    return {"content-type": "application/json"}


@dataclass(frozen=True, slots=True)
class WebhookDestination:
    url: str
    timeout_s: float = 10.0
    headers: dict[str, str] = field(default_factory=_default_headers)

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

        try:
            client = httpx.Client(timeout=self.timeout_s, transport=self.transport)
            try:
                resp = client.post(
                    self.url, headers=headers, content=json.dumps(payload, ensure_ascii=False)
                )
            finally:
                client.close()

            ok = 200 <= resp.status_code < 300
            return DeliveryReceipt(
                destination=self.name,
                ok=ok,
                details={
                    "status_code": resp.status_code,
                    "response_text": resp.text[:500],
                },
            )
        except httpx.HTTPError as e:
            return DeliveryReceipt(
                destination=self.name,
                ok=False,
                details={"exc_type": type(e).__name__, "exc": str(e)},
            )
