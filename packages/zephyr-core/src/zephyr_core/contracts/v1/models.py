from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from zephyr_core.contracts.v1.enums import PartitionStrategy


@dataclass(frozen=True, slots=True)
class EngineInfo:
    """
    哪个引擎、什么后端、什么版本、用的什么策略。
    例：
      name="unstructured"
      backend="local" / "http"
      version="0.16.23"
      strategy=PartitionStrategy.FAST
    """

    name: str
    backend: str
    version: str
    strategy: PartitionStrategy


@dataclass(frozen=True, slots=True)
class DocumentMetadata:
    filename: str
    mime_type: str | None
    sha256: str
    size_bytes: int
    created_at_utc: str  # ISO 8601 string, e.g. "2026-03-12T12:34:56Z"


@dataclass(frozen=True, slots=True)
class ZephyrElement:
    element_id: str
    type: str
    text: str
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PartitionResult:
    document: DocumentMetadata
    engine: EngineInfo
    elements: list[ZephyrElement]
    normalized_text: str
    # warnings: list[str] = field(default_factory=list)
    # 关键修复：使用 lambda 或 list[str] 让 pyright 正确推断类型
    warnings: list[str] = field(default_factory=lambda: [])
