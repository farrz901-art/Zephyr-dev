from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class EngineInfo:
    name: str  # e.g. "unstructured"
    backend: str  # e.g. "local"
    version: str  # e.g. unstructured.__version__
    strategy: str  # e.g. "fast"/"hi_res"/...


@dataclass(frozen=True, slots=True)
class DocumentMetadata:
    filename: str
    mime_type: str | None
    sha256: str
    size_bytes: int
    created_at_utc: str  # ISO string to keep it simple


@dataclass(frozen=True, slots=True)
class ZephyrElement:
    element_id: str
    type: str
    text: str
    metadata: dict[str, Any]  # keep as dict to remain future-proof


@dataclass(frozen=True, slots=True)
class PartitionResult:
    document: DocumentMetadata
    engine: EngineInfo
    elements: list[ZephyrElement]
    normalized_text: str
    warnings: list[str]
