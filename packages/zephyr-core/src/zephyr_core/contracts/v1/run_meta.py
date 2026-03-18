from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from zephyr_core.contracts.v1.models import DocumentMetadata
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class EngineMetaV1:
    name: str
    backend: str
    version: str
    strategy: str


@dataclass(frozen=True, slots=True)
class MetricsV1:
    duration_ms: int | None = None
    elements_count: int | None = None
    normalized_text_len: int | None = None


@dataclass(frozen=True, slots=True)
class ErrorInfoV1:
    code: str
    message: str
    details: dict[str, Any] | None = None


def _empty_warnings() -> list[str]:
    return []


@dataclass(frozen=True, slots=True)
class RunMetaV1:
    run_id: str
    pipeline_version: str
    timestamp_utc: str
    schema_version: int = RUN_META_SCHEMA_VERSION

    document: DocumentMetadata | None = None
    engine: EngineMetaV1 | None = None
    metrics: MetricsV1 = field(default_factory=MetricsV1)
    # warnings: list[str] = field(default_factory=list)
    # warnings: list[str] = field(default_factory=lambda: [])
    warnings: list[str] = field(default_factory=_empty_warnings)
    error: ErrorInfoV1 | None = None

    def to_dict(self) -> dict[str, Any]:
        # 手写 dict，避免 Enum / dataclass 嵌套导致 JSON 序列化意外
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "pipeline_version": self.pipeline_version,
            "timestamp_utc": self.timestamp_utc,
            "document": None
            if self.document is None
            else {
                "filename": self.document.filename,
                "mime_type": self.document.mime_type,
                "sha256": self.document.sha256,
                "size_bytes": self.document.size_bytes,
                "created_at_utc": self.document.created_at_utc,
            },
            "engine": None
            if self.engine is None
            else {
                "name": self.engine.name,
                "backend": self.engine.backend,
                "version": self.engine.version,
                "strategy": self.engine.strategy,
            },
            "metrics": {
                "duration_ms": self.metrics.duration_ms,
                "elements_count": self.metrics.elements_count,
                "normalized_text_len": self.metrics.normalized_text_len,
            },
            "warnings": list(self.warnings),
            "error": None
            if self.error is None
            else {
                "code": self.error.code,
                "message": self.error.message,
                "details": self.error.details,
            },
        }
