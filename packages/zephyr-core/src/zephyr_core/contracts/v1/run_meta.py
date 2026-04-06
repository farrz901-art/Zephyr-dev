from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, NotRequired, TypedDict

from zephyr_core.contracts.v1.enums import RunOutcome
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
    attempts: int | None = None


@dataclass(frozen=True, slots=True)
class ErrorInfoV1:
    code: str
    message: str
    details: dict[str, Any] | None = None


RunOriginV1 = Literal["intake", "resume", "redrive", "requeue"]
DeliveryOriginV1 = Literal["primary", "replay"]
ExecutionModeV1 = Literal["batch", "worker"]


class RunProvenanceV1Dict(TypedDict):
    run_origin: NotRequired[RunOriginV1]
    delivery_origin: NotRequired[DeliveryOriginV1]
    execution_mode: NotRequired[ExecutionModeV1]
    task_id: NotRequired[str]
    checkpoint_identity_key: NotRequired[str]
    task_identity_key: NotRequired[str]


@dataclass(frozen=True, slots=True)
class RunProvenanceV1:
    run_origin: RunOriginV1 | None = None
    delivery_origin: DeliveryOriginV1 | None = None
    execution_mode: ExecutionModeV1 | None = None
    task_id: str | None = None
    checkpoint_identity_key: str | None = None
    task_identity_key: str | None = None

    def to_dict(self) -> RunProvenanceV1Dict:
        payload: RunProvenanceV1Dict = {}
        if self.run_origin is not None:
            payload["run_origin"] = self.run_origin
        if self.delivery_origin is not None:
            payload["delivery_origin"] = self.delivery_origin
        if self.execution_mode is not None:
            payload["execution_mode"] = self.execution_mode
        if self.task_id is not None:
            payload["task_id"] = self.task_id
        if self.checkpoint_identity_key is not None:
            payload["checkpoint_identity_key"] = self.checkpoint_identity_key
        if self.task_identity_key is not None:
            payload["task_identity_key"] = self.task_identity_key
        return payload


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

    outcome: RunOutcome | None = None
    provenance: RunProvenanceV1 | None = None

    def to_dict(self) -> dict[str, Any]:
        # 手写 dict，避免 Enum / dataclass 嵌套导致 JSON 序列化意外
        return {
            "schema_version": self.schema_version,
            "outcome": None if self.outcome is None else str(self.outcome),
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
                "attempts": self.metrics.attempts,
            },
            "warnings": list(self.warnings),
            "error": None
            if self.error is None
            else {
                "code": self.error.code,
                "message": self.error.message,
                "details": self.error.details,
            },
            "provenance": None if self.provenance is None else self.provenance.to_dict(),
        }
