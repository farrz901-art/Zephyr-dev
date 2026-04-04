from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, cast

from uns_stream.backends.base import PartitionBackend
from uns_stream.partition.auto import partition as auto_partition
from zephyr_core import DocumentRef, PartitionResult, PartitionStrategy

FlowKind = Literal["uns", "it"]
DEFAULT_FLOW_KIND: FlowKind = "uns"


class PartitionFn(Protocol):
    def __call__(
        self,
        *,
        filename: str,
        strategy: PartitionStrategy = PartitionStrategy.AUTO,
        unique_element_ids: bool = True,
        backend: object | None = None,
        run_id: str | None = None,
        pipeline_version: str | None = None,
        sha256: str | None = None,
        size_bytes: int | None = None,
    ) -> PartitionResult: ...


class FlowProcessor(Protocol):
    def process(
        self,
        *,
        doc: DocumentRef,
        strategy: PartitionStrategy,
        unique_element_ids: bool,
        run_id: str | None,
        pipeline_version: str | None,
        sha256: str,
    ) -> PartitionResult: ...


@dataclass(frozen=True, slots=True)
class CallableFlowProcessor:
    """Compatibility adapter for legacy ``partition_fn=...`` call sites."""

    partition_fn: PartitionFn
    backend: object | None = None

    def process(
        self,
        *,
        doc: DocumentRef,
        strategy: PartitionStrategy,
        unique_element_ids: bool,
        run_id: str | None,
        pipeline_version: str | None,
        sha256: str,
    ) -> PartitionResult:
        return self.partition_fn(
            filename=doc.uri,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=self.backend,
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=doc.size_bytes,
        )


@dataclass(frozen=True, slots=True)
class UnsFlowProcessor:
    backend: object | None = None

    def process(
        self,
        *,
        doc: DocumentRef,
        strategy: PartitionStrategy,
        unique_element_ids: bool,
        run_id: str | None,
        pipeline_version: str | None,
        sha256: str,
    ) -> PartitionResult:
        return auto_partition(
            filename=doc.uri,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=cast("PartitionBackend | None", self.backend),
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=doc.size_bytes,
        )


def build_default_flow_processor(*, backend: object | None = None) -> FlowProcessor:
    """Build the default processor used when no explicit processor is supplied."""

    return build_processor_for_flow_kind(flow_kind=DEFAULT_FLOW_KIND, backend=backend)


def build_processor_for_flow_kind(
    *,
    flow_kind: FlowKind | str = DEFAULT_FLOW_KIND,
    backend: object | None = None,
) -> FlowProcessor:
    if flow_kind == "uns":
        return UnsFlowProcessor(backend=backend)
    if flow_kind == "it":
        raise NotImplementedError("Flow kind 'it' is not implemented")
    raise ValueError(f"Unsupported flow kind: {flow_kind}")
