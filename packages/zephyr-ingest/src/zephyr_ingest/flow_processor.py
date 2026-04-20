from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, cast

from it_stream import normalize_it_input_identity_sha
from it_stream.service import process_file as process_it_file
from uns_stream.backends.base import PartitionBackend
from uns_stream.sources import normalize_uns_input_identity_sha
from uns_stream.sources import process_file as process_uns_file
from zephyr_core import DocumentRef, PartitionResult, PartitionStrategy

FlowKind = Literal["uns", "it"]
DEFAULT_FLOW_KIND: FlowKind = "uns"
SharedFieldSemantics = Literal["applied", "orchestration_context_only"]


@dataclass(frozen=True, slots=True)
class FlowProcessorSharedFieldSemantics:
    flow_kind: FlowKind
    unique_element_ids: SharedFieldSemantics
    run_id: SharedFieldSemantics
    pipeline_version: SharedFieldSemantics
    note: str


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

    def shared_field_semantics(self) -> FlowProcessorSharedFieldSemantics:
        return FlowProcessorSharedFieldSemantics(
            flow_kind=DEFAULT_FLOW_KIND,
            unique_element_ids="applied",
            run_id="applied",
            pipeline_version="applied",
            note=(
                "legacy partition_fn adapter receives shared fields directly; exact semantics "
                "belong to the supplied callable."
            ),
        )

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

    def shared_field_semantics(self) -> FlowProcessorSharedFieldSemantics:
        return describe_flow_processor_shared_field_semantics(flow_kind="uns")

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
        return process_uns_file(
            filename=doc.uri,
            strategy=strategy,
            unique_element_ids=unique_element_ids,
            backend=cast("PartitionBackend | None", self.backend),
            run_id=run_id,
            pipeline_version=pipeline_version,
            sha256=sha256,
            size_bytes=doc.size_bytes,
        )


@dataclass(frozen=True, slots=True)
class ItFlowProcessor:
    def shared_field_semantics(self) -> FlowProcessorSharedFieldSemantics:
        return describe_flow_processor_shared_field_semantics(flow_kind="it")

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
        _acknowledge_it_orchestration_context(
            unique_element_ids=unique_element_ids,
            run_id=run_id,
            pipeline_version=pipeline_version,
        )
        return process_it_file(
            filename=doc.uri,
            strategy=strategy,
            sha256=sha256,
            size_bytes=doc.size_bytes,
        )


def build_default_flow_processor(*, backend: object | None = None) -> FlowProcessor:
    """Build the default processor used when no explicit processor is supplied."""

    return build_processor_for_flow_kind(flow_kind=DEFAULT_FLOW_KIND, backend=backend)


def normalize_flow_input_identity_sha(
    *,
    flow_kind: FlowKind | str,
    filename: str,
    default_sha: str,
) -> str:
    if flow_kind == "it":
        return normalize_it_input_identity_sha(filename=filename, default_sha=default_sha)
    if flow_kind == "uns":
        return normalize_uns_input_identity_sha(filename=filename, default_sha=default_sha)
    return default_sha


def _acknowledge_it_orchestration_context(
    *,
    unique_element_ids: bool,
    run_id: str | None,
    pipeline_version: str | None,
) -> None:
    # These are shared orchestration context fields. Current it-stream processing owns
    # record/checkpoint artifact identity locally, so these inputs are intentionally
    # not forwarded as artifact-shaping controls.
    del unique_element_ids, run_id, pipeline_version


def describe_flow_processor_shared_field_semantics(
    *,
    flow_kind: FlowKind | str,
) -> FlowProcessorSharedFieldSemantics:
    if flow_kind == "uns":
        return FlowProcessorSharedFieldSemantics(
            flow_kind="uns",
            unique_element_ids="applied",
            run_id="applied",
            pipeline_version="applied",
            note=(
                "uns-stream applies shared partition context directly through the document "
                "partition path."
            ),
        )
    if flow_kind == "it":
        return FlowProcessorSharedFieldSemantics(
            flow_kind="it",
            unique_element_ids="orchestration_context_only",
            run_id="orchestration_context_only",
            pipeline_version="orchestration_context_only",
            note=(
                "it-stream keeps record/checkpoint artifact semantics flow-local; shared fields "
                "remain orchestration context and do not shape it artifacts directly."
            ),
        )
    raise ValueError(f"Unsupported flow kind: {flow_kind}")


def build_processor_for_flow_kind(
    *,
    flow_kind: FlowKind | str = DEFAULT_FLOW_KIND,
    backend: object | None = None,
) -> FlowProcessor:
    if flow_kind == "uns":
        return UnsFlowProcessor(backend=backend)
    if flow_kind == "it":
        return ItFlowProcessor()
    raise ValueError(f"Unsupported flow kind: {flow_kind}")
