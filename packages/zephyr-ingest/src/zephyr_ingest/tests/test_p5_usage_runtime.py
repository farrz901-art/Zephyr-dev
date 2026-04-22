from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from zephyr_core import (
    DocumentMetadata,
    DocumentRef,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    RunMetaV1,
    ZephyrElement,
)
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.run_meta import (
    ErrorInfoV1,
    MetricsV1,
    RunProvenanceV1,
)
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.runner import RunnerConfig, run_documents
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskKind,
    TaskV1,
)
from zephyr_ingest.usage_record import (
    USAGE_RECORD_FILENAME,
    build_batch_usage_record_v1,
    build_usage_record_v1,
    write_batch_usage_record_v1,
    write_usage_record_v1,
)


def _doc(tmp_path: Path, *, source: str = "http_document_v1") -> DocumentRef:
    path = tmp_path / "source.txt"
    path.write_text("hello", encoding="utf-8")
    return DocumentRef(
        uri=str(path),
        source=source,
        discovered_at_utc="2026-04-22T00:00:00Z",
        filename="source.txt",
        extension=".txt",
        size_bytes=path.stat().st_size,
    )


def _task(
    tmp_path: Path,
    *,
    kind: TaskKind = "uns",
    source: str = "http_document_v1",
) -> TaskV1:
    doc = _doc(tmp_path, source=source)
    task_kind: TaskKind = "uns" if kind == "uns" else "it"
    return TaskV1(
        task_id=f"task-{task_kind}",
        kind=task_kind,
        inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(doc)),
        execution=TaskExecutionV1(strategy=PartitionStrategy.AUTO, unique_element_ids=True),
        identity=TaskIdentityV1(pipeline_version="p-usage", sha256=f"sha-{task_kind}"),
    )


def _meta(
    *,
    outcome: RunOutcome = RunOutcome.SUCCESS,
    run_origin: str = "intake",
    delivery_origin: str = "primary",
    error: ErrorInfoV1 | None = None,
) -> RunMetaV1:
    run_origin_value = "intake"
    if run_origin in {"resume", "redrive", "requeue"}:
        run_origin_value = run_origin
    delivery_origin_value = "replay" if delivery_origin == "replay" else "primary"
    return RunMetaV1(
        run_id="r-usage",
        pipeline_version="p-usage",
        timestamp_utc="2026-04-22T00:00:00Z",
        outcome=outcome,
        metrics=MetricsV1(duration_ms=12, elements_count=1, normalized_text_len=5, attempts=1),
        error=error,
        provenance=RunProvenanceV1(
            run_origin=cast(Any, run_origin_value),
            delivery_origin=cast(Any, delivery_origin_value),
            execution_mode="batch",
            task_id="task-uns",
            task_identity_key='{"kind":"uns"}',
            checkpoint_identity_key="checkpoint-1",
        ),
    )


def _partition_ok(**kwargs: Any) -> PartitionResult:
    return PartitionResult(
        document=DocumentMetadata(
            filename="source.txt",
            mime_type="text/plain",
            sha256="sha",
            size_bytes=5,
            created_at_utc="2026-04-22T00:00:00Z",
        ),
        engine=EngineInfo(
            name="unstructured",
            backend="local",
            version="0.0.0",
            strategy=PartitionStrategy.AUTO,
        ),
        elements=[ZephyrElement(element_id="1", type="Text", text="hello", metadata={})],
        normalized_text="hello",
        warnings=[],
    )


class OkDest:
    name = "filesystem"

    def __call__(
        self,
        *,
        out_root: Path,
        sha256: str,
        meta: RunMetaV1,
        result: PartitionResult | None = None,
    ) -> DeliveryReceipt:
        out_dir = out_root / sha256
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "run_meta.json").write_text(
            json.dumps(meta.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return DeliveryReceipt(destination=self.name, ok=True, details={"out_dir": str(out_dir)})


@pytest.mark.auth_contract
def test_usage_record_runtime_shape_keeps_uns_base_not_naive_pro(tmp_path: Path) -> None:
    record = build_usage_record_v1(
        task=_task(tmp_path, kind="uns", source="http_document_v1"),
        meta=_meta(),
        receipt=DeliveryReceipt(destination="filesystem", ok=True),
    )

    assert record["record_kind"] == "raw_technical_usage_fact_v1"
    assert record["non_billing"] is True
    assert record["source"]["source_contract_id"] == "http_document_v1"
    assert record["source"]["source_contract_status"] == "linked"
    assert record["raw_unit"]["primary_raw_unit"] == "document"
    assert "page" in record["raw_unit"]["not_primary_units"]
    assert record["capability_domains_used"] == {
        "base": True,
        "pro": False,
        "extra": False,
        "evidence": [
            "task.kind=uns",
            "current retained source/destination surface=base",
            "source_contract_id=http_document_v1",
        ],
    }
    for prohibited in ("billing", "pricing", "license entitlement", "quota decision"):
        assert prohibited in record["not_claimed"]


@pytest.mark.auth_contract
def test_usage_record_runtime_shape_keeps_it_raw_unit_bounded(tmp_path: Path) -> None:
    record = build_usage_record_v1(
        task=_task(tmp_path, kind="it", source="kafka_partition_offset_v1"),
        meta=_meta(),
        receipt=DeliveryReceipt(destination="filesystem", ok=True),
    )

    assert record["source"]["source_contract_id"] == "kafka_partition_offset_v1"
    assert record["source"]["raw_unit_relation"] == "structured_emitted_item"
    assert record["raw_unit"]["primary_raw_unit"] == "record_or_emitted_item"
    assert record["raw_unit"]["maturity"] == "bounded_not_fully_first_class"
    assert record["capability_domains_used"]["pro"] is False
    assert record["capability_domains_used"]["extra"] is False


@pytest.mark.auth_contract
def test_usage_record_runtime_shape_covers_failure_and_delivery_failure(tmp_path: Path) -> None:
    run_failure = build_usage_record_v1(
        task=_task(tmp_path),
        meta=_meta(
            outcome=RunOutcome.FAILED,
            error=ErrorInfoV1(code="ZE-RUN-FAILED", message="failed", details=None),
        ),
        receipt=None,
    )
    assert run_failure["result"]["status"] == "failed"
    assert run_failure["result"]["failure_stage"] == "run"
    assert run_failure["result"]["error_code"] == "ZE-RUN-FAILED"

    delivery_failure = build_usage_record_v1(
        task=_task(tmp_path),
        meta=_meta(),
        receipt=DeliveryReceipt(
            destination="webhook",
            ok=False,
            details={
                "retryable": True,
                "failure_kind": "timeout",
                "error_code": "ZE-DELIVERY-FAILED",
            },
        ),
    )
    assert delivery_failure["result"]["status"] == "failed"
    assert delivery_failure["result"]["failure_stage"] == "delivery"
    assert delivery_failure["result"]["failure_retryability"] == "retryable"
    assert delivery_failure["result"]["failure_kind"] == "timeout"


@pytest.mark.auth_contract
def test_usage_record_runtime_shape_keeps_recovery_paths_distinct(tmp_path: Path) -> None:
    task = _task(tmp_path)
    resume = build_usage_record_v1(task=task, meta=_meta(run_origin="resume"), receipt=None)
    replay = build_usage_record_v1(task=task, meta=_meta(delivery_origin="replay"), receipt=None)
    requeue = build_usage_record_v1(task=task, meta=_meta(run_origin="requeue"), receipt=None)
    redrive = build_usage_record_v1(task=task, meta=_meta(run_origin="redrive"), receipt=None)

    assert resume["recovery"]["recovery_kind"] == "resume"
    assert replay["recovery"]["recovery_kind"] == "replay"
    assert requeue["recovery"]["recovery_kind"] == "requeue"
    assert redrive["recovery"]["recovery_kind"] == "redrive"


@pytest.mark.auth_contract
def test_write_usage_record_v1_persists_dedicated_runtime_artifact(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    path = write_usage_record_v1(
        out_dir=out_dir,
        task=_task(tmp_path, source="local_file"),
        meta=_meta(),
        receipt=DeliveryReceipt(destination="filesystem", ok=True),
    )

    assert path == out_dir / USAGE_RECORD_FILENAME
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["source"]["source_contract_id"] is None
    assert loaded["source"]["source_contract_status"] == "flow_family_only"
    assert loaded["capability_domains_used"]["base"] is True


@pytest.mark.auth_contract
def test_batch_usage_record_preserves_batch_success_failure_symmetry(tmp_path: Path) -> None:
    partial = build_batch_usage_record_v1(
        run_id="r-batch",
        pipeline_version="p-batch",
        docs_total=3,
        docs_success=2,
        docs_failed=1,
        docs_skipped=0,
        delivery_total=2,
        delivery_ok=1,
        delivery_failed=1,
    )
    assert partial["record_kind"] == "raw_technical_usage_batch_summary_v1"
    assert partial["result"] == {"status": "partial", "failure_stage": "batch"}
    assert partial["aggregate_counts"]["docs_failed"] == 1
    assert partial["capability_domains_used"] == {
        "base": True,
        "pro": False,
        "extra": False,
        "evidence": ["batch aggregate over current retained source/destination surface"],
    }

    path = write_batch_usage_record_v1(
        out_root=tmp_path / "batch-out",
        run_id="r-empty",
        pipeline_version="p-batch",
        docs_total=0,
        docs_success=0,
        docs_failed=0,
        docs_skipped=0,
        delivery_total=0,
        delivery_ok=0,
        delivery_failed=0,
    )
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["record_kind"] == "raw_technical_usage_batch_summary_v1"
    assert loaded["result"] == {"status": "empty", "failure_stage": "none"}


@pytest.mark.auth_contract
def test_runner_emits_usage_record_json_beside_run_meta_and_delivery_receipt(
    tmp_path: Path,
) -> None:
    doc = _doc(tmp_path, source="http_document_v1")
    out_root = tmp_path / "out"
    cfg = RunnerConfig(out_root=out_root, strategy=PartitionStrategy.AUTO, destination=OkDest())
    ctx = RunContext.new(
        pipeline_version="p-usage",
        run_id="r-usage",
        timestamp_utc="2026-04-22T00:00:00Z",
    )

    stats = run_documents(
        docs=[doc],
        cfg=cfg,
        ctx=ctx,
        partition_fn=_partition_ok,
        destination=OkDest(),
    )

    assert stats.success == 1
    batch_usage_path = out_root / USAGE_RECORD_FILENAME
    assert batch_usage_path.exists()
    batch_usage = json.loads(batch_usage_path.read_text(encoding="utf-8"))
    assert batch_usage["record_kind"] == "raw_technical_usage_batch_summary_v1"
    assert batch_usage["result"] == {"status": "succeeded", "failure_stage": "none"}
    assert batch_usage["aggregate_counts"]["docs_total"] == 1

    out_dirs = [path for path in out_root.iterdir() if path.is_dir()]
    assert len(out_dirs) == 1
    out_dir = out_dirs[0]
    assert (out_dir / "run_meta.json").exists()
    assert (out_dir / "delivery_receipt.json").exists()
    usage_path = out_dir / USAGE_RECORD_FILENAME
    assert usage_path.exists()
    usage_obj: object = json.loads(usage_path.read_text(encoding="utf-8"))
    assert isinstance(usage_obj, dict)
    usage = cast(dict[str, object], usage_obj)
    assert usage["record_kind"] == "raw_technical_usage_fact_v1"
    source = cast(dict[str, object], usage["source"])
    assert source["source_contract_id"] == "http_document_v1"
    domains = cast(dict[str, object], usage["capability_domains_used"])
    assert domains["base"] is True
    assert domains["pro"] is False
    assert domains["extra"] is False
