from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from tools.p5_m4_closeout_report import main as p5_m4_closeout_main

from zephyr_core import DocumentRef, PartitionStrategy
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.run_meta import MetricsV1, RunMetaV1, RunProvenanceV1
from zephyr_ingest.governance_action import (
    backfill_governance_history_from_evidence,
    governance_action_receipt_dir,
    verify_recovery_result,
)
from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.queue_inspect import inspect_local_queue, write_queue_inspect_receipt
from zephyr_ingest.source_contracts import (
    SOURCE_SPEC_ID_BY_CONTRACT_ID,
    normalize_source_contract_id,
)
from zephyr_ingest.spec.registry import get_spec, list_spec_ids
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)
from zephyr_ingest.testing.p5_m4_s14_closeout import (
    P5_GOVERNANCE_VOCABULARY_MATRIX_PATH,
    P5_INSPECT_RECEIPT_CONTRACT_PATH,
    P5_M4_CLOSEOUT_TRUTH_MATRIX_PATH,
    P5_M4_RESIDUAL_RISK_PATH,
    P5_M4_S14_CLOSEOUT_REPORT_PATH,
    P5_SOURCE_LINKAGE_V2_PATH,
    P5_VERIFY_RECOVERY_RESULT_CONTRACT_PATH,
    build_p5_governance_vocabulary_matrix,
    build_p5_inspect_receipt_contract,
    build_p5_m4_closeout_truth_matrix,
    build_p5_m4_residual_risk,
    build_p5_source_linkage_v2,
    build_p5_verify_recovery_result_contract,
    format_p5_m4_s14_closeout_results,
    validate_p5_m4_s14_closeout_artifacts,
)
from zephyr_ingest.usage_record import build_usage_record_v1


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


def _task(task_id: str, *, source: str = "http_document_v1") -> TaskV1:
    doc = DocumentRef(
        uri=f"file:///{task_id}.txt",
        source=source,
        discovered_at_utc="2026-04-23T00:00:00Z",
        filename=f"{task_id}.txt",
        extension=".txt",
        size_bytes=12,
    )
    return TaskV1(
        task_id=task_id,
        kind="uns",
        inputs=TaskInputsV1(
            document=TaskDocumentInputV1.from_document_ref(
                doc,
                source_contract_id=normalize_source_contract_id(
                    task_kind="uns",
                    task_document_source=source,
                ),
            )
        ),
        execution=TaskExecutionV1(strategy=PartitionStrategy.AUTO, unique_element_ids=True),
        identity=TaskIdentityV1(pipeline_version="p5-m4-s14", sha256=f"sha-{task_id}"),
    )


def _meta(task_id: str) -> RunMetaV1:
    return RunMetaV1(
        run_id="r-s14",
        pipeline_version="p5-m4-s14",
        timestamp_utc="2026-04-23T00:00:00Z",
        outcome=RunOutcome.SUCCESS,
        metrics=MetricsV1(duration_ms=1, attempts=1),
        provenance=RunProvenanceV1(
            run_origin="intake",
            delivery_origin="primary",
            execution_mode="batch",
            task_id=task_id,
            task_identity_key='{"kind":"uns"}',
        ),
    )


@pytest.mark.auth_contract
def test_s14_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_VERIFY_RECOVERY_RESULT_CONTRACT_PATH) == (
        build_p5_verify_recovery_result_contract()
    )
    assert _load_json_object(P5_INSPECT_RECEIPT_CONTRACT_PATH) == (
        build_p5_inspect_receipt_contract()
    )
    assert _load_json_object(P5_SOURCE_LINKAGE_V2_PATH) == build_p5_source_linkage_v2()
    assert _load_json_object(P5_GOVERNANCE_VOCABULARY_MATRIX_PATH) == (
        build_p5_governance_vocabulary_matrix()
    )
    assert _load_json_object(P5_M4_CLOSEOUT_TRUTH_MATRIX_PATH) == (
        build_p5_m4_closeout_truth_matrix()
    )
    assert _load_json_object(P5_M4_RESIDUAL_RISK_PATH) == build_p5_m4_residual_risk()
    assert P5_M4_S14_CLOSEOUT_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_s14_artifacts_pass_closeout_checks() -> None:
    checks = validate_p5_m4_s14_closeout_artifacts()

    assert all(check.ok for check in checks), format_p5_m4_s14_closeout_results(checks)


@pytest.mark.auth_contract
def test_verify_recovery_result_is_first_class_and_receipt_backed(tmp_path: Path) -> None:
    evidence = tmp_path / "usage_record.json"
    evidence.write_text('{"ok": true}', encoding="utf-8")

    result = verify_recovery_result(
        artifact_root=tmp_path,
        evidence_paths=(evidence,),
        task_id="task-verify",
        run_id="r-verify",
        usage_record_ref=str(evidence),
        source_contract_id="http_document_v1",
    )

    assert result.ok is True
    receipt = _load_json_object(result.receipt_path)
    linkage = cast(dict[str, object], receipt["linkage"])
    summary = cast(dict[str, object], receipt["result_summary"])
    assert receipt["action_kind"] == "verify_recovery_result"
    assert receipt["action_category"] == "verification"
    assert linkage["source_contract_id"] == "http_document_v1"
    assert summary["changed_state"] is False


@pytest.mark.auth_contract
def test_inspect_queue_receipt_is_opt_in_and_does_not_mutate_queue(tmp_path: Path) -> None:
    queue_root = tmp_path / "queue"
    queue = build_local_queue_backend(kind="sqlite", root=queue_root)
    queue.enqueue(_task("task-inspect"))
    before = inspect_local_queue(root=queue_root, bucket="pending", backend_kind="sqlite")

    receipt_path = write_queue_inspect_receipt(
        artifact_root=tmp_path / "audit",
        inspect_result=before,
    )
    after = inspect_local_queue(root=queue_root, bucket="pending", backend_kind="sqlite")

    assert after.summary.to_dict() == before.summary.to_dict()
    receipt = _load_json_object(receipt_path)
    linkage = cast(dict[str, object], receipt["linkage"])
    summary = cast(dict[str, object], receipt["result_summary"])
    assert receipt["action_kind"] == "inspect_queue"
    assert receipt["action_category"] == "read_only"
    assert linkage["source_contract_id"] == "http_document_v1"
    assert summary["changed_state"] is False


@pytest.mark.auth_contract
def test_retained_sources_have_spec_registry_parity() -> None:
    spec_ids = set(list_spec_ids())
    for contract_id, spec_id in SOURCE_SPEC_ID_BY_CONTRACT_ID.items():
        spec = get_spec(spec_id=spec_id)
        assert spec_id in spec_ids
        assert spec is not None
        assert spec["kind"] == "source", contract_id
        assert spec["fields"], contract_id
        assert any(field["required"] for field in spec["fields"]), contract_id


@pytest.mark.auth_contract
def test_canonical_source_contract_id_is_carried_task_to_usage() -> None:
    task = _task("task-canonical", source="google_drive")
    assert task.inputs.document.source_contract_id == "google_drive_document_v1"

    restored = TaskV1.from_dict(task.to_dict())
    record = build_usage_record_v1(task=restored, meta=_meta(task.task_id), receipt=None)

    assert restored.inputs.document.source_contract_id == "google_drive_document_v1"
    assert record["source"]["source_contract_id"] == "google_drive_document_v1"
    assert record["source"]["source_contract_status"] == "linked"


@pytest.mark.auth_contract
def test_governance_history_backfill_writes_receipts_and_marks_insufficient(
    tmp_path: Path,
) -> None:
    requeue_evidence = tmp_path / "historical_requeue.json"
    requeue_evidence.write_text(
        json.dumps(
            {
                "action": "requeue",
                "task_id": "task-requeue",
                "task_identity_key": "task-key",
                "source_contract_id": "http_document_v1",
            }
        ),
        encoding="utf-8",
    )
    insufficient = tmp_path / "insufficient.json"
    insufficient.write_text('{"note": "not enough"}', encoding="utf-8")

    result = backfill_governance_history_from_evidence(
        artifact_root=tmp_path / "audit",
        evidence_paths=(requeue_evidence, insufficient),
    )

    assert result.written_receipts == 1
    assert result.insufficient_evidence == 1
    receipt_paths = sorted(
        governance_action_receipt_dir(artifact_root=tmp_path / "audit").glob("*.json")
    )
    assert len(receipt_paths) == 1
    receipt = _load_json_object(receipt_paths[0])
    assert receipt["action_kind"] == "requeue"
    entries = result.to_dict()["entries"]
    assert entries[1]["backfill_status"] == "insufficient_evidence"


@pytest.mark.auth_contract
def test_s14_closeout_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert p5_m4_closeout_main(["--print-closeout-matrix-path"]) == 0
    matrix_output = capsys.readouterr()
    assert Path(matrix_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_m4_closeout_truth_matrix.json",
    )

    assert p5_m4_closeout_main(["--json"]) == 0
    json_output = capsys.readouterr()
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "m4_closeout_truth_matrix" in rendered
    assert "m4_residual_risk" in rendered

    assert p5_m4_closeout_main(["--check-artifacts"]) == 0
    check_output = capsys.readouterr()
    assert "s14_must_fix_items_resolved -> ok" in check_output.out

    assert p5_m4_closeout_main([]) == 0
    summary_output = capsys.readouterr()
    assert "S14-A must-fix gate is resolved" in summary_output.out
