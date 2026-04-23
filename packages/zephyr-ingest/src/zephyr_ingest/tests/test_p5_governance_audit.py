from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import cast

import httpx
import pytest
from tools.p5_governance_audit_report import main as p5_governance_main

from zephyr_core import DocumentRef, PartitionStrategy
from zephyr_core.contracts.v1.run_meta import (
    ErrorInfoV1,
    MetricsV1,
    RunMetaV1,
    RunProvenanceV1,
)
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.governance_action import (
    build_governance_action_receipt_v1,
    governance_action_receipt_dir,
    write_governance_action_receipt_v1,
)
from zephyr_ingest.obs.events import log_event
from zephyr_ingest.queue_backend_factory import build_local_queue_backend
from zephyr_ingest.queue_recover import requeue_local_task
from zephyr_ingest.replay_delivery import replay_delivery_dlq
from zephyr_ingest.task_v1 import (
    TaskDocumentInputV1,
    TaskExecutionV1,
    TaskIdentityV1,
    TaskInputsV1,
    TaskV1,
)
from zephyr_ingest.testing.p5_governance_audit import (
    P5_GOVERNANCE_ACTION_MATRIX_PATH,
    P5_GOVERNANCE_AUDIT_REPORT_PATH,
    P5_GOVERNANCE_USAGE_LINKAGE_PATH,
    P5_MANUAL_ACTION_AUDIT_MANIFEST_PATH,
    P5_SOURCE_SPEC_PARITY_PATH,
    P5_SOURCE_SPEC_PARITY_REPORT_PATH,
    build_p5_governance_action_matrix,
    build_p5_governance_usage_linkage,
    build_p5_manual_action_audit_manifest,
    build_p5_source_spec_parity,
    format_p5_governance_audit_results,
    validate_p5_governance_audit_artifacts,
)
from zephyr_ingest.usage_record import normalize_source_contract_id


def _load_json_object(path: Path) -> dict[str, object]:
    loaded_obj: object = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded_obj, dict)
    return cast(dict[str, object], loaded_obj)


def _make_task(task_id: str, *, source: str = "http_document_v1") -> TaskV1:
    doc = DocumentRef(
        uri=f"file:///{task_id}.txt",
        source=source,
        discovered_at_utc="2026-04-22T00:00:00Z",
        filename=f"{task_id}.txt",
        extension=".txt",
        size_bytes=12,
    )
    return TaskV1(
        task_id=task_id,
        kind="uns",
        inputs=TaskInputsV1(document=TaskDocumentInputV1.from_document_ref(doc)),
        execution=TaskExecutionV1(strategy=PartitionStrategy.AUTO, unique_element_ids=True),
        identity=TaskIdentityV1(pipeline_version="p5-m4-s13", sha256=f"sha-{task_id}"),
    )


def _make_failed_dlq(out_root: Path) -> Path:
    meta = RunMetaV1(
        run_id="r-governance-replay",
        pipeline_version="p5-m4-s13",
        timestamp_utc="2026-04-22T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
        outcome=None,
        metrics=MetricsV1(duration_ms=10, attempts=1),
        error=ErrorInfoV1(
            code="ZE-DELIVERY-FAILED",
            message="delivery failed",
            details={"retryable": True},
        ),
        provenance=RunProvenanceV1(
            run_origin="intake",
            delivery_origin="primary",
            execution_mode="batch",
            task_id="task-replay",
            task_identity_key='{"kind":"uns","pipeline_version":"p5-m4-s13","sha256":"sha-task"}',
        ),
    )
    receipt = DeliveryReceipt(destination="webhook", ok=False, details={"status_code": 500})
    return write_delivery_dlq(out_root=out_root, sha256="sha-task", meta=meta, receipt=receipt)


@pytest.mark.auth_contract
def test_p5_governance_artifacts_match_helper_truth() -> None:
    assert _load_json_object(P5_GOVERNANCE_ACTION_MATRIX_PATH) == (
        build_p5_governance_action_matrix()
    )
    assert _load_json_object(P5_MANUAL_ACTION_AUDIT_MANIFEST_PATH) == (
        build_p5_manual_action_audit_manifest()
    )
    assert _load_json_object(P5_GOVERNANCE_USAGE_LINKAGE_PATH) == (
        build_p5_governance_usage_linkage()
    )
    assert _load_json_object(P5_SOURCE_SPEC_PARITY_PATH) == build_p5_source_spec_parity()
    assert P5_GOVERNANCE_AUDIT_REPORT_PATH.exists()
    assert P5_SOURCE_SPEC_PARITY_REPORT_PATH.exists()


@pytest.mark.auth_contract
def test_p5_governance_artifacts_pass_consistency_checks() -> None:
    checks = validate_p5_governance_audit_artifacts()

    assert all(check.ok for check in checks), format_p5_governance_audit_results(checks)


@pytest.mark.auth_contract
def test_p5_governance_matrix_preserves_action_differences() -> None:
    matrix = _load_json_object(P5_GOVERNANCE_ACTION_MATRIX_PATH)
    actions = cast(list[dict[str, object]], matrix["actions"])
    by_kind = {cast(str, action["action_kind"]): action for action in actions}

    assert by_kind["requeue"]["category"] == "state_changing"
    assert by_kind["requeue"]["receipt_support"] == "automatic_persisted_receipt"
    assert by_kind["replay_delivery"]["provenance_linkage"] == "delivery_origin=replay"
    assert by_kind["inspect_queue"]["category"] == "read_only"
    assert by_kind["inspect_queue"]["receipt_support"] == "opt_in_persisted_receipt"
    assert by_kind["verify_recovery_result"]["runtime_behavior"] == "real"
    assert "RBAC" in cast(list[object], matrix["not_claimed"])


@pytest.mark.auth_contract
def test_p5_manual_action_audit_manifest_declares_receipt_and_event_taxonomy() -> None:
    manifest = _load_json_object(P5_MANUAL_ACTION_AUDIT_MANIFEST_PATH)
    required_fields = cast(list[object], manifest["required_fields"])
    event_taxonomy = cast(dict[str, object], manifest["event_taxonomy"])
    runtime_events = cast(list[object], event_taxonomy["runtime_events"])

    assert manifest["receipt_record_kind"] == "governance_action_receipt_v1"
    assert manifest["physical_layout"] == "_governance/actions/<action_id>.json"
    assert "linkage.usage_record_ref" in required_fields
    assert "linkage.source_contract_id" in required_fields
    for event_name in (
        "governance_action_start",
        "governance_action_result",
        "audit_linkage_written",
    ):
        assert event_name in runtime_events
    assert "manual_verify_result" in cast(list[object], event_taxonomy["reserved_or_helper_events"])


@pytest.mark.auth_contract
def test_requeue_writes_governance_action_receipt_with_task_provenance_linkage(
    tmp_path: Path,
) -> None:
    queue_root = tmp_path / "queue"
    queue = build_local_queue_backend(kind="sqlite", root=queue_root, max_task_attempts=1)
    task = _make_task("task-requeue")
    queue.enqueue(task)
    claimed = queue.claim_next()
    assert claimed is not None
    queue.ack_failure(claimed)

    recovery = requeue_local_task(
        root=queue_root,
        source_bucket="poison",
        task_id="task-requeue",
        backend_kind="sqlite",
    )

    receipt_paths = sorted(governance_action_receipt_dir(artifact_root=queue_root).glob("*.json"))
    assert len(receipt_paths) == 1
    receipt = _load_json_object(receipt_paths[0])
    linkage = cast(dict[str, object], receipt["linkage"])
    result_summary = cast(dict[str, object], receipt["result_summary"])

    assert receipt["record_kind"] == "governance_action_receipt_v1"
    assert receipt["action_kind"] == "requeue"
    assert receipt["status"] == "succeeded"
    assert receipt["recovery_kind"] == "requeue"
    assert linkage["task_id"] == "task-requeue"
    assert linkage["task_identity_key"] == recovery.task_identity_key
    assert result_summary["backend_kind"] == "sqlite"
    assert result_summary["underlying_audit_support"] == "result_only"


@pytest.mark.auth_contract
def test_replay_delivery_writes_governance_action_receipt_with_run_linkage(
    tmp_path: Path,
) -> None:
    out_root = tmp_path / "out"
    out_root.mkdir(parents=True, exist_ok=True)
    dlq_path = _make_failed_dlq(out_root)
    assert dlq_path.exists()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    stats = replay_delivery_dlq(
        out_root=out_root,
        webhook_url="https://example.test/hook",
        timeout_s=1.0,
        transport=httpx.MockTransport(handler),
        dry_run=False,
        move_done=True,
    )

    assert stats.succeeded == 1
    receipt_paths = sorted(governance_action_receipt_dir(artifact_root=out_root).glob("*.json"))
    assert len(receipt_paths) == 1
    receipt = _load_json_object(receipt_paths[0])
    linkage = cast(dict[str, object], receipt["linkage"])
    result_summary = cast(dict[str, object], receipt["result_summary"])

    assert receipt["action_kind"] == "replay_delivery"
    assert receipt["recovery_kind"] == "replay"
    assert linkage["task_id"] == "task-replay"
    assert linkage["run_id"] == "r-governance-replay"
    assert result_summary["succeeded"] == 1
    assert result_summary["destination"] == "webhook"


@pytest.mark.auth_contract
def test_governance_action_receipt_helper_supports_read_only_inspect_and_usage_linkage(
    tmp_path: Path,
) -> None:
    receipt = build_governance_action_receipt_v1(
        action_kind="inspect_queue",
        action_category="read_only",
        status="observed",
        audit_support="persisted_receipt",
        recovery_kind=None,
        task_id="task-inspect",
        run_id="r-inspect",
        usage_record_ref="out/usage_record.json",
        source_contract_id="http_document_v1",
        result_summary={"listed_tasks": 1, "changed_state": False},
        evidence_refs=({"kind": "queue_inspect_result", "ref": "queue.sqlite3#task=task-inspect"},),
        action_id="gov-inspect-test",
        recorded_at_utc="2026-04-22T00:00:00Z",
    )
    path = write_governance_action_receipt_v1(artifact_root=tmp_path, receipt=receipt)
    loaded = _load_json_object(path)
    linkage = cast(dict[str, object], loaded["linkage"])

    assert loaded["action_kind"] == "inspect_queue"
    assert loaded["action_category"] == "read_only"
    assert linkage["usage_record_ref"] == "out/usage_record.json"
    assert linkage["source_contract_id"] == "http_document_v1"
    assert "RBAC approval workflow" in cast(list[object], loaded["not_claimed"])


@pytest.mark.auth_contract
def test_source_spec_parity_keeps_partial_truth_and_canonical_mapping() -> None:
    parity = _load_json_object(P5_SOURCE_SPEC_PARITY_PATH)
    mappings = cast(list[dict[str, object]], parity["canonical_source_spec_direction"])
    runtime_linkage = cast(dict[str, object], parity["canonical_runtime_linkage"])

    assert parity["full_source_spec_registry_parity_today"] is True
    assert len(mappings) == 10
    assert mappings[0]["proposed_source_spec_id"] == "source.uns.http_document.v1"
    assert runtime_linkage["normalizer"] == (
        "zephyr_ingest.usage_record.normalize_source_contract_id"
    )
    assert (
        normalize_source_contract_id(
            task_kind="uns",
            task_document_source="google_drive",
        )
        == "google_drive_document_v1"
    )
    assert normalize_source_contract_id(task_kind="it", task_document_source="kafka") == (
        "kafka_partition_offset_v1"
    )


@pytest.mark.auth_contract
def test_governance_event_taxonomy_accepts_new_event_names(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("zephyr_ingest.tests.governance_events")
    with caplog.at_level(logging.INFO, logger=logger.name):
        log_event(
            logger,
            level=logging.INFO,
            event="governance_action_start",
            action_kind="inspect_queue",
        )
        log_event(
            logger,
            level=logging.INFO,
            event="manual_verify_result",
            action_kind="verify_recovery_result",
            ok=True,
        )
        log_event(
            logger,
            level=logging.INFO,
            event="audit_linkage_written",
            action_id="gov-test",
        )

    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "governance_action_start action_kind=inspect_queue" in text
    assert "manual_verify_result action_kind=verify_recovery_result ok=True" in text
    assert "audit_linkage_written action_id=gov-test" in text


@pytest.mark.auth_contract
def test_p5_governance_audit_cli_prints_paths_json_checks_and_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert p5_governance_main(["--print-action-matrix-path"]) == 0
    matrix_output = capsys.readouterr()
    assert Path(matrix_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_governance_action_matrix.json",
    )

    assert p5_governance_main(["--print-source-spec-parity-path"]) == 0
    parity_output = capsys.readouterr()
    assert Path(parity_output.out.strip()).parts[-2:] == (
        "validation",
        "p5_source_spec_parity.json",
    )

    assert p5_governance_main(["--json"]) == 0
    json_output = capsys.readouterr()
    rendered_obj: object = json.loads(json_output.out)
    assert isinstance(rendered_obj, dict)
    rendered = cast(dict[str, object], rendered_obj)
    assert "governance_action_matrix" in rendered
    assert "source_spec_parity" in rendered

    assert p5_governance_main(["--check-artifacts"]) == 0
    check_output = capsys.readouterr()
    assert "governance_action_matrix_matches_helper -> ok" in check_output.out
    assert "source_spec_parity_is_complete_for_retained_sources -> ok" in check_output.out

    assert p5_governance_main([]) == 0
    summary_output = capsys.readouterr()
    assert "bounded and non-RBAC" in summary_output.out
    assert "source spec parity is complete" in summary_output.out
