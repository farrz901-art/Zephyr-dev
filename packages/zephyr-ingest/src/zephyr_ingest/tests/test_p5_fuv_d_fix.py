from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from tools.p5_fuv_d_fix_report import main as p5_fuv_d_fix_report_main
from tools.p45_s3_bootstrap import main as p45_s3_bootstrap_main

from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    RunContext,
    RunMetaV1,
    ZephyrElement,
)
from zephyr_core.contracts.v1.enums import RunOutcome
from zephyr_core.contracts.v1.run_meta import EngineMetaV1
from zephyr_core.contracts.v2.delivery_payload import DeliveryContentEvidenceV1, DeliveryPayloadV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_payload import (
    DELIVERY_NORMALIZED_TEXT_PREVIEW_MAX_CHARS,
    DELIVERY_RECORDS_PREVIEW_MAX_RECORDS,
    build_delivery_payload_v1,
)
from zephyr_ingest.destinations import opensearch as opensearch_module
from zephyr_ingest.destinations import webhook as webhook_module
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.destinations.fanout import FanoutDestination
from zephyr_ingest.destinations.kafka import KafkaDestination
from zephyr_ingest.destinations.opensearch import OpenSearchDestination
from zephyr_ingest.destinations.s3 import S3Destination
from zephyr_ingest.destinations.webhook import WebhookDestination
from zephyr_ingest.testing.p5_fuv_d_fix import (
    PACKAGE_PYPROJECT_PATH,
    collect_p45_s3_bucket_status,
    summarize_fanout_receipt,
    validate_p5_fuv_d_fix_artifacts,
)
from zephyr_ingest.testing.p45 import P45_HOME_ENV_NAME, load_p45_env


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _require_content_evidence(payload: DeliveryPayloadV1) -> DeliveryContentEvidenceV1:
    content_evidence = payload.get("content_evidence")
    assert content_evidence is not None
    return content_evidence


def _build_meta(*, engine_name: str = "unstructured") -> RunMetaV1:
    ctx = RunContext.new()
    return RunMetaV1(
        run_id=ctx.run_id,
        pipeline_version=ctx.pipeline_version,
        timestamp_utc=ctx.timestamp_utc,
        schema_version=ctx.run_meta_schema_version,
        outcome=RunOutcome.SUCCESS,
        engine=EngineMetaV1(
            name=engine_name,
            backend="local",
            version="0.1.0",
            strategy="auto",
        ),
        document=None,
        error=None,
        warnings=[],
    )


def _build_uns_result(*, marker: str) -> PartitionResult:
    return PartitionResult(
        document=DocumentMetadata(
            filename="direct_uns.txt",
            mime_type="text/plain",
            sha256="abc123",
            size_bytes=len(marker),
            created_at_utc="2026-03-21T00:00:00Z",
        ),
        engine=EngineInfo(
            name="unstructured",
            backend="local",
            version="0.1.0",
            strategy=PartitionStrategy.AUTO,
        ),
        elements=[
            ZephyrElement(
                element_id="e1",
                type="NarrativeText",
                text=marker,
                metadata={"flow_kind": "uns"},
            )
        ],
        normalized_text=marker,
    )


@dataclass
class FakeProducer:
    calls: list[dict[str, Any]]

    def produce(
        self,
        *,
        topic: str,
        key: bytes | None = None,
        value: bytes | None = None,
    ) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value})

    def flush(self, *, timeout: float | None = None) -> int:
        del timeout
        return 0


@dataclass
class FakeS3Client:
    calls: list[dict[str, Any]]

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "Bucket": Bucket,
                "Key": Key,
                "Body": Body,
                "ContentType": ContentType,
            }
        )
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


@dataclass
class FakeHttpResponse:
    status_code: int
    payload: dict[str, object] | None = None
    text: str = ""
    headers: dict[str, str] | None = None

    def json(self) -> dict[str, object]:
        return {} if self.payload is None else self.payload


@dataclass
class FakeWebhookClient:
    seen_payloads: list[dict[str, object]]

    def post(self, url: str, *, headers: dict[str, str], content: str) -> FakeHttpResponse:
        del url, headers
        self.seen_payloads.append(json.loads(content))
        return FakeHttpResponse(status_code=200, payload={"ok": True}, headers={})

    def close(self) -> None:
        return None


@dataclass
class FakeOpenSearchClient:
    seen_documents: list[dict[str, object]]

    def put(
        self,
        url: str,
        *,
        json: dict[str, object],
        headers: dict[str, str],
    ) -> FakeHttpResponse:
        del url, headers
        self.seen_documents.append(json)
        return FakeHttpResponse(status_code=201, payload={"result": "created"}, headers={})

    def close(self) -> None:
        return None


@dataclass
class FakeBucketAdminClient:
    buckets: list[str]
    created: list[str]

    def list_buckets(self) -> dict[str, object]:
        return {"Buckets": [{"Name": bucket} for bucket in self.buckets]}

    def create_bucket(self, *, Bucket: str) -> dict[str, object]:
        self.created.append(Bucket)
        if Bucket not in self.buckets:
            self.buckets.append(Bucket)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


class OkDest:
    name = "filesystem"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: RunMetaV1, result: object | None = None
    ) -> DeliveryReceipt:
        del out_root, sha256, meta, result
        return DeliveryReceipt(destination=self.name, ok=True, details={"out_dir": "out/demo"})


class WebhookLikeDest:
    name = "webhook"

    def __call__(
        self, *, out_root: Path, sha256: str, meta: RunMetaV1, result: object | None = None
    ) -> DeliveryReceipt:
        del out_root, sha256, meta, result
        return DeliveryReceipt(
            destination=self.name,
            ok=True,
            details={"status_code": 200},
        )


def _fake_webhook_client_factory(
    client: FakeWebhookClient,
) -> object:
    def _factory(**kwargs: object) -> FakeWebhookClient:
        del kwargs
        return client

    return _factory


def _fake_opensearch_client_factory(
    client: FakeOpenSearchClient,
) -> object:
    def _factory(**kwargs: object) -> FakeOpenSearchClient:
        del kwargs
        return client

    return _factory


def test_p5_fuv_d_fix_artifacts_match_helper_and_report_cli() -> None:
    checks = validate_p5_fuv_d_fix_artifacts()
    assert all(check.ok for check in checks)
    assert p5_fuv_d_fix_report_main(["--check-artifacts"]) == 0
    assert p5_fuv_d_fix_report_main(["--check-payload-content-evidence"]) == 0
    assert p5_fuv_d_fix_report_main(["--check-s3-dependency"]) == 0
    assert p5_fuv_d_fix_report_main(["--json"]) == 0


def test_delivery_payload_builder_adds_bounded_content_evidence_for_uns_and_it(
    tmp_path: Path,
) -> None:
    out_root = tmp_path / "out"
    sha_uns = "uns123"
    uns_dir = out_root / sha_uns
    marker = "delivery-marker-visible"
    _write_text(
        uns_dir / "normalized.txt",
        marker + "-" + ("x" * DELIVERY_NORMALIZED_TEXT_PREVIEW_MAX_CHARS),
    )
    _write_text(uns_dir / "elements.json", json.dumps([{"id": 1}, {"id": 2}], ensure_ascii=False))

    uns_payload = build_delivery_payload_v1(out_root=out_root, sha256=sha_uns, meta=_build_meta())
    uns_evidence = _require_content_evidence(uns_payload)
    assert uns_payload["schema_version"] == 1
    assert "artifacts" in uns_payload
    assert "normalized_text_preview" in uns_evidence
    assert "normalized_text_len" in uns_evidence
    assert "normalized_text_truncated" in uns_evidence
    assert "elements_count" in uns_evidence
    assert uns_evidence["normalized_text_preview"]
    assert marker in uns_evidence["normalized_text_preview"]
    assert (
        uns_evidence["normalized_text_len"]
        == len(marker) + 1 + DELIVERY_NORMALIZED_TEXT_PREVIEW_MAX_CHARS
    )
    assert uns_evidence["normalized_text_truncated"] is True
    assert uns_evidence["elements_count"] == 2

    sha_it = "it123"
    it_dir = out_root / sha_it
    lines: list[str] = []
    for index in range(DELIVERY_RECORDS_PREVIEW_MAX_RECORDS + 1):
        lines.append(
            json.dumps(
                {
                    "stream": "events",
                    "record_index": index,
                    "data": {"marker": f"it-marker-{index}"},
                },
                ensure_ascii=False,
            )
        )
    _write_text(it_dir / "records.jsonl", "\n".join(lines) + "\n")
    _write_text(it_dir / "elements.json", "[]")

    it_payload = build_delivery_payload_v1(
        out_root=out_root,
        sha256=sha_it,
        meta=_build_meta(engine_name="it-stream"),
    )
    it_evidence = _require_content_evidence(it_payload)
    assert "records_preview" in it_evidence
    assert "records_count" in it_evidence
    assert "records_truncated" in it_evidence
    assert "normalized_text_status" in it_evidence
    records_preview = it_evidence["records_preview"]
    assert it_evidence["records_count"] == DELIVERY_RECORDS_PREVIEW_MAX_RECORDS + 1
    assert it_evidence["records_truncated"] is True
    assert records_preview[0]["data"] == {"marker": "it-marker-0"}
    assert it_evidence["normalized_text_status"] == "missing"


def test_delivery_payload_missing_optional_files_does_not_break_shape(tmp_path: Path) -> None:
    payload = build_delivery_payload_v1(out_root=tmp_path / "out", sha256="abc", meta=_build_meta())
    evidence = _require_content_evidence(payload)
    assert payload["schema_version"] == 1
    assert "run_meta" in payload
    assert "artifacts" in payload
    assert "evidence_kind" in evidence
    assert "normalized_text_status" in evidence
    assert evidence["evidence_kind"] == "artifact_reference_only_v1"
    assert evidence["normalized_text_status"] == "missing"


def test_destinations_carry_content_evidence_marker_for_kafka_webhook_opensearch_and_s3(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    out_root = tmp_path / "out"
    sha = "abc123"
    marker = "visible-delivery-marker"
    meta = _build_meta()
    result = _build_uns_result(marker=marker)

    producer = FakeProducer(calls=[])
    kafka_dest = KafkaDestination(topic="zephyr", producer=producer)
    kafka_receipt = kafka_dest(out_root=out_root, sha256=sha, meta=meta, result=result)
    assert kafka_receipt.ok is True
    kafka_payload = json.loads(cast(bytes, producer.calls[0]["value"]).decode("utf-8"))
    assert kafka_payload["content_evidence"]["normalized_text_preview"] == marker

    seen_webhook_payloads: list[dict[str, object]] = []
    fake_webhook_client = FakeWebhookClient(seen_payloads=seen_webhook_payloads)
    monkeypatch.setattr(
        webhook_module.httpx,
        "Client",
        _fake_webhook_client_factory(fake_webhook_client),
    )

    webhook_dest = WebhookDestination(
        url="https://example.test/webhook",
    )
    webhook_receipt = webhook_dest(out_root=out_root, sha256=sha, meta=meta, result=result)
    assert webhook_receipt.ok is True
    assert (
        cast(
            dict[str, object],
            seen_webhook_payloads[0]["content_evidence"],
        )["normalized_text_preview"]
        == marker
    )

    seen_opensearch_documents: list[dict[str, object]] = []
    fake_opensearch_client = FakeOpenSearchClient(seen_documents=seen_opensearch_documents)
    monkeypatch.setattr(
        opensearch_module.httpx,
        "Client",
        _fake_opensearch_client_factory(fake_opensearch_client),
    )

    opensearch_dest = OpenSearchDestination(
        url="https://search.example.test",
        index="zephyr-docs",
    )
    opensearch_receipt = opensearch_dest(
        out_root=out_root,
        sha256=sha,
        meta=meta,
        result=result,
    )
    assert opensearch_receipt.ok is True
    assert (
        cast(
            dict[str, object],
            cast(dict[str, object], seen_opensearch_documents[0]["payload"])["content_evidence"],
        )["normalized_text_preview"]
        == marker
    )

    s3_client = FakeS3Client(calls=[])
    s3_dest = S3Destination(
        bucket="zephyr",
        region="us-east-1",
        access_key="ak",
        secret_key="sk",
        client=s3_client,
    )
    s3_receipt = s3_dest(out_root=out_root, sha256=sha, meta=meta, result=result)
    assert s3_receipt.ok is True
    s3_payload = json.loads(cast(bytes, s3_client.calls[0]["Body"]).decode("utf-8"))
    assert s3_payload["content_evidence"]["normalized_text_preview"] == marker


def test_s3_bucket_helper_uses_sanitized_runtime_home_configuration_and_can_ensure_bucket(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    runtime_home = tmp_path / "runtime-home"
    env_path = runtime_home / "env" / ".env.p45.local"
    _write_text(
        env_path,
        "\n".join(
            (
                "ZEPHYR_P45_S3_ENDPOINT=http://127.0.0.1:59000",
                "ZEPHYR_P45_S3_REGION=us-east-1",
                "ZEPHYR_P45_S3_ACCESS_KEY=minioadmin",
                "ZEPHYR_P45_S3_SECRET_KEY=minioadmin",
                "ZEPHYR_P45_S3_BUCKET=zephyr-p45-bucket",
            )
        )
        + "\n",
    )
    monkeypatch.setenv(P45_HOME_ENV_NAME, str(runtime_home))
    loaded_env = load_p45_env(
        repo_root_path=tmp_path / "repo", environ={P45_HOME_ENV_NAME: str(runtime_home)}
    )

    fake_client = FakeBucketAdminClient(buckets=[], created=[])
    before = collect_p45_s3_bucket_status(loaded_env=loaded_env, client=fake_client)
    assert before["bucket_exists"] is False
    assert before["available_buckets"] == []
    assert "minioadmin" not in json.dumps(before, ensure_ascii=False)

    after = collect_p45_s3_bucket_status(
        ensure_bucket=True, loaded_env=loaded_env, client=fake_client
    )
    assert after["bucket_exists"] is True
    assert after["created_bucket"] is True
    assert fake_client.created == ["zephyr-p45-bucket"]


def test_p45_s3_bootstrap_cli_renders_json_without_secrets(
    tmp_path: Path,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    runtime_home = tmp_path / "runtime-home"
    _write_text(
        runtime_home / "env" / ".env.p45.local",
        "\n".join(
            (
                "ZEPHYR_P45_S3_ENDPOINT=http://127.0.0.1:59000",
                "ZEPHYR_P45_S3_REGION=us-east-1",
                "ZEPHYR_P45_S3_ACCESS_KEY=minioadmin",
                "ZEPHYR_P45_S3_SECRET_KEY=minioadmin",
                "ZEPHYR_P45_S3_BUCKET=zephyr-p45-bucket",
            )
        )
        + "\n",
    )
    monkeypatch.setenv(P45_HOME_ENV_NAME, str(runtime_home))

    def fake_collect(*, ensure_bucket: bool = False) -> dict[str, object]:
        return {
            "bucket_exists": not ensure_bucket or True,
            "dependency_installed": True,
            "available_buckets": ["zephyr-p45-bucket"],
            "bucket": "zephyr-p45-bucket",
            "endpoint_url": "http://127.0.0.1:59000",
            "secrets_redacted": True,
        }

    monkeypatch.setattr(
        "tools.p45_s3_bootstrap.collect_p45_s3_bucket_status",
        fake_collect,
    )
    assert p45_s3_bootstrap_main(["--check-bucket", "--json"]) == 0
    captured = capsys.readouterr()
    assert "minioadmin" not in captured.out
    assert "zephyr-p45-bucket" in captured.out


def test_fanout_receipt_keeps_children_visible_and_summary_readable(tmp_path: Path) -> None:
    meta = RunMetaV1(
        run_id="r",
        pipeline_version="p1",
        timestamp_utc="2026-03-21T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
    )
    receipt = FanoutDestination(destinations=(OkDest(), WebhookLikeDest()))(
        out_root=tmp_path,
        sha256="abc",
        meta=meta,
        result=None,
    )
    assert receipt.ok is True
    assert receipt.details is not None
    children = cast(list[dict[str, object]], receipt.details["children"])
    assert [child["destination"] for child in children] == ["filesystem", "webhook"]
    assert cast(dict[str, object], children[0]["summary"])["delivery_outcome"] == "delivered"
    summary = summarize_fanout_receipt(receipt=receipt)
    assert summary["destination"] == "fanout"
    assert summary["child_count"] == 2
    summarized_children = cast(list[dict[str, object]], summary["children"])
    assert summarized_children[1]["destination"] == "webhook"


def test_s3_dependency_declared_in_package_pyproject() -> None:
    pyproject_text = PACKAGE_PYPROJECT_PATH.read_text(encoding="utf-8")
    assert "[project.optional-dependencies]" in pyproject_text
    assert "s3 = [" in pyproject_text
    assert "boto3>=" in pyproject_text
