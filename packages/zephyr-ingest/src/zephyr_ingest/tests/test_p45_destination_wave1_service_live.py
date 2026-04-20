from __future__ import annotations

import json
import os
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

import httpx
import pytest

from zephyr_ingest.destinations.clickhouse import ClickHouseDestination
from zephyr_ingest.destinations.opensearch import OpenSearchDestination
from zephyr_ingest.destinations.s3 import S3Destination, build_s3_delivery_object_key
from zephyr_ingest.destinations.webhook import DeliveryRetryConfig, WebhookDestination
from zephyr_ingest.testing.p5_benchmark import build_p5_benchmark_resource_name
from zephyr_ingest.testing.p45 import (
    LoadedP45Env,
    get_p45_opensearch_auth,
    get_p45_opensearch_url,
    get_p45_opensearch_verify_tls,
)
from zephyr_ingest.tests._p45_destination_wave1_helpers import (
    SigV4MinioClient,
    as_dict,
    read_json_dict,
    run_batch_case,
    run_worker_case,
    webhook_events,
    webhook_reset,
)


def _s3_client(env: LoadedP45Env) -> SigV4MinioClient:
    return SigV4MinioClient(
        endpoint_url=env.require("ZEPHYR_P45_S3_ENDPOINT"),
        access_key=env.require("ZEPHYR_P45_S3_ACCESS_KEY"),
        secret_key=env.require("ZEPHYR_P45_S3_SECRET_KEY"),
        region=env.require("ZEPHYR_P45_S3_REGION"),
    )


@pytest.mark.auth_local_real
@pytest.mark.auth_service_live
def test_p45_wave1_webhook_live_success_failure_and_worker_consistency(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    base_url = p45_env.require("ZEPHYR_P45_WEBHOOK_ECHO_URL").removesuffix("/ingest")
    webhook_reset(base_url=base_url)

    destination = WebhookDestination(
        url=p45_env.require("ZEPHYR_P45_WEBHOOK_ECHO_URL"),
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=2, base_backoff_ms=0, max_backoff_ms=0
        ),
    )
    batch = run_batch_case(
        tmp_root=tmp_path / "webhook-batch",
        destination=destination,
        run_id="webhook-batch",
        text="webhook success",
    )
    worker = run_worker_case(
        tmp_root=tmp_path / "webhook-worker",
        destination=destination,
        run_id="webhook-worker",
        doc=batch.doc,
    )

    retry_success = WebhookDestination(
        url=f"{base_url}/flaky-once/503",
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=2, base_backoff_ms=0, max_backoff_ms=0
        ),
    )
    retry_batch = run_batch_case(
        tmp_root=tmp_path / "webhook-retry-success",
        destination=retry_success,
        run_id="webhook-retry-success",
        text="webhook retry success",
    )
    retry_receipt = read_json_dict(
        retry_batch.out_root / retry_batch.sha256 / "delivery_receipt.json"
    )

    retryable_failure = WebhookDestination(
        url=f"{base_url}/status/503",
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=2, base_backoff_ms=0, max_backoff_ms=0
        ),
    )
    retryable_batch = run_batch_case(
        tmp_root=tmp_path / "webhook-retryable-failure",
        destination=retryable_failure,
        run_id="webhook-retryable-failure",
        text="webhook retryable failure",
    )
    retryable_receipt = read_json_dict(
        retryable_batch.out_root / retryable_batch.sha256 / "delivery_receipt.json"
    )

    non_retryable = WebhookDestination(
        url=f"{base_url}/status/400",
        retry=DeliveryRetryConfig(
            enabled=True, max_attempts=2, base_backoff_ms=0, max_backoff_ms=0
        ),
    )
    non_retryable_batch = run_batch_case(
        tmp_root=tmp_path / "webhook-non-retryable",
        destination=non_retryable,
        run_id="webhook-non-retryable",
        text="webhook non retryable",
    )
    non_retryable_receipt = read_json_dict(
        non_retryable_batch.out_root / non_retryable_batch.sha256 / "delivery_receipt.json"
    )

    events = webhook_events(base_url=base_url)
    ingest_events = [event for event in events if event["path"] == "/ingest"]
    batch_payload = cast(dict[str, object], ingest_events[0]["payload"])
    worker_payload = cast(dict[str, object], ingest_events[1]["payload"])
    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    batch_run_meta = as_dict(batch_payload["run_meta"])
    worker_run_meta = as_dict(worker_payload["run_meta"])
    batch_provenance = as_dict(batch_run_meta["provenance"])
    worker_provenance = as_dict(worker_run_meta["provenance"])
    retry_summary = as_dict(retry_receipt["summary"])
    retryable_summary = as_dict(retryable_receipt["summary"])
    non_retryable_summary = as_dict(non_retryable_receipt["summary"])

    assert batch_by_destination["webhook"] == {
        "total": 1,
        "ok": 1,
        "failed": 0,
    }
    assert batch_provenance["delivery_origin"] == "primary"
    assert batch_provenance["execution_mode"] == "batch"
    assert worker_provenance["delivery_origin"] == "primary"
    assert worker_provenance["execution_mode"] == "worker"
    assert retry_summary["delivery_outcome"] == "delivered"
    assert retry_summary["attempt_count"] == 2
    assert retryable_summary["failure_retryability"] == "retryable"
    assert retryable_summary["failure_kind"] == "server_error"
    assert non_retryable_summary["failure_retryability"] == "non_retryable"
    assert non_retryable_summary["failure_kind"] == "client_error"
    del worker


@pytest.mark.auth_service_live
def test_p45_wave1_s3_live_success_failures_and_worker_consistency(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    client = _s3_client(p45_env)
    bucket = p45_env.require("ZEPHYR_P45_S3_BUCKET")
    client.ensure_bucket(bucket=bucket)

    success_prefix = "p45-wave1/s3/success"
    destination = S3Destination(
        bucket=bucket,
        region=p45_env.require("ZEPHYR_P45_S3_REGION"),
        access_key="unused",
        secret_key="unused",
        prefix=success_prefix,
        client=client,
    )
    batch = run_batch_case(
        tmp_root=tmp_path / "s3-batch",
        destination=destination,
        run_id="s3-batch",
        text="s3 success",
    )
    worker = run_worker_case(
        tmp_root=tmp_path / "s3-worker",
        destination=destination,
        run_id="s3-worker",
        doc=batch.doc,
    )
    object_key = build_s3_delivery_object_key(
        prefix=success_prefix,
        idempotency_key=f"{batch.sha256}:s3-batch",
    )
    batch_payload = json.loads(client.get_object_text(bucket=bucket, key=object_key))

    missing_bucket_destination = S3Destination(
        bucket="zephyr-p45-wave1-missing",
        region=p45_env.require("ZEPHYR_P45_S3_REGION"),
        access_key="unused",
        secret_key="unused",
        prefix="p45-wave1/s3/missing",
        client=client,
    )
    missing_bucket_batch = run_batch_case(
        tmp_root=tmp_path / "s3-missing-bucket",
        destination=missing_bucket_destination,
        run_id="s3-missing-bucket",
        text="s3 missing bucket",
    )
    missing_bucket_receipt = read_json_dict(
        missing_bucket_batch.out_root / missing_bucket_batch.sha256 / "delivery_receipt.json"
    )

    bad_auth_destination = S3Destination(
        bucket=bucket,
        region=p45_env.require("ZEPHYR_P45_S3_REGION"),
        access_key="unused",
        secret_key="unused",
        prefix="p45-wave1/s3/bad-auth",
        client=SigV4MinioClient(
            endpoint_url=p45_env.require("ZEPHYR_P45_S3_ENDPOINT"),
            access_key=p45_env.require("ZEPHYR_P45_S3_ACCESS_KEY"),
            secret_key="wrong-secret",
            region=p45_env.require("ZEPHYR_P45_S3_REGION"),
        ),
    )
    bad_auth_batch = run_batch_case(
        tmp_root=tmp_path / "s3-bad-auth",
        destination=bad_auth_destination,
        run_id="s3-bad-auth",
        text="s3 bad auth",
    )
    bad_auth_receipt = read_json_dict(
        bad_auth_batch.out_root / bad_auth_batch.sha256 / "delivery_receipt.json"
    )

    retryable_destination = S3Destination(
        bucket=bucket,
        region=p45_env.require("ZEPHYR_P45_S3_REGION"),
        access_key="unused",
        secret_key="unused",
        prefix="p45-wave1/s3/connection",
        client=SigV4MinioClient(
            endpoint_url="http://127.0.0.1:59001",
            access_key=p45_env.require("ZEPHYR_P45_S3_ACCESS_KEY"),
            secret_key=p45_env.require("ZEPHYR_P45_S3_SECRET_KEY"),
            region=p45_env.require("ZEPHYR_P45_S3_REGION"),
        ),
    )
    retryable_batch = run_batch_case(
        tmp_root=tmp_path / "s3-retryable",
        destination=retryable_destination,
        run_id="s3-retryable",
        text="s3 retryable",
    )
    retryable_receipt = read_json_dict(
        retryable_batch.out_root / retryable_batch.sha256 / "delivery_receipt.json"
    )
    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    batch_run_meta = as_dict(batch_payload["run_meta"])
    batch_provenance = as_dict(batch_run_meta["provenance"])
    missing_bucket_summary = as_dict(missing_bucket_receipt["summary"])
    bad_auth_summary = as_dict(bad_auth_receipt["summary"])
    retryable_summary = as_dict(retryable_receipt["summary"])

    assert batch_by_destination["s3"] == {
        "total": 1,
        "ok": 1,
        "failed": 0,
    }
    assert batch_provenance["delivery_origin"] == "primary"
    assert batch_provenance["execution_mode"] == "batch"
    assert missing_bucket_summary["failure_retryability"] == "non_retryable"
    assert missing_bucket_summary["failure_kind"] == "client_error"
    assert bad_auth_summary["failure_retryability"] == "non_retryable"
    assert bad_auth_summary["failure_kind"] == "client_error"
    assert retryable_summary["failure_retryability"] == "retryable"
    assert retryable_summary["failure_kind"] == "connection"
    del worker


@pytest.mark.auth_service_live
def test_p45_wave1_opensearch_live_success_failures_and_worker_consistency(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    base_url = get_p45_opensearch_url(p45_env).rstrip("/")
    verify_tls = get_p45_opensearch_verify_tls(p45_env)
    auth = get_p45_opensearch_auth(p45_env)
    index = os.environ.get("ZEPHYR_P45_BENCHMARK_OPENSEARCH_INDEX")
    if index is None:
        index = build_p5_benchmark_resource_name(
            case_id="opensearch_heavier_delivery",
            run_id=tmp_path.name,
            resource_kind="index",
        )
    assert index != "zephyr-p45-wave1-opensearch"
    assert index == index.lower()
    with httpx.Client(timeout=10.0, trust_env=False, verify=verify_tls, auth=auth) as client:
        client.put(f"{base_url}/{index}", json={}).raise_for_status()

    destination = OpenSearchDestination(
        url=base_url,
        index=index,
        verify_tls=verify_tls,
        username=None if auth is None else auth[0],
        password=None if auth is None else auth[1],
    )
    batch = run_batch_case(
        tmp_root=tmp_path / "opensearch-batch",
        destination=destination,
        run_id="opensearch-batch",
        text="opensearch success",
    )
    worker = run_worker_case(
        tmp_root=tmp_path / "opensearch-worker",
        destination=destination,
        run_id="opensearch-worker",
        doc=batch.doc,
    )
    with httpx.Client(timeout=10.0, trust_env=False, verify=verify_tls, auth=auth) as client:
        document = client.get(f"{base_url}/{index}/_doc/{batch.sha256}:opensearch-batch").json()

    invalid_index_destination = OpenSearchDestination(
        url=base_url,
        index="INVALID INDEX",
        verify_tls=verify_tls,
        username=None if auth is None else auth[0],
        password=None if auth is None else auth[1],
    )
    invalid_index_batch = run_batch_case(
        tmp_root=tmp_path / "opensearch-invalid",
        destination=invalid_index_destination,
        run_id="opensearch-invalid",
        text="opensearch invalid",
    )
    invalid_index_receipt = read_json_dict(
        invalid_index_batch.out_root / invalid_index_batch.sha256 / "delivery_receipt.json"
    )

    bad_auth_receipt: dict[str, object] | None = None
    if auth is not None:
        bad_auth_destination = OpenSearchDestination(
            url=base_url,
            index=index,
            verify_tls=verify_tls,
            username=auth[0],
            password="wrong-password",
        )
        bad_auth_batch = run_batch_case(
            tmp_root=tmp_path / "opensearch-bad-auth",
            destination=bad_auth_destination,
            run_id="opensearch-bad-auth",
            text="opensearch bad auth",
        )
        bad_auth_receipt = read_json_dict(
            bad_auth_batch.out_root / bad_auth_batch.sha256 / "delivery_receipt.json"
        )

    parsed_base = urlsplit(base_url)
    retryable_url = f"{parsed_base.scheme}://{parsed_base.hostname}:{(parsed_base.port or 0) + 1}"
    retryable_destination = OpenSearchDestination(
        url=retryable_url,
        index=index,
        verify_tls=verify_tls,
        username=None if auth is None else auth[0],
        password=None if auth is None else auth[1],
    )
    retryable_batch = run_batch_case(
        tmp_root=tmp_path / "opensearch-retryable",
        destination=retryable_destination,
        run_id="opensearch-retryable",
        text="opensearch retryable",
    )
    retryable_receipt = read_json_dict(
        retryable_batch.out_root / retryable_batch.sha256 / "delivery_receipt.json"
    )
    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    source_obj = as_dict(document["_source"])
    payload_obj = as_dict(source_obj["payload"])
    run_meta_obj = as_dict(payload_obj["run_meta"])
    provenance = as_dict(run_meta_obj["provenance"])
    invalid_index_summary = as_dict(invalid_index_receipt["summary"])
    bad_auth_summary = None if bad_auth_receipt is None else as_dict(bad_auth_receipt["summary"])
    retryable_summary = as_dict(retryable_receipt["summary"])

    assert batch_by_destination["opensearch"] == {
        "total": 1,
        "ok": 1,
        "failed": 0,
    }
    assert provenance["delivery_origin"] == "primary"
    assert provenance["execution_mode"] == "batch"
    assert invalid_index_summary["failure_retryability"] == "non_retryable"
    assert invalid_index_summary["failure_kind"] == "client_error"
    if bad_auth_summary is not None:
        assert bad_auth_summary["failure_retryability"] == "non_retryable"
        assert bad_auth_summary["failure_kind"] == "client_error"
    assert retryable_summary["failure_retryability"] == "retryable"
    assert retryable_summary["failure_kind"] == "connection"
    del worker


@pytest.mark.auth_service_live
def test_p45_wave1_clickhouse_live_success_failures_and_worker_consistency(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    base_url = p45_env.require("ZEPHYR_P45_CLICKHOUSE_URL").rstrip("/")
    database = p45_env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE")
    username = p45_env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME")
    password = p45_env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD")
    table = "zephyr_p45_wave1_delivery"
    create_query = (
        f"CREATE TABLE IF NOT EXISTS {database}.{table} ("
        "identity_key String, sha256 String, run_id String, schema_version UInt32, "
        "payload_json String, delivered_at_utc String"
        ") ENGINE = ReplacingMergeTree() ORDER BY identity_key"
    )
    with httpx.Client(timeout=10.0, trust_env=False) as client:
        client.post(
            base_url, params={"query": create_query}, auth=(username, password)
        ).raise_for_status()

    destination = ClickHouseDestination(
        url=base_url,
        database=database,
        table=table,
        username=username,
        password=password,
    )
    batch = run_batch_case(
        tmp_root=tmp_path / "clickhouse-batch",
        destination=destination,
        run_id="clickhouse-batch",
        text="clickhouse success",
    )
    worker = run_worker_case(
        tmp_root=tmp_path / "clickhouse-worker",
        destination=destination,
        run_id="clickhouse-worker",
        doc=batch.doc,
    )
    with httpx.Client(timeout=10.0, trust_env=False) as client:
        response = client.get(
            base_url,
            params={
                "query": (
                    f"SELECT payload_json FROM {database}.{table} "
                    f"WHERE identity_key = '{batch.sha256}:clickhouse-batch' "
                    "ORDER BY delivered_at_utc DESC LIMIT 1 FORMAT JSONEachRow"
                )
            },
            auth=(username, password),
        )
        response.raise_for_status()
        batch_row = json.loads(response.text.strip())

    bad_auth_destination = ClickHouseDestination(
        url=base_url,
        database=database,
        table=table,
        username=username,
        password="wrong-password",
    )
    bad_auth_batch = run_batch_case(
        tmp_root=tmp_path / "clickhouse-bad-auth",
        destination=bad_auth_destination,
        run_id="clickhouse-bad-auth",
        text="clickhouse bad auth",
    )
    bad_auth_receipt = read_json_dict(
        bad_auth_batch.out_root / bad_auth_batch.sha256 / "delivery_receipt.json"
    )

    missing_table_destination = ClickHouseDestination(
        url=base_url,
        database=database,
        table="zephyr_p45_missing_table",
        username=username,
        password=password,
    )
    missing_table_batch = run_batch_case(
        tmp_root=tmp_path / "clickhouse-missing-table",
        destination=missing_table_destination,
        run_id="clickhouse-missing-table",
        text="clickhouse missing table",
    )
    missing_table_receipt = read_json_dict(
        missing_table_batch.out_root / missing_table_batch.sha256 / "delivery_receipt.json"
    )

    retryable_destination = ClickHouseDestination(
        url="http://127.0.0.1:58124",
        database=database,
        table=table,
        username=username,
        password=password,
    )
    retryable_batch = run_batch_case(
        tmp_root=tmp_path / "clickhouse-retryable",
        destination=retryable_destination,
        run_id="clickhouse-retryable",
        text="clickhouse retryable",
    )
    retryable_receipt = read_json_dict(
        retryable_batch.out_root / retryable_batch.sha256 / "delivery_receipt.json"
    )

    payload = json.loads(batch_row["payload_json"])
    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    payload_run_meta = as_dict(payload["run_meta"])
    payload_provenance = as_dict(payload_run_meta["provenance"])
    bad_auth_summary = as_dict(bad_auth_receipt["summary"])
    missing_table_summary = as_dict(missing_table_receipt["summary"])
    retryable_summary = as_dict(retryable_receipt["summary"])

    assert batch_by_destination["clickhouse"] == {
        "total": 1,
        "ok": 1,
        "failed": 0,
    }
    assert payload_provenance["delivery_origin"] == "primary"
    assert payload_provenance["execution_mode"] == "batch"
    assert bad_auth_summary["failure_retryability"] == "non_retryable"
    assert bad_auth_summary["failure_kind"] == "client_error"
    assert missing_table_summary["failure_retryability"] == "non_retryable"
    assert missing_table_summary["failure_kind"] == "constraint"
    assert retryable_summary["failure_retryability"] == "retryable"
    assert retryable_summary["failure_kind"] == "connection"
    del worker
