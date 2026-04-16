from __future__ import annotations

import json
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

import pytest

from zephyr_ingest._internal.kafka_producer import make_kafka_producer
from zephyr_ingest.delivery_idempotency import normalize_weaviate_delivery_object_id
from zephyr_ingest.destinations.kafka import KafkaDestination
from zephyr_ingest.destinations.loki import LokiDestination
from zephyr_ingest.destinations.mongodb import MongoDBDestination
from zephyr_ingest.destinations.weaviate import WeaviateDestination
from zephyr_ingest.testing.p45 import LoadedP45Env
from zephyr_ingest.tests._p45_destination_wave1_helpers import (
    as_dict,
    make_text_document,
    read_json_dict,
    run_batch_case,
    run_worker_case,
)
from zephyr_ingest.tests._p45_destination_wave2_helpers import (
    LiveWeaviateCollection,
    MongoShellCollection,
    consume_kafka_records,
    current_timestamp_utc,
    drop_mongodb_collection,
    ensure_weaviate_collection,
    find_mongodb_document,
    get_weaviate_object,
    kafka_topic_name,
    list_loki_series,
    loki_query_streams,
    run_batch_case_with_timestamp,
    run_worker_case_with_timestamp,
    weaviate_base_url,
    weaviate_collection_name,
)


@pytest.mark.auth_service_live
def test_p45_wave2_kafka_live_success_failures_and_worker_consistency(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    brokers = p45_env.require("ZEPHYR_P45_KAFKA_BROKERS")
    topic = kafka_topic_name(prefix="zephyr_p45_m3_kafka")
    destination = KafkaDestination(topic=topic, producer=make_kafka_producer(brokers=brokers))
    batch = run_batch_case(
        tmp_root=tmp_path / "kafka-batch",
        destination=destination,
        run_id="kafka-batch",
        text="kafka success batch",
    )
    worker_doc = make_text_document(
        root=tmp_path / "kafka-worker-docs",
        name="worker.txt",
        text="kafka success worker",
    )
    worker = run_worker_case(
        tmp_root=tmp_path / "kafka-worker",
        destination=destination,
        run_id="kafka-worker",
        doc=worker_doc,
    )
    records = consume_kafka_records(brokers=brokers, topic=topic, expected_count=2)

    retryable_destination = KafkaDestination(
        topic=kafka_topic_name(prefix="zephyr_p45_m3_kafka_retryable"),
        producer=make_kafka_producer(brokers="127.0.0.1:59093"),
        flush_timeout_s=2.0,
    )
    retryable_batch = run_batch_case(
        tmp_root=tmp_path / "kafka-retryable",
        destination=retryable_destination,
        run_id="kafka-retryable",
        text="kafka retryable failure",
    )
    retryable_receipt = read_json_dict(
        retryable_batch.out_root / retryable_batch.sha256 / "delivery_receipt.json"
    )

    invalid_topic_destination = KafkaDestination(
        topic="bad\x00topic",
        producer=make_kafka_producer(brokers=brokers),
    )
    invalid_topic_batch = run_batch_case(
        tmp_root=tmp_path / "kafka-invalid-topic",
        destination=invalid_topic_destination,
        run_id="kafka-invalid-topic",
        text="kafka invalid topic",
    )
    invalid_topic_receipt = read_json_dict(
        invalid_topic_batch.out_root / invalid_topic_batch.sha256 / "delivery_receipt.json"
    )

    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    batch_payload = records[0].value
    worker_payload = records[1].value
    batch_run_meta = as_dict(batch_payload["run_meta"])
    worker_run_meta = as_dict(worker_payload["run_meta"])
    batch_provenance = as_dict(batch_run_meta["provenance"])
    worker_provenance = as_dict(worker_run_meta["provenance"])
    retryable_summary = as_dict(retryable_receipt["summary"])
    invalid_topic_summary = as_dict(invalid_topic_receipt["summary"])

    assert batch_by_destination["kafka"] == {"total": 1, "ok": 1, "failed": 0}
    assert batch_provenance["delivery_origin"] == "primary"
    assert batch_provenance["execution_mode"] == "batch"
    assert worker_provenance["delivery_origin"] == "primary"
    assert worker_provenance["execution_mode"] == "worker"
    assert retryable_summary["failure_retryability"] == "retryable"
    assert retryable_summary["failure_kind"] == "timeout"
    assert invalid_topic_summary["failure_retryability"] == "non_retryable"
    assert invalid_topic_summary["failure_kind"] == "client_error"
    del worker


@pytest.mark.auth_service_live
def test_p45_wave2_weaviate_live_success_failures_and_worker_consistency(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    api_key = p45_env.require("ZEPHYR_P45_WEAVIATE_API_KEY")
    base_url = weaviate_base_url(p45_env)
    collection_name = weaviate_collection_name()
    ensure_weaviate_collection(base_url=base_url, api_key=api_key, collection_name=collection_name)

    destination = WeaviateDestination(
        collection_name=collection_name,
        collection=LiveWeaviateCollection(
            base_url=base_url,
            collection_name=collection_name,
            api_key=api_key,
        ),
    )
    batch = run_batch_case(
        tmp_root=tmp_path / "weaviate-batch",
        destination=destination,
        run_id="weaviate-batch",
        text="weaviate success batch",
    )
    worker_doc = make_text_document(
        root=tmp_path / "weaviate-worker-docs",
        name="worker.txt",
        text="weaviate success worker",
    )
    worker = run_worker_case(
        tmp_root=tmp_path / "weaviate-worker",
        destination=destination,
        run_id="weaviate-worker",
        doc=worker_doc,
    )
    batch_object = get_weaviate_object(
        base_url=base_url,
        api_key=api_key,
        collection_name=collection_name,
        object_id=normalize_weaviate_delivery_object_id(sha256=batch.sha256),
    )
    worker_object = get_weaviate_object(
        base_url=base_url,
        api_key=api_key,
        collection_name=collection_name,
        object_id=normalize_weaviate_delivery_object_id(sha256=worker.sha256),
    )

    invalid_class_name = "Bad-Class"
    config_failure_destination = WeaviateDestination(
        collection_name=invalid_class_name,
        collection=LiveWeaviateCollection(
            base_url=base_url,
            collection_name=invalid_class_name,
            api_key=api_key,
        ),
    )
    config_failure_batch = run_batch_case(
        tmp_root=tmp_path / "weaviate-missing-class",
        destination=config_failure_destination,
        run_id="weaviate-missing-class",
        text="weaviate missing class",
    )
    config_failure_receipt = read_json_dict(
        config_failure_batch.out_root / config_failure_batch.sha256 / "delivery_receipt.json"
    )

    bad_auth_destination = WeaviateDestination(
        collection_name=collection_name,
        collection=LiveWeaviateCollection(
            base_url=base_url,
            collection_name=collection_name,
            api_key="wrong-api-key",
        ),
    )
    bad_auth_batch = run_batch_case(
        tmp_root=tmp_path / "weaviate-bad-auth",
        destination=bad_auth_destination,
        run_id="weaviate-bad-auth",
        text="weaviate bad auth",
    )
    bad_auth_receipt = read_json_dict(
        bad_auth_batch.out_root / bad_auth_batch.sha256 / "delivery_receipt.json"
    )

    parsed = urlsplit(base_url)
    retryable_destination = WeaviateDestination(
        collection_name=collection_name,
        collection=LiveWeaviateCollection(
            base_url=f"{parsed.scheme}://{parsed.hostname}:{(parsed.port or 0) + 1}",
            collection_name=collection_name,
            api_key=api_key,
        ),
    )
    retryable_batch = run_batch_case(
        tmp_root=tmp_path / "weaviate-retryable",
        destination=retryable_destination,
        run_id="weaviate-retryable",
        text="weaviate retryable",
    )
    retryable_receipt = read_json_dict(
        retryable_batch.out_root / retryable_batch.sha256 / "delivery_receipt.json"
    )

    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    batch_properties = as_dict(batch_object["properties"])
    worker_properties = as_dict(worker_object["properties"])
    config_failure_summary = as_dict(config_failure_receipt["summary"])
    bad_auth_summary = as_dict(bad_auth_receipt["summary"])
    retryable_summary = as_dict(retryable_receipt["summary"])

    assert batch_by_destination["weaviate"] == {"total": 1, "ok": 1, "failed": 0}
    assert batch_properties["delivery_origin"] == "primary"
    assert batch_properties["execution_mode"] == "batch"
    assert worker_properties["delivery_origin"] == "primary"
    assert worker_properties["execution_mode"] == "worker"
    assert config_failure_summary["failure_retryability"] == "non_retryable"
    assert config_failure_summary["failure_kind"] == "constraint"
    assert bad_auth_summary["failure_retryability"] == "non_retryable"
    assert bad_auth_summary["failure_kind"] == "client_error"
    assert retryable_summary["failure_retryability"] == "retryable"
    assert retryable_summary["failure_kind"] in {"connection", "server_error"}
    del worker


@pytest.mark.auth_service_live
def test_p45_wave2_mongodb_live_success_failures_and_worker_consistency(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    uri = p45_env.require("ZEPHYR_P45_MONGODB_URI")
    database = p45_env.require("ZEPHYR_P45_MONGODB_DATABASE")
    password = p45_env.require("ZEPHYR_P45_MONGODB_PASSWORD")
    mongo_port = p45_env.require("ZEPHYR_P45_MONGODB_PORT")
    collection = f"delivery_records_m3_{tmp_path.name[-6:]}"
    drop_mongodb_collection(env=p45_env, uri=uri, database=database, collection=collection)

    destination = MongoDBDestination(
        uri=uri,
        database=database,
        collection=collection,
        collection_obj=MongoShellCollection(
            env=p45_env,
            uri=uri,
            database=database,
            collection=collection,
        ),
    )
    batch = run_batch_case(
        tmp_root=tmp_path / "mongodb-batch",
        destination=destination,
        run_id="mongodb-batch",
        text="mongodb success batch",
    )
    worker = run_worker_case(
        tmp_root=tmp_path / "mongodb-worker",
        destination=destination,
        run_id="mongodb-worker",
        doc=batch.doc,
    )
    batch_doc = find_mongodb_document(
        env=p45_env,
        uri=uri,
        database=database,
        collection=collection,
        document_id=f"{batch.sha256}:mongodb-batch",
    )
    worker_doc = find_mongodb_document(
        env=p45_env,
        uri=uri,
        database=database,
        collection=collection,
        document_id=f"{worker.sha256}:mongodb-worker",
    )

    bad_auth_uri = uri.replace(password, "wrong-password", 1)
    bad_auth_destination = MongoDBDestination(
        uri=bad_auth_uri,
        database=database,
        collection=collection,
        collection_obj=MongoShellCollection(
            env=p45_env,
            uri=bad_auth_uri,
            database=database,
            collection=collection,
        ),
    )
    bad_auth_batch = run_batch_case(
        tmp_root=tmp_path / "mongodb-bad-auth",
        destination=bad_auth_destination,
        run_id="mongodb-bad-auth",
        text="mongodb bad auth",
    )
    bad_auth_receipt = read_json_dict(
        bad_auth_batch.out_root / bad_auth_batch.sha256 / "delivery_receipt.json"
    )

    retryable_uri = uri.replace(f"127.0.0.1:{mongo_port}", f"127.0.0.1:{int(mongo_port) + 1}")
    retryable_destination = MongoDBDestination(
        uri=retryable_uri,
        database=database,
        collection=collection,
        collection_obj=MongoShellCollection(
            env=p45_env,
            uri=retryable_uri,
            database=database,
            collection=collection,
        ),
    )
    retryable_batch = run_batch_case(
        tmp_root=tmp_path / "mongodb-retryable",
        destination=retryable_destination,
        run_id="mongodb-retryable",
        text="mongodb retryable",
    )
    retryable_receipt = read_json_dict(
        retryable_batch.out_root / retryable_batch.sha256 / "delivery_receipt.json"
    )

    assert batch_doc is not None
    assert worker_doc is not None
    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    batch_payload = as_dict(batch_doc["payload"])
    worker_payload = as_dict(worker_doc["payload"])
    batch_run_meta = as_dict(batch_payload["run_meta"])
    worker_run_meta = as_dict(worker_payload["run_meta"])
    batch_provenance = as_dict(batch_run_meta["provenance"])
    worker_provenance = as_dict(worker_run_meta["provenance"])
    bad_auth_summary = as_dict(bad_auth_receipt["summary"])
    retryable_summary = as_dict(retryable_receipt["summary"])

    assert batch_by_destination["mongodb"] == {"total": 1, "ok": 1, "failed": 0}
    assert batch_provenance["delivery_origin"] == "primary"
    assert batch_provenance["execution_mode"] == "batch"
    assert worker_provenance["delivery_origin"] == "primary"
    assert worker_provenance["execution_mode"] == "worker"
    assert bad_auth_summary["failure_retryability"] == "non_retryable"
    assert bad_auth_summary["failure_kind"] == "client_error"
    assert retryable_summary["failure_retryability"] == "retryable"
    assert retryable_summary["failure_kind"] == "connection"
    del worker


@pytest.mark.auth_service_live
def test_p45_wave2_loki_live_success_failures_and_worker_consistency(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    url = p45_env.require("ZEPHYR_P45_LOKI_URL")
    tenant_id = p45_env.require("ZEPHYR_P45_LOKI_TENANT_ID")
    stream = f"zephyr_p45_m3_{tmp_path.name[-6:]}"
    timestamp_utc = current_timestamp_utc()
    destination = LokiDestination(url=url, stream=stream, tenant_id=tenant_id)
    batch = run_batch_case_with_timestamp(
        tmp_root=tmp_path / "loki-batch",
        destination=destination,
        run_id="loki-batch",
        text="loki success batch",
        timestamp_utc=timestamp_utc,
    )
    worker = run_worker_case_with_timestamp(
        tmp_root=tmp_path / "loki-worker",
        destination=destination,
        run_id="loki-worker",
        doc=batch.doc,
        timestamp_utc=timestamp_utc,
    )
    batch_streams = loki_query_streams(
        url=url,
        tenant_id=tenant_id,
        selector=f'{{zephyr_delivery_identity="{batch.sha256}:loki-batch"}}',
        timestamp_utc=timestamp_utc,
    )
    worker_streams = loki_query_streams(
        url=url,
        tenant_id=tenant_id,
        selector=f'{{zephyr_delivery_identity="{worker.sha256}:loki-worker"}}',
        timestamp_utc=timestamp_utc,
    )

    auth_failure_destination = LokiDestination(url=url, stream=stream, tenant_id=None)
    auth_failure_batch = run_batch_case_with_timestamp(
        tmp_root=tmp_path / "loki-auth-failure",
        destination=auth_failure_destination,
        run_id="loki-auth-failure",
        text="loki auth failure",
        timestamp_utc=timestamp_utc,
    )
    auth_failure_receipt = read_json_dict(
        auth_failure_batch.out_root / auth_failure_batch.sha256 / "delivery_receipt.json"
    )

    retryable_destination = LokiDestination(
        url="http://127.0.0.1:53101",
        stream=stream,
        tenant_id=tenant_id,
    )
    retryable_batch = run_batch_case_with_timestamp(
        tmp_root=tmp_path / "loki-retryable",
        destination=retryable_destination,
        run_id="loki-retryable",
        text="loki retryable failure",
        timestamp_utc=timestamp_utc,
    )
    retryable_receipt = read_json_dict(
        retryable_batch.out_root / retryable_batch.sha256 / "delivery_receipt.json"
    )

    batch_delivery = as_dict(batch.batch_report["delivery"])
    batch_by_destination = as_dict(batch_delivery["by_destination"])
    batch_values = as_dict(batch_streams[0])
    worker_values = as_dict(worker_streams[0])
    batch_lines_obj = batch_values["values"]
    worker_lines_obj = worker_values["values"]
    assert isinstance(batch_lines_obj, list)
    assert isinstance(worker_lines_obj, list)
    batch_lines = cast(list[object], batch_lines_obj)
    worker_lines = cast(list[object], worker_lines_obj)
    assert isinstance(batch_lines[0], list)
    assert isinstance(worker_lines[0], list)
    batch_line_text = cast(str, cast(list[object], batch_lines[0])[1])
    worker_line_text = cast(str, cast(list[object], worker_lines[0])[1])
    batch_line = json.loads(batch_line_text)
    worker_line = json.loads(worker_line_text)
    batch_run_meta = as_dict(as_dict(batch_line)["run_meta"])
    worker_run_meta = as_dict(as_dict(worker_line)["run_meta"])
    batch_provenance = as_dict(batch_run_meta["provenance"])
    worker_provenance = as_dict(worker_run_meta["provenance"])
    auth_failure_summary = as_dict(auth_failure_receipt["summary"])
    retryable_summary = as_dict(retryable_receipt["summary"])
    series = list_loki_series(
        url=url,
        tenant_id=tenant_id,
        selector=f'{{zephyr_delivery_identity="{batch.sha256}:loki-batch"}}',
    )

    assert batch_by_destination["loki"] == {"total": 1, "ok": 1, "failed": 0}
    assert batch_provenance["delivery_origin"] == "primary"
    assert batch_provenance["execution_mode"] == "batch"
    assert worker_provenance["delivery_origin"] == "primary"
    assert worker_provenance["execution_mode"] == "worker"
    assert auth_failure_summary["failure_retryability"] == "non_retryable"
    assert auth_failure_summary["failure_kind"] == "client_error"
    assert retryable_summary["failure_retryability"] == "retryable"
    assert retryable_summary["failure_kind"] == "connection"
    assert len(series) == 1
    del worker
