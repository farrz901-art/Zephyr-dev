from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from zephyr_core import RunMetaV1
from zephyr_core.contracts.v1.run_meta import RunProvenanceV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest._internal.kafka_producer import make_kafka_producer
from zephyr_ingest.delivery_idempotency import normalize_weaviate_delivery_object_id
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.replay_delivery import (
    KafkaReplaySink,
    LokiReplaySink,
    MongoDBReplaySink,
    WeaviateReplaySink,
    replay_delivery_dlq,
)
from zephyr_ingest.testing.p45 import LoadedP45Env
from zephyr_ingest.tests._p45_destination_wave1_helpers import as_dict
from zephyr_ingest.tests._p45_destination_wave2_helpers import (
    LiveWeaviateCollection,
    MongoShellCollection,
    consume_kafka_records,
    count_mongodb_documents,
    current_timestamp_utc,
    drop_mongodb_collection,
    ensure_weaviate_collection,
    find_mongodb_document,
    get_weaviate_object,
    kafka_topic_name,
    list_loki_series,
    loki_query_streams,
    weaviate_base_url,
    weaviate_collection_name,
)


def _write_replay_record(*, out_root: Path, sha256: str, run_id: str, timestamp_utc: str) -> None:
    source_dir = out_root / sha256
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "elements.json").write_text(json.dumps([{"element_id": "e1"}]), encoding="utf-8")
    (source_dir / "normalized.txt").write_text("wave2-replay", encoding="utf-8")
    write_delivery_dlq(
        out_root=out_root,
        sha256=sha256,
        meta=RunMetaV1(
            run_id=run_id,
            pipeline_version="p45-m3-wave2",
            timestamp_utc=timestamp_utc,
            schema_version=RUN_META_SCHEMA_VERSION,
            provenance=RunProvenanceV1(
                run_origin="intake",
                delivery_origin="primary",
                execution_mode="batch",
                task_id=sha256,
            ),
        ),
        receipt=DeliveryReceipt(
            destination="wave2",
            ok=False,
            details={
                "retryable": True,
                "failure_kind": "connection",
                "error_code": "ZE-DELIVERY-FAILED",
            },
        ),
    )


@pytest.mark.auth_recovery_drill
def test_p45_wave2_service_live_replay_and_duplicate_behavior_are_truthful(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    out_root = tmp_path / "replay-service-live"
    sha256 = "sha-wave2-replay"
    run_id = "wave2-replay"
    timestamp_utc = current_timestamp_utc()
    _write_replay_record(
        out_root=out_root,
        sha256=sha256,
        run_id=run_id,
        timestamp_utc=timestamp_utc,
    )

    brokers = p45_env.require("ZEPHYR_P45_KAFKA_BROKERS")
    kafka_topic = kafka_topic_name(prefix="zephyr_p45_m3_replay")
    kafka_sink = KafkaReplaySink(
        topic=kafka_topic,
        producer=make_kafka_producer(brokers=brokers),
    )
    kafka_stats = replay_delivery_dlq(
        out_root=out_root,
        sink=kafka_sink,
        dry_run=False,
        move_done=False,
    )
    kafka_stats_dup = replay_delivery_dlq(
        out_root=out_root,
        sink=kafka_sink,
        dry_run=False,
        move_done=False,
    )
    kafka_records = consume_kafka_records(brokers=brokers, topic=kafka_topic, expected_count=2)

    weaviate_api_key = p45_env.require("ZEPHYR_P45_WEAVIATE_API_KEY")
    weaviate_url = weaviate_base_url(p45_env)
    weaviate_collection = weaviate_collection_name(prefix="ZephyrP45M3Replay")
    ensure_weaviate_collection(
        base_url=weaviate_url,
        api_key=weaviate_api_key,
        collection_name=weaviate_collection,
    )
    weaviate_sink = WeaviateReplaySink(
        collection_name=weaviate_collection,
        collection=LiveWeaviateCollection(
            base_url=weaviate_url,
            collection_name=weaviate_collection,
            api_key=weaviate_api_key,
        ),
    )
    weaviate_stats = replay_delivery_dlq(
        out_root=out_root,
        sink=weaviate_sink,
        dry_run=False,
        move_done=False,
    )
    weaviate_stats_dup = replay_delivery_dlq(
        out_root=out_root,
        sink=weaviate_sink,
        dry_run=False,
        move_done=False,
    )
    weaviate_object = get_weaviate_object(
        base_url=weaviate_url,
        api_key=weaviate_api_key,
        collection_name=weaviate_collection,
        object_id=normalize_weaviate_delivery_object_id(sha256=sha256),
    )

    mongodb_uri = p45_env.require("ZEPHYR_P45_MONGODB_URI")
    mongodb_db = p45_env.require("ZEPHYR_P45_MONGODB_DATABASE")
    mongodb_collection = f"delivery_records_m3_replay_{tmp_path.name[-6:]}"
    drop_mongodb_collection(
        env=p45_env,
        uri=mongodb_uri,
        database=mongodb_db,
        collection=mongodb_collection,
    )
    mongodb_sink = MongoDBReplaySink(
        database=mongodb_db,
        collection=mongodb_collection,
        collection_obj=MongoShellCollection(
            env=p45_env,
            uri=mongodb_uri,
            database=mongodb_db,
            collection=mongodb_collection,
        ),
    )
    mongodb_stats = replay_delivery_dlq(
        out_root=out_root,
        sink=mongodb_sink,
        dry_run=False,
        move_done=False,
    )
    mongodb_stats_dup = replay_delivery_dlq(
        out_root=out_root,
        sink=mongodb_sink,
        dry_run=False,
        move_done=False,
    )
    mongodb_count = count_mongodb_documents(
        env=p45_env,
        uri=mongodb_uri,
        database=mongodb_db,
        collection=mongodb_collection,
        document_id=f"{sha256}:{run_id}",
    )
    mongodb_doc = find_mongodb_document(
        env=p45_env,
        uri=mongodb_uri,
        database=mongodb_db,
        collection=mongodb_collection,
        document_id=f"{sha256}:{run_id}",
    )

    loki_url = p45_env.require("ZEPHYR_P45_LOKI_URL")
    loki_tenant = p45_env.require("ZEPHYR_P45_LOKI_TENANT_ID")
    loki_stream = f"zephyr_p45_m3_replay_{tmp_path.name[-6:]}"
    loki_sink = LokiReplaySink(url=loki_url, stream=loki_stream, tenant_id=loki_tenant)
    loki_stats = replay_delivery_dlq(
        out_root=out_root,
        sink=loki_sink,
        dry_run=False,
        move_done=False,
    )
    loki_stats_dup = replay_delivery_dlq(
        out_root=out_root,
        sink=loki_sink,
        dry_run=False,
        move_done=False,
    )
    loki_series = list_loki_series(
        url=loki_url,
        tenant_id=loki_tenant,
        selector=f'{{zephyr_delivery_identity="{sha256}:{run_id}"}}',
    )
    loki_streams = loki_query_streams(
        url=loki_url,
        tenant_id=loki_tenant,
        selector=f'{{zephyr_delivery_identity="{sha256}:{run_id}"}}',
        timestamp_utc=timestamp_utc,
    )

    assert kafka_stats.succeeded == 1
    assert kafka_stats_dup.succeeded == 1
    assert len(kafka_records) == 2
    assert kafka_records[0].key == f"{sha256}:{run_id}"
    assert kafka_records[1].key == f"{sha256}:{run_id}"
    assert kafka_records[0].value["run_meta"] == kafka_records[1].value["run_meta"]

    assert weaviate_stats.succeeded == 1
    assert weaviate_stats_dup.failed == 1
    weaviate_properties = weaviate_object["properties"]
    assert isinstance(weaviate_properties, dict)
    assert weaviate_properties["delivery_origin"] == "replay"

    assert mongodb_stats.succeeded == 1
    assert mongodb_stats_dup.succeeded == 1
    assert mongodb_count == 1
    assert mongodb_doc is not None
    mongodb_payload = as_dict(mongodb_doc["payload"])
    mongodb_run_meta = as_dict(mongodb_payload["run_meta"])
    mongodb_provenance = as_dict(mongodb_run_meta["provenance"])
    assert mongodb_provenance["delivery_origin"] == "replay"

    assert loki_stats.succeeded == 1
    assert loki_stats_dup.succeeded == 1
    assert len(loki_series) == 1
    assert len(loki_streams) == 1
    loki_values_obj = loki_streams[0]["values"]
    assert isinstance(loki_values_obj, list)
    loki_values = cast(list[object], loki_values_obj)
    assert len(loki_values) == 1
