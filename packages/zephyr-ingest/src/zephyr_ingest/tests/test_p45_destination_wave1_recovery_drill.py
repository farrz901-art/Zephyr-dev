from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from zephyr_core import RunMetaV1
from zephyr_core.contracts.v1.run_meta import RunProvenanceV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_dlq import write_delivery_dlq
from zephyr_ingest.destinations.base import DeliveryReceipt
from zephyr_ingest.replay_delivery import (
    ClickHouseReplaySink,
    OpenSearchReplaySink,
    S3ReplaySink,
    WebhookReplaySink,
    replay_delivery_dlq,
)
from zephyr_ingest.testing.p45 import (
    LoadedP45Env,
    get_p45_opensearch_auth,
    get_p45_opensearch_url,
    get_p45_opensearch_verify_tls,
)
from zephyr_ingest.tests._p45_destination_wave1_helpers import (
    SigV4MinioClient,
    webhook_events,
    webhook_reset,
)


def _write_replay_record(*, out_root: Path, sha256: str, run_id: str) -> None:
    source_dir = out_root / sha256
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "elements.json").write_text(json.dumps([{"element_id": "e1"}]), encoding="utf-8")
    (source_dir / "normalized.txt").write_text("replay-text", encoding="utf-8")
    write_delivery_dlq(
        out_root=out_root,
        sha256=sha256,
        meta=RunMetaV1(
            run_id=run_id,
            pipeline_version="p45-m2-wave1",
            timestamp_utc="2026-04-15T00:00:00Z",
            schema_version=RUN_META_SCHEMA_VERSION,
            provenance=RunProvenanceV1(
                run_origin="intake",
                delivery_origin="primary",
                execution_mode="batch",
                task_id=sha256,
            ),
        ),
        receipt=DeliveryReceipt(
            destination="wave1",
            ok=False,
            details={
                "retryable": True,
                "failure_kind": "server_error",
                "error_code": "ZE-DELIVERY-FAILED",
            },
        ),
    )


@pytest.mark.auth_recovery_drill
def test_p45_wave1_service_live_replay_and_duplicate_replay_are_stable(
    tmp_path: Path,
    p45_env: LoadedP45Env,
) -> None:
    out_root = tmp_path / "replay-service-live"
    sha256 = "sha-wave1-replay"
    run_id = "wave1-replay"
    _write_replay_record(out_root=out_root, sha256=sha256, run_id=run_id)

    webhook_base = p45_env.require("ZEPHYR_P45_WEBHOOK_ECHO_URL").removesuffix("/ingest")
    webhook_reset(base_url=webhook_base)
    webhook_sink = WebhookReplaySink(url=p45_env.require("ZEPHYR_P45_WEBHOOK_ECHO_URL"))
    webhook_stats = replay_delivery_dlq(
        out_root=out_root, sink=webhook_sink, dry_run=False, move_done=False
    )
    webhook_stats_dup = replay_delivery_dlq(
        out_root=out_root, sink=webhook_sink, dry_run=False, move_done=False
    )
    webhook_payloads = webhook_events(base_url=webhook_base)

    s3_client = SigV4MinioClient(
        endpoint_url=p45_env.require("ZEPHYR_P45_S3_ENDPOINT"),
        access_key=p45_env.require("ZEPHYR_P45_S3_ACCESS_KEY"),
        secret_key=p45_env.require("ZEPHYR_P45_S3_SECRET_KEY"),
        region=p45_env.require("ZEPHYR_P45_S3_REGION"),
    )
    bucket = p45_env.require("ZEPHYR_P45_S3_BUCKET")
    s3_client.ensure_bucket(bucket=bucket)
    s3_sink = S3ReplaySink(bucket=bucket, prefix="p45-wave1/replay", client=s3_client)
    s3_stats = replay_delivery_dlq(out_root=out_root, sink=s3_sink, dry_run=False, move_done=False)
    s3_stats_dup = replay_delivery_dlq(
        out_root=out_root, sink=s3_sink, dry_run=False, move_done=False
    )
    s3_keys = s3_client.list_keys(bucket=bucket, prefix="p45-wave1/replay/")

    opensearch_base = get_p45_opensearch_url(p45_env).rstrip("/")
    opensearch_verify_tls = get_p45_opensearch_verify_tls(p45_env)
    opensearch_auth = get_p45_opensearch_auth(p45_env)
    opensearch_index = f"zephyr-p45-wave1-replay-{tmp_path.name[-6:]}"
    with httpx.Client(
        timeout=10.0,
        trust_env=False,
        verify=opensearch_verify_tls,
        auth=opensearch_auth,
    ) as client:
        client.delete(f"{opensearch_base}/{opensearch_index}")
        create_response = client.put(f"{opensearch_base}/{opensearch_index}", json={})
        create_response.raise_for_status()
    opensearch_sink = OpenSearchReplaySink(
        url=opensearch_base,
        index=opensearch_index,
        verify_tls=opensearch_verify_tls,
        username=None if opensearch_auth is None else opensearch_auth[0],
        password=None if opensearch_auth is None else opensearch_auth[1],
    )
    opensearch_stats = replay_delivery_dlq(
        out_root=out_root, sink=opensearch_sink, dry_run=False, move_done=False
    )
    opensearch_stats_dup = replay_delivery_dlq(
        out_root=out_root, sink=opensearch_sink, dry_run=False, move_done=False
    )
    with httpx.Client(
        timeout=10.0,
        trust_env=False,
        verify=opensearch_verify_tls,
        auth=opensearch_auth,
    ) as client:
        document = client.get(f"{opensearch_base}/{opensearch_index}/_doc/{sha256}:{run_id}").json()

    clickhouse_base = p45_env.require("ZEPHYR_P45_CLICKHOUSE_URL").rstrip("/")
    clickhouse_db = p45_env.require("ZEPHYR_P45_CLICKHOUSE_DATABASE")
    clickhouse_user = p45_env.require("ZEPHYR_P45_CLICKHOUSE_USERNAME")
    clickhouse_password = p45_env.require("ZEPHYR_P45_CLICKHOUSE_PASSWORD")
    clickhouse_table = "zephyr_p45_wave1_replay"
    create_query = (
        f"CREATE TABLE IF NOT EXISTS {clickhouse_db}.{clickhouse_table} ("
        "identity_key String, sha256 String, run_id String, schema_version UInt32, "
        "payload_json String, delivered_at_utc String"
        ") ENGINE = ReplacingMergeTree() ORDER BY identity_key"
    )
    with httpx.Client(timeout=10.0, trust_env=False) as client:
        client.post(
            clickhouse_base,
            params={"query": create_query},
            auth=(clickhouse_user, clickhouse_password),
        ).raise_for_status()
    clickhouse_sink = ClickHouseReplaySink(
        url=clickhouse_base,
        database=clickhouse_db,
        table=clickhouse_table,
        username=clickhouse_user,
        password=clickhouse_password,
    )
    clickhouse_stats = replay_delivery_dlq(
        out_root=out_root, sink=clickhouse_sink, dry_run=False, move_done=False
    )
    clickhouse_stats_dup = replay_delivery_dlq(
        out_root=out_root, sink=clickhouse_sink, dry_run=False, move_done=False
    )
    with httpx.Client(timeout=10.0, trust_env=False) as client:
        count_response = client.get(
            clickhouse_base,
            params={
                "query": (
                    f"SELECT countDistinct(identity_key) FROM {clickhouse_db}.{clickhouse_table} "
                    f"WHERE identity_key = '{sha256}:{run_id}'"
                )
            },
            auth=(clickhouse_user, clickhouse_password),
        )
        count_response.raise_for_status()

    assert webhook_stats.succeeded == 1
    assert webhook_stats_dup.succeeded == 1
    assert s3_stats.succeeded == 1
    assert s3_stats_dup.succeeded == 1
    assert opensearch_stats.succeeded == 1
    assert opensearch_stats_dup.succeeded == 1
    assert clickhouse_stats.succeeded == 1
    assert clickhouse_stats_dup.succeeded == 1
    assert len([event for event in webhook_payloads if event["path"] == "/ingest"]) == 2
    assert len(s3_keys) == 1
    assert document["_source"]["payload"]["run_meta"]["provenance"]["delivery_origin"] == "replay"
    assert count_response.text.strip() == "1"
