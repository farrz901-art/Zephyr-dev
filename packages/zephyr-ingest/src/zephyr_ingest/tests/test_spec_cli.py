from __future__ import annotations

import json

import pytest

from zephyr_ingest import cli
from zephyr_ingest.config.constants import UNS_API_KEY_ENV_NAMES
from zephyr_ingest.spec.registry import get_spec

SOURCE_SPEC_IDS = (
    "source.uns.http_document.v1",
    "source.uns.s3_document.v1",
    "source.uns.git_document.v1",
    "source.uns.google_drive_document.v1",
    "source.uns.confluence_document.v1",
    "source.it.http_json_cursor.v1",
    "source.it.postgresql_incremental.v1",
    "source.it.clickhouse_incremental.v1",
    "source.it.kafka_partition_offset.v1",
    "source.it.mongodb_incremental.v1",
)


def test_spec_list(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["spec", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    obj = json.loads(out)
    ids = obj["spec_ids"]
    assert "destination.webhook.v1" in ids
    assert "destination.kafka.v1" in ids
    assert "destination.weaviate.v1" in ids
    assert "destination.s3.v1" in ids
    assert "destination.opensearch.v1" in ids
    assert "destination.clickhouse.v1" in ids
    assert "destination.mongodb.v1" in ids
    assert "destination.loki.v1" in ids
    assert "destination.sqlite.v1" in ids
    assert "backend.uns_api.v1" in ids
    for spec_id in SOURCE_SPEC_IDS:
        assert spec_id in ids


def test_spec_show_zephyr(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["spec", "show", "--id", "destination.kafka.v1", "--format", "zephyr"])
    assert rc == 0
    out = capsys.readouterr().out
    obj = json.loads(out)
    assert obj["id"] == "destination.kafka.v1"
    assert obj["version"] == 1
    assert isinstance(obj["fields"], list)
    assert any(f["name"] == "destinations.kafka.brokers" for f in obj["fields"])


def test_spec_show_jsonschema(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["spec", "show", "--id", "destination.weaviate.v1", "--format", "jsonschema"])
    assert rc == 0
    out = capsys.readouterr().out
    obj = json.loads(out)
    assert obj["type"] == "object"
    props = obj["properties"]
    assert "destinations.weaviate.collection" in props


def test_spec_show_unknown_id_returns_2() -> None:
    rc = cli.main(["spec", "show", "--id", "nope", "--format", "zephyr"])
    assert rc == 2


def test_spec_show_toml_destination_kafka(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["spec", "show", "--id", "destination.kafka.v1", "--format", "toml"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "[destinations.kafka]" in out
    assert "topic" in out
    assert "brokers" in out
    assert "flush_timeout_s" in out
    assert "max_inflight" in out
    assert "rate_limit" in out


def test_spec_show_toml_backend_uns_api_includes_env_hint(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = cli.main(["spec", "show", "--id", "backend.uns_api.v1", "--format", "toml"])
    assert rc == 0
    out = capsys.readouterr().out
    # no [run] header by design (snippet is meant to be pasted into [run])
    assert 'backend = "uns-api"' in out
    assert "uns_api_url" in out
    assert "uns_api_timeout_s" in out
    assert "uns_api_key" in out
    for env in UNS_API_KEY_ENV_NAMES:
        assert env in out


def test_spec_show_toml_destination_mongodb(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["spec", "show", "--id", "destination.mongodb.v1", "--format", "toml"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "[destinations.mongodb]" in out
    assert "uri" in out
    assert "database" in out
    assert "collection" in out
    assert "write_mode" in out


def test_spec_show_toml_destination_loki(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["spec", "show", "--id", "destination.loki.v1", "--format", "toml"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "[destinations.loki]" in out
    assert "url" in out
    assert "stream" in out
    assert "tenant_id" in out


def test_spec_show_toml_destination_sqlite(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["spec", "show", "--id", "destination.sqlite.v1", "--format", "toml"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "[destinations.sqlite]" in out
    assert "file_path" in out
    assert "table_name" in out
    assert "timeout_s" in out
    assert "mode" in out


@pytest.mark.parametrize("spec_id", SOURCE_SPEC_IDS)
def test_source_specs_are_not_empty(spec_id: str) -> None:
    spec = get_spec(spec_id=spec_id)
    assert spec is not None
    assert spec["kind"] == "source"
    assert len(spec["fields"]) > 0
    assert any(field["required"] for field in spec["fields"])


@pytest.mark.parametrize(
    ("spec_id", "required_tokens"),
    [
        ("source.uns.http_document.v1", ("[source]", 'kind = "http_document_v1"', "url")),
        ("source.uns.s3_document.v1", ("[source]", 'kind = "s3_document_v1"', "bucket", "key")),
        ("source.uns.git_document.v1", ("[source]", 'kind = "git_document_v1"', "repo_root")),
        (
            "source.uns.google_drive_document.v1",
            ("[source]", 'kind = "google_drive_document_v1"', "file_id", "access_token"),
        ),
        (
            "source.uns.confluence_document.v1",
            ("[source]", 'kind = "confluence_document_v1"', "site_url", "page_id"),
        ),
        (
            "source.it.http_json_cursor.v1",
            ("[source]", 'kind = "http_json_cursor_v1"', "stream", "url"),
        ),
        (
            "source.it.postgresql_incremental.v1",
            ("[source]", 'kind = "postgresql_incremental_v1"', "dsn", "cursor_column"),
        ),
        (
            "source.it.clickhouse_incremental.v1",
            ("[source]", 'kind = "clickhouse_incremental_v1"', "url", "cursor_column"),
        ),
        (
            "source.it.kafka_partition_offset.v1",
            ("[source]", 'kind = "kafka_partition_offset_v1"', "brokers", "topic"),
        ),
        (
            "source.it.mongodb_incremental.v1",
            ("[source]", 'kind = "mongodb_incremental_v1"', "uri", "cursor_field"),
        ),
    ],
)
def test_spec_show_toml_sources(
    capsys: pytest.CaptureFixture[str], spec_id: str, required_tokens: tuple[str, ...]
) -> None:
    rc = cli.main(["spec", "show", "--id", spec_id, "--format", "toml"])
    assert rc == 0
    out = capsys.readouterr().out
    for token in required_tokens:
        assert token in out


@pytest.mark.parametrize(
    ("spec_id", "field_name"),
    [
        ("source.uns.google_drive_document.v1", "source.access_token"),
        ("source.uns.confluence_document.v1", "source.access_token"),
        ("source.uns.s3_document.v1", "source.secret_key"),
        ("source.it.postgresql_incremental.v1", "source.dsn"),
        ("source.it.clickhouse_incremental.v1", "source.password"),
        ("source.it.mongodb_incremental.v1", "source.uri"),
    ],
)
def test_source_secret_fields_are_marked_secret(spec_id: str, field_name: str) -> None:
    spec = get_spec(spec_id=spec_id)
    assert spec is not None
    field = next(field for field in spec["fields"] if field["name"] == field_name)
    assert field.get("secret") is True
