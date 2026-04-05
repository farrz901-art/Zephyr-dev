from __future__ import annotations

import json

import pytest

from zephyr_ingest import cli
from zephyr_ingest.config.constants import UNS_API_KEY_ENV_NAMES


def test_spec_list(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["spec", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    obj = json.loads(out)
    ids = obj["spec_ids"]
    assert "destination.webhook.v1" in ids
    assert "destination.kafka.v1" in ids
    assert "destination.weaviate.v1" in ids
    assert "backend.uns_api.v1" in ids


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
