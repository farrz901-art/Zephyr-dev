from __future__ import annotations

import json

import pytest

from zephyr_ingest import cli


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
