from __future__ import annotations

from pathlib import Path

import pytest

from zephyr_ingest import cli
from zephyr_ingest.config.constants import UNS_API_KEY_ENV_NAMES, WEAVIATE_API_KEY_ENV_NAMES
from zephyr_ingest.spec.registry import get_spec
from zephyr_ingest.spec.toml_template import render_config_init_toml_v1


def test_config_init_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["config", "init"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "schema_version = 1" in out
    assert "[run]" in out
    assert 'backend = "local"' in out


def test_config_init_writes_file(tmp_path: Path) -> None:
    out_path = tmp_path / "cfg.toml"
    rc = cli.main(["config", "init", "--out", str(out_path)])
    assert rc == 0
    text = out_path.read_text(encoding="utf-8")
    assert "schema_version = 1" in text
    assert "[retry]" in text


def test_config_init_refuses_overwrite(tmp_path: Path) -> None:
    out_path = tmp_path / "cfg.toml"
    out_path.write_text("x", encoding="utf-8")
    rc = cli.main(["config", "init", "--out", str(out_path)])
    assert rc == 2


def test_config_init_only_weaviate_limits_blocks(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["config", "init", "--only", "weaviate"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "# [destinations.weaviate]" in out
    assert "# [destinations.kafka]" not in out
    assert "# [destinations.webhook]" not in out


def test_config_init_only_excludes_uns_api_hint(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["config", "init", "--only", "kafka"])
    assert rc == 0
    out = capsys.readouterr().out
    # uns-api hint header should be absent unless --only uns-api (or default all)
    assert 'When backend = "uns-api", configure the following:' not in out


def test_config_init_only_includes_uns_api_hint_when_requested(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = cli.main(["config", "init", "--only", "uns-api"])
    assert rc == 0
    out = capsys.readouterr().out
    assert 'When backend = "uns-api", configure the following:' in out


def test_config_init_contains_spec_driven_fields() -> None:
    """
    Verify the generated template includes fields from spec registry.
    """
    toml = render_config_init_toml_v1()

    # schema_version
    assert "schema_version = 1" in toml

    # [run] section
    assert "[run]" in toml
    assert 'backend = "local"' in toml
    assert "strategy" in toml

    # [retry] section
    assert "[retry]" in toml
    assert "enabled = true" in toml
    assert "max_attempts = 3" in toml

    # destinations as commented blocks
    assert "# [destinations.webhook]" in toml
    assert "# [destinations.kafka]" in toml
    assert "# [destinations.weaviate]" in toml
    assert "# [destinations.s3]" in toml
    assert "# [destinations.opensearch]" in toml
    assert "# [destinations.clickhouse]" in toml


def test_config_init_includes_env_hints_for_secrets() -> None:
    """
    Verify that secret fields include ENV injection hints.
    """
    toml = render_config_init_toml_v1()

    # UNS API key env hint
    for env in UNS_API_KEY_ENV_NAMES:
        assert env in toml, f"Expected {env} in template"

    # Weaviate API key env hint
    for env in WEAVIATE_API_KEY_ENV_NAMES:
        assert env in toml, f"Expected {env} in template"


def test_config_init_includes_spec_defaults() -> None:
    """
    Verify that spec default values appear in the template.
    """
    toml = render_config_init_toml_v1()

    # Check uns-api defaults
    uns_spec = get_spec(spec_id="backend.uns_api.v1")
    assert uns_spec is not None
    for f in uns_spec["fields"]:
        if "default" in f and f["name"] != "backend.kind":
            default = f["default"]
            if isinstance(default, str):
                assert default in toml or f'"{default}"' in toml

    # Check webhook defaults
    webhook_spec = get_spec(spec_id="destination.webhook.v1")
    assert webhook_spec is not None
    for f in webhook_spec["fields"]:
        if "default" in f:
            default = f["default"]
            assert str(default) in toml


def test_config_init_weaviate_fields_present() -> None:
    """Verify weaviate-specific fields are in template."""
    toml = render_config_init_toml_v1()
    assert "collection" in toml
    assert "http_host" in toml
    assert "grpc_port" in toml
    assert "skip_init_checks" in toml
