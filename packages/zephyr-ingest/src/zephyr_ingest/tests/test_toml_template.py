from __future__ import annotations

import tomllib

from zephyr_ingest.spec.toml_template import render_config_init_toml_v1


def test_rendered_toml_is_valid_toml() -> None:
    """
    The generated template must be valid TOML (parseable).
    This catches syntax errors in the renderer.
    """
    toml_str = render_config_init_toml_v1()

    # Remove comment lines to parse the active TOML
    active_lines = [line for line in toml_str.splitlines() if not line.strip().startswith("#")]
    active_toml = "\n".join(active_lines)

    # Should parse without error
    parsed = tomllib.loads(active_toml)

    # Validate structure
    assert parsed.get("schema_version") == 1
    assert "run" in parsed
    assert "retry" in parsed


def test_rendered_toml_run_section_has_backend_and_strategy() -> None:
    toml_str = render_config_init_toml_v1()

    active_lines = [line for line in toml_str.splitlines() if not line.strip().startswith("#")]
    active_toml = "\n".join(active_lines)
    parsed = tomllib.loads(active_toml)

    assert parsed["run"]["backend"] == "local"
    assert parsed["run"]["strategy"] == "auto"


def test_rendered_toml_retry_section_has_defaults() -> None:
    toml_str = render_config_init_toml_v1()

    active_lines = [line for line in toml_str.splitlines() if not line.strip().startswith("#")]
    active_toml = "\n".join(active_lines)
    parsed = tomllib.loads(active_toml)

    assert parsed["retry"]["enabled"] is True
    assert parsed["retry"]["max_attempts"] == 3
    assert parsed["retry"]["base_backoff_ms"] == 200
    assert parsed["retry"]["max_backoff_ms"] == 5000
