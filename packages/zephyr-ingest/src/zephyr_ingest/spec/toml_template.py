from __future__ import annotations

from zephyr_ingest.spec.registry import get_spec
from zephyr_ingest.spec.types import ConnectorSpecV1, SpecFieldV1


def _format_value(val: str | int | float | bool | None) -> str:
    """Format a value for TOML output."""
    if val is None:
        return '""'
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, str):
        return f'"{val}"'
    return str(val)


def _field_to_toml_key(field: SpecFieldV1) -> str:
    """
    Extract the last part of the dotted field name as TOML key.
    e.g. "destinations.webhook.url" -> "url"
         "backend.kind" -> "backend" (special case for [run] section)
    """
    name = field["name"]
    parts = name.split(".")
    return parts[-1]


def _render_field_line(
    *,
    field: SpecFieldV1,
    commented: bool = False,
    use_placeholder: bool = False,
) -> list[str]:
    """
    Render a single field as TOML line(s) with optional help/env hint comments.
    """
    lines: list[str] = []
    prefix = "# " if commented else ""
    key = _field_to_toml_key(field)

    # Add help as comment above the field
    help_text = field.get("help")
    if help_text and commented:
        lines.append(f"# {help_text}")

    # Add env hint for secrets
    env_names = field.get("env_names", [])
    if field.get("secret", False) and env_names:
        env_hint = ", ".join(env_names)
        lines.append(f"# Prefer ENV injection: {env_hint}")

    # Determine value
    if use_placeholder:
        value_str = '"..."'
    elif "default" in field:
        value_str = _format_value(field["default"])
    else:
        value_str = '"..."'

    lines.append(f"{prefix}{key} = {value_str}")
    return lines


def _render_run_section() -> list[str]:
    """
    Render the [run] section.
    - backend.kind from spec (default: local)
    - strategy (hardcoded, not from spec since it's in _add_runlike_args)
    - uns-api fields as comments (only relevant when backend="uns-api")
    """
    lines: list[str] = []
    lines.append("[run]")

    # backend.kind
    uns_spec = get_spec(spec_id="backend.uns_api.v1")
    if uns_spec:
        for f in uns_spec["fields"]:
            if f["name"] == "backend.kind":
                lines.append(f"backend = {_format_value(f.get('default', 'local'))}")
                break

    # strategy (not from spec, but from cli._add_runlike_args)
    lines.append('strategy = "auto"  # auto/fast/hi_res/ocr_only')
    lines.append("")

    # uns-api fields as comments
    lines.append('# When backend = "uns-api", configure the following:')
    if uns_spec:
        for f in uns_spec["fields"]:
            if f["name"] == "backend.kind":
                continue  # already handled
            field_lines = _render_field_line(field=f, commented=True)
            lines.extend(field_lines)

    return lines


def _render_retry_section() -> list[str]:
    """
    Render the [retry] section with hardcoded defaults.
    (These are not in spec since they're runner-level, not connector-level.)
    """
    return [
        "",
        "[retry]",
        "enabled = true",
        "max_attempts = 3",
        "base_backoff_ms = 200",
        "max_backoff_ms = 5000",
    ]


def _render_destination_section(*, spec: ConnectorSpecV1, table_name: str) -> list[str]:
    """
    Render a destination section as commented block.
    e.g. # [destinations.webhook]
    """
    lines: list[str] = []
    lines.append("")
    lines.append(f"# {spec['description']}")
    lines.append(f"# [{table_name}]")

    for f in spec["fields"]:
        # Skip fields that don't belong to this destination
        if not f["name"].startswith(table_name.replace("[", "").replace("]", "")):
            continue

        # Required fields get placeholder, optional get default
        use_placeholder = f["required"] and "default" not in f
        field_lines = _render_field_line(field=f, commented=True, use_placeholder=use_placeholder)
        lines.extend(field_lines)

    return lines


def render_config_init_toml_v1() -> str:
    """
    Render a complete config init TOML template from spec registry.

    Structure:
    - schema_version = 1
    - [run]: backend, strategy, uns-api fields (commented)
    - [retry]: defaults
    - [destinations.webhook]: commented block
    - [destinations.kafka]: commented block
    - [destinations.weaviate]: commented block
    """
    lines: list[str] = []

    # Header
    lines.append("# Zephyr Ingest Configuration (auto-generated from spec)")
    lines.append("# See: zephyr-ingest config resolve --config <this-file>")
    lines.append("")
    lines.append("schema_version = 1")
    lines.append("")

    # [run]
    lines.extend(_render_run_section())

    # [retry]
    lines.extend(_render_retry_section())

    # Destinations header
    lines.append("")
    lines.append("# " + "=" * 50)
    lines.append("# Destinations (uncomment to enable)")
    lines.append("# " + "=" * 50)

    # Webhook
    webhook_spec = get_spec(spec_id="destination.webhook.v1")
    if webhook_spec:
        lines.extend(
            _render_destination_section(spec=webhook_spec, table_name="destinations.webhook")
        )

    # Kafka
    kafka_spec = get_spec(spec_id="destination.kafka.v1")
    if kafka_spec:
        lines.extend(_render_destination_section(spec=kafka_spec, table_name="destinations.kafka"))

    # Weaviate
    weaviate_spec = get_spec(spec_id="destination.weaviate.v1")
    if weaviate_spec:
        lines.extend(
            _render_destination_section(spec=weaviate_spec, table_name="destinations.weaviate")
        )

    lines.append("")
    return "\n".join(lines)
