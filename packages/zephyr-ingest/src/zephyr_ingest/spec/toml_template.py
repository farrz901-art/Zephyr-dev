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


def _placeholder_for_type(field: SpecFieldV1) -> str:
    t = field["type"]
    if t == "bool":
        return "false"
    if t in ("int", "float"):
        return "0"
    return '"..."'


def _backend_field_to_run_key(name: str) -> str | None:
    # Spec keys -> config file [run] keys
    mapping: dict[str, str] = {
        "backend.kind": "backend",
        "backend.url": "uns_api_url",
        "backend.timeout_s": "uns_api_timeout_s",
        "backend.api_key": "uns_api_key",
    }
    return mapping.get(name)


def _field_to_toml_key(field: SpecFieldV1) -> str:
    """
    Extract the last part of the dotted field name as TOML key.
    e.g. "destinations.webhook.url" -> "url"
         "backend.kind" -> "backend" (special case for [run] section)
    """
    name = field["name"]
    parts = name.split(".")
    return parts[-1]


def render_spec_toml_snippet_v1(*, spec: ConnectorSpecV1) -> str:
    """
    Render a pasteable TOML snippet for a single spec.

    - destination.* specs -> render `[destinations.<name>]` section
    - backend.uns_api.v1 -> render lines intended for `[run]` table (no table header)
    """
    lines: list[str] = []
    lines.append(f"# Spec: {spec['id']}")
    lines.append(f"# {spec['description']}")
    lines.append("")

    if spec["kind"] == "destination":
        # spec.id: destination.<name>.v1
        parts = spec["id"].split(".")
        if len(parts) < 3:
            raise ValueError(f"invalid spec id: {spec['id']}")
        dest_name = parts[1]
        table = f"destinations.{dest_name}"
        lines.append(f"[{table}]")

        for f in spec["fields"]:
            # field names are flat dotted keys;
            # destination fields start with destinations.<dest_name>.
            prefix = f"destinations.{dest_name}."
            if not f["name"].startswith(prefix):
                continue

            help_text = f.get("help")
            if help_text is not None:
                lines.append(f"# {help_text}")

            if f.get("secret", False):
                envs = f.get("env_names", [])
                if envs:
                    lines.append("# Prefer ENV injection: " + ", ".join(envs))

            key = f["name"].split(".")[-1]
            v = _format_value(f["default"]) if "default" in f else _placeholder_for_type(f)
            lines.append(f"{key} = {v}")

        lines.append("")
        return "\n".join(lines)

    # backend spec
    if spec["id"] == "backend.uns_api.v1":
        lines.append("# Paste these into your [run] table:")
        for f in spec["fields"]:
            run_key = _backend_field_to_run_key(f["name"])
            if run_key is None:
                continue

            help_text = f.get("help")
            if help_text is not None:
                lines.append(f"# {help_text}")

            if f["name"] == "backend.kind":
                # when using this backend, it should be uns-api
                lines.append('backend = "uns-api"')
                continue

            if f.get("secret", False):
                envs = f.get("env_names", [])
                if envs:
                    lines.append("# Prefer ENV injection: " + ", ".join(envs))
                lines.append(f'{run_key} = "..."')
                continue

            v = _format_value(f["default"]) if "default" in f else _placeholder_for_type(f)
            lines.append(f"{run_key} = {v}")

        lines.append("")
        return "\n".join(lines)

    raise ValueError(f"unsupported backend spec for toml snippet: {spec['id']}")


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


def _render_run_section(*, include_uns_api_hint: bool) -> list[str]:
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

    # uns-api fields as comments (optional)
    if include_uns_api_hint:
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


def render_config_init_toml_v1(*, only: set[str] | None = None) -> str:
    """
    Render a complete config init TOML template from spec registry.

    Structure:
    - schema_version = 1
    - [run]: backend, strategy, uns-api fields (commented)
    - [retry]: defaults
    - [destinations.webhook]: commented block
    - [destinations.kafka]: commented block
    - [destinations.weaviate]: commented block
    - [destinations.s3]: commented block
    - [destinations.opensearch]: commented block
    - [destinations.clickhouse]: commented block
    - [destinations.mongodb]: commented block
    - [destinations.loki]: commented block
    - [destinations.sqlite]: commented block
    """
    lines: list[str] = []

    target_only: set[str] = only if only is not None else set()

    include_all = not target_only or "all" in target_only
    # include_all = only is None or len(only) == 0 or "all" in only
    include_uns_api_hint = include_all or ("uns-api" in target_only)
    include_webhook = include_all or ("webhook" in target_only)
    include_kafka = include_all or ("kafka" in target_only)
    include_weaviate = include_all or ("weaviate" in target_only)
    include_s3 = include_all or ("s3" in target_only)
    include_opensearch = include_all or ("opensearch" in target_only)
    include_clickhouse = include_all or ("clickhouse" in target_only)
    include_mongodb = include_all or ("mongodb" in target_only)
    include_loki = include_all or ("loki" in target_only)
    include_sqlite = include_all or ("sqlite" in target_only)

    # Header
    lines.append("# Zephyr Ingest Configuration (auto-generated from spec)")
    lines.append("# See: zephyr-ingest config resolve --config <this-file>")
    lines.append("")
    lines.append("schema_version = 1")
    lines.append("")

    # [run]
    lines.extend(_render_run_section(include_uns_api_hint=include_uns_api_hint))

    # [retry]
    lines.extend(_render_retry_section())

    if (
        include_webhook
        or include_kafka
        or include_weaviate
        or include_s3
        or include_opensearch
        or include_clickhouse
        or include_mongodb
        or include_loki
        or include_sqlite
    ):
        lines.append("")
        lines.append("# " + "=" * 50)
        lines.append("# Destinations (uncomment to enable)")
        lines.append("# " + "=" * 50)

    # Webhook
    if include_webhook:
        webhook_spec = get_spec(spec_id="destination.webhook.v1")
        if webhook_spec:
            lines.extend(
                _render_destination_section(spec=webhook_spec, table_name="destinations.webhook")
            )

    # Kafka
    if include_kafka:
        kafka_spec = get_spec(spec_id="destination.kafka.v1")
        if kafka_spec:
            lines.extend(
                _render_destination_section(spec=kafka_spec, table_name="destinations.kafka")
            )

    # Weaviate
    if include_weaviate:
        weaviate_spec = get_spec(spec_id="destination.weaviate.v1")
        if weaviate_spec:
            lines.extend(
                _render_destination_section(spec=weaviate_spec, table_name="destinations.weaviate")
            )

    if include_s3:
        s3_spec = get_spec(spec_id="destination.s3.v1")
        if s3_spec:
            lines.extend(_render_destination_section(spec=s3_spec, table_name="destinations.s3"))

    if include_opensearch:
        opensearch_spec = get_spec(spec_id="destination.opensearch.v1")
        if opensearch_spec:
            lines.extend(
                _render_destination_section(
                    spec=opensearch_spec,
                    table_name="destinations.opensearch",
                )
            )

    if include_clickhouse:
        clickhouse_spec = get_spec(spec_id="destination.clickhouse.v1")
        if clickhouse_spec:
            lines.extend(
                _render_destination_section(
                    spec=clickhouse_spec,
                    table_name="destinations.clickhouse",
                )
            )

    if include_mongodb:
        mongodb_spec = get_spec(spec_id="destination.mongodb.v1")
        if mongodb_spec:
            lines.extend(
                _render_destination_section(
                    spec=mongodb_spec,
                    table_name="destinations.mongodb",
                )
            )

    if include_loki:
        loki_spec = get_spec(spec_id="destination.loki.v1")
        if loki_spec:
            lines.extend(
                _render_destination_section(
                    spec=loki_spec,
                    table_name="destinations.loki",
                )
            )

    if include_sqlite:
        sqlite_spec = get_spec(spec_id="destination.sqlite.v1")
        if sqlite_spec:
            lines.extend(
                _render_destination_section(
                    spec=sqlite_spec,
                    table_name="destinations.sqlite",
                )
            )

    lines.append("")
    return "\n".join(lines)
