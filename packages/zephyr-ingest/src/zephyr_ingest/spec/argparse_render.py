from __future__ import annotations

import argparse

from zephyr_ingest.spec.types import ConnectorSpecV1, SpecFieldTypeV1, SpecFieldV1


def _field_default(*, field: SpecFieldV1) -> str | int | float | bool | None:
    if "default" in field:
        return field["default"]
    if field["type"] == "bool":
        return False
    return None


def _field_type_to_argparse_type(t: SpecFieldTypeV1):
    if t == "string":
        return str
    if t == "int":
        return int
    if t == "float":
        return float
    return None


def add_spec_to_parser(*, p: argparse.ArgumentParser, spec: ConnectorSpecV1) -> None:
    """
    Render argparse arguments from ConnectorSpecV1.

    Notes:
    - `required` in spec means "required when connector is enabled",
    so argparse does NOT set required=True.
    - Validation remains in ConfigV1.from_namespace / config-file parser.
    """
    for field in spec["fields"]:
        flags = field.get("cli_flags", [])
        if not flags:
            continue

        help_text = field.get("help")
        default = _field_default(field=field)

        if field["type"] == "bool":
            if default not in (False, None):
                raise ValueError(f"bool field default must be False/None: {field['name']}")
            p.add_argument(*flags, action="store_true", default=False, help=help_text)
            continue

        arg_type = _field_type_to_argparse_type(field["type"])
        if arg_type is None:
            raise ValueError(f"Unsupported field type: {field['type']}")

        kwargs: dict[str, object] = {
            "default": default,
            "type": arg_type,
        }
        if help_text is not None:
            kwargs["help"] = help_text

        # if "choices" in field:
        #     kwargs["choices"] = field["choices"]
        #
        # p.add_argument(*flags, **kwargs)
        if "choices" in field:
            p.add_argument(
                *flags,
                default=default,
                type=arg_type,
                choices=field["choices"],
                help=help_text,
            )
        else:
            p.add_argument(
                *flags,
                default=default,
                type=arg_type,
                help=help_text,
            )


def add_specs_to_parser(*, p: argparse.ArgumentParser, specs: list[ConnectorSpecV1]) -> None:
    for s in specs:
        add_spec_to_parser(p=p, spec=s)
