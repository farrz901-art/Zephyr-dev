from __future__ import annotations

from typing import Literal, NotRequired, TypeAlias, TypedDict

SpecFieldTypeV1: TypeAlias = Literal["string", "int", "float", "bool"]


class SpecFieldV1(TypedDict):
    """
    Machine-readable connector field definition.
    `name` follows the flat dotted convention aligned with config_snapshot.sources keys.
    """

    name: str
    type: SpecFieldTypeV1
    required: bool

    default: NotRequired[str | int | float | bool | None]
    secret: NotRequired[bool]
    cli_flags: NotRequired[list[str]]
    env_names: NotRequired[list[str]]
    choices: NotRequired[list[str]]
    help: NotRequired[str]
    examples: NotRequired[list[str]]


class ConnectorSpecV1(TypedDict):
    id: str
    version: Literal[1]
    kind: Literal["destination", "backend", "source"]
    name: str
    description: str
    fields: list[SpecFieldV1]
