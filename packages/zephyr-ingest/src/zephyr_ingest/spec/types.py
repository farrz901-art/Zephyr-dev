from __future__ import annotations

from typing import Literal, NotRequired, TypeAlias, TypedDict

SpecFieldTypeV1: TypeAlias = Literal["string", "int", "float", "bool"]


class SpecFieldV1(TypedDict):
    """
    A single config field spec.

    `name` uses a flat dotted convention aligned with config_snapshot.sources keys:
      backend.kind
      backend.api_key
      destinations.webhook.url
      ...
    """

    name: str
    type: SpecFieldTypeV1
    required: bool

    # for docs/UI only (not required for runtime)
    default: NotRequired[str | int | float | bool | None]
    secret: NotRequired[bool]
    cli_flags: NotRequired[list[str]]
    env_names: NotRequired[list[str]]
    choices: NotRequired[list[str]]
    help: NotRequired[str]
    examples: NotRequired[list[str]]


class ConnectorSpecV1(TypedDict):
    """
    Connector spec (destination or backend).
    """

    id: str
    version: Literal[1]
    kind: Literal["destination", "backend"]
    name: str
    description: str
    fields: list[SpecFieldV1]
