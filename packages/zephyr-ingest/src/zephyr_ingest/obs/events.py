from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

EventName = Literal[
    "run_start",
    "run_done",
    "doc_start",
    "doc_done",
    "delivery_start",
    "delivery_done",
    "delivery_failed",
    "dlq_written",
    "replay_start",
    "replay_done",
    "replay_attempt",
    "replay_result",
]


def _normalize_value(v: object) -> object:
    if isinstance(v, Path):
        return str(v)
    # Keep primitives as-is; everything else becomes string to avoid surprises.
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


def log_event(
    logger: logging.Logger,
    *,
    level: int,
    event: EventName,
    **fields: object,
) -> None:
    """
    Log a stable, parseable event line:

        <event> k1=v1 k2=v2 ...

    Keys are sorted for deterministic output.
    Values are normalized to avoid dumping large complex objects.
    """
    keys = sorted(fields.keys())
    fmt: str = event
    if keys:
        fmt += " " + " ".join(f"{k}=%s" for k in keys)
    args: list[Any] = []
    for k in keys:
        args.append(_normalize_value(fields[k]))
    logger.log(level, fmt, *args)
