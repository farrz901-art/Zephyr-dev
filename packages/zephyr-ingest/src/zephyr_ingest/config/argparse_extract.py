from __future__ import annotations

import argparse
from typing import Any, Sequence, TypeGuard, cast

from zephyr_ingest.config.errors import ConfigError


def _is_str(x: object) -> TypeGuard[str]:
    return isinstance(x, str)


def _is_int(x: object) -> TypeGuard[int]:
    # bool is subclass of int, we don't want that
    return isinstance(x, int) and not isinstance(x, bool)


def _is_float(x: object) -> TypeGuard[float]:
    return isinstance(x, float)


def get_opt_str(ns: argparse.Namespace, name: str) -> str | None:
    x = getattr(ns, name, None)
    if x is None:
        return None
    if _is_str(x):
        return x
    raise ConfigError(f"--{name.replace('_', '-')} must be a string")


def get_req_str(ns: argparse.Namespace, name: str) -> str:
    x = get_opt_str(ns, name)
    if x is None or x == "":
        raise ConfigError(f"--{name.replace('_', '-')} is required")
    return x


def get_bool(ns: argparse.Namespace, name: str) -> bool:
    x = getattr(ns, name, None)
    if isinstance(x, bool):
        return x
    raise ConfigError(f"--{name.replace('_', '-')} must be a bool")


def get_int(ns: argparse.Namespace, name: str) -> int:
    x = getattr(ns, name, None)
    if _is_int(x):
        return x
    raise ConfigError(f"--{name.replace('_', '-')} must be an int")


def get_opt_int(ns: argparse.Namespace, name: str) -> int | None:
    x = getattr(ns, name, None)
    if x is None:
        return None
    if _is_int(x):
        return x
    raise ConfigError(f"--{name.replace('_', '-')} must be an int")


def get_float(ns: argparse.Namespace, name: str) -> float:
    x = getattr(ns, name, None)
    if _is_float(x):
        return x
    if _is_int(x):
        return float(x)
    raise ConfigError(f"--{name.replace('_', '-')} must be a float")


def get_str_list(ns: argparse.Namespace, name: str) -> list[str]:
    x = getattr(ns, name, None)
    # 1. 物理检查：确保它是个列表
    if isinstance(x, list) and all(isinstance(i, str) for i in cast(Sequence[Any], x)):
        return cast(list[str], x)
    raise ConfigError(f"--{name.replace('_', '-')} must be a list[str]")
