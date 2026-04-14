from __future__ import annotations

import os
from typing import Final, cast

import pytest

from zephyr_ingest.testing.p45 import (
    P45_GATED_TIERS,
    P45_MARKERS,
    LoadedP45Env,
    load_p45_env,
)
from zephyr_ingest.testing.p45_matrix import (
    P45_TIERS,
    P45TruthMatrix,
    load_p45_truth_matrix,
)

_SKIP_REASON: Final[str] = "requires explicit P4.5 authenticity tier selection via --auth-tier"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--auth-tier",
        action="append",
        choices=P45_TIERS,
        default=[],
        help="Enable gated P4.5 authenticity tiers. May be passed multiple times.",
    )


def _selected_tiers(config: pytest.Config) -> frozenset[str]:
    selected = set(cast(list[str], config.getoption("--auth-tier")))
    env_value = os.environ.get("ZEPHYR_P45_AUTH_TIERS")
    if env_value is not None:
        for raw_part in env_value.split(","):
            part = raw_part.strip()
            if part in P45_TIERS:
                selected.add(part)
    return frozenset(selected)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    selected = _selected_tiers(config)
    for item in items:
        for tier in P45_GATED_TIERS:
            marker_name = P45_MARKERS[tier]
            if item.get_closest_marker(marker_name) is not None and tier not in selected:
                item.add_marker(pytest.mark.skip(reason=f"{_SKIP_REASON}: {tier}"))
                break


@pytest.fixture(scope="session")
def p45_env() -> LoadedP45Env:
    return load_p45_env()


@pytest.fixture(scope="session")
def p45_truth_matrix() -> P45TruthMatrix:
    return load_p45_truth_matrix()
