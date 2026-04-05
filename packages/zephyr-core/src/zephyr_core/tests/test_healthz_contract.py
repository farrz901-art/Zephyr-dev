from __future__ import annotations

from zephyr_core import HealthCheckKind, HealthCheckProvider, HealthCheckResult
from zephyr_core.contracts.v2 import HealthCheckKind as HealthCheckKindV2
from zephyr_core.contracts.v2 import HealthCheckProvider as HealthCheckProviderV2
from zephyr_core.contracts.v2 import HealthCheckResult as HealthCheckResultV2


def test_health_check_kind_values_are_stable() -> None:
    assert [kind.value for kind in HealthCheckKind] == [
        "liveness",
        "readiness",
        "startup",
    ]


def test_health_check_result_defaults_are_stable() -> None:
    result = HealthCheckResult(kind=HealthCheckKind.READINESS, healthy=True)

    assert result.kind is HealthCheckKind.READINESS
    assert result.healthy is True
    assert result.reason is None
    assert result.details == {}


def test_healthz_contract_is_exported_from_v2_and_package_root() -> None:
    assert HealthCheckKindV2 is HealthCheckKind
    assert HealthCheckResultV2 is HealthCheckResult
    assert HealthCheckProviderV2 is HealthCheckProvider


def test_runtime_checkable_health_provider_protocol_surface() -> None:
    class DummyProvider:
        def check(self, kind: HealthCheckKind) -> HealthCheckResult:
            return HealthCheckResult(kind=kind, healthy=True)

    assert isinstance(DummyProvider(), HealthCheckProvider)
