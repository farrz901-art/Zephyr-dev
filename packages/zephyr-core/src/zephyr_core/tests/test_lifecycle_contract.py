from __future__ import annotations

from zephyr_core import Lifecycle, WorkerPhase
from zephyr_core.contracts.v2 import Lifecycle as LifecycleV2
from zephyr_core.contracts.v2 import WorkerPhase as WorkerPhaseV2


def test_worker_phase_values_are_stable() -> None:
    assert [phase.value for phase in WorkerPhase] == [
        "starting",
        "running",
        "draining",
        "stopping",
        "stopped",
        "failed",
    ]


def test_lifecycle_contract_is_exported_from_v2_and_package_root() -> None:
    assert WorkerPhaseV2 is WorkerPhase
    assert LifecycleV2 is Lifecycle


def test_runtime_checkable_lifecycle_protocol_surface() -> None:
    class DummyLifecycle:
        @property
        def phase(self) -> WorkerPhase:
            return WorkerPhase.RUNNING

        def request_draining(self) -> None:
            return None

        def request_stopping(self) -> None:
            return None

    assert isinstance(DummyLifecycle(), Lifecycle)
