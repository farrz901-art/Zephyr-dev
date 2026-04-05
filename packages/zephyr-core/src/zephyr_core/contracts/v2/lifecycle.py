from __future__ import annotations

from enum import StrEnum
from typing import Protocol, runtime_checkable


class WorkerPhase(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@runtime_checkable
class Lifecycle(Protocol):
    @property
    def phase(self) -> WorkerPhase: ...

    def request_draining(self) -> None: ...

    def request_stopping(self) -> None: ...
