from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ItTaskIdentityV1:
    pipeline_version: str
    sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "kind": "it",
            "pipeline_version": self.pipeline_version,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class ItCheckpointIdentityV1:
    task: ItTaskIdentityV1
    stream: str | None
    progress: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": "it",
            "task": self.task.to_dict(),
            "stream": self.stream,
            "progress": self.progress,
        }


def normalize_it_task_identity_key(*, identity: ItTaskIdentityV1) -> str:
    return json.dumps(
        identity.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    )


def normalize_it_checkpoint_identity_key(*, identity: ItCheckpointIdentityV1) -> str:
    return json.dumps(
        identity.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    )
