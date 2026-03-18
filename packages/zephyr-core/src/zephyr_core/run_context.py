from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from zephyr_core.versioning import PIPELINE_VERSION, RUN_META_SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class RunContext:
    run_id: str
    pipeline_version: str
    timestamp_utc: str
    run_meta_schema_version: int

    @staticmethod
    def new(
        *,
        pipeline_version: str = PIPELINE_VERSION,
        timestamp_utc: str | None = None,
        run_id: str | None = None,
        run_meta_schema_version: int = RUN_META_SCHEMA_VERSION,
    ) -> "RunContext":
        return RunContext(
            run_id=run_id or str(uuid.uuid4()),
            pipeline_version=pipeline_version,
            timestamp_utc=timestamp_utc or datetime.now(timezone.utc).isoformat(),
            run_meta_schema_version=run_meta_schema_version,
        )
