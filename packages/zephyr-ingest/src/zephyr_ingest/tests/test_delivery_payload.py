from __future__ import annotations

import json
from pathlib import Path

from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_core.versioning import RUN_META_SCHEMA_VERSION
from zephyr_ingest._internal.delivery_payload import build_delivery_payload_v1


def test_build_delivery_payload_v1_is_json_serializable(tmp_path: Path) -> None:
    meta = RunMetaV1(
        run_id="r1",
        pipeline_version="p1",
        timestamp_utc="2026-03-24T00:00:00Z",
        schema_version=RUN_META_SCHEMA_VERSION,
    )

    payload = build_delivery_payload_v1(out_root=tmp_path / "out", sha256="abc", meta=meta)

    # must be JSON-serializable
    s = json.dumps(payload, ensure_ascii=False)
    obj = json.loads(s)

    assert obj["schema_version"] == 1
    assert obj["sha256"] == "abc"
    assert "run_meta" in obj
    assert "artifacts" in obj
    assert obj["artifacts"]["out_dir"].endswith("out/abc") or obj["artifacts"]["out_dir"].endswith(
        "out\\abc"
    )
