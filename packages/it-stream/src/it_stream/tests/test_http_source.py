from __future__ import annotations

import json
from email.message import Message
from pathlib import Path
from typing import cast
from urllib.error import HTTPError

import pytest

from it_stream import (
    ItCheckpointIdentityV1,
    ItTaskIdentityV1,
    dump_it_artifacts,
    normalize_it_checkpoint_identity_key,
    normalize_it_input_identity_sha,
    process_file,
)
from it_stream.sources import http_source
from zephyr_core import ErrorCode, PartitionStrategy, ZephyrError


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload, ensure_ascii=False).encode("utf-8")

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


def _write_http_source_spec(
    path: Path,
    *,
    url: str,
    stream: str = "customers",
    cursor_param: str = "cursor",
    query: dict[str, str] | None = None,
    reverse_order: bool = False,
) -> None:
    payload = {
        "source": {
            "kind": "http_json_cursor_v1",
            "stream": stream,
            "url": url,
            "cursor_param": cursor_param,
            "query": query or {},
        }
    }
    if reverse_order:
        payload = {
            "source": {
                "query": query or {},
                "cursor_param": cursor_param,
                "url": url,
                "stream": stream,
                "kind": "http_json_cursor_v1",
            }
        }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_http_source_config_validation_rejects_missing_stream(tmp_path: Path) -> None:
    path = tmp_path / "source.json"
    path.write_text(
        json.dumps(
            {
                "source": {
                    "kind": "http_json_cursor_v1",
                    "url": "https://example.test/api/customers",
                    "cursor_param": "cursor",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ZephyrError) as exc_info:
        process_file(
            filename=str(path),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-http-invalid",
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details == {
        "retryable": False,
        "source_kind": "http_json_cursor_v1",
    }


def test_http_source_identity_sha_is_stable_for_equivalent_specs(tmp_path: Path) -> None:
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    third = tmp_path / "third.json"

    _write_http_source_spec(
        left,
        url="https://example.test/api/customers",
        query={"limit": "2", "region": "us"},
    )
    _write_http_source_spec(
        right,
        url="https://example.test/api/customers",
        query={"region": "us", "limit": "2"},
        reverse_order=True,
    )
    _write_http_source_spec(
        third,
        url="https://example.test/api/customers",
        query={"limit": "5", "region": "us"},
    )

    first = normalize_it_input_identity_sha(filename=str(left), default_sha="fallback-left")
    second = normalize_it_input_identity_sha(filename=str(right), default_sha="fallback-right")
    different = normalize_it_input_identity_sha(filename=str(third), default_sha="fallback-third")

    assert first == second
    assert first != different


def test_http_source_builds_zephyr_owned_records_and_checkpoint_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.json"
    _write_http_source_spec(path, url="https://example.test/api/customers", query={"limit": "2"})

    def fake_urlopen(request: object, timeout: float = 10.0) -> _FakeResponse:
        del timeout
        full_url = cast("str", getattr(request, "full_url"))
        if "cursor=c-1" in full_url:
            return _FakeResponse(
                {
                    "records": [{"id": 2, "name": "Bob"}],
                    "next_cursor": None,
                }
            )
        return _FakeResponse(
            {
                "records": [{"id": 1, "name": "Ada"}],
                "next_cursor": "c-1",
            }
        )

    monkeypatch.setattr(http_source, "urlopen", fake_urlopen)

    result = process_file(
        filename=str(path),
        strategy=PartitionStrategy.AUTO,
        sha256="sha-http-001",
        size_bytes=path.stat().st_size,
    )

    assert result.engine.name == "it-stream"
    assert result.engine.backend == "http-json-cursor"
    assert [element.metadata["artifact_kind"] for element in result.elements] == [
        "record",
        "record",
        "state",
        "log",
        "log",
        "log",
    ]
    assert result.elements[2].metadata["data"] == {
        "cursor": "c-1",
        "page_number": 1,
        "source_url": "https://example.test/api/customers",
        "record_count": 1,
    }

    out_dir = tmp_path / "artifacts"
    artifacts = dump_it_artifacts(out_dir=out_dir, result=result, pipeline_version="p-http")
    checkpoint_row = json.loads((out_dir / "checkpoint.json").read_text(encoding="utf-8"))
    record_lines = (out_dir / "records.jsonl").read_text(encoding="utf-8").splitlines()
    log_lines = (out_dir / "logs.jsonl").read_text(encoding="utf-8").splitlines()

    assert len(artifacts.records) == 2
    assert checkpoint_row["checkpoints"] == [
        {
            "checkpoint_identity_key": normalize_it_checkpoint_identity_key(
                identity=ItCheckpointIdentityV1(
                    task=ItTaskIdentityV1(
                        pipeline_version="p-http",
                        sha256="sha-http-001",
                    ),
                    stream="customers",
                    progress={
                        "cursor": "c-1",
                        "page_number": 1,
                        "source_url": "https://example.test/api/customers",
                        "record_count": 1,
                    },
                )
            ),
            "checkpoint_index": 0,
            "progress": {
                "cursor": "c-1",
                "page_number": 1,
                "record_count": 1,
                "source_url": "https://example.test/api/customers",
            },
        }
    ]
    assert json.loads(record_lines[0]) == {
        "data": {"id": 1, "name": "Ada"},
        "emitted_at": None,
        "record_index": 0,
        "stream": "customers",
    }
    assert json.loads(log_lines[0]) == {
        "level": "INFO",
        "log_index": 0,
        "message": "fetched page=1 records=1 next_cursor=c-1",
    }
    assert json.loads(log_lines[-1]) == {
        "level": "INFO",
        "log_index": 2,
        "message": "source exhausted after page=2",
    }
    assert "records" not in checkpoint_row
    assert "next_cursor" not in checkpoint_row


@pytest.mark.parametrize(
    ("status_code", "expected_retryable"),
    [
        (503, True),
        (404, False),
    ],
)
def test_http_source_maps_basic_http_failures_to_retryable_semantics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status_code: int,
    expected_retryable: bool,
) -> None:
    path = tmp_path / "source.json"
    _write_http_source_spec(path, url="https://example.test/api/customers")

    def fake_urlopen(request: object, timeout: float = 10.0) -> _FakeResponse:
        del timeout
        raise HTTPError(
            cast("str", getattr(request, "full_url")),
            status_code,
            "boom",
            hdrs=Message(),
            fp=None,
        )

    monkeypatch.setattr(http_source, "urlopen", fake_urlopen)

    with pytest.raises(ZephyrError) as exc_info:
        process_file(
            filename=str(path),
            strategy=PartitionStrategy.AUTO,
            sha256="sha-http-err",
            size_bytes=path.stat().st_size,
        )

    assert exc_info.value.code == ErrorCode.IO_READ_FAILED
    assert exc_info.value.details is not None
    assert exc_info.value.details["retryable"] is expected_retryable
    assert exc_info.value.details["status_code"] == status_code
