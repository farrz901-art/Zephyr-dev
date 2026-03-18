# from __future__ import annotations
#
# import json
# from pathlib import Path
# from typing import Any
#
# from uns_stream._internal.artifacts import dump_partition_error, dump_partition_success
# from uns_stream.service import partition_file
# from zephyr_core import ErrorCode, PartitionStrategy, ZephyrElement, ZephyrError
#
#
# class DummyBackend:
#     name = "dummy"
#     backend = "test"
#     version = "0.0.0"
#
#     def partition_elements(
#         self,
#         *,
#         filename: str,
#         kind: str,
#         strategy: PartitionStrategy,
#         unique_element_ids: bool = True,
#         **kwargs: Any,
#     ) -> list[ZephyrElement]:
#         return [ZephyrElement(element_id="1", type="Title", text="Hello", metadata={})]
#
#
# def test_dump_partition_success_writes_three_files(tmp_path: Path) -> None:
#     f = tmp_path / "a.txt"
#     f.write_text("hello", encoding="utf-8")
#
#     res = partition_file(filename=str(f), kind="text", backend=DummyBackend())
#     out_dir = dump_partition_success(
#         out_root=tmp_path / "out",
#         result=res,
#         run_id="r1",
#         pipeline_version="p1",
#         timestamp_utc="2026-03-17T00:00:00Z",
#         duration_ms=123,
#     )
#
#     assert (out_dir / "elements.json").exists()
#     assert (out_dir / "normalized.txt").exists()
#     assert (out_dir / "run_meta.json").exists()
#
#     meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
#     assert meta["run_id"] == "r1"
#     assert meta["pipeline_version"] == "p1"
#     assert "document" in meta
#     assert "engine" in meta
#     assert meta["metrics"]["elements_count"] == 1
#
#
# def test_dump_partition_error_writes_run_meta(tmp_path: Path) -> None:
#     err = ZephyrError(
#         code=ErrorCode.UNS_PARTITION_FAILED, message="fail", details={"retryable": False}
#     )
#     out_dir = dump_partition_error(
#         out_root=tmp_path / "out",
#         sha256="abc",
#         error=err,
#         run_id="r2",
#         pipeline_version="p1",
#         timestamp_utc="2026-03-17T00:00:00Z",
#         duration_ms=456,
#     )
#
#     assert (out_dir / "run_meta.json").exists()
#     meta = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
#     assert meta["run_id"] == "r2"
#     assert meta["error"]["code"] == str(ErrorCode.UNS_PARTITION_FAILED)
#     assert meta["metrics"]["duration_ms"] == 456
