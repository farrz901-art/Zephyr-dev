# from __future__ import annotations
#
# import importlib
# from pathlib import Path
#
# import pytest
#
# from uns_stream.partition.docx import partition_docx
#
#
# FIX = Path(__file__).parent / "fixtures" / "sample.docx"
#
#
# @pytest.mark.integration
# def test_partition_docx_real() -> None:
#     if not FIX.exists():
#         pytest.skip("Missing fixture: tests/fixtures/sample.docx")
#
#     try:
#         importlib.import_module("unstructured.partition.docx")
#     except ModuleNotFoundError:
#         pytest.skip("unstructured docx extra not installed")
#
#     res = partition_docx(filename=str(FIX))
#     assert res.engine.name == "unstructured"
#     assert res.engine.backend == "local"
#     assert len(res.elements) > 0
