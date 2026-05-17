from __future__ import annotations

import pytest

from uns_stream._internal.paddleocr_languages import normalize_languages_for_paddleocr

pytestmark = [pytest.mark.uns, pytest.mark.unit]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (["eng"], ["en"]),
        (["en"], ["en"]),
        (["zho"], ["ch"]),
        (["zh"], ["ch"]),
        (["chi_sim"], ["ch"]),
        (["chi_tra"], ["chinese_cht"]),
        (["zho", "eng"], ["ch"]),
        (["chi_sim", "eng"], ["ch"]),
        (["unknown"], ["en"]),
        ("chi_sim+eng", ["ch"]),
        ("zho+eng", ["ch"]),
    ],
)
def test_normalize_languages_for_paddleocr_expected_cases(
    value: object,
    expected: list[str] | None,
) -> None:
    assert normalize_languages_for_paddleocr(value) == expected


def test_normalize_languages_for_paddleocr_never_emits_tesseract_combined_string() -> None:
    observed = normalize_languages_for_paddleocr(["zho", "eng"])

    assert observed == ["ch"]
    assert observed != ["zho", "eng"]
    assert observed != ["chi_sim+eng"]
    assert observed != ["zho+eng"]
