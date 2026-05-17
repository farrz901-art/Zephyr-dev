from __future__ import annotations

from typing import cast

_SIMPLIFIED_CODES = {
    "zho",
    "zh",
    "zh-cn",
    "zh_hans",
    "zh-hans",
    "chinese",
    "simplified_chinese",
    "chi_sim",
}

_TRADITIONAL_CODES = {
    "chi_tra",
    "traditional_chinese",
    "zh_tra",
    "zh-tw",
    "zh_hant",
    "zh-hant",
}

_ENGLISH_CODES = {
    "eng",
    "en",
    "english",
}


def _language_tokens(languages: object) -> list[str]:
    if languages is None:
        return []
    if isinstance(languages, str):
        normalized = languages.replace(",", "+")
        return [token.strip().lower() for token in normalized.split("+") if token.strip()]
    if isinstance(languages, (list, tuple, set, frozenset)):
        values = cast(
            "list[object] | tuple[object, ...] | set[object] | frozenset[object]", languages
        )
        tokens: list[str] = []
        for value in values:
            if isinstance(value, str):
                tokens.extend(_language_tokens(value))
        return tokens
    return []


def normalize_languages_for_paddleocr(languages: object) -> list[str] | None:
    if languages is None:
        return None

    tokens = _language_tokens(languages)
    if not tokens:
        return ["en"]

    if any(token in _SIMPLIFIED_CODES for token in tokens):
        return ["ch"]
    if any(token in _TRADITIONAL_CODES for token in tokens):
        return ["chinese_cht"]
    if any(token in _ENGLISH_CODES for token in tokens):
        return ["en"]
    return ["en"]
