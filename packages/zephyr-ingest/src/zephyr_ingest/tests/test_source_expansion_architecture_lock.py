from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def test_source_expansion_architecture_lock_sections_remain_explicit() -> None:
    agents_path = _repo_root() / "AGENTS.md"
    content = agents_path.read_text(encoding="utf-8")

    required_markers = (
        "For future source connector placement:",
        "For future source connector onboarding, review against this checklist:",
        "For source connector work, treat the source placement rules, "
        "onboarding checklist, and first-wave",
        "First-wave source connector candidates for P4:",
        "Not first-wave by default:",
        "do not classify by vendor similarity or packaging convenience alone;",
        "connector config must enter through Zephyr-owned typed/spec surfaces;",
        "`uns-stream`: local document collection source.",
        "`uns-stream`: object storage / blob document collection source.",
        "`it-stream`: cursor or pagination-driven structured API source.",
    )

    for marker in required_markers:
        assert marker in content


def test_destination_expansion_architecture_lock_sections_remain_explicit() -> None:
    agents_path = _repo_root() / "AGENTS.md"
    content = agents_path.read_text(encoding="utf-8")

    required_markers = (
        "For destination connector work, treat the destination boundary rules, "
        "onboarding checklist, and",
        "For future destination connector placement and review:",
        "For future destination connector onboarding, review against this checklist:",
        "First-wave destination connector candidates for P4:",
        "do not bypass the shared delivery path by letting a destination "
        "define its own payload contract,",
        "destination config must enter through the existing Zephyr config/spec discipline and",
        "object storage / blob archive destination.",
        "relational row/upsert destination.",
        "search/index document destination.",
        "warehouse/bulk-load job destinations that "
        "require async manifest upload, staged commit, or",
    )

    for marker in required_markers:
        assert marker in content
