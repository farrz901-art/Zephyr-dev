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
