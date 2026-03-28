from __future__ import annotations

from typing import Iterable


def collect_present_flags(argv: Iterable[str]) -> set[str]:
    present: set[str] = set()
    for tok in argv:
        if not tok.startswith("--"):
            continue
        if tok == "--":
            continue
        flag = tok.split("=", 1)[0]
        present.add(flag)
    return present


def any_flag_present(present: set[str], *flags: str) -> bool:
    return any(f in present for f in flags)
