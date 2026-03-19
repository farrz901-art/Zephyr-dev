from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from zephyr_core.contracts.v1.document_ref import DocumentRef


@dataclass(frozen=True, slots=True)
class LocalFileSource:
    path: Path
    glob: str = "**/*"
    include_files: bool = True

    def iter_documents(self) -> Iterator[DocumentRef]:
        p = self.path.expanduser().resolve()

        files: list[Path]
        files = [p] if p.is_file() else [x for x in p.glob(self.glob) if x.is_file()]

        # deterministic order for reproducibility
        files.sort(key=lambda x: str(x))

        now = datetime.now(timezone.utc).isoformat()

        for f in files:
            ext = f.suffix.lower()
            yield DocumentRef(
                uri=str(f),
                source="local_file",
                discovered_at_utc=now,
                filename=f.name,
                extension=ext,
                size_bytes=f.stat().st_size,
            )
