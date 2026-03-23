from __future__ import annotations

import re
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

        patterns = [self.glob]
        if "{" in self.glob and "}" in self.glob:
            # 找到 {} 内部的内容
            match = re.search(r"\{(.*)\}", self.glob)
            if match:
                prefix = self.glob[: match.start()]
                suffix = self.glob[match.end() :]
                options = match.group(1).split(",")
                patterns = [f"{prefix}{opt}{suffix}" for opt in options]

        files: list[Path] = []
        # files = [p] if p.is_file() else [x for x in p.glob(self.glob) if x.is_file()]

        if p.is_file():
            files = [p]
        else:
            # 遍历解析出来的所有子模式
            for pat in patterns:
                files.extend([x for x in p.glob(pat) if x.is_file()])

        # deterministic order for reproducibility
        # files.sort(key=lambda x: str(x))
        unique_files = sorted(set(files), key=lambda x: str(x))

        now = datetime.now(timezone.utc).isoformat()

        for f in unique_files:
            ext = f.suffix.lower()
            yield DocumentRef(
                uri=str(f),
                source="local_file",
                discovered_at_utc=now,
                filename=f.name,
                extension=ext,
                size_bytes=f.stat().st_size,
            )
