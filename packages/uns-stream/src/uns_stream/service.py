from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

from uns_stream.backends.base import PartitionBackend
from uns_stream.backends.local_unstructured import LocalUnstructuredBackend
from zephyr_core import (
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
)


def _sha256_file(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def partition_file(
    *,
    filename: str,
    kind: str,
    strategy: PartitionStrategy = PartitionStrategy.AUTO,
    unique_element_ids: bool = True,
    backend: PartitionBackend | None = None,
) -> PartitionResult:
    p = Path(filename)

    # === 关键修改：只构造一次 backend ===
    b: PartitionBackend = backend or LocalUnstructuredBackend()

    elements: list[ZephyrElement] = b.partition_elements(
        filename=str(p),
        kind=kind,
        strategy=strategy,
        unique_element_ids=unique_element_ids,
    )

    normalized_text = "\n\n".join([e.text for e in elements if e.text])

    doc = DocumentMetadata(
        filename=p.name,
        mime_type=None,
        sha256=_sha256_file(p),
        size_bytes=p.stat().st_size,
        created_at_utc=datetime.now(timezone.utc).isoformat(),
    )

    engine = EngineInfo(name=b.name, backend=b.backend, version=b.version, strategy=strategy)

    return PartitionResult(
        document=doc,
        engine=engine,
        elements=elements,
        normalized_text=normalized_text,
        warnings=[],
    )
