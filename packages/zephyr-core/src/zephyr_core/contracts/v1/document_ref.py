from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DocumentRef:
    """Reference to an input document discovered by a source connector."""

    uri: str  # e.g. absolute path, s3://..., http://...
    source: str  # e.g. "local_file"
    discovered_at_utc: str  # ISO 8601 string
    filename: str
    extension: str  # normalized lower, e.g. ".pdf"
    size_bytes: int
