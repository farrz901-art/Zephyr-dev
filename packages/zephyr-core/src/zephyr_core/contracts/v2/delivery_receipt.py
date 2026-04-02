from __future__ import annotations

from typing import TypedDict


class DeliveryReceiptV1(TypedDict):
    """
    Minimal stable delivery receipt.

    Destination implementations may add arbitrary fields inside `details`,
    but MUST keep:
      - ok: bool
      - destination: str
    """

    destination: str
    ok: bool
    details: dict[str, object]
