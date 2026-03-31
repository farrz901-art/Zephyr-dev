from __future__ import annotations

import shutil
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DlqPruneStats:
    out_root: str
    now_epoch_s: int
    older_than_days: int
    threshold_epoch_s: int

    include_pending: bool
    include_done: bool
    apply: bool

    scanned: int
    selected: int
    moved: int
    skipped: int
    errors: int
    selected_bytes: int

    selected_pending: int
    selected_done: int
    moved_pending: int
    moved_done: int

    move_to: str

    def to_dict(self) -> dict[str, object]:
        return {
            "out_root": self.out_root,
            "now_epoch_s": self.now_epoch_s,
            "older_than_days": self.older_than_days,
            "threshold_epoch_s": self.threshold_epoch_s,
            "include_pending": self.include_pending,
            "include_done": self.include_done,
            "apply": self.apply,
            "scanned": self.scanned,
            "selected": self.selected,
            "moved": self.moved,
            "skipped": self.skipped,
            "errors": self.errors,
            "selected_bytes": self.selected_bytes,
            "selected_pending": self.selected_pending,
            "selected_done": self.selected_done,
            "moved_pending": self.moved_pending,
            "moved_done": self.moved_done,
            "move_to": self.move_to,
        }


def _iter_dlq_files(*, root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted([p for p in root.glob("*.json") if p.is_file()])


def prune_delivery_dlq(
    *,
    out_root: Path,
    older_than_days: int,
    include_pending: bool,
    include_done: bool,
    apply: bool,
    move_to: Path,
    now_epoch_s: int | None = None,
) -> DlqPruneStats:
    """
    Prune (move) delivery DLQ JSON records older than N days.

    - pending dir: <out_root>/_dlq/delivery/
    - done dir:    <out_root>/_dlq/delivery_done/
    - pruned dir:  <out_root>/<move_to>/delivery/ and /delivery_done/

    Default safety:
      include_pending=False, include_done=True
    """
    out_root = out_root.expanduser().resolve()

    if older_than_days <= 0:
        raise ValueError("older_than_days must be > 0")

    now_s = int(time.time()) if now_epoch_s is None else int(now_epoch_s)
    threshold_s = now_s - (older_than_days * 86400)

    pending_dir = out_root / "_dlq" / "delivery"
    done_dir = out_root / "_dlq" / "delivery_done"

    scanned = selected = moved = skipped = errors = 0
    selected_bytes = 0
    selected_pending = selected_done = 0
    moved_pending = moved_done = 0

    def consider_file(fp: Path, *, origin: str) -> None:
        nonlocal scanned, selected, moved, skipped, errors, selected_bytes
        nonlocal selected_pending, selected_done, moved_pending, moved_done

        scanned += 1
        try:
            st = fp.stat()
            mtime_s = int(st.st_mtime)
            if mtime_s >= threshold_s:
                skipped += 1
                return

            selected += 1
            selected_bytes += int(st.st_size)
            if origin == "pending":
                selected_pending += 1
            else:
                selected_done += 1

            if not apply:
                return

            # Ensure move_to dirs exist. Separate by origin to avoid name collisions.
            sub_dir_name = "delivery" if origin == "pending" else "delivery_done"
            origin_dir = move_to_abs / sub_dir_name

            origin_dir.mkdir(parents=True, exist_ok=True)
            target = origin_dir / fp.name

            # fp.replace(target)
            shutil.move(str(fp), str(target))

            moved += 1
            if origin == "pending":
                moved_pending += 1
            else:
                moved_done += 1
        except Exception as e:
            print(f"DEBUG: Move failed: {e}")
            errors += 1

    # move_to is relative or absolute; normalize under out_root unless absolute
    move_to_abs = move_to if move_to.is_absolute() else (out_root / move_to)
    move_to_abs = move_to_abs.resolve()

    if include_pending:
        for fp in _iter_dlq_files(root=pending_dir):
            consider_file(fp, origin="pending")

    if include_done:
        for fp in _iter_dlq_files(root=done_dir):
            consider_file(fp, origin="done")

    return DlqPruneStats(
        out_root=str(out_root),
        now_epoch_s=now_s,
        older_than_days=older_than_days,
        threshold_epoch_s=threshold_s,
        include_pending=include_pending,
        include_done=include_done,
        apply=apply,
        scanned=scanned,
        selected=selected,
        moved=moved,
        skipped=skipped,
        errors=errors,
        selected_bytes=selected_bytes,
        selected_pending=selected_pending,
        selected_done=selected_done,
        moved_pending=moved_pending,
        moved_done=moved_done,
        move_to=str(move_to_abs),
    )
