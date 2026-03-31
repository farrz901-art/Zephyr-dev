from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast


@dataclass(frozen=True, slots=True)
class DlqPruneStats:
    out_root: str
    now_epoch_s: int
    older_than_days: int
    threshold_epoch_s: int

    include_pending: bool
    include_done: bool
    apply: bool
    max_total_mb: int | None
    keep_last: int | None
    keep_last_per_destination: bool

    total_bytes: int
    max_total_bytes: int | None
    bytes_over_limit: int | None

    scanned: int
    selected: int
    moved: int
    skipped: int
    errors: int
    selected_bytes: int

    selected_by_age: int
    selected_by_size: int
    protected: int

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
            "max_total_mb": self.max_total_mb,
            "keep_last": self.keep_last,
            "keep_last_per_destination": self.keep_last_per_destination,
            "total_bytes": self.total_bytes,
            "max_total_bytes": self.max_total_bytes,
            "bytes_over_limit": self.bytes_over_limit,
            "scanned": self.scanned,
            "selected": self.selected,
            "moved": self.moved,
            "skipped": self.skipped,
            "errors": self.errors,
            "selected_bytes": self.selected_bytes,
            "selected_by_age": self.selected_by_age,
            "selected_by_size": self.selected_by_size,
            "protected": self.protected,
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


@dataclass(frozen=True, slots=True)
class _DlqFile:
    path: Path
    origin: Literal["pending", "done"]
    mtime_s: int
    size_bytes: int
    destination: str


def _extract_destination_from_record(obj: object) -> str | None:
    if not isinstance(obj, dict):
        return None

    data = cast(dict[str, Any], obj)
    dr = data.get("destination_receipt")
    if isinstance(dr, dict):
        dr_data = cast(dict[str, Any], dr)
        d = dr_data.get("destination")
        if isinstance(d, str) and d.strip():
            return d
    # fallback: some older shapes might put it top-level
    d2 = data.get("destination")
    if isinstance(d2, str) and d2.strip():
        return d2
    return None


def _destination_for_file(fp: Path) -> str:
    try:
        obj = json.loads(fp.read_text(encoding="utf-8"))
        d = _extract_destination_from_record(obj)
        return d if d is not None else "unknown"
    except Exception:
        return "unknown"


def prune_delivery_dlq(
    *,
    out_root: Path,
    older_than_days: int,
    include_pending: bool,
    include_done: bool,
    apply: bool,
    move_to: Path,
    max_total_mb: int | None = None,
    keep_last: int | None = None,
    keep_last_per_destination: bool = False,
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
    if max_total_mb is not None and max_total_mb < 0:
        raise ValueError("max_total_mb must be >= 0")
    if keep_last is not None and keep_last < 0:
        raise ValueError("keep_last must be >= 0")

    now_s = int(time.time()) if now_epoch_s is None else int(now_epoch_s)
    threshold_s = now_s - (older_than_days * 86400)

    pending_dir = out_root / "_dlq" / "delivery"
    done_dir = out_root / "_dlq" / "delivery_done"

    # move_to is relative or absolute; normalize under out_root unless absolute
    move_to_abs = move_to if move_to.is_absolute() else (out_root / move_to)
    move_to_abs = move_to_abs.resolve()

    files: list[_DlqFile] = []

    def add_dir(root: Path, *, origin: Literal["pending", "done"]) -> None:
        for fp in _iter_dlq_files(root=root):
            st = fp.stat()
            destination = _destination_for_file(fp) if keep_last_per_destination else "unknown"
            files.append(
                _DlqFile(
                    path=fp,
                    origin=origin,
                    mtime_s=int(st.st_mtime),
                    size_bytes=int(st.st_size),
                    destination=destination,
                )
            )

    if include_pending:
        add_dir(pending_dir, origin="pending")
    if include_done:
        add_dir(done_dir, origin="done")

    scanned = len(files)
    total_bytes = sum(f.size_bytes for f in files)

    # protected set (keep_last)
    protected_paths: set[Path] = set()
    if keep_last is not None and keep_last > 0 and files:
        if keep_last_per_destination:
            by_dest: dict[str, list[_DlqFile]] = {}
            for f in files:
                by_dest.setdefault(f.destination, []).append(f)
            for group in by_dest.values():
                group_sorted = sorted(group, key=lambda x: x.mtime_s, reverse=True)
                for f in group_sorted[:keep_last]:
                    protected_paths.add(f.path)
        else:
            files_sorted = sorted(files, key=lambda x: x.mtime_s, reverse=True)
            for f in files_sorted[:keep_last]:
                protected_paths.add(f.path)

    # selection set
    selected_paths: set[Path] = set()
    selected_by_age = 0
    selected_by_size = 0

    # age-based selection
    for f in files:
        if f.path in protected_paths:
            continue
        if f.mtime_s < threshold_s:
            selected_paths.add(f.path)
    selected_by_age = len(selected_paths)

    # size-based selection (may select newer files too, but never protected)
    max_total_bytes: int | None = None
    bytes_over_limit: int | None = None
    if max_total_mb is not None:
        max_total_bytes = int(max_total_mb) * 1024 * 1024
        if total_bytes > max_total_bytes:
            bytes_over_limit = total_bytes - max_total_bytes
            # choose oldest files first (excluding protected & already selected)
            # until we free enough
            freed = sum(f.size_bytes for f in files if f.path in selected_paths)
            candidates = [
                f for f in files if (f.path not in protected_paths and f.path not in selected_paths)
            ]
            candidates_sorted = sorted(candidates, key=lambda x: x.mtime_s)
            for f in candidates_sorted:
                if freed >= bytes_over_limit:
                    break
                selected_paths.add(f.path)
                freed += f.size_bytes
            selected_by_size = len(selected_paths) - selected_by_age

    # execute (move)
    selected = len(selected_paths)
    selected_bytes = sum(f.size_bytes for f in files if f.path in selected_paths)
    skipped = scanned - selected - len(protected_paths)
    moved = errors = 0
    selected_pending = selected_done = 0
    moved_pending = moved_done = 0

    # map path -> metadata for origin stats
    meta_by_path: dict[Path, _DlqFile] = {f.path: f for f in files}

    for p in selected_paths:
        f = meta_by_path[p]
        if f.origin == "pending":
            selected_pending += 1
        else:
            selected_done += 1

        if not apply:
            continue

        try:
            sub_dir_name = "delivery" if f.origin == "pending" else "delivery_done"
            origin_dir = move_to_abs / sub_dir_name
            origin_dir.mkdir(parents=True, exist_ok=True)
            target = origin_dir / p.name
            shutil.move(str(p), str(target))
            moved += 1
            if f.origin == "pending":
                moved_pending += 1
            else:
                moved_done += 1
        except Exception:
            errors += 1

    return DlqPruneStats(
        out_root=str(out_root),
        now_epoch_s=now_s,
        older_than_days=older_than_days,
        threshold_epoch_s=threshold_s,
        include_pending=include_pending,
        include_done=include_done,
        apply=apply,
        max_total_mb=max_total_mb,
        keep_last=keep_last,
        keep_last_per_destination=keep_last_per_destination,
        total_bytes=total_bytes,
        max_total_bytes=max_total_bytes,
        bytes_over_limit=bytes_over_limit,
        scanned=scanned,
        selected=selected,
        moved=moved,
        skipped=skipped,
        errors=errors,
        selected_bytes=selected_bytes,
        selected_by_age=selected_by_age,
        selected_by_size=selected_by_size,
        protected=len(protected_paths),
        selected_pending=selected_pending,
        selected_done=selected_done,
        moved_pending=moved_pending,
        moved_done=moved_done,
        move_to=str(move_to_abs),
    )
