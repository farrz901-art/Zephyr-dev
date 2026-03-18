from __future__ import annotations

from hashlib import sha256
from pathlib import Path


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """
    流式计算文件的 SHA256，避免大文件撑爆内存。
    默认 chunk_size 为 1MB。
    """
    h = sha256()
    with path.open("rb") as f:
        # 使用 iter(callable, sentinel) 语法进行优雅的块读取
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()
