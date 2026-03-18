"""Zephyr Core - 核心契约与基础能力包"""

__version__ = "0.0.1"  # 项目初期版本

from zephyr_core.contracts import (  # noqa: E402
    DocumentMetadata,
    EngineInfo,
    PartitionResult,
    PartitionStrategy,
    ZephyrElement,
)
from zephyr_core.contracts.v1.run_meta import RunMetaV1
from zephyr_core.errors import ErrorCode, ZephyrError  # noqa: E402

__all__ = [
    "__version__",
    "PartitionStrategy",
    "EngineInfo",
    "DocumentMetadata",
    "ZephyrElement",
    "PartitionResult",
    "ErrorCode",
    "ZephyrError",
    "RunMetaV1",
]
