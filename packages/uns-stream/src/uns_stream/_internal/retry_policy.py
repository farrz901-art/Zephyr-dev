from __future__ import annotations

import socket
from typing import Any, cast

from zephyr_core import ErrorCode, ZephyrError


def is_retryable_exception(e: Exception) -> bool:
    """
    统一的故障分诊中心：判定一个异常是否值得重试。

    分类逻辑：
    - 可重试 (True): 临时性、随机发生的错误（如网络抖动、IO 繁忙）。
    - 不可重试 (False): 结构性、确定性的错误（如代码 Bug、缺依赖、文件损坏）。
    """

    # 1. 如果已经是 ZephyrError，根据错误码判断
    if isinstance(e, ZephyrError):
        # 缺依赖或格式不支持，重试 100 次也是错
        # 使用 getattr 绕过 Pyright 跨包属性识别问题
        code = getattr(e, "code", None)
        if code in {ErrorCode.UNS_EXTRA_MISSING, ErrorCode.UNS_UNSUPPORTED_TYPE}:
            return False

        # 如果 details 里已经标记了 retryable，以它为准
        details = getattr(e, "details", None)
        if isinstance(details, dict):
            # 使用 cast 阻断 Unknown 传播，解决 .get() 报错
            typed_details = cast("dict[str, Any]", details)
            val = typed_details.get("retryable")

            # 显式逻辑比较，避开 bool() 构造函数引发的 __new__ 报错
            if val is True or val == "true":
                return True
            if val is False or val == "false":
                return False

    # 2. 标准的 Python 临时性错误
    # TimeoutError (3.11+), socket.timeout, ConnectionError 均在此列
    transient_errors = (
        TimeoutError,
        ConnectionError,
        socket.timeout,
        InterruptedError,
    )
    if isinstance(e, transient_errors):
        return True

    # 3. 针对 Unstructured 内部特定的库错误（未来扩展点）
    # 例如：如果是 HTTP 429 (Rate Limit)，应该返回 True
    exc_msg = str(e).lower()

    return "rate limit" in exc_msg or "too many requests" in exc_msg
