"""T233 - 频控与限流工具"""

import time
from typing import Any

# 内存计数器（生产使用 Redis）
_counters: dict[str, list[float]] = {}


async def check_rate_limit(
    key: str,
    max_requests: int,
    window_seconds: int = 60,
) -> dict[str, Any]:
    """检查频率限制"""
    now = time.time()
    window_start = now - window_seconds

    if key not in _counters:
        _counters[key] = []

    # 清理窗口外的记录
    _counters[key] = [t for t in _counters[key] if t > window_start]

    current_count = len(_counters[key])
    if current_count >= max_requests:
        return {
            "allowed": False,
            "current": current_count,
            "limit": max_requests,
            "window": window_seconds,
            "retry_after": int(window_seconds - (now - _counters[key][0])) + 1,
        }

    _counters[key].append(now)
    return {
        "allowed": True,
        "current": current_count + 1,
        "limit": max_requests,
        "window": window_seconds,
        "remaining": max_requests - current_count - 1,
    }


async def reset_counter(key: str) -> bool:
    """重置计数器"""
    _counters.pop(key, None)
    return True


async def get_self_optimize_rate_key(tenant_id: str) -> str:
    """自优化触发频控键"""
    return f"self_optimize:{tenant_id}"


async def check_optimize_allowed(
    tenant_id: str,
    max_per_hour: int = 5,
) -> dict[str, Any]:
    """检查自优化是否允许"""
    key = await get_self_optimize_rate_key(tenant_id)
    return await check_rate_limit(key, max_per_hour, 3600)
