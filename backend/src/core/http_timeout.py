"""T250 - 接口超时策略与网关限时"""

import os
from typing import Any, Callable

from fastapi import HTTPException, Request

# 超时配置（毫秒）
DEFAULT_TIMEOUT_MS = int(os.getenv("API_DEFAULT_TIMEOUT_MS", "30000"))
AI_TIMEOUT_MS = int(os.getenv("API_AI_TIMEOUT_MS", "60000"))
TRADE_TIMEOUT_MS = int(os.getenv("API_TRADE_TIMEOUT_MS", "10000"))
MARKET_TIMEOUT_MS = int(os.getenv("API_MARKET_TIMEOUT_MS", "5000"))

# 路径前缀 -> 超时配置
TIMEOUT_POLICIES: dict[str, int] = {
    "/api/v1/ai": AI_TIMEOUT_MS,
    "/api/v1/trade": TRADE_TIMEOUT_MS,
    "/api/v1/market": MARKET_TIMEOUT_MS,
    "/api/v1/backtest": AI_TIMEOUT_MS,
    "/api/v1/strategy/generate": AI_TIMEOUT_MS,
}


def resolve_timeout(path: str) -> int:
    """根据路径解析超时时间"""
    for prefix, timeout in TIMEOUT_POLICIES.items():
        if path.startswith(prefix):
            return timeout
    return DEFAULT_TIMEOUT_MS


async def timeout_middleware(request: Request, call_next: Callable) -> Any:
    """超时中间件"""
    import asyncio

    timeout_ms = resolve_timeout(request.url.path)
    timeout_sec = timeout_ms / 1000.0

    try:
        response = await asyncio.wait_for(call_next(request), timeout=timeout_sec)
        response.headers["X-Timeout-Ms"] = str(timeout_ms)
        return response
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"Request timeout after {timeout_ms}ms",
        )


def get_timeout_config() -> dict[str, Any]:
    """获取超时配置"""
    return {
        "default_ms": DEFAULT_TIMEOUT_MS,
        "policies": TIMEOUT_POLICIES,
    }
