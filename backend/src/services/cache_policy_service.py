"""T207 - 热数据缓存策略与 TTL 管理"""

import os
from typing import Any, Optional

from src.core.streams import get_redis_client

DEFAULT_TTL = int(os.getenv("CACHE_DEFAULT_TTL", "300"))  # 5 min
MARKET_TTL = int(os.getenv("CACHE_MARKET_TTL", "10"))     # 10 sec
STRATEGY_TTL = int(os.getenv("CACHE_STRATEGY_TTL", "600")) # 10 min

# 缓存键前缀与 TTL 策略
CACHE_POLICIES: dict[str, dict[str, Any]] = {
    "market:quote": {"ttl": MARKET_TTL, "priority": "high"},
    "market:depth": {"ttl": MARKET_TTL, "priority": "high"},
    "strategy:list": {"ttl": STRATEGY_TTL, "priority": "medium"},
    "strategy:detail": {"ttl": STRATEGY_TTL, "priority": "medium"},
    "user:profile": {"ttl": 600, "priority": "low"},
    "backtest:result": {"ttl": 1800, "priority": "low"},
    "config:global": {"ttl": 3600, "priority": "low"},
}


async def get_cached(key: str) -> Optional[str]:
    """获取缓存"""
    client = await get_redis_client()
    return await client.get(key)


async def set_cached(
    key: str, value: str, ttl: Optional[int] = None
) -> bool:
    """设置缓存（自动匹配 TTL 策略）"""
    client = await get_redis_client()
    resolved_ttl = ttl or _resolve_ttl(key)
    await client.set(key, value, ex=resolved_ttl)
    return True


async def invalidate(key: str) -> bool:
    """使缓存失效"""
    client = await get_redis_client()
    result = await client.delete(key)
    return result > 0


async def invalidate_pattern(pattern: str) -> int:
    """按模式批量失效"""
    client = await get_redis_client()
    keys = []
    async for key in client.scan_iter(match=pattern, count=100):
        keys.append(key)
    if keys:
        return await client.delete(*keys)
    return 0


async def get_cache_stats() -> dict[str, Any]:
    """获取缓存统计"""
    client = await get_redis_client()
    info = await client.info("memory")
    return {
        "used_memory_mb": round(info.get("used_memory", 0) / 1024 / 1024, 2),
        "peak_memory_mb": round(info.get("used_memory_peak", 0) / 1024 / 1024, 2),
        "policies": CACHE_POLICIES,
    }


async def update_policy(prefix: str, ttl: int, priority: str = "medium") -> dict[str, Any]:
    """更新缓存策略"""
    CACHE_POLICIES[prefix] = {"ttl": ttl, "priority": priority}
    return {"prefix": prefix, "ttl": ttl, "priority": priority, "updated": True}


def _resolve_ttl(key: str) -> int:
    """根据键前缀解析 TTL"""
    for prefix, policy in CACHE_POLICIES.items():
        if key.startswith(prefix):
            return policy["ttl"]
    return DEFAULT_TTL
