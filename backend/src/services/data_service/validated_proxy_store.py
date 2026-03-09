"""本地已校验代理池 Redis 读写（key 前缀 validated_proxy，与 proxy_redis 同实例）。支持可选 redis_url 覆盖环境变量。"""

import logging
import os
from typing import Any, Optional

import redis.asyncio as redis

logger = logging.getLogger(__name__)

VALIDATED_PROXY_SET = "validated_proxy:set"
VALIDATED_PROXY_META_PREFIX = "validated_proxy:meta:"
PROXY_RAW_SET = "proxy_raw:set"

_proxy_redis_pool: Optional[redis.ConnectionPool] = None
_pools_by_url: dict[str, redis.ConnectionPool] = {}


def _get_proxy_redis_url() -> str:
    url = os.getenv("PROXY_REDIS_URL", "").strip()
    if url:
        return url
    url = os.getenv("REDIS_URL", "redis://127.0.0.1:6380/0")
    logger.info("未配置 PROXY_REDIS_URL，validated_proxy 使用 REDIS_URL")
    return url


def _get_pool_for_url(url: str) -> redis.ConnectionPool:
    """按 url 缓存连接池，多租户不同 redis_url 时复用同 url 的池。"""
    global _pools_by_url
    if url not in _pools_by_url:
        _pools_by_url[url] = redis.ConnectionPool.from_url(url, decode_responses=True)
    return _pools_by_url[url]


def _get_proxy_redis_pool(redis_url: Optional[str] = None) -> redis.ConnectionPool:
    global _proxy_redis_pool
    if redis_url and redis_url.strip():
        return _get_pool_for_url(redis_url.strip())
    if _proxy_redis_pool is None:
        url = _get_proxy_redis_url()
        _proxy_redis_pool = redis.ConnectionPool.from_url(url, decode_responses=True)
    return _proxy_redis_pool


async def get_validated_proxy_client(redis_url: Optional[str] = None) -> redis.Redis:
    """返回用于 validated_proxy 的 Redis 客户端。redis_url 非空时使用该 URL 的池（按 url 缓存），否则用环境变量。"""
    pool = _get_proxy_redis_pool(redis_url)
    return redis.Redis(connection_pool=pool)


async def validated_proxy_sadd(proxy: str, redis_url: Optional[str] = None) -> bool:
    """SADD validated_proxy:set proxy。返回是否新增（1 为新增）。"""
    client = await get_validated_proxy_client(redis_url)
    n = await client.sadd(VALIDATED_PROXY_SET, proxy)
    return bool(n)


async def validated_proxy_srem(proxy: str, redis_url: Optional[str] = None) -> int:
    """SREM validated_proxy:set proxy。返回删除数量。"""
    client = await get_validated_proxy_client(redis_url)
    return await client.srem(VALIDATED_PROXY_SET, proxy)


async def validated_proxy_scard(redis_url: Optional[str] = None) -> int:
    """SCARD validated_proxy:set。"""
    client = await get_validated_proxy_client(redis_url)
    return await client.scard(VALIDATED_PROXY_SET)


async def validated_proxy_srandmember(count: int = 1, redis_url: Optional[str] = None) -> list[str]:
    """SRANDMEMBER validated_proxy:set count。返回列表（可能少于 count）。"""
    client = await get_validated_proxy_client(redis_url)
    if count <= 0:
        return []
    if count == 1:
        member = await client.srandmember(VALIDATED_PROXY_SET)
        return [member] if member is not None else []
    members = await client.srandmember(VALIDATED_PROXY_SET, count)
    return list(members) if members else []


async def validated_proxy_smembers(redis_url: Optional[str] = None) -> list[str]:
    """SMEMBERS validated_proxy:set。"""
    client = await get_validated_proxy_client(redis_url)
    members = await client.smembers(VALIDATED_PROXY_SET)
    return list(members) if members else []


def _meta_key(proxy: str) -> str:
    return f"{VALIDATED_PROXY_META_PREFIX}{proxy}"


async def validated_proxy_set_meta(proxy: str, meta: dict, redis_url: Optional[str] = None) -> None:
    """HSET validated_proxy:meta:{proxy}，字段如 protocol, latency_ms, region, source, validated_at, updated_at。"""
    client = await get_validated_proxy_client(redis_url)
    key = _meta_key(proxy)
    mapping = {k: str(v) for k, v in meta.items() if v is not None}
    if mapping:
        await client.hset(key, mapping=mapping)


async def validated_proxy_del_meta(proxy: str, redis_url: Optional[str] = None) -> int:
    """DEL validated_proxy:meta:{proxy}。返回删除的 key 数。"""
    client = await get_validated_proxy_client(redis_url)
    return await client.delete(_meta_key(proxy))


async def validated_proxy_get_meta(proxy: str, redis_url: Optional[str] = None) -> dict[str, Any]:
    """HGETALL validated_proxy:meta:{proxy}。"""
    client = await get_validated_proxy_client(redis_url)
    raw = await client.hgetall(_meta_key(proxy))
    return dict(raw) if raw else {}


# ---------------------------------------------------------------------------
# proxy_raw:set — 从代理文件 URL 解析出的候选，与 API 候选合并后参与业务校验
# ---------------------------------------------------------------------------


async def proxy_raw_delete(redis_url: Optional[str] = None) -> int:
    """DEL proxy_raw:set。返回删除的 key 数。"""
    client = await get_validated_proxy_client(redis_url)
    return await client.delete(PROXY_RAW_SET)


async def proxy_raw_sadd(proxies: list[str], redis_url: Optional[str] = None) -> int:
    """SADD proxy_raw:set 若干成员。返回新增数量。"""
    if not proxies:
        return 0
    client = await get_validated_proxy_client(redis_url)
    return await client.sadd(PROXY_RAW_SET, *proxies)


async def proxy_raw_smembers(redis_url: Optional[str] = None) -> list[str]:
    """SMEMBERS proxy_raw:set。"""
    client = await get_validated_proxy_client(redis_url)
    members = await client.smembers(PROXY_RAW_SET)
    return list(members) if members else []


async def clear_validated_proxy_pool(redis_url: Optional[str] = None) -> int:
    """清空本地已校验代理池：删除所有 validated_proxy:meta:* 并 DEL validated_proxy:set。返回删除的代理数量。"""
    client = await get_validated_proxy_client(redis_url)
    members = await client.smembers(VALIDATED_PROXY_SET)
    count = 0
    for proxy in members or []:
        key = f"{VALIDATED_PROXY_META_PREFIX}{proxy}"
        await client.delete(key)
        count += 1
    await client.delete(VALIDATED_PROXY_SET)
    return count
