"""T208 - 服务注册与心跳写入 Redis"""

import asyncio
import os
import time
from typing import Any, Optional

from src.core.streams import get_redis_client

SERVICE_REGISTRY_PREFIX = "service:registry:"
HEARTBEAT_TTL = int(os.getenv("SERVICE_HEARTBEAT_TTL", "30"))  # seconds
HEARTBEAT_INTERVAL = int(os.getenv("SERVICE_HEARTBEAT_INTERVAL", "10"))


async def register_service(
    service_name: str,
    host: str,
    port: int,
    metadata: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """注册服务实例"""
    client = await get_redis_client()
    key = f"{SERVICE_REGISTRY_PREFIX}{service_name}:{host}:{port}"
    info = {
        "service_name": service_name,
        "host": host,
        "port": str(port),
        "registered_at": str(time.time()),
        "status": "healthy",
        **(metadata or {}),
    }
    await client.hset(key, mapping=info)
    await client.expire(key, HEARTBEAT_TTL)
    return {"key": key, **info}


async def deregister_service(
    service_name: str, host: str, port: int
) -> bool:
    """注销服务实例"""
    client = await get_redis_client()
    key = f"{SERVICE_REGISTRY_PREFIX}{service_name}:{host}:{port}"
    return (await client.delete(key)) > 0


async def send_heartbeat(
    service_name: str, host: str, port: int
) -> bool:
    """发送心跳"""
    client = await get_redis_client()
    key = f"{SERVICE_REGISTRY_PREFIX}{service_name}:{host}:{port}"
    exists = await client.exists(key)
    if not exists:
        return False
    await client.hset(key, "last_heartbeat", str(time.time()))
    await client.expire(key, HEARTBEAT_TTL)
    return True


async def discover_services(service_name: str) -> list[dict[str, str]]:
    """发现服务实例"""
    client = await get_redis_client()
    pattern = f"{SERVICE_REGISTRY_PREFIX}{service_name}:*"
    instances = []
    async for key in client.scan_iter(match=pattern, count=50):
        info = await client.hgetall(key)
        if info:
            instances.append(info)
    return instances


async def get_service_health(service_name: str) -> dict[str, Any]:
    """获取服务健康状态"""
    instances = await discover_services(service_name)
    healthy = [i for i in instances if i.get("status") == "healthy"]
    return {
        "service_name": service_name,
        "total_instances": len(instances),
        "healthy_instances": len(healthy),
        "instances": instances,
    }


async def start_heartbeat_loop(
    service_name: str, host: str, port: int
) -> None:
    """启动心跳循环（后台任务）"""
    while True:
        await send_heartbeat(service_name, host, port)
        await asyncio.sleep(HEARTBEAT_INTERVAL)
