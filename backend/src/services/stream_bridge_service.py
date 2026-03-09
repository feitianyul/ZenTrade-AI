"""T206 - Redis Streams 生产/消费桥接服务"""

import asyncio
from typing import Any, Callable, Coroutine, Optional

from src.core.streams import ack_stream, consume_stream, get_redis_client, publish_stream

StreamHandler = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]

# 消费者组注册
_consumer_groups: dict[str, StreamHandler] = {}


async def produce(stream: str, payload: dict[str, Any]) -> str:
    """生产消息到 Redis Stream"""
    msg_id = await publish_stream(stream, payload)
    return msg_id


async def register_consumer(
    stream: str,
    group: str,
    consumer_name: str,
    handler: StreamHandler,
) -> None:
    """注册消费者"""
    key = f"{stream}:{group}:{consumer_name}"
    _consumer_groups[key] = handler


async def consume_once(
    stream: str,
    group: str,
    consumer_name: str,
    count: int = 10,
) -> list[dict[str, Any]]:
    """单次消费"""
    messages = await consume_stream(stream, group, consumer_name, count)
    results = []
    for msg_id, data in messages:
        key = f"{stream}:{group}:{consumer_name}"
        handler = _consumer_groups.get(key)
        if handler:
            await handler(msg_id, data)
        results.append({"msg_id": msg_id, "data": data})
        await ack_stream(stream, group, [msg_id])
    return results


async def start_consumer_loop(
    stream: str,
    group: str,
    consumer_name: str,
    handler: StreamHandler,
    poll_interval: float = 1.0,
) -> None:
    """启动持续消费循环"""
    await register_consumer(stream, group, consumer_name, handler)
    while True:
        try:
            messages = await consume_stream(stream, group, consumer_name, count=10)
            for msg_id, data in messages:
                await handler(msg_id, data)
                await ack_stream(stream, group, [msg_id])
        except Exception:
            pass
        await asyncio.sleep(poll_interval)


async def get_stream_info(stream: str) -> dict[str, Any]:
    """获取 Stream 信息"""
    client = await get_redis_client()
    try:
        info = await client.xinfo_stream(stream)
        return {
            "stream": stream,
            "length": info.get("length", 0),
            "first_entry": str(info.get("first-entry", "")),
            "last_entry": str(info.get("last-entry", "")),
        }
    except Exception:
        return {"stream": stream, "length": 0, "error": "stream not found"}


async def get_consumer_groups(stream: str) -> list[dict[str, Any]]:
    """获取消费者组信息"""
    client = await get_redis_client()
    try:
        groups = await client.xinfo_groups(stream)
        return [
            {
                "name": g.get("name", ""),
                "consumers": g.get("consumers", 0),
                "pending": g.get("pending", 0),
            }
            for g in groups
        ]
    except Exception:
        return []
