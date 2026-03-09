import json
import os
from typing import Any, Optional

import redis.asyncio as redis

_redis_pool: Optional[redis.ConnectionPool] = None

def get_redis_pool() -> redis.ConnectionPool:
    global _redis_pool
    if _redis_pool is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _redis_pool = redis.ConnectionPool.from_url(redis_url, decode_responses=True)
    return _redis_pool

async def get_redis_client() -> redis.Redis:
    pool = get_redis_pool()
    return redis.Redis(connection_pool=pool)

async def publish_stream(stream: str, payload: dict[str, Any]) -> str:
    """
    Publish a message to a Redis Stream.
    Returns the message ID.
    """
    client = await get_redis_client()
    # Ensure all values are strings for simple JSON serialization if needed,
    # or rely on Redis to handle primitives. 
    # Redis streams keys/values are bytes or strings.
    # We'll convert dict values to strings if they are complex types.
    message = {}
    for k, v in payload.items():
        if isinstance(v, (dict, list)):
            message[k] = json.dumps(v)
        else:
            message[k] = str(v)
            
    msg_id = await client.xadd(stream, message)
    return str(msg_id)

async def consume_stream(
    stream: str,
    group: str,
    consumer: str,
    count: int = 1,
) -> list[tuple[str, dict[str, Any]]]:
    """
    Consume messages from a Redis Stream using a consumer group.
    """
    client = await get_redis_client()
    try:
        await client.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    messages = await client.xreadgroup(group, consumer, {stream: ">"}, count=count)
    result = []
    for stream_name, msgs in messages:
        for msg_id, data in msgs:
            result.append((msg_id, data))
    return result

async def ack_stream(stream: str, group: str, msg_ids: list[str]) -> int:
    client = await get_redis_client()
    return await client.xack(stream, group, *msg_ids)
