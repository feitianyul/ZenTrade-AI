"""T187 - 消息总线与主题路由"""

import asyncio
from datetime import datetime
from typing import Any, Callable, Coroutine, Optional

from src.core.events import EventHandler

MessageHandler = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]


class MessageBus:
    """轻量消息总线，支持主题路由与通配符"""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[MessageHandler]] = {}
        self._queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()
        self._running = False

    def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """订阅主题（支持通配符 `*` 和 `#`）"""
        if topic not in self._subscribers:
            self._subscribers[topic] = []
        self._subscribers[topic].append(handler)

    def unsubscribe(self, topic: str, handler: MessageHandler) -> None:
        if topic in self._subscribers:
            self._subscribers[topic] = [
                h for h in self._subscribers[topic] if h is not handler
            ]

    async def publish(self, topic: str, payload: dict[str, Any]) -> int:
        """发布消息到主题"""
        message = {
            **payload,
            "_topic": topic,
            "_published_at": datetime.utcnow().isoformat(),
        }
        matched = 0
        for pattern, handlers in self._subscribers.items():
            if self._match_topic(pattern, topic):
                for handler in handlers:
                    try:
                        await handler(topic, message)
                        matched += 1
                    except Exception:
                        pass
        return matched

    async def enqueue(self, topic: str, payload: dict[str, Any]) -> None:
        """入队消息（异步处理）"""
        await self._queue.put((topic, payload))

    async def start_consumer(self) -> None:
        """启动异步消费者"""
        self._running = True
        while self._running:
            try:
                topic, payload = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self.publish(topic, payload)
            except asyncio.TimeoutError:
                continue

    def stop(self) -> None:
        """停止消费者"""
        self._running = False

    @staticmethod
    def _match_topic(pattern: str, topic: str) -> bool:
        """主题匹配（简单通配符）"""
        if pattern == topic:
            return True
        if pattern == "*":
            return True
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            return topic.startswith(prefix + ".")
        if pattern.endswith(".#"):
            prefix = pattern[:-2]
            return topic.startswith(prefix)
        return False

    def get_topics(self) -> list[str]:
        return list(self._subscribers.keys())

    def get_subscriber_count(self, topic: str) -> int:
        return len(self._subscribers.get(topic, []))


# 全局消息总线实例
_bus: Optional[MessageBus] = None


def get_message_bus() -> MessageBus:
    global _bus
    if _bus is None:
        _bus = MessageBus()
    return _bus
