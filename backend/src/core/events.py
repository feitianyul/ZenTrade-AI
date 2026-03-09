"""T186 - WebSocket 通道与事件主题"""

from datetime import datetime
from typing import Any, Callable, Coroutine

# 事件主题常量
TOPIC_ORDER_STATUS = "order_status"
TOPIC_POSITION_UPDATE = "position_update"
TOPIC_MARKET_UPDATE = "market_update"
TOPIC_TRADE_UPDATE = "trade_update"
TOPIC_ALERT = "alert"
TOPIC_SYSTEM = "system"
TOPIC_HEARTBEAT = "heartbeat"
TOPIC_CONFIG_CHANGE = "config_change"
TOPIC_AI_RESULT = "ai_result"

ALL_TOPICS = [
    TOPIC_ORDER_STATUS,
    TOPIC_POSITION_UPDATE,
    TOPIC_MARKET_UPDATE,
    TOPIC_TRADE_UPDATE,
    TOPIC_ALERT,
    TOPIC_SYSTEM,
    TOPIC_HEARTBEAT,
    TOPIC_CONFIG_CHANGE,
    TOPIC_AI_RESULT,
]

# 事件处理器类型
EventHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]

# 事件总线：主题 -> 处理器列表
_handlers: dict[str, list[EventHandler]] = {}


def subscribe(topic: str, handler: EventHandler) -> None:
    """订阅事件主题"""
    if topic not in _handlers:
        _handlers[topic] = []
    _handlers[topic].append(handler)


def unsubscribe(topic: str, handler: EventHandler) -> None:
    """取消订阅"""
    if topic in _handlers:
        _handlers[topic] = [h for h in _handlers[topic] if h is not handler]


async def publish(topic: str, payload: dict[str, Any]) -> int:
    """发布事件到主题"""
    event = {
        "topic": topic,
        "payload": payload,
        "timestamp": datetime.utcnow().isoformat(),
    }
    handlers = _handlers.get(topic, [])
    for handler in handlers:
        try:
            await handler(event)
        except Exception:
            pass  # 生产中记录日志
    return len(handlers)


def list_topics() -> list[str]:
    """列出所有已注册主题"""
    return ALL_TOPICS


def get_subscriber_count(topic: str) -> int:
    """获取主题订阅者数量"""
    return len(_handlers.get(topic, []))


def make_event(topic: str, payload: dict[str, Any]) -> dict[str, Any]:
    """构造标准事件格式"""
    return {
        "type": topic,
        "payload": payload,
        "timestamp": datetime.utcnow().isoformat(),
    }
