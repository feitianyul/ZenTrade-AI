"""多渠道通知与交易确认服务。

FR-030: 多渠道交易确认机制
  - 在线弹窗确认
  - 离线短信/微信链接确认
  - 超时时间可配置（默认30分钟）
  - 超时未确认自动取消

FR-028: 多渠道提醒（APP/微信/短信）
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# 通知渠道
# ---------------------------------------------------------------------------

class NotifyChannel(str, Enum):
    APP = "app"          # APP内推送/弹窗
    SMS = "sms"          # 短信
    WECHAT = "wechat"    # 微信
    EMAIL = "email"      # 邮件


# ---------------------------------------------------------------------------
# 确认状态
# ---------------------------------------------------------------------------

class ConfirmStatus(str, Enum):
    PENDING = "pending"        # 待确认
    CONFIRMED = "confirmed"    # 已确认
    REJECTED = "rejected"      # 已拒绝
    EXPIRED = "expired"        # 已超时
    CANCELLED = "cancelled"    # 已取消


# ---------------------------------------------------------------------------
# 确认请求
# ---------------------------------------------------------------------------

@dataclass
class ConfirmationRequest:
    """交易确认请求（FR-030）。"""

    confirm_id: str = field(default_factory=lambda: f"cfm_{uuid.uuid4().hex[:8]}")
    order_id: str = ""
    user_id: str = ""
    tenant_id: str = ""
    channels: List[str] = field(default_factory=lambda: [NotifyChannel.APP.value])
    timeout_minutes: int = 30       # 超时时间（默认30分钟）
    status: str = ConfirmStatus.PENDING.value
    created_at: float = field(default_factory=time.time)
    confirmed_at: Optional[float] = None
    confirm_channel: Optional[str] = None  # 用户通过哪个渠道确认的


# ---------------------------------------------------------------------------
# 通知消息
# ---------------------------------------------------------------------------

@dataclass
class Notification:
    """通知消息。"""

    notification_id: str = field(default_factory=lambda: f"ntf_{uuid.uuid4().hex[:8]}")
    user_id: str = ""
    tenant_id: str = ""
    channel: str = NotifyChannel.APP.value
    title: str = ""
    content: str = ""
    level: str = "info"  # info / warning / urgent
    sent_at: float = field(default_factory=time.time)
    read: bool = False


# ---------------------------------------------------------------------------
# 通知频率控制
# ---------------------------------------------------------------------------

# 同一内容通知的最小间隔（秒）
MIN_NOTIFY_INTERVAL = 300  # 5分钟

# 内存缓存：记录最近发送时间 {(user_id, content_hash): timestamp}
_recent_sends: Dict[tuple, float] = {}


def _should_throttle(user_id: str, content_key: str) -> bool:
    """检查是否应限流（避免信息轰炸）。"""
    key = (user_id, content_key)
    last_sent = _recent_sends.get(key, 0.0)
    if time.time() - last_sent < MIN_NOTIFY_INTERVAL:
        return True
    return False


def _record_send(user_id: str, content_key: str) -> None:
    _recent_sends[(user_id, content_key)] = time.time()


# ---------------------------------------------------------------------------
# 服务函数
# ---------------------------------------------------------------------------

async def send_notification(
    channel: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """发送通知到指定渠道。"""
    user_id = payload.get("user_id", "")
    content_key = payload.get("content_key", payload.get("title", ""))

    if _should_throttle(user_id, content_key):
        return {
            "channel": channel,
            "status": "throttled",
            "reason": f"5分钟内已发送过相同通知，请稍后重试",
        }

    # TODO: 实际调用短信/微信/APP推送服务
    _record_send(user_id, content_key)

    return {
        "channel": channel,
        "status": "sent",
        "notification_id": f"ntf_{uuid.uuid4().hex[:8]}",
        "payload": payload,
    }


async def send_multi_channel(
    channels: List[str],
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """多渠道同时发送通知。"""
    results = []
    for ch in channels:
        result = await send_notification(ch, payload)
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# 交易确认
# ---------------------------------------------------------------------------

async def create_confirmation(
    order_id: str,
    user_id: str,
    tenant_id: str,
    channels: Optional[List[str]] = None,
    timeout_minutes: int = 30,
) -> ConfirmationRequest:
    """创建交易确认请求（FR-030）。"""
    cfm = ConfirmationRequest(
        order_id=order_id,
        user_id=user_id,
        tenant_id=tenant_id,
        channels=channels or [NotifyChannel.APP.value],
        timeout_minutes=timeout_minutes,
    )

    # 向所有渠道发送确认请求
    for ch in cfm.channels:
        await send_notification(ch, {
            "user_id": user_id,
            "title": "交易确认",
            "content": f"您有一笔订单（{order_id}）等待确认，请在{timeout_minutes}分钟内确认",
            "content_key": f"trade_confirm_{order_id}",
            "confirm_id": cfm.confirm_id,
            "confirm_url": f"/api/v1/trade/confirm/{cfm.confirm_id}",
        })

    return cfm


async def process_confirmation(
    confirm: ConfirmationRequest,
    action: str,
    channel: str,
) -> Dict[str, Any]:
    """处理用户确认操作。"""
    # 检查是否已超时
    elapsed_minutes = (time.time() - confirm.created_at) / 60
    if elapsed_minutes > confirm.timeout_minutes:
        confirm.status = ConfirmStatus.EXPIRED.value
        return {
            "confirm_id": confirm.confirm_id,
            "status": ConfirmStatus.EXPIRED.value,
            "message": f"确认已超时（{confirm.timeout_minutes}分钟），订单已自动取消",
        }

    if action == "confirm":
        confirm.status = ConfirmStatus.CONFIRMED.value
        confirm.confirmed_at = time.time()
        confirm.confirm_channel = channel
    elif action == "reject":
        confirm.status = ConfirmStatus.REJECTED.value
    else:
        return {"status": "invalid_action", "message": f"无效操作: {action}"}

    return {
        "confirm_id": confirm.confirm_id,
        "status": confirm.status,
        "channel": channel,
    }


async def check_expired_confirmations(
    pending: List[ConfirmationRequest],
) -> List[ConfirmationRequest]:
    """检查并标记超时的确认请求，超时未确认自动取消（FR-030）。"""
    expired: List[ConfirmationRequest] = []
    now = time.time()

    for cfm in pending:
        if cfm.status != ConfirmStatus.PENDING.value:
            continue
        elapsed = (now - cfm.created_at) / 60
        if elapsed > cfm.timeout_minutes:
            cfm.status = ConfirmStatus.EXPIRED.value
            expired.append(cfm)

            # 通知用户订单已因超时自动取消
            await send_notification(NotifyChannel.APP.value, {
                "user_id": cfm.user_id,
                "title": "订单已自动取消",
                "content": f"您的订单（{cfm.order_id}）因超过{cfm.timeout_minutes}分钟未确认已自动取消",
                "content_key": f"trade_expired_{cfm.order_id}",
            })

    return expired
