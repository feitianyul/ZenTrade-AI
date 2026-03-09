"""T246 - 私信服务"""

from typing import Any, Optional

from sqlalchemy import or_, select

from src.core.db import get_session, with_tenant
from src.models.community_message import CommunityMessage


async def send_message(
    tenant_id: str,
    sender_id: str,
    receiver_id: str,
    content: str,
) -> dict[str, Any]:
    """发送私信"""
    async for session in get_session():
        msg = CommunityMessage(
            tenant_id=tenant_id,
            sender_id=sender_id,
            receiver_id=receiver_id,
            content=content,
            status="sent",
        )
        session.add(msg)
        await session.commit()
        await session.refresh(msg)
        return {
            "message_id": msg.id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "status": "sent",
        }
    return {"error": "no session"}


async def get_conversations(
    tenant_id: str, user_id: str, limit: int = 20
) -> list[dict[str, Any]]:
    """获取对话列表"""
    async for session in get_session():
        query = (
            with_tenant(select(CommunityMessage), CommunityMessage, tenant_id)
            .where(
                or_(
                    CommunityMessage.sender_id == user_id,
                    CommunityMessage.receiver_id == user_id,
                )
            )
            .order_by(CommunityMessage.created_at.desc())
            .limit(limit)
        )
        result = await session.execute(query)
        messages = result.scalars().all()
        return [
            {
                "message_id": m.id,
                "sender_id": m.sender_id,
                "receiver_id": m.receiver_id,
                "content": m.content,
                "status": m.status,
                "created_at": m.created_at.isoformat() if m.created_at else "",
            }
            for m in messages
        ]
    return []


async def mark_as_read(
    tenant_id: str, user_id: str, message_id: str
) -> dict[str, Any]:
    """标记已读"""
    async for session in get_session():
        query = (
            with_tenant(select(CommunityMessage), CommunityMessage, tenant_id)
            .where(CommunityMessage.id == message_id)
            .where(CommunityMessage.receiver_id == user_id)
        )
        result = await session.execute(query)
        msg = result.scalar_one_or_none()
        if msg:
            msg.status = "read"
            await session.commit()
            return {"message_id": message_id, "status": "read"}
    return {"error": "message not found"}
