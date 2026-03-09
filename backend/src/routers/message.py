"""T246 - 私信路由"""

from typing import Any, Dict, List

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from src.core.db import get_session
from src.models.community_message import CommunityMessage
from src.schemas.response import BaseResponse, ok
from src.services.auth_service import verify_token
from src.services.message_service import get_conversations, mark_as_read, send_message

router = APIRouter(tags=["Message"])


class MessageCreate(BaseModel):
    receiver_id: str
    content: str = Field(..., min_length=1, max_length=1000)


async def _require_user(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    return await verify_token(token)


def _require_level(user, min_level: str = "intermediate"):
    """检查用户等级是否满足最低要求"""
    level_order = {"basic": 0, "intermediate": 1, "advanced": 2}
    if level_order.get(user.level, 0) < level_order.get(min_level, 1):
        raise HTTPException(status_code=403, detail=f"私信功能需{min_level}及以上等级")


@router.post("/messages", response_model=BaseResponse[Dict[str, Any]])
async def send(
    body: MessageCreate,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_user(authorization)
    _require_level(user, "intermediate")
    result = await send_message(user.tenant_id, user.user_id, body.receiver_id, body.content)
    return ok(result)


@router.get("/messages", response_model=BaseResponse[List[Dict[str, Any]]])
async def list_messages(
    limit: int = 20,
    authorization: str | None = Header(default=None),
) -> BaseResponse[List[Dict[str, Any]]]:
    user = await _require_user(authorization)
    conversations = await get_conversations(user.tenant_id, user.user_id, limit)
    return ok(conversations)


@router.put("/messages/{message_id}/read", response_model=BaseResponse[Dict[str, Any]])
async def read_message(
    message_id: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_user(authorization)
    result = await mark_as_read(user.tenant_id, user.user_id, message_id)
    return ok(result)


@router.get("/messages/unread-count", response_model=BaseResponse[Dict[str, Any]])
async def unread_count(
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    """查询当前用户未读消息计数"""
    user = await _require_user(authorization)
    async for session in get_session():
        query = (
            select(func.count(CommunityMessage.id))
            .where(CommunityMessage.receiver_id == user.user_id)
            .where(CommunityMessage.status == "unread")
        )
        count = (await session.execute(query)).scalar() or 0
        return ok({"count": count})
    return ok({"count": 0})
