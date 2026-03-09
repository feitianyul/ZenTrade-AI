from fastapi import APIRouter, Header, HTTPException

from src.schemas.community import (
    CommunityInteractionCreate,
    CommunityInteractionOut,
    CommunityMessageCreate,
    CommunityMessageOut,
    CommunityPostCreate,
    CommunityPostOut,
    CommunityRankItem,
)
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.community_rank_service import build_rankings
from src.services.community_service import add_interaction, create_post, send_message

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.post("/community/post", response_model=BaseResponse[CommunityPostOut])
async def create_community_post(
    payload: CommunityPostCreate,
    authorization: str | None = Header(default=None),
) -> BaseResponse[CommunityPostOut]:
    user = await _require_user(authorization)
    record = await create_post(
        user.tenant_id,
        user.user_id,
        payload.title,
        payload.content,
        payload.tags or {},
        payload.post_type,
    )
    return ok(
        CommunityPostOut(
            post_id=record.id,
            tenant_id=record.tenant_id,
            author_id=record.author_id,
            title=record.title,
            content=record.content,
            tags=record.tags,
            post_type=record.post_type,
            like_count=record.like_count,
            comment_count=record.comment_count,
        )
    )


@router.post("/community/interaction", response_model=BaseResponse[CommunityInteractionOut])
async def create_interaction(
    payload: CommunityInteractionCreate,
    authorization: str | None = Header(default=None),
) -> BaseResponse[CommunityInteractionOut]:
    user = await _require_user(authorization)
    record = await add_interaction(
        user.tenant_id,
        payload.post_id,
        user.user_id,
        payload.interaction_type,
        payload.content or "",
    )
    return ok(
        CommunityInteractionOut(
            interaction_id=record.id,
            post_id=record.post_id,
            user_id=record.user_id,
            interaction_type=record.interaction_type,
            content=record.content,
        )
    )


@router.post("/community/message", response_model=BaseResponse[CommunityMessageOut])
async def create_message(
    payload: CommunityMessageCreate,
    authorization: str | None = Header(default=None),
) -> BaseResponse[CommunityMessageOut]:
    user = await _require_user(authorization)
    record = await send_message(
        user.tenant_id,
        user.user_id,
        payload.receiver_id,
        payload.content,
    )
    return ok(
        CommunityMessageOut(
            message_id=record.id,
            sender_id=record.sender_id,
            receiver_id=record.receiver_id,
            content=record.content,
            status=record.status,
        )
    )


@router.get("/community/rankings", response_model=BaseResponse[list[CommunityRankItem]])
async def get_rankings(
    authorization: str | None = Header(default=None),
) -> BaseResponse[list[CommunityRankItem]]:
    await _require_user(authorization)
    items = await build_rankings()
    return ok(list(items))
