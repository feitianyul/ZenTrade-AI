"""T245 - 关注与好友关系路由"""

from typing import Any, Dict, List

from fastapi import APIRouter, Header, HTTPException

from src.schemas.response import BaseResponse, ok
from src.services.auth_service import verify_token
from src.services.social_relation_service import (
    check_relation,
    follow_user,
    get_followers,
    get_following,
    unfollow_user,
)

router = APIRouter(tags=["Social"])


async def _require_user(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    return await verify_token(token)


@router.post("/social/follow/{user_id}", response_model=BaseResponse[Dict[str, Any]])
async def follow(
    user_id: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_user(authorization)
    result = await follow_user(user.tenant_id, user.user_id, user_id)
    return ok(result)


@router.delete("/social/follow/{user_id}", response_model=BaseResponse[Dict[str, Any]])
async def unfollow(
    user_id: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_user(authorization)
    result = await unfollow_user(user.tenant_id, user.user_id, user_id)
    return ok(result)


@router.get("/social/followers", response_model=BaseResponse[List[Dict[str, Any]]])
async def list_followers(
    limit: int = 50,
    authorization: str | None = Header(default=None),
) -> BaseResponse[List[Dict[str, Any]]]:
    user = await _require_user(authorization)
    followers = await get_followers(user.tenant_id, user.user_id, limit)
    return ok(followers)


@router.get("/social/following", response_model=BaseResponse[List[Dict[str, Any]]])
async def list_following(
    limit: int = 50,
    authorization: str | None = Header(default=None),
) -> BaseResponse[List[Dict[str, Any]]]:
    user = await _require_user(authorization)
    following = await get_following(user.tenant_id, user.user_id, limit)
    return ok(following)


@router.get("/social/relation/{user_id}", response_model=BaseResponse[Dict[str, Any]])
async def get_relation(
    user_id: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[Dict[str, Any]]:
    user = await _require_user(authorization)
    relation = await check_relation(user.tenant_id, user.user_id, user_id)
    return ok({"relation": relation})
