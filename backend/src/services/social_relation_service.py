"""T245 - 关注与好友关系服务"""

from typing import Any

from sqlalchemy import select

from src.core.db import get_session, with_tenant
from src.models.community_relation import CommunityRelation


async def follow_user(
    tenant_id: str, follower_id: str, followee_id: str
) -> dict[str, Any]:
    """关注用户"""
    if follower_id == followee_id:
        return {"error": "cannot follow self"}
    async for session in get_session():
        relation = CommunityRelation(
            tenant_id=tenant_id,
            user_id=follower_id,
            target_user_id=followee_id,
            relation_type="follow",
        )
        session.add(relation)
        await session.commit()
        return {"status": "followed", "followee_id": followee_id}
    return {"error": "no session"}


async def unfollow_user(
    tenant_id: str, follower_id: str, followee_id: str
) -> dict[str, Any]:
    """取消关注"""
    async for session in get_session():
        query = (
            with_tenant(select(CommunityRelation), CommunityRelation, tenant_id)
            .where(CommunityRelation.user_id == follower_id)
            .where(CommunityRelation.target_user_id == followee_id)
            .where(CommunityRelation.relation_type == "follow")
        )
        result = await session.execute(query)
        relation = result.scalar_one_or_none()
        if relation:
            await session.delete(relation)
            await session.commit()
            return {"status": "unfollowed"}
        return {"status": "not_following"}
    return {"error": "no session"}


async def get_followers(
    tenant_id: str, user_id: str, limit: int = 50
) -> list[dict[str, Any]]:
    """获取粉丝列表"""
    async for session in get_session():
        query = (
            with_tenant(select(CommunityRelation), CommunityRelation, tenant_id)
            .where(CommunityRelation.target_user_id == user_id)
            .where(CommunityRelation.relation_type == "follow")
            .limit(limit)
        )
        result = await session.execute(query)
        relations = result.scalars().all()
        return [{"user_id": r.user_id, "since": r.created_at.isoformat()} for r in relations]
    return []


async def get_following(
    tenant_id: str, user_id: str, limit: int = 50
) -> list[dict[str, Any]]:
    """获取关注列表"""
    async for session in get_session():
        query = (
            with_tenant(select(CommunityRelation), CommunityRelation, tenant_id)
            .where(CommunityRelation.user_id == user_id)
            .where(CommunityRelation.relation_type == "follow")
            .limit(limit)
        )
        result = await session.execute(query)
        relations = result.scalars().all()
        return [{"user_id": r.target_user_id, "since": r.created_at.isoformat()} for r in relations]
    return []


async def check_relation(
    tenant_id: str, from_id: str, to_id: str
) -> str:
    """检查关系"""
    async for session in get_session():
        query = (
            with_tenant(select(CommunityRelation), CommunityRelation, tenant_id)
            .where(CommunityRelation.user_id == from_id)
            .where(CommunityRelation.target_user_id == to_id)
        )
        result = await session.execute(query)
        relation = result.scalar_one_or_none()
        if relation:
            return relation.relation_type
    return "stranger"
