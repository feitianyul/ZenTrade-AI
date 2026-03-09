import re
from typing import Iterable

from sqlalchemy import func, select

from src.core.db import get_session, with_tenant
from src.models.community_interaction import CommunityInteraction
from src.models.community_message import CommunityMessage
from src.models.community_post import CommunityPost

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _sanitize(text: str) -> str:
    """Remove HTML/script tags to prevent XSS."""
    return _HTML_TAG_RE.sub("", text)


async def create_post(
    tenant_id: str,
    author_id: str,
    title: str,
    content: str,
    tags: dict[str, str],
    post_type: str,
) -> CommunityPost:
    async for session in get_session():
        record = CommunityPost(
            tenant_id=tenant_id,
            author_id=author_id,
            title=_sanitize(title),
            content=_sanitize(content),
            tags=tags,
            post_type=post_type,
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record
    raise RuntimeError("session unavailable")


async def list_posts(
    tenant_id: str, page: int, limit: int
) -> tuple[Iterable[CommunityPost], int]:
    async for session in get_session():
        base_query = with_tenant(select(CommunityPost), CommunityPost, tenant_id)
        total_stmt = select(func.count()).select_from(base_query.subquery())
        total_result = await session.execute(total_stmt)
        total = int(total_result.scalar() or 0)
        result = await session.execute(
            base_query.order_by(CommunityPost.created_at.desc())
            .offset((page - 1) * limit)
            .limit(limit)
        )
        return result.scalars().all(), total
    return [], 0


async def add_interaction(
    tenant_id: str,
    post_id: str,
    user_id: str,
    interaction_type: str,
    content: str,
) -> CommunityInteraction:
    async for session in get_session():
        record = CommunityInteraction(
            tenant_id=tenant_id,
            post_id=post_id,
            user_id=user_id,
            interaction_type=interaction_type,
            content=_sanitize(content),
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record
    raise RuntimeError("session unavailable")


async def send_message(
    tenant_id: str,
    sender_id: str,
    receiver_id: str,
    content: str,
) -> CommunityMessage:
    async for session in get_session():
        record = CommunityMessage(
            tenant_id=tenant_id,
            sender_id=sender_id,
            receiver_id=receiver_id,
            content=_sanitize(content),
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record
    raise RuntimeError("session unavailable")
