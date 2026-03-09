"""T223 - 知识库服务"""

from typing import Any, Optional

from sqlalchemy import func, select

from src.core.db import get_session, with_tenant
from src.models.knowledge_base import KnowledgeBase, KnowledgeEntry


async def create_knowledge_base(
    tenant_id: str, name: str, description: str, owner_id: str, category: str = "general"
) -> KnowledgeBase:
    async for session in get_session():
        kb = KnowledgeBase(
            tenant_id=tenant_id,
            name=name,
            description=description,
            owner_id=owner_id,
            category=category,
        )
        session.add(kb)
        await session.commit()
        await session.refresh(kb)
        return kb
    raise RuntimeError("no session")


async def add_entry(
    tenant_id: str, kb_id: str, title: str, content: str, source: str = "", tags: Optional[dict] = None
) -> KnowledgeEntry:
    async for session in get_session():
        entry = KnowledgeEntry(
            tenant_id=tenant_id,
            kb_id=kb_id,
            title=title,
            content=content,
            source=source,
            tags_json=tags or {},
        )
        session.add(entry)
        await session.commit()
        await session.refresh(entry)
        return entry
    raise RuntimeError("no session")


async def list_knowledge_bases(tenant_id: str, limit: int = 50) -> list[KnowledgeBase]:
    async for session in get_session():
        query = (
            with_tenant(select(KnowledgeBase), KnowledgeBase, tenant_id)
            .where(KnowledgeBase.is_active.is_(True))
            .order_by(KnowledgeBase.created_at.desc())
            .limit(limit)
        )
        result = await session.execute(query)
        return list(result.scalars().all())
    return []


async def search_entries(tenant_id: str, kb_id: str, query_text: str, limit: int = 10) -> list[KnowledgeEntry]:
    """搜索知识条目（简单文本匹配，生产中使用向量检索）"""
    async for session in get_session():
        q = (
            with_tenant(select(KnowledgeEntry), KnowledgeEntry, tenant_id)
            .where(KnowledgeEntry.kb_id == kb_id)
            .where(KnowledgeEntry.content.contains(query_text))
            .limit(limit)
        )
        result = await session.execute(q)
        return list(result.scalars().all())
    return []


async def delete_knowledge_base(tenant_id: str, kb_id: str) -> bool:
    async for session in get_session():
        q = with_tenant(select(KnowledgeBase), KnowledgeBase, tenant_id).where(KnowledgeBase.id == kb_id)
        result = await session.execute(q)
        kb = result.scalar_one_or_none()
        if kb:
            kb.is_active = False
            await session.commit()
            return True
    return False
