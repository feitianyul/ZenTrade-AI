from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.context_index import ContextIndex


class ContextRuleService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_context_rules(self, tenant_id: str, context_type: str) -> dict:
        # Load rules from AI Config or DB
        # For now, return hardcoded defaults
        return {
            "max_tokens": 4096,
            "retention_days": 30,
            "embedding_model": "text-embedding-ada-002",
        }

    async def index_session(
        self,
        tenant_id: str,
        session_id: str,
        context_type: str,
        meta_data: dict,
    ):
        idx = ContextIndex(
            tenant_id=tenant_id,
            session_id=session_id,
            context_type=context_type,
            meta_data=meta_data,
        )
        self.db.add(idx)
        await self.db.commit()
        return idx

    async def get_session_context(self, tenant_id: str, session_id: str):
        stmt = select(ContextIndex).where(
            ContextIndex.tenant_id == tenant_id,
            ContextIndex.session_id == session_id,
        )
        result = await self.db.execute(stmt)
        return result.scalars().all()
