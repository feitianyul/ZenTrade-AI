"""因子服务 — 查 user_factors 表获取公开因子列表"""

import logging
from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_session
from src.models.user_factor import UserFactor

logger = logging.getLogger(__name__)


class FactorService:
    def __init__(self, db: AsyncSession | None = None):
        self.db = db

    async def get_public_factors(self, category: str = None) -> List[dict]:
        """查询 user_factors 表中 is_public=True 的因子"""
        try:
            async for session in get_session():
                query = select(UserFactor).where(UserFactor.is_public.is_(True))
                result = await session.execute(query)
                factors = result.scalars().all()

                if not factors:
                    return []

                return [
                    {
                        "id": str(f.id),
                        "name": f.name,
                        "category": "Custom",
                        "author": str(f.tenant_id) if hasattr(f, "tenant_id") else "user",
                        "description": f.description or "",
                    }
                    for f in factors
                ]
        except Exception as exc:
            logger.warning("get_public_factors failed: %s", exc)
            return []
