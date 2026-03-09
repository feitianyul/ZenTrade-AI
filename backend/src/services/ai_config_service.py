import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.ai_config import AIConfig

logger = logging.getLogger(__name__)


class AIConfigService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_config(self, tenant_id: str, key: str) -> Optional[AIConfig]:
        stmt = (
            select(AIConfig)
            .where(
                AIConfig.tenant_id == tenant_id,
                AIConfig.key == key,
                AIConfig.is_active.is_(True),
            )
            .order_by(AIConfig.version.desc())
        )
        result = await self.db.execute(stmt)
        return result.scalars().first()

    async def list_configs(self, tenant_id: str) -> List[AIConfig]:
        stmt = (
            select(AIConfig)
            .where(
                AIConfig.tenant_id == tenant_id,
                AIConfig.is_active.is_(True),
            )
            .order_by(AIConfig.key, AIConfig.version.asc())
        )
        result = await self.db.execute(stmt)
        rows = result.scalars().all()
        logger.warning("[list_configs] raw rows=%d, tenant=%s", len(rows), tenant_id)
        # Deduplicate: keep only the latest version per key
        seen: Dict[str, AIConfig] = {}
        for r in rows:
            seen[r.key] = r
        logger.warning("[list_configs] after dedup=%d", len(seen))
        return list(seen.values())

    async def set_config(
        self,
        tenant_id: str,
        key: str,
        value: Dict[str, Any],
        description: str = None,
    ) -> AIConfig:
        # Check existing to bump version
        current = await self.get_config(tenant_id, key)
        new_version = 1
        if current:
            new_version = current.version + 1
            # Deactivate old version so list_configs only returns the latest
            current.is_active = False

        new_config = AIConfig(
            tenant_id=tenant_id,
            key=key,
            value=value,
            version=new_version,
            description=description,
            is_active=True,
        )
        self.db.add(new_config)
        await self.db.commit()
        await self.db.refresh(new_config)
        return new_config

    async def delete_config(self, tenant_id: str, key: str):
        stmt = select(AIConfig).where(
            AIConfig.tenant_id == tenant_id,
            AIConfig.key == key,
        )
        result = await self.db.execute(stmt)
        configs = result.scalars().all()
        for c in configs:
            c.is_active = False
        await self.db.commit()
