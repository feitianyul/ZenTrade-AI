"""T244 - 策略一键复制服务"""

from typing import Any

from sqlalchemy import select

from src.core.db import get_session, with_tenant
from src.models.strategy import Strategy


async def clone_strategy(
    tenant_id: str,
    source_strategy_id: str,
    target_owner_id: str,
    new_name: str | None = None,
) -> dict[str, Any]:
    """一键复制策略"""
    async for session in get_session():
        query = with_tenant(
            select(Strategy), Strategy, tenant_id
        ).where(Strategy.id == source_strategy_id)
        result = await session.execute(query)
        source = result.scalar_one_or_none()
        if not source:
            return {"error": "source strategy not found"}

        cloned = Strategy(
            tenant_id=tenant_id,
            name=new_name or f"{source.name} (副本)",
            logic_code=source.logic_code,
            params_json=source.params_json,
            status="draft",
            owner_id=target_owner_id,
            is_deleted=False,
        )
        session.add(cloned)
        await session.commit()
        await session.refresh(cloned)
        return {
            "cloned_id": cloned.id,
            "source_id": source_strategy_id,
            "name": cloned.name,
            "status": "cloned",
        }
    return {"error": "no session"}
