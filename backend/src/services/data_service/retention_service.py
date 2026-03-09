"""T205 - 模拟数据清理与留存策略"""

import os
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select

from src.core.db import get_session, with_tenant
from src.models.order import Order

SIM_RETENTION_DAYS = int(os.getenv("SIM_DATA_RETENTION_DAYS", "30"))


async def get_sim_retention_policy(tenant_id: str) -> dict[str, Any]:
    """获取模拟数据留存策略"""
    return {
        "tenant_id": tenant_id,
        "retention_days": SIM_RETENTION_DAYS,
        "auto_cleanup": True,
        "cleanup_target": "sim",
    }


async def cleanup_sim_data(
    tenant_id: str,
    retention_days: int | None = None,
) -> dict[str, Any]:
    """清理过期模拟数据"""
    days = retention_days or SIM_RETENTION_DAYS
    cutoff = datetime.utcnow() - timedelta(days=days)
    deleted_count = 0
    async for session in get_session():
        query = (
            delete(Order)
            .where(Order.tenant_id == tenant_id)
            .where(Order.env == "sim")
            .where(Order.created_at < cutoff)
        )
        result = await session.execute(query)
        await session.commit()
        deleted_count = result.rowcount  # type: ignore[assignment]
    return {
        "tenant_id": tenant_id,
        "cutoff": cutoff.isoformat(),
        "deleted_count": deleted_count,
        "env": "sim",
    }


async def get_sim_data_stats(tenant_id: str) -> dict[str, Any]:
    """获取模拟数据统计"""
    async for session in get_session():
        total_q = (
            with_tenant(select(func.count(Order.id)), Order, tenant_id)
            .where(Order.env == "sim")
        )
        result = await session.execute(total_q)
        total = result.scalar() or 0

        cutoff = datetime.utcnow() - timedelta(days=SIM_RETENTION_DAYS)
        expired_q = (
            select(func.count(Order.id))
            .where(Order.tenant_id == tenant_id)
            .where(Order.env == "sim")
            .where(Order.created_at < cutoff)
        )
        result2 = await session.execute(expired_q)
        expired = result2.scalar() or 0

        return {
            "tenant_id": tenant_id,
            "total_sim_orders": total,
            "expired_sim_orders": expired,
            "retention_days": SIM_RETENTION_DAYS,
        }
    return {}
