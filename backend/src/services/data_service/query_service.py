"""T203 - 模拟/实盘数据标签与查询接口"""

from typing import Any, Optional

from sqlalchemy import select

from src.core.db import get_session, with_tenant
from src.models.order import Order


async def query_orders_by_env(
    tenant_id: str,
    user_id: str,
    env: str = "sim",
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """按环境标签查询订单"""
    async for session in get_session():
        query = (
            with_tenant(select(Order), Order, tenant_id)
            .where(Order.user_id == user_id)
            .where(Order.env == env)
            .where(Order.is_deleted.is_(False))
            .order_by(Order.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await session.execute(query)
        orders = result.scalars().all()
        return [
            {
                "order_id": o.id,
                "symbol": o.symbol,
                "direction": o.direction,
                "price": o.price,
                "volume": o.volume,
                "status": o.status,
                "env": o.env,
                "is_sim": o.env == "sim",
                "created_at": o.created_at.isoformat() if o.created_at else "",
            }
            for o in orders
        ]
    return []


async def get_env_statistics(
    tenant_id: str, user_id: str
) -> dict[str, Any]:
    """获取模拟/实盘统计"""
    sim_orders = await query_orders_by_env(tenant_id, user_id, "sim", limit=10000)
    real_orders = await query_orders_by_env(tenant_id, user_id, "real", limit=10000)
    return {
        "sim_count": len(sim_orders),
        "real_count": len(real_orders),
        "total": len(sim_orders) + len(real_orders),
    }


async def tag_order_environment(
    order_data: dict[str, Any]
) -> dict[str, Any]:
    """为订单数据添加环境标签"""
    env = order_data.get("env", "sim")
    return {
        **order_data,
        "is_sim": env == "sim",
        "env_label": "[模拟]" if env == "sim" else "[实盘]",
        "env_color": "warning" if env == "sim" else "success",
    }
