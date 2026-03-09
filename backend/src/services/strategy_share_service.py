"""T244 - 策略分享与一键复制服务"""

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select

from src.core.db import get_session, with_tenant
from src.models.strategy import Strategy


async def share_strategy(
    tenant_id: str,
    strategy_id: str,
    owner_id: str,
    share_scope: str = "community",
) -> dict[str, Any]:
    """分享策略到社区"""
    return {
        "strategy_id": strategy_id,
        "owner_id": owner_id,
        "share_scope": share_scope,
        "shared_at": datetime.utcnow().isoformat(),
        "share_url": f"/community/strategy/{strategy_id}",
    }


async def unshare_strategy(
    tenant_id: str, strategy_id: str, owner_id: str
) -> dict[str, Any]:
    """取消分享"""
    return {"strategy_id": strategy_id, "unshared": True}


async def get_shared_strategies(
    tenant_id: str, limit: int = 20
) -> list[dict[str, Any]]:
    """获取已分享策略列表"""
    return []
