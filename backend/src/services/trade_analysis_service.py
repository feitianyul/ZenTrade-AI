from typing import Any

from src.core.db import get_session
from src.models.trade_analysis import TradeAnalysis


async def create_trade_analysis(
    tenant_id: str,
    trade_id: str,
    metrics: dict[str, Any],
    summary: str,
) -> TradeAnalysis:
    async for session in get_session():
        record = TradeAnalysis(
            tenant_id=tenant_id,
            trade_id=trade_id,
            metrics_json=metrics,
            summary=summary,
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record
    raise RuntimeError("session unavailable")
