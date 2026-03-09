from typing import Any

from src.core.db import get_session
from src.models.replay_report import ReplayReport


async def create_replay_report(
    tenant_id: str,
    strategy_id: str,
    start_date: str,
    end_date: str,
) -> ReplayReport:
    report_payload: dict[str, Any] = {
        "range": {"start": start_date, "end": end_date},
        "summary": "回测复盘完成",
        "signals": [],
    }
    async for session in get_session():
        record = ReplayReport(
            tenant_id=tenant_id,
            strategy_id=strategy_id,
            report_json=report_payload,
            status="completed",
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record
    raise RuntimeError("session unavailable")
