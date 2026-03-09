"""AI 分析报告版本：创建、按标的分页列表、获取单条、删除、更新状态。"""
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import with_tenant
from src.core.time_util import utc_to_beijing_str
from src.models.ai_analysis_report import AiAnalysisReport


def _main_summary_from_snapshot(snapshot: Optional[dict[str, Any]]) -> Optional[str]:
    if not snapshot or not isinstance(snapshot, dict):
        return None
    main = snapshot.get("main")
    if isinstance(main, dict):
        s = main.get("summary")
        if isinstance(s, str):
            return (s[:200] + "…") if len(s) > 200 else s
    return None


def _format_created_at(created_at) -> Optional[str]:
    """将存储的 UTC 时间格式化为北京时间字符串 YYYY-MM-DD HH:MM:SS，供前端展示。"""
    if created_at is None:
        return None
    try:
        return utc_to_beijing_str(created_at)
    except Exception:
        return str(created_at)


async def create_report(
    db: AsyncSession,
    *,
    tenant_id: str,
    created_by: str,
    symbol: str,
    report_snapshot: dict[str, Any],
    user_notes: Optional[str] = None,
    status: str = "draft",
) -> str:
    """写入一条报告，返回 id。"""
    report = AiAnalysisReport(
        tenant_id=tenant_id,
        symbol=symbol,
        report_snapshot=report_snapshot,
        user_notes=user_notes,
        status=status,
        created_by=created_by,
    )
    db.add(report)
    await db.commit()
    await db.refresh(report)
    return report.id


async def list_by_symbol(
    db: AsyncSession,
    tenant_id: str,
    symbol: str,
    page: int = 1,
    page_size: int = 10,
    sort_by: str = "created_at",
    order: str = "desc",
) -> tuple[list[tuple[str, str, Optional[str], str, Optional[str]]], int]:
    """分页列表，返回 (items, total)。item = (id, symbol, main_summary, status, created_at)。"""
    where_clause = with_tenant(select(AiAnalysisReport), AiAnalysisReport, tenant_id).where(
        AiAnalysisReport.symbol == symbol
    )
    # total
    count_q = select(func.count()).select_from(AiAnalysisReport).where(
        AiAnalysisReport.tenant_id == tenant_id,
        AiAnalysisReport.symbol == symbol,
    )
    total_result = await db.execute(count_q)
    total = total_result.scalar() or 0
    # order
    base = where_clause
    order_col = getattr(AiAnalysisReport, sort_by, AiAnalysisReport.created_at)
    if order and order.lower() == "asc":
        base = base.order_by(order_col.asc())
    else:
        base = base.order_by(order_col.desc())
    base = base.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(base)
    rows = result.scalars().all()
    items = [
        (
            r.id,
            r.symbol,
            _main_summary_from_snapshot(r.report_snapshot),
            r.status or "draft",
            _format_created_at(r.created_at),
        )
        for r in rows
    ]
    return items, total


async def get_by_id(
    db: AsyncSession,
    tenant_id: str,
    report_id: str,
) -> Optional[AiAnalysisReport]:
    """按 id 取一条，校验 tenant。"""
    q = with_tenant(select(AiAnalysisReport), AiAnalysisReport, tenant_id).where(
        AiAnalysisReport.id == report_id
    )
    result = await db.execute(q)
    return result.scalar_one_or_none()


async def delete_report(db: AsyncSession, tenant_id: str, report_id: str) -> bool:
    """按 id 删除，校验 tenant。返回是否删除了记录。"""
    report = await get_by_id(db, tenant_id, report_id)
    if report is None:
        return False
    await db.delete(report)
    await db.commit()
    return True


async def update_status(
    db: AsyncSession,
    tenant_id: str,
    report_id: str,
    status: str,
) -> bool:
    """更新状态（如 adopted），校验 tenant。返回是否更新了记录。"""
    report = await get_by_id(db, tenant_id, report_id)
    if report is None:
        return False
    report.status = status
    await db.commit()
    await db.refresh(report)
    return True
