"""AI 个股分析：生成分析、确认采纳、报告版本 CRUD API。"""
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.auth import get_current_user
from src.core.db import get_db
from src.schemas.ai_analysis import (
    AiAnalysisConfirmRequest,
    AiAnalysisGenerateRequest,
    AiAnalysisReportCreate,
    AiAnalysisReportItem,
    AiAnalysisReportList,
    AiAnalysisReportOut,
    AiAnalysisResult,
)
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.ai_analysis_report_service import (
    _format_created_at,
    create_report,
    delete_report,
    get_by_id,
    list_by_symbol,
    update_status,
)
from src.services.ai_analysis_service import confirm_analysis, generate_analysis

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ai-analysis", tags=["AI 分析"])


@router.post("/generate", response_model=BaseResponse[AiAnalysisResult])
async def post_generate(
    body: AiAnalysisGenerateRequest,
    current_user: UserOut = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[AiAnalysisResult]:
    """生成个股多维度 AI 分析（技术面/资金面/基本面/舆情 + 主控结论与建议）。"""
    result = await generate_analysis(
        symbol=body.symbol,
        user_notes=body.user_notes,
        tenant_id=current_user.tenant_id,
        db=db,
    )
    return ok(result)


@router.post("/confirm", response_model=BaseResponse[dict])
async def post_confirm(
    body: AiAnalysisConfirmRequest,
    request: Request,
    current_user: UserOut = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[dict]:
    """确认（采纳）分析结论，写入审计日志；若带 report_id 则将该报告标为 adopted。"""
    ip_address = request.client.host if request.client else ""
    user_agent = (request.headers.get("user-agent") or "")[:256]
    try:
        await confirm_analysis(
            tenant_id=current_user.tenant_id,
            actor_id=current_user.user_id,
            symbol=body.symbol,
            user_notes=body.user_notes,
            main_summary=body.main_summary,
            suggest_text=body.suggest_text,
            ip_address=ip_address,
            user_agent=user_agent,
        )
    except Exception as e:
        logger.warning("ai_analysis confirm_analysis (audit) failed: %s", e, exc_info=True)
    if body.report_id:
        try:
            await update_status(db, current_user.tenant_id, body.report_id, "adopted")
        except Exception as e:
            logger.warning("ai_analysis update_status report_id=%s failed: %s", body.report_id, e, exc_info=True)
    return ok({"ok": True, "symbol": body.symbol})


# ----- 报告版本 -----


@router.post("/reports", response_model=BaseResponse[dict])
async def post_report(
    body: AiAnalysisReportCreate,
    current_user: UserOut = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[dict]:
    """创建一条分析报告（draft），返回 id。"""
    report_id = await create_report(
        db,
        tenant_id=current_user.tenant_id,
        created_by=current_user.user_id,
        symbol=body.symbol,
        report_snapshot=body.report_snapshot,
        user_notes=body.user_notes,
        status=body.status or "draft",
    )
    return ok({"id": report_id})


@router.get("/reports", response_model=BaseResponse[AiAnalysisReportList])
async def get_reports(
    symbol: str,
    page: int = 1,
    page_size: int = 10,
    sort_by: str = "created_at",
    order: str = "desc",
    current_user: UserOut = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[AiAnalysisReportList]:
    """按标的分页列出报告。"""
    page = max(1, page)
    page_size = min(max(1, page_size), 50)
    if sort_by not in ("created_at", "status"):
        sort_by = "created_at"
    if order not in ("asc", "desc"):
        order = "desc"
    items_tuples, total = await list_by_symbol(
        db,
        tenant_id=current_user.tenant_id,
        symbol=symbol,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        order=order,
    )
    items = [
        AiAnalysisReportItem(
            id=t[0],
            symbol=t[1],
            main_summary=t[2],
            status=t[3],
            created_at=t[4],
        )
        for t in items_tuples
    ]
    return ok(
        AiAnalysisReportList(items=items, total=total, page=page, page_size=page_size)
    )


@router.get("/reports/{report_id}", response_model=BaseResponse[AiAnalysisReportOut])
async def get_report(
    report_id: str,
    current_user: UserOut = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[AiAnalysisReportOut]:
    """获取单条报告详情（含 report_snapshot，用于「查看」恢复）。"""
    report = await get_by_id(db, current_user.tenant_id, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="report not found")
    return ok(
        AiAnalysisReportOut(
            id=report.id,
            symbol=report.symbol,
            created_at=_format_created_at(report.created_at),
            report_snapshot=report.report_snapshot or {},
        )
    )


@router.delete("/reports/{report_id}", response_model=BaseResponse[dict])
async def delete_report_endpoint(
    report_id: str,
    current_user: UserOut = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BaseResponse[dict]:
    """删除一条报告。"""
    deleted = await delete_report(db, current_user.tenant_id, report_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="report not found")
    return ok({"ok": True})
