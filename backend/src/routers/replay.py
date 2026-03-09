from fastapi import APIRouter, Header, HTTPException

from src.schemas.replay import ExportRequest, ReplayReportOut, ReplayRequest, TradeAnalysisOut
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.export_service import export_payload
from src.services.replay_service import create_replay_report
from src.services.trade_analysis_service import create_trade_analysis

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.post("/replay/report", response_model=BaseResponse[ReplayReportOut])
async def create_report(
    payload: ReplayRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[ReplayReportOut]:
    user = await _require_user(authorization)
    if user.level == "basic":
        raise HTTPException(status_code=403, detail="复盘功能需进阶及以上等级")
    record = await create_replay_report(
        user.tenant_id, payload.strategy_id, payload.start_date, payload.end_date
    )
    return ok(
        ReplayReportOut(
            report_id=record.id,
            strategy_id=record.strategy_id,
            status=record.status,
            report_json=record.report_json,
        )
    )


@router.post("/replay/analysis", response_model=BaseResponse[TradeAnalysisOut])
async def create_analysis(
    trade_id: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[TradeAnalysisOut]:
    user = await _require_user(authorization)
    if user.level == "basic":
        raise HTTPException(status_code=403, detail="复盘分析需进阶及以上等级")
    record = await create_trade_analysis(
        user.tenant_id,
        trade_id,
        {"win_rate": 0.55, "risk": "medium"},
        "策略复盘完成",
    )
    return ok(
        TradeAnalysisOut(
            analysis_id=record.id,
            trade_id=record.trade_id,
            metrics_json=record.metrics_json,
            summary=record.summary,
        )
    )


@router.post("/replay/export", response_model=BaseResponse[dict[str, str]])
async def export_report(
    payload: ExportRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[dict[str, str]]:
    user = await _require_user(authorization)
    if user.level == "basic":
        raise HTTPException(status_code=403, detail="复盘导出需进阶及以上等级")
    result = await export_payload(payload.export_type, payload.payload or {})
    return ok({"status": result["status"]})
