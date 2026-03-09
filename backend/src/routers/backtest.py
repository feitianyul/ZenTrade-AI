from typing import List

from fastapi import APIRouter, Header, HTTPException, Query

from src.schemas.backtest import (
    BacktestRequest,
    BacktestResult,
    BenchmarkComparison,
    FeeBreakdown,
    TradeDetail,
)
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.auth_service import get_user_from_token
from src.services.backtest_service import create_pending_backtest_task
from src.services.strategy_service import get_strategy

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.post(
    "/strategy/{strategy_id}/backtest",
    response_model=BaseResponse[BacktestResult],
    summary="运行策略回测",
    description="对指定策略执行回测，支持多标的、自定义费率、指数对标。返回完整回测结果包括交易明细和费用明细。",
)
async def run_backtest(
    strategy_id: str,
    payload: BacktestRequest,
    authorization: str | None = Header(default=None),
) -> BaseResponse[BacktestResult]:
    """创建回测任务（status=pending），立即返回 backtest_id。前端轮询 GET 详情直至完成。
    若环境变量 BACKTEST_INLINE=1，则同步执行并返回完整结果（无需 Worker）。"""
    import os
    from src.services.backtest_service import get_backtest_task, run_backtest_job

    user = await _require_user(authorization)
    strategy = await get_strategy(user.tenant_id, strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="strategy not found")
    task = await create_pending_backtest_task(user.tenant_id, strategy_id, payload)
    # #region agent log
    try:
        _lp = __import__("pathlib").Path(__file__).resolve().parent.parent.parent.parent / ".cursor" / "debug.log"
        _lp.parent.mkdir(parents=True, exist_ok=True)
        _inline = os.environ.get("BACKTEST_INLINE", "")
        with open(_lp, "a", encoding="utf-8") as _f:
            __import__("json").dump({"hypothesisId": "H1,H2", "location": "backtest.run_backtest", "message": "task_created", "data": {"backtest_id": task.id, "BACKTEST_INLINE": _inline, "will_run_inline": _inline == "1"}, "timestamp": __import__("time").time()}, _f, ensure_ascii=False)
            _f.write("\n")
    except Exception:
        pass
    # #endregion
    if os.environ.get("BACKTEST_INLINE") == "1":
        await run_backtest_job(user.tenant_id, strategy_id, task.id)
        task = await get_backtest_task(user.tenant_id, strategy_id, task.id)
        rj = task.result_metrics_json or {}
        return ok(
            BacktestResult(
                backtest_id=task.id,
                strategy_id=strategy_id,
                strategy_name=strategy.name or "",
                status="completed",
                metrics=rj.get("metrics", {}),
                fee_breakdown=FeeBreakdown(**rj.get("fee_breakdown", {})),
                trade_details=[TradeDetail(**t) for t in rj.get("trade_details", [])],
                benchmark=BenchmarkComparison(**rj.get("benchmark", {})),
                equity_curve=rj.get("equity_curve", []),
                kline_data=rj.get("kline_data", []),
                ai_suggestion=rj.get("ai_suggestion", ""),
                grade=rj.get("grade", ""),
                grade_text=rj.get("grade_text", ""),
            )
        )
    return ok(
        BacktestResult(
            backtest_id=task.id,
            strategy_id=strategy_id,
            strategy_name=strategy.name or "",
            status="pending",
        )
    )


# ---- 回测历史 ----
from pydantic import BaseModel as PydanticBaseModel


class _TargetBrief(PydanticBaseModel):
    code: str = ""
    name: str = ""


class BacktestHistoryItem(PydanticBaseModel):
    backtest_id: str
    start_date: str
    end_date: str
    grade: str = ""
    total_return_pct: float = 0.0
    win_rate: float = 0.0
    max_drawdown_pct: float = 0.0
    created_at: str = ""
    targets: list[_TargetBrief] = []
    trade_count: int = 0


@router.get(
    "/strategy/{strategy_id}/backtest-history",
    response_model=BaseResponse[List[BacktestHistoryItem]],
    summary="获取策略回测历史",
)
async def get_backtest_history(
    strategy_id: str,
    limit: int = Query(default=5, ge=1, le=20),
    authorization: str | None = Header(default=None),
) -> BaseResponse[List[BacktestHistoryItem]]:
    user = await _require_user(authorization)
    from src.services.backtest_service import list_backtest_history
    items = await list_backtest_history(user.tenant_id, strategy_id, limit)
    return ok(items)


@router.get(
    "/strategy/{strategy_id}/backtest/{backtest_id}",
    response_model=BaseResponse[BacktestResult],
    summary="获取单次回测详情(含K线数据和交易明细)",
)
async def get_backtest_detail(
    strategy_id: str,
    backtest_id: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[BacktestResult]:
    """获取回测详情，支持轮询。pending/running 时返回 status，completed 时返回完整结果。"""
    user = await _require_user(authorization)
    from src.services.backtest_service import get_backtest_task
    task = await get_backtest_task(user.tenant_id, strategy_id, backtest_id)
    if not task:
        raise HTTPException(status_code=404, detail="backtest not found")
    strategy = await get_strategy(user.tenant_id, strategy_id)
    result_json = task.result_metrics_json or {}
    metrics = result_json.get("metrics", {})
    return ok(
        BacktestResult(
            backtest_id=task.id,
            strategy_id=strategy_id,
            strategy_name=(strategy.name or "") if strategy else "",
            status=task.status or "completed",
            error_detail=task.error_detail,
            metrics=metrics,
            fee_breakdown=FeeBreakdown(**result_json.get("fee_breakdown", {})),
            trade_details=[TradeDetail(**t) for t in result_json.get("trade_details", [])],
            benchmark=BenchmarkComparison(**result_json.get("benchmark", {})),
            equity_curve=result_json.get("equity_curve", []),
            kline_data=result_json.get("kline_data", []),
            ai_suggestion=result_json.get("ai_suggestion", ""),
            grade=result_json.get("grade", ""),
            grade_text=result_json.get("grade_text", ""),
        )
    )
