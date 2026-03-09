from typing import Any, Dict, List

from fastapi import APIRouter, Header, HTTPException

from src.schemas.response import BaseResponse, ok
from src.services.auth_service import verify_token

router = APIRouter(tags=["Data"])

async def _require_user(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.replace("Bearer ", "")
    return await verify_token(token)

@router.get("/data/market", response_model=BaseResponse[List[Dict[str, Any]]])
async def query_market_data(
    symbol: str,
    start_date: str,
    end_date: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[List[Dict[str, Any]]]:
    """获取行情数据 — 调用 market_source_service.fetch_kline 获取真实K线。"""
    await _require_user(authorization)
    from src.services.data_service.market_source_service import fetch_kline
    # 计算日期区间估算所需bar数 (近似: 每天1条)
    try:
        from datetime import datetime
        d1 = datetime.strptime(start_date, "%Y-%m-%d")
        d2 = datetime.strptime(end_date, "%Y-%m-%d")
        count = max(int((d2 - d1).days), 30)
    except Exception:
        count = 120
    bars = await fetch_kline(symbol, "daily", count)
    # fetch_kline 可能返回 list 或 dict (降级时)
    if isinstance(bars, dict):
        data = bars.get("bars", [])
    else:
        data = bars or []
    return ok(data)
