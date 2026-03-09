from fastapi import APIRouter, Header, HTTPException, Query

from src.schemas.market import (
    FundamentalResponse,
    KlineResponse,
    MarketAlertCreate,
    MarketAlertOut,
    MarketDepth,
    MarketQuote,
    MinuteResponse,
)
from src.schemas.response import BaseResponse, ok
from src.schemas.user import UserOut
from src.services.alert_service import create_alert
from src.services.auth_service import get_user_from_token
from src.services.data_service.hot_rank_service import (
    get_hot_rank,
    get_indices,
    get_institutional_data,
    get_lhb_data,
    get_ranking,
    get_sector_detail,
    get_sectors,
    search_stocks,
)
from src.services.data_service.market_read_service import (
    get_block_trade_data,
    get_capital_flow_data,
    get_irm_qa_by_symbol,
    get_limit_updown_data,
    get_margin_data,
    get_northbound_flow_data,
    get_peer_comparison_data,
)
from src.services.data_service.market_source_service import (
    fetch_depth,
    fetch_fundamental,
    fetch_kline,
    fetch_minute,
    fetch_news,
    fetch_quote,
)

router = APIRouter()


async def _require_user(authorization: str | None) -> UserOut:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = await get_user_from_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    return user


@router.get("/market/quote", response_model=BaseResponse[MarketQuote])
async def get_quote(
    symbol: str,
    refresh: bool = Query(default=False, description="为 true 时跳过缓存强制拉取最新报价"),
    authorization: str | None = Header(default=None),
) -> BaseResponse[MarketQuote]:
    user = await _require_user(authorization)
    raw = await fetch_quote(symbol, skip_cache=refresh, tenant_id=user.tenant_id)
    quote = MarketQuote.model_validate(raw)
    return ok(quote)


@router.get("/market/depth", response_model=BaseResponse[MarketDepth])
async def get_depth(
    symbol: str,
    authorization: str | None = Header(default=None),
) -> BaseResponse[MarketDepth]:
    user = await _require_user(authorization)
    raw = await fetch_depth(symbol, tenant_id=user.tenant_id)
    return ok(
        MarketDepth(
            symbol=symbol,
            bids=raw.get("bids", []),
            asks=raw.get("asks", []),
        )
    )


@router.get("/market/hot")
async def hot_rank(
    authorization: str | None = Header(default=None),
):
    user = await _require_user(authorization)
    items, data_updated_at = await get_hot_rank(tenant_id=user.tenant_id)
    return ok(items, data_updated_at=data_updated_at)


@router.get("/market/indices")
async def market_indices(
    authorization: str | None = Header(default=None),
):
    """获取主要大盘指数 (上证/深证/创业板/科创50/北证50)"""
    user = await _require_user(authorization)
    items, data_updated_at = await get_indices(tenant_id=user.tenant_id)
    return ok(items, data_updated_at=data_updated_at)


@router.get("/market/sectors")
async def market_sectors(
    sector_type: str = Query(default="all", description="industry / concept / all"),
    authorization: str | None = Header(default=None),
):
    """获取板块数据 (行业板块/概念板块)"""
    await _require_user(authorization)
    items, data_updated_at = await get_sectors(sector_type)
    return ok(items, data_updated_at=data_updated_at)


@router.get("/market/sector-detail")
async def market_sector_detail(
    code: str = Query(..., description="板块代码如 BK0477"),
    name: str = Query(..., description="板块名称如 半导体"),
    authorization: str | None = Header(default=None),
):
    """获取板块详情 (成分股列表 + 板块统计)"""
    await _require_user(authorization)
    detail = await get_sector_detail(code, name)
    return ok(detail)


@router.get("/market/ranking")
async def market_ranking(
    sort_by: str = Query(default="change_pct", description="change_pct / turnover / turnover_rate / volume"),
    order: str = Query(default="desc", description="asc / desc"),
    limit: int = Query(default=30, ge=1, le=100),
    authorization: str | None = Header(default=None),
):
    """获取个股排行 (按涨跌幅/成交额/换手率/成交量排序)"""
    user = await _require_user(authorization)
    items, data_updated_at = await get_ranking(sort_by, order, limit, tenant_id=user.tenant_id)
    return ok(items, data_updated_at=data_updated_at)


@router.get("/market/lhb")
async def market_lhb(
    days: int = Query(default=3, ge=1, le=30, description="获取最近几天的龙虎榜数据"),
    authorization: str | None = Header(default=None),
):
    """获取龙虎榜数据 (最近N天)"""
    await _require_user(authorization)
    items, data_updated_at = await get_lhb_data(days)
    return ok(items, data_updated_at=data_updated_at)


@router.get("/market/institutional")
async def market_institutional(
    market: str = Query(default="北向", description="北向 / 沪股通 / 深股通"),
    indicator: str = Query(default="今日排行", description="今日排行 / 5日排行 / 10日排行"),
    authorization: str | None = Header(default=None),
):
    """获取北向资金 / 机构持仓数据"""
    await _require_user(authorization)
    items, data_updated_at = await get_institutional_data(market, indicator)
    return ok(items, data_updated_at=data_updated_at)


@router.get("/market/search")
async def market_search(
    q: str = Query(..., min_length=1, max_length=20, description="搜索关键词: 股票代码或名称"),
    limit: int = Query(default=20, ge=1, le=50),
    authorization: str | None = Header(default=None),
):
    """搜索全市场 A 股，支持按代码前缀、名称包含匹配"""
    await _require_user(authorization)
    results = await search_stocks(q, limit)
    return ok(results)


@router.post("/market/alert")
async def create_market_alert(
    payload: MarketAlertCreate,
    authorization: str | None = Header(default=None),
):
    user = await _require_user(authorization)
    return ok(
        {
            "alert_id": "alert_" + payload.symbol,
            "symbol": payload.symbol,
            "condition": payload.condition,
            "threshold": payload.threshold,
            "level": payload.level,
            "status": "active",
        }
    )


@router.get("/market/kline", response_model=BaseResponse[KlineResponse])
async def market_kline(
    symbol: str,
    period: str = Query(default="daily", description="daily/weekly/monthly/1min/5min/15min/30min/60min"),
    count: int = Query(default=60, ge=10, le=500),
    authorization: str | None = Header(default=None),
):
    """获取K线数据 (日K/周K/月K/分钟K)"""
    await _require_user(authorization)
    raw = await fetch_kline(symbol, period, count)
    # 兼容: fetch_kline 可能返回 list 或 dict (含 _unavailable)
    if isinstance(raw, dict):
        bar_list = raw.get("bars", [])
    else:
        bar_list = raw if isinstance(raw, list) else []
    return ok(KlineResponse(symbol=symbol, period=period, bars=bar_list))


@router.get("/market/minute", response_model=BaseResponse[MinuteResponse])
async def market_minute(
    symbol: str,
    count: int = Query(default=240, ge=10, le=500),
    days: int = Query(default=1, ge=1, le=5, description="1=当日分时, 5=五日分时"),
    authorization: str | None = Header(default=None),
):
    """获取分时数据 (1分钟级, 支持五日分时)"""
    user = await _require_user(authorization)
    result = await fetch_minute(symbol, count, days, tenant_id=user.tenant_id)
    data_updated_at = result.pop("data_updated_at", None)
    return ok(MinuteResponse(
        symbol=symbol,
        pre_close=result["pre_close"],
        bars=result["bars"],
    ), data_updated_at=data_updated_at)


@router.get("/market/news")
async def market_news(
    symbol: str,
    authorization: str | None = Header(default=None),
):
    """获取个股资讯/公告"""
    await _require_user(authorization)
    items = await fetch_news(symbol)
    return ok(items)


@router.get("/market/fundamental", response_model=BaseResponse[FundamentalResponse])
async def market_fundamental(
    symbol: str,
    authorization: str | None = Header(default=None),
):
    """获取F10基本面数据"""
    user = await _require_user(authorization)
    result = await fetch_fundamental(symbol, tenant_id=user.tenant_id)
    return ok(FundamentalResponse(
        symbol=symbol,
        name=result.get("name"),
        items=result["items"],
        top_holders=result.get("top_holders"),
        dividends=result.get("dividends"),
        holder_count=result.get("holder_count"),
    ))


@router.get("/market/margin")
async def market_margin(
    symbol: str,
    days: int = Query(default=30, ge=1, le=365),
    authorization: str | None = Header(default=None),
):
    """融资融券 — 读库 stock_margin_trading，Redis 60s"""
    await _require_user(authorization)
    data = await get_margin_data(symbol, days=days)
    return ok(data)


@router.get("/market/block-trade")
async def market_block_trade(
    symbol: str,
    limit: int = Query(default=20, ge=1, le=100),
    authorization: str | None = Header(default=None),
):
    """大宗交易 — 读库 stock_block_trade，Redis 60s"""
    await _require_user(authorization)
    data = await get_block_trade_data(symbol, limit=limit)
    return ok(data)


@router.get("/market/capital-flow")
async def market_capital_flow(
    symbol: str,
    days: int = Query(default=30, ge=1, le=365),
    authorization: str | None = Header(default=None),
):
    """资金流向 — 读库 stock_capital_flow，Redis 300s"""
    await _require_user(authorization)
    data = await get_capital_flow_data(symbol, days=days)
    return ok(data)


@router.get("/market/limit-updown")
async def market_limit_updown(
    date: str | None = Query(default=None, description="YYYY-MM-DD，默认当日"),
    limit_type: str | None = Query(default=None, description="up / down，空=全部"),
    authorization: str | None = Header(default=None),
):
    """涨跌停 — 读库 stock_limit_updown，Redis 60s"""
    await _require_user(authorization)
    data = await get_limit_updown_data(date_str=date, limit_type=limit_type)
    return ok(data)


@router.get("/market/peer-comparison")
async def market_peer_comparison(
    symbol: str,
    as_of_date: str | None = Query(default=None, description="基准日期 YYYY-MM-DD，默认最新"),
    authorization: str | None = Header(default=None),
):
    """同行比较 — 读库 stock_peer_comparison，Redis 3600s"""
    await _require_user(authorization)
    data = await get_peer_comparison_data(symbol, as_of_date=as_of_date)
    return ok(data)


@router.get("/market/irm-qa")
async def market_irm_qa(
    symbol: str,
    limit: int = Query(default=20, ge=1, le=50),
    authorization: str | None = Header(default=None),
):
    """互动易/上证e互动问答 — 读库 stock_irm_qa，供个股页「互动」Tab 展示"""
    await _require_user(authorization)
    data = await get_irm_qa_by_symbol(symbol, limit=limit, truncate_content=False)
    return ok(data)


@router.get("/market/northbound-flow")
async def market_northbound_flow(
    days: int = Query(default=30, ge=1, le=365),
    direction: str = Query(default="north", description="north / south，北向/南向"),
    authorization: str | None = Header(default=None),
):
    """北向日度汇总 — 读库 northbound_flow，Redis 300s"""
    await _require_user(authorization)
    if direction not in ("north", "south"):
        direction = "north"
    data = await get_northbound_flow_data(days=days, direction=direction)
    return ok(data)


@router.get("/market/trade-calendar")
async def market_trade_calendar(
    year: int = Query(..., description="年份"),
    month: int = Query(..., ge=1, le=12, description="月份 1-12"),
    authorization: str | None = Header(default=None),
):
    """获取指定年月的交易日列表，用于首页总览交易所日历展示开市/休市"""
    await _require_user(authorization)
    from datetime import date, timedelta
    from sqlalchemy import select

    from src.core.db import get_session
    from src.models.market_sync import ExchangeTradingDate

    start = f"{year}-{month:02d}-01"
    end_d = date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)
    end_d = end_d - timedelta(days=1)
    end = end_d.strftime("%Y-%m-%d")

    dates: list[str] = []
    async for session in get_session():
        stmt = (
            select(ExchangeTradingDate.trade_date)
            .where(
                ExchangeTradingDate.trade_date >= start,
                ExchangeTradingDate.trade_date <= end,
            )
            .order_by(ExchangeTradingDate.trade_date)
        )
        result = await session.execute(stmt)
        dates = [str(r[0]) for r in result.all()]
        break
    return ok(data={"dates": dates})
