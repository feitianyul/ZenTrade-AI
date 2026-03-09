"""Phase 2 — 读库展示：融资融券、大宗交易、资金流向、涨跌停、同行比较、北向等。"""

import json
import logging
from typing import Any

from sqlalchemy import select

logger = logging.getLogger(__name__)


def _normalize_symbol(symbol: str) -> str:
    """统一为 6 位代码。"""
    s = symbol.strip().upper()
    if "." in s:
        s = s.split(".")[0]
    for prefix in ("SH", "SZ", "BJ"):
        if s.startswith(prefix) and len(s) > 2:
            s = s[2:]
            break
    return s[:6] if len(s) >= 6 else s


async def get_margin_data(symbol: str, days: int = 30) -> dict[str, Any]:
    """融资融券 — 读 stock_margin_trading，Redis 60s。"""
    code = _normalize_symbol(symbol)
    redis_key = f"market:margin:{code}"
    try:
        from src.services.cache_policy_service import get_cached
        raw = await get_cached(redis_key)
        if raw:
            data = json.loads(raw)
            if data.get("items") is not None:
                return data
    except Exception:
        pass
    items = []
    try:
        from src.core.db import get_session
        from src.models.market_sync import StockMarginTrading

        async for session in get_session():
            stmt = (
                select(StockMarginTrading)
                .where(StockMarginTrading.symbol == code)
                .order_by(StockMarginTrading.trade_date.desc())
                .limit(days)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            for r in rows:
                items.append({
                    "trade_date": r.trade_date,
                    "rz_balance": r.rz_balance,
                    "rz_buy": r.rz_buy,
                    "rz_repay": r.rz_repay,
                    "rq_balance": r.rq_balance,
                    "rq_sell": r.rq_sell,
                    "rq_repay": r.rq_repay,
                    "rz_rq_balance": r.rz_rq_balance,
                })
            break
    except Exception as e:
        logger.warning("get_margin_data failed for %s: %s", symbol, e)
    out = {"items": items}
    try:
        from src.services.cache_policy_service import set_cached
        await set_cached(redis_key, json.dumps(out, ensure_ascii=False), ttl=60)
    except Exception:
        pass
    return out


async def get_block_trade_data(symbol: str, limit: int = 20) -> dict[str, Any]:
    """大宗交易 — 读 stock_block_trade，Redis 60s。"""
    code = _normalize_symbol(symbol)
    redis_key = f"market:block_trade:{code}"
    try:
        from src.services.cache_policy_service import get_cached
        raw = await get_cached(redis_key)
        if raw:
            data = json.loads(raw)
            if data.get("items") is not None:
                return data
    except Exception:
        pass
    items = []
    try:
        from src.core.db import get_session
        from src.models.market_sync import StockBlockTrade

        async for session in get_session():
            stmt = (
                select(StockBlockTrade)
                .where(StockBlockTrade.symbol == code)
                .order_by(StockBlockTrade.trade_date.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            for r in rows:
                items.append({
                    "trade_date": r.trade_date,
                    "price": r.price,
                    "volume": r.volume,
                    "turnover": r.turnover,
                    "buyer": r.buyer,
                    "seller": r.seller,
                    "premium": r.premium,
                })
            break
    except Exception as e:
        logger.warning("get_block_trade_data failed for %s: %s", symbol, e)
    out = {"items": items}
    try:
        from src.services.cache_policy_service import set_cached
        await set_cached(redis_key, json.dumps(out, ensure_ascii=False), ttl=60)
    except Exception:
        pass
    return out


async def get_capital_flow_data(symbol: str, days: int = 30) -> dict[str, Any]:
    """资金流向 — 读 stock_capital_flow，Redis 300s。"""
    code = _normalize_symbol(symbol)
    redis_key = f"market:capital_flow:{code}"
    try:
        from src.services.cache_policy_service import get_cached
        raw = await get_cached(redis_key)
        if raw:
            data = json.loads(raw)
            if data.get("items") is not None:
                return data
    except Exception:
        pass
    items = []
    try:
        from src.core.db import get_session
        from src.models.market_sync import StockCapitalFlow

        async for session in get_session():
            stmt = (
                select(StockCapitalFlow)
                .where(StockCapitalFlow.symbol == code)
                .order_by(StockCapitalFlow.trade_date.desc())
                .limit(days)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            for r in rows:
                items.append({
                    "trade_date": r.trade_date,
                    "main_net_inflow": r.main_net_inflow,
                    "super_large_net_inflow": r.super_large_net_inflow,
                    "large_net_inflow": r.large_net_inflow,
                    "medium_net_inflow": r.medium_net_inflow,
                    "small_net_inflow": r.small_net_inflow,
                })
            break
    except Exception as e:
        logger.warning("get_capital_flow_data failed for %s: %s", symbol, e)
    out = {"items": items}
    try:
        from src.services.cache_policy_service import set_cached
        await set_cached(redis_key, json.dumps(out, ensure_ascii=False), ttl=300)
    except Exception:
        pass
    return out


async def get_fundamental_extended_from_db(code: str) -> dict[str, Any]:
    """F10 扩展：十大股东、分红配股、股东户数 — 供 fetch_fundamental 合并，TTL 与 fundamental 一致。"""
    out: dict[str, Any] = {"top_holders": [], "dividends": [], "holder_count": []}
    try:
        from src.core.db import get_session
        from src.models.market_sync import (
            StockDividend,
            StockHolderCount,
            StockTopHolder,
        )

        async for session in get_session():
            # 十大股东：按 report_date 倒序，取最近几期
            stmt_holder = (
                select(StockTopHolder)
                .where(StockTopHolder.symbol == code)
                .order_by(StockTopHolder.report_date.desc())
                .limit(50)
            )
            r1 = await session.execute(stmt_holder)
            for r in r1.scalars().all():
                out["top_holders"].append({
                    "report_date": r.report_date,
                    "holder_name": r.holder_name,
                    "hold_count": r.hold_count,
                    "hold_ratio": r.hold_ratio,
                    "change_type": r.change_type,
                    "change_count": r.change_count,
                })

            # 分红配股
            stmt_div = (
                select(StockDividend)
                .where(StockDividend.symbol == code)
                .order_by(StockDividend.report_date.desc())
                .limit(30)
            )
            r2 = await session.execute(stmt_div)
            for r in r2.scalars().all():
                out["dividends"].append({
                    "report_date": r.report_date,
                    "ex_date": r.ex_date,
                    "record_date": r.record_date,
                    "bonus_ratio": r.bonus_ratio,
                    "convert_ratio": r.convert_ratio,
                    "dividend_per_share": r.dividend_per_share,
                })

            # 股东户数
            stmt_cnt = (
                select(StockHolderCount)
                .where(StockHolderCount.symbol == code)
                .order_by(StockHolderCount.end_date.desc())
                .limit(20)
            )
            r3 = await session.execute(stmt_cnt)
            for r in r3.scalars().all():
                out["holder_count"].append({
                    "end_date": r.end_date,
                    "holder_count": r.holder_count,
                    "holder_count_change": r.holder_count_change,
                    "avg_hold_amount": r.avg_hold_amount,
                })
            break
    except Exception as e:
        logger.warning("get_fundamental_extended_from_db failed for %s: %s", code, e)
    return out


async def get_peer_comparison_data(
    symbol: str,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    """同行比较 — 读 stock_peer_comparison，4 组 sub_type，Redis 3600s。"""
    code = _normalize_symbol(symbol)
    redis_key = f"market:peer_comparison:{code}"
    if as_of_date:
        redis_key += f":{as_of_date}"
    try:
        from src.services.cache_policy_service import get_cached
        raw = await get_cached(redis_key)
        if raw:
            data = json.loads(raw)
            if data.get("sub_types") is not None or data.get("growth") is not None:
                return data
    except Exception:
        pass
    sub_types: list[dict[str, Any]] = []
    try:
        from src.core.db import get_session
        from src.models.market_sync import StockPeerComparison

        async for session in get_session():
            stmt = (
                select(StockPeerComparison)
                .where(StockPeerComparison.symbol == code)
            )
            if as_of_date:
                stmt = stmt.where(StockPeerComparison.as_of_date == as_of_date)
            else:
                # 取最新 as_of_date
                subq = (
                    select(StockPeerComparison.as_of_date)
                    .where(StockPeerComparison.symbol == code)
                    .order_by(StockPeerComparison.as_of_date.desc())
                    .limit(1)
                )
                subr = await session.execute(subq)
                latest_date = subr.scalar_one_or_none()
                if latest_date:
                    stmt = stmt.where(StockPeerComparison.as_of_date == latest_date)
            stmt = stmt.order_by(StockPeerComparison.sub_type)
            result = await session.execute(stmt)
            rows = result.scalars().all()
            for r in rows:
                data_list = []
                if r.raw_data:
                    try:
                        data_list = json.loads(r.raw_data) if isinstance(r.raw_data, str) else r.raw_data
                    except Exception:
                        pass
                sub_types.append({"sub_type": r.sub_type, "data": data_list})
            break
    except Exception as e:
        logger.warning("get_peer_comparison_data failed for %s: %s", symbol, e)
    out = {"sub_types": sub_types}
    try:
        from src.services.cache_policy_service import set_cached
        await set_cached(redis_key, json.dumps(out, ensure_ascii=False), ttl=3600)
    except Exception:
        pass
    return out


async def get_northbound_flow_data(
    days: int = 30,
    direction: str = "north",
) -> dict[str, Any]:
    """北向日度汇总 — 读 northbound_flow，Redis 300s。direction: north/south，前端 in/out/net 映射为 north。"""
    redis_key = f"market:northbound_flow:{direction}:{days}"
    try:
        from src.services.cache_policy_service import get_cached
        raw = await get_cached(redis_key)
        if raw:
            data = json.loads(raw)
            if data.get("items") is not None:
                return data
    except Exception:
        pass
    items = []
    try:
        from src.core.db import get_session
        from src.models.market_sync import NorthboundFlow

        async for session in get_session():
            stmt = (
                select(NorthboundFlow)
                .where(NorthboundFlow.direction == direction)
                .order_by(NorthboundFlow.trade_date.desc())
                .limit(days)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            for r in rows:
                items.append({
                    "trade_date": r.trade_date,
                    "sh_net_buy": r.sh_net_buy,
                    "sz_net_buy": r.sz_net_buy,
                    "total_net_buy": r.total_net_buy,
                })
            break
    except Exception as e:
        logger.warning("get_northbound_flow_data failed: %s", e)

    # 无可用数据时触发一次北向同步再读库（Redis/MySQL 无或净买额全为空）
    def _has_usable_data(lst: list) -> bool:
        if not lst:
            return False
        return any(
            r.get("total_net_buy") is not None or r.get("sh_net_buy") is not None or r.get("sz_net_buy") is not None
            for r in lst
        )

    if not _has_usable_data(items):
        try:
            from src.services.data_service.data_sync_service import sync_northbound
            from src.core.db import get_session as _get_session
            from src.models.market_sync import NorthboundFlow as _NorthboundFlow
            await sync_northbound("full")
            items = []
            async for session in _get_session():
                stmt = (
                    select(_NorthboundFlow)
                    .where(_NorthboundFlow.direction == direction)
                    .order_by(_NorthboundFlow.trade_date.desc())
                    .limit(days)
                )
                result = await session.execute(stmt)
                rows = result.scalars().all()
                for r in rows:
                    items.append({
                        "trade_date": r.trade_date,
                        "sh_net_buy": r.sh_net_buy,
                        "sz_net_buy": r.sz_net_buy,
                        "total_net_buy": r.total_net_buy,
                    })
                break
        except Exception as e:
            logger.warning("get_northbound_flow_data sync fallback failed: %s", e)

    out = {"items": items}
    try:
        from src.services.cache_policy_service import set_cached
        await set_cached(redis_key, json.dumps(out, ensure_ascii=False), ttl=300)
    except Exception:
        pass
    return out


async def get_limit_updown_data(
    date_str: str | None = None,
    limit_type: str | None = None,
) -> dict[str, Any]:
    """涨跌停 — 读 stock_limit_updown，Redis 60s。limit_type: up / down / 空=全部"""
    if not date_str:
        from datetime import date
        date_str = date.today().isoformat()
    redis_key = f"market:limit_updown:{date_str}"
    if limit_type:
        redis_key += f":{limit_type}"
    try:
        from src.services.cache_policy_service import get_cached
        raw = await get_cached(redis_key)
        if raw:
            data = json.loads(raw)
            if data.get("items") is not None:
                return data
    except Exception:
        pass
    items = []
    try:
        from src.core.db import get_session
        from src.models.market_sync import StockLimitUpDown

        async for session in get_session():
            stmt = (
                select(StockLimitUpDown)
                .where(StockLimitUpDown.trade_date == date_str)
            )
            if limit_type and limit_type in ("up", "down"):
                stmt = stmt.where(StockLimitUpDown.limit_type == limit_type)
            stmt = stmt.order_by(StockLimitUpDown.symbol)
            result = await session.execute(stmt)
            rows = result.scalars().all()
            for r in rows:
                items.append({
                    "trade_date": r.trade_date,
                    "symbol": r.symbol,
                    "symbol_name": r.symbol_name,
                    "limit_type": r.limit_type,
                    "close_price": r.close_price,
                    "change_pct": r.change_pct,
                    "first_limit_time": r.first_limit_time,
                    "last_limit_time": r.last_limit_time,
                    "open_count": r.open_count,
                    "continuous_days": r.continuous_days,
                })
            break
    except Exception as e:
        logger.warning("get_limit_updown_data failed for %s: %s", date_str, e)
    out = {"date": date_str, "items": items}
    try:
        from src.services.cache_policy_service import set_cached
        await set_cached(redis_key, json.dumps(out, ensure_ascii=False), ttl=60)
    except Exception:
        pass
    return out


async def get_irm_qa_by_symbol(
    symbol: str,
    limit: int = 5,
    truncate_content: bool = True,
) -> list[dict[str, Any]]:
    """按标的查询最近 N 条互动易/上证e互动问答。truncate_content=True 时截断 500 字（供舆情 agent）；False 时返回完整正文（供页面展示）。"""
    code = _normalize_symbol(symbol)
    out: list[dict[str, Any]] = []
    try:
        from src.core.db import get_session
        from src.models.market_sync import StockIrmQa

        async for session in get_session():
            stmt = (
                select(StockIrmQa)
                .where(StockIrmQa.symbol == code)
                .order_by(StockIrmQa.answer_time.desc(), StockIrmQa.ask_time.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            for r in rows:
                q_raw = (r.question_content or r.content or "") or ""
                a_raw = (r.answer_content or "") or ""
                if truncate_content:
                    q = q_raw[:500]
                    a = a_raw[:500]
                else:
                    q = q_raw
                    a = a_raw
                out.append({
                    "question_content": q,
                    "answer_content": a,
                    "content": (q + " " + a).strip() or (r.content or "")[:500] if truncate_content else (q + " " + a).strip(),
                    "ask_time": r.ask_time or "",
                    "answer_time": r.answer_time or "",
                    "source": getattr(r, "source", None) or "",
                })
            break
    except Exception as e:
        logger.warning("get_irm_qa_by_symbol failed for %s: %s", code, e)
    return out
