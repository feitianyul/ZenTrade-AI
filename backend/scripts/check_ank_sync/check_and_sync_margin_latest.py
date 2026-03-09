#!/usr/bin/env python3
"""
独立脚本：融资融券增量同步。拉取上交所+深交所两融明细，仅写入库中缺失的 (symbol, trade_date)。
主备逻辑（仿 check_and_sync_top_holder_latest）：直连交易所接口为主，akshare 解析为备。
用法：
  cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_margin_latest.py [--dry-run]
  过去 3 个月：python scripts/check_ank_sync/check_and_sync_margin_latest.py --months 3
  单只：python scripts/check_ank_sync/check_and_sync_margin_latest.py --symbol 510050
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_backend_dir = Path(__file__).resolve().parent.parent.parent
if str(_backend_dir) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(_backend_dir))

_env_file = Path(os.getenv("ENV_FILE", _backend_dir / ".env"))
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=True)
    except Exception:
        pass
os.chdir(_backend_dir)

from scripts.check_ank_sync.sync_script_utils import (
    get_watermark,
    set_watermark,
    get_expected_latest_date,
    get_last_trading_date_str,
    get_trading_dates_last_n_months,
    _safe_float,
    log_stage_start,
    log_stage_end,
    write_failed_list,
    setup_script_log_file,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
CATEGORY_NAME = "融资融券"
CATEGORY_ID = "margin"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)

def _norm_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace(",", "").strip()
        return float(s) if s else None
    except (TypeError, ValueError):
        return None


# ---------- 上交所：直连 query.sse.com.cn（主） ----------
_SSE_URL = "https://query.sse.com.cn/marketdata/tradedata/queryMargin.do"
_SSE_HEADERS = {
    "Referer": "https://www.sse.com.cn/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
}


def _fetch_margin_sse_direct(date_yyyymmdd: str) -> list[dict]:
    """直连上交所融资融券明细 API，返回统一格式 list[dict]。空或异常返回 []。"""
    import requests
    params = {
        "isPagination": "true",
        "tabType": "mxtype",
        "detailsDate": date_yyyymmdd,
        "stockCode": "",
        "beginDate": "",
        "endDate": "",
        "pageHelp.pageSize": "5000",
        "pageHelp.pageCount": "50",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "21",
    }
    try:
        r = requests.get(_SSE_URL, params=params, headers=_SSE_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        result = data.get("result")
        if not isinstance(result, list) or len(result) == 0:
            return []
        trade_date_iso = date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8]
        out = []
        for rec in result:
            symbol = (rec.get("stockCode") or "").strip()
            if not symbol:
                continue
            out.append({
                "symbol": symbol,
                "trade_date": trade_date_iso,
                "rz_balance": _norm_float(rec.get("rzye")),
                "rz_buy": _norm_float(rec.get("rzmre")),
                "rz_repay": _norm_float(rec.get("rzche")),
                "rq_balance": _norm_float(rec.get("rqyl")),
                "rq_sell": _norm_float(rec.get("rqmcl")),
                "rq_repay": _norm_float(rec.get("rqchl")),
            })
        return out
    except Exception as e:
        logger.debug("上交所直连失败 %s: %s", date_yyyymmdd, e)
        return []


def _fetch_margin_sse_akshare(date_yyyymmdd: str) -> list[dict]:
    """兜底：akshare 上交所两融明细，返回统一格式 list[dict]。解析异常返回 []。"""
    try:
        import akshare as ak
        import pandas as pd
        df = ak.stock_margin_detail_sse(date=date_yyyymmdd)
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return []
        trade_date_iso = date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8]
        out = []
        for _, row in df.iterrows():
            symbol = (str(row.get("标的证券代码", row.get("证券代码", ""))) or "").strip()
            if not symbol:
                continue
            out.append({
                "symbol": symbol,
                "trade_date": trade_date_iso,
                "rz_balance": _safe_float(row, "融资余额"),
                "rz_buy": _safe_float(row, "融资买入额"),
                "rz_repay": _safe_float(row, "融资偿还额"),
                "rq_balance": _safe_float(row, "融券余量"),
                "rq_sell": _safe_float(row, "融券卖出量"),
                "rq_repay": _safe_float(row, "融券偿还量"),
            })
        return out
    except Exception as e:
        logger.debug("上交所 akshare 兜底失败 %s: %s", date_yyyymmdd, e)
        return []


# ---------- 深交所：直连 ShowReport Excel（主） ----------
_SZSE_URL = "https://www.szse.cn/api/report/ShowReport"
_SZSE_HEADERS = {
    "Referer": "https://www.szse.cn/disclosure/margin/margin/index.html",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
}


def _fetch_margin_szse_direct(date_yyyymmdd: str) -> list[dict]:
    """直连深交所融资融券明细（Excel），返回统一格式 list[dict]。深交所无融资偿还/融券偿还字段，填 None。"""
    import requests
    import pandas as pd
    params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": "1837_xxpl",
        "txtDate": date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8],
        "tab2PAGENO": "1",
        "random": "0.24279342734085696",
        "TABKEY": "tab2",
    }
    try:
        r = requests.get(_SZSE_URL, params=params, headers=_SZSE_HEADERS, timeout=20)
        r.raise_for_status()
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            df = pd.read_excel(r.content, engine="openpyxl", dtype={"证券代码": str})
        if df is None or df.empty:
            return []
        # 列名与 akshare 一致
        need_cols = ["证券代码", "融资买入额", "融资余额", "融券卖出量", "融券余量"]
        for c in need_cols:
            if c not in df.columns:
                return []
        trade_date_iso = date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8]
        out = []
        for _, row in df.iterrows():
            symbol = str(row.get("证券代码", "") or "").strip()
            if not symbol:
                continue
            out.append({
                "symbol": symbol,
                "trade_date": trade_date_iso,
                "rz_balance": _norm_float(row.get("融资余额")),
                "rz_buy": _norm_float(row.get("融资买入额")),
                "rz_repay": None,
                "rq_balance": _norm_float(row.get("融券余量")),
                "rq_sell": _norm_float(row.get("融券卖出量")),
                "rq_repay": None,
            })
        return out
    except Exception as e:
        logger.debug("深交所直连失败 %s: %s", date_yyyymmdd, e)
        return []


def _fetch_margin_szse_akshare(date_yyyymmdd: str) -> list[dict]:
    """兜底：akshare 深交所两融明细，返回统一格式 list[dict]。"""
    try:
        import akshare as ak
        import pandas as pd
        df = ak.stock_margin_detail_szse(date=date_yyyymmdd)
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return []
        trade_date_iso = date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8]
        out = []
        for _, row in df.iterrows():
            symbol = str(row.get("证券代码", "") or "").strip()
            if not symbol:
                continue
            out.append({
                "symbol": symbol,
                "trade_date": trade_date_iso,
                "rz_balance": _safe_float(row, "融资余额"),
                "rz_buy": _safe_float(row, "融资买入额"),
                "rz_repay": None,
                "rq_balance": _safe_float(row, "融券余量"),
                "rq_sell": _safe_float(row, "融券卖出量"),
                "rq_repay": None,
            })
        return out
    except Exception as e:
        logger.debug("深交所 akshare 兜底失败 %s: %s", date_yyyymmdd, e)
        return []


def _fetch_margin_one_date(date_yyyymmdd: str) -> tuple[list[dict], str]:
    """拉取某一交易日的沪深两市数据，主用直连、失败用 akshare。返回 (统一格式 rows, 日期 YYYY-MM-DD)。"""
    trade_date_iso = date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8]
    rows_sse = _fetch_margin_sse_direct(date_yyyymmdd)
    if not rows_sse:
        rows_sse = _fetch_margin_sse_akshare(date_yyyymmdd)
        if rows_sse:
            logger.info("上交所直连无数据，改用 akshare 兜底 %s: %d 条", trade_date_iso, len(rows_sse))
    else:
        logger.info("上交所直连 %s: %d 条", trade_date_iso, len(rows_sse))

    rows_szse = _fetch_margin_szse_direct(date_yyyymmdd)
    if not rows_szse:
        rows_szse = _fetch_margin_szse_akshare(date_yyyymmdd)
        if rows_szse:
            logger.info("深交所直连无数据，改用 akshare 兜底 %s: %d 条", trade_date_iso, len(rows_szse))
    else:
        logger.info("深交所直连 %s: %d 条", trade_date_iso, len(rows_szse))

    # 合并：同一 symbol 同一天两市可能都有（理论上沪/深不会重复代码），以先出现的为准，避免重复
    seen = set()
    merged = []
    for r in rows_sse + rows_szse:
        s = r.get("symbol", "")
        if s and s not in seen:
            seen.add(s)
            merged.append(r)
    return merged, trade_date_iso


async def _write_margin_rows_for_one_date(
    session,
    all_rows: list[dict],
    trade_date_iso: str,
    symbol_filter: str | None,
    existing_symbols: set[str],
    use_mysql_upsert: bool,
    now_utc: datetime,
) -> int:
    """对单日合并结果按「缺失」写入，返回本日写入条数。"""
    from sqlalchemy import text

    count = 0
    for row in all_rows:
        symbol = row.get("symbol", "")
        if not symbol:
            continue
        if symbol_filter and symbol != symbol_filter:
            continue
        if not symbol_filter and existing_symbols and symbol in existing_symbols:
            continue
        if use_mysql_upsert:
            sql = text(
                "INSERT INTO stock_margin_trading (symbol, trade_date, rz_balance, rz_buy, rz_repay, rq_balance, rq_sell, rq_repay, updated_at) "
                "VALUES (:symbol, :trade_date, :rz_balance, :rz_buy, :rz_repay, :rq_balance, :rq_sell, :rq_repay, :updated_at) AS v "
                "ON DUPLICATE KEY UPDATE rz_balance=v.rz_balance, rz_buy=v.rz_buy, rz_repay=v.rz_repay, "
                "rq_balance=v.rq_balance, rq_sell=v.rq_sell, rq_repay=v.rq_repay, updated_at=v.updated_at"
            )
        else:
            sql = text(
                "INSERT INTO stock_margin_trading (symbol, trade_date, rz_balance, rz_buy, rz_repay, rq_balance, rq_sell, rq_repay, updated_at) "
                "VALUES (:symbol, :trade_date, :rz_balance, :rz_buy, :rz_repay, :rq_balance, :rq_sell, :rq_repay, :updated_at) "
                "ON CONFLICT(symbol, trade_date) DO UPDATE SET rz_balance=excluded.rz_balance, rz_buy=excluded.rz_buy, rz_repay=excluded.rz_repay, "
                "rq_balance=excluded.rq_balance, rq_sell=excluded.rq_sell, rq_repay=excluded.rq_repay, updated_at=excluded.updated_at"
            )
        await session.execute(
            sql,
            {
                "symbol": symbol,
                "trade_date": trade_date_iso,
                "rz_balance": row.get("rz_balance"),
                "rz_buy": row.get("rz_buy"),
                "rz_repay": row.get("rz_repay"),
                "rq_balance": row.get("rq_balance"),
                "rq_sell": row.get("rq_sell"),
                "rq_repay": row.get("rq_repay"),
                "updated_at": now_utc,
            },
        )
        count += 1
    return count


async def run(dry_run: bool = False, symbol_filter: str | None = None, months: int = 0) -> None:
    wm = await get_watermark(CATEGORY_ID, "")
    trade_date = await get_expected_latest_date()
    trade_date_iso = trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:8]
    if symbol_filter:
        logger.info("单只拉取模式: %s，忽略 watermark，仅同步最近交易日 %s", symbol_filter, trade_date_iso)
    if months > 0:
        logger.info("范围拉取: 过去 %d 个月交易日，忽略 watermark", months)
    log_stage_start(logger, CATEGORY_NAME, expected_date=trade_date_iso, watermark=wm, dry_run=dry_run)

    # 范围模式：过去 N 个月交易日，逐日拉取并只写缺失
    if months > 0:
        date_list = await get_trading_dates_last_n_months(months)
        if not date_list:
            logger.warning("未获取到交易日列表，请确保 exchange_trading_dates 有数据")
            log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
            return
        logger.info("共 %d 个交易日待处理: %s ... %s", len(date_list), date_list[0], date_list[-1])

        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import StockMarginTrading

        _dsn = os.environ.get("MYSQL_DSN", "")
        use_mysql_upsert = "sqlite" not in _dsn.lower()
        now_utc = datetime.now(timezone.utc)
        total_count = 0
        failed_list: list[str] = []

        async for session in get_session():
            for i, date_yyyymmdd in enumerate(date_list):
                trade_date_iso = date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8]
                merged, _ = await asyncio.to_thread(_fetch_margin_one_date, date_yyyymmdd)
                if not merged:
                    if (i + 1) % 10 == 0 or i == 0:
                        logger.info("[%d/%d] %s 无数据，跳过", i + 1, len(date_list), trade_date_iso)
                    continue
                if symbol_filter:
                    merged = [r for r in merged if r.get("symbol") == symbol_filter]
                    if not merged:
                        continue
                symbols_in_source = {r["symbol"] for r in merged}
                existing_symbols: set[str] = set()
                if not symbol_filter:
                    try:
                        result = await session.execute(
                            select(StockMarginTrading.symbol).where(
                                StockMarginTrading.trade_date == trade_date_iso
                            )
                        )
                        existing_symbols = {str(r[0]) for r in result.fetchall()}
                    except Exception:
                        existing_symbols = set()
                if dry_run:
                    to_add = len(symbols_in_source - existing_symbols)
                    total_count += to_add
                else:
                    cnt = await _write_margin_rows_for_one_date(
                        session,
                        merged,
                        trade_date_iso,
                        symbol_filter,
                        existing_symbols,
                        use_mysql_upsert,
                        now_utc,
                    )
                    total_count += cnt
                if (i + 1) % 5 == 0 or i == len(date_list) - 1:
                    logger.info("[%d/%d] 已处理至 %s，累计写入 %d 条", i + 1, len(date_list), trade_date_iso, total_count)
            await session.commit()

        if not dry_run and not symbol_filter and date_list:
            await set_watermark(CATEGORY_ID, date_list[0], "")
        logger.info("范围拉取完成，共写入 %d 条", total_count)
        log_stage_end(logger, CATEGORY_NAME, total_count, len(failed_list), 0, category_id=CATEGORY_ID)
        return

    # 单日模式：仅最近交易日，受 watermark 控制
    if not symbol_filter and wm is not None and trade_date <= wm.replace("-", ""):
        logger.info("最近交易日 %s 已同步，跳过", trade_date_iso)
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    failed_list: list[str] = []
    dates_to_try = [trade_date, await get_last_trading_date_str(include_today=False)]
    all_rows: list[dict] = []
    chosen_trade_date_iso = ""
    for date_yyyymmdd in dates_to_try:
        merged, trade_date_iso = await asyncio.to_thread(_fetch_margin_one_date, date_yyyymmdd)
        if merged:
            all_rows = merged
            chosen_trade_date_iso = trade_date_iso
            break
        if date_yyyymmdd == dates_to_try[0]:
            logger.warning("预期交易日 %s 两市均无数据(可能非交易日或数据未出)，尝试上一交易日", trade_date_iso)
        else:
            logger.warning("上一交易日 %s 仍无数据", trade_date_iso)

    if not all_rows:
        last_tried_iso = dates_to_try[-1][:4] + "-" + dates_to_try[-1][4:6] + "-" + dates_to_try[-1][6:8]
        logger.info("无数据，仍更新 watermark 为最近尝试日 %s", last_tried_iso)
        if not dry_run and not symbol_filter:
            await set_watermark(CATEGORY_ID, dates_to_try[-1], "")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    trade_date_iso = chosen_trade_date_iso
    if symbol_filter:
        all_rows = [r for r in all_rows if r.get("symbol") == symbol_filter]
        if not all_rows:
            logger.info("单只 %s 在当日两市合并结果中未找到", symbol_filter)
            log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
            return

    symbols_in_source = {r["symbol"] for r in all_rows}
    count = 0
    if dry_run:
        count = len(all_rows)
        logger.info("dry_run: 当日两市合并共 %d 条，涉及 %d 只标的，跳过落库", count, len(symbols_in_source))
        log_stage_end(logger, CATEGORY_NAME, count, 0, 0, category_id=CATEGORY_ID)
        return

    from sqlalchemy import select, text
    from src.core.db import get_session
    from src.models.market_sync import StockMarginTrading

    _dsn = os.environ.get("MYSQL_DSN", "")
    use_mysql_upsert = "sqlite" not in _dsn.lower()
    now_utc = datetime.now(timezone.utc)

    async for session in get_session():
        existing_symbols: set[str] = set()
        if not symbol_filter:
            try:
                result = await session.execute(
                    select(StockMarginTrading.symbol).where(StockMarginTrading.trade_date == trade_date_iso)
                )
                existing_symbols = {str(r[0]) for r in result.fetchall()}
                to_add = symbols_in_source - existing_symbols
                logger.info(
                    "融资融券差异统计(沪深合并): 源数据标的 %d 只，数据库已有 %d 只，预计增补 %d 只",
                    len(symbols_in_source),
                    len(existing_symbols),
                    len(to_add),
                )
            except Exception as e:
                logger.warning("查询当日已存在的融资融券记录失败，将全量写入: %s", e)
                existing_symbols = set()

        count = await _write_margin_rows_for_one_date(
            session,
            all_rows,
            trade_date_iso,
            symbol_filter,
            existing_symbols,
            use_mysql_upsert,
            now_utc,
        )
        await session.commit()

    if not dry_run and not symbol_filter:
        await set_watermark(CATEGORY_ID, trade_date_iso.replace("-", ""), "")
        logger.info("写入 %d 条，watermark 已更新", count)
    else:
        logger.info("写入 %d 条", count)
    log_stage_end(logger, CATEGORY_NAME, count, len(failed_list), 0, category_id=CATEGORY_ID)


def main() -> None:
    parser = argparse.ArgumentParser(description="融资融券增量同步（沪深两市，直连主+akshare备）")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument(
        "--months",
        type=int,
        default=0,
        metavar="N",
        help="拉取过去 N 个月的交易日数据（逐日、仅补缺失）；默认 0 表示仅拉最近一日",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="",
        help="仅拉取该股票（6 位代码如 000001）；范围模式时对每个交易日过滤该只",
    )
    args = parser.parse_args()
    asyncio.run(
        run(
            dry_run=args.dry_run,
            symbol_filter=args.symbol or None,
            months=args.months,
        )
    )


if __name__ == "__main__":
    main()
