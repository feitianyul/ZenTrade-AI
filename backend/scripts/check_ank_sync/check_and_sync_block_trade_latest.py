#!/usr/bin/env python3
"""
独立脚本：大宗交易增量同步。拉取东财大宗每日明细，按日全量覆盖（删除该日旧数据后写入），仅处理库中缺失的交易日或指定范围。
主备逻辑（仿 check_and_sync_margin_latest）：直连东财 API 为主，akshare 解析为备。
用法：
  cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_block_trade_latest.py [--dry-run]
  过去 3 个月：python scripts/check_ank_sync/check_and_sync_block_trade_latest.py --months 3
  单只：python scripts/check_ank_sync/check_and_sync_block_trade_latest.py --symbol 000001
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
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
CATEGORY_NAME = "大宗交易"
CATEGORY_ID = "block_trade"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)

# 东财大宗每日明细 API（与 akshare stock_dzjy_mrmx 一致）
_EM_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_EM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
}


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


def _norm_str(v: Any, max_len: int = 128) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN"):
        return None
    return s[:max_len] if len(s) > max_len else s


def _fetch_block_trade_direct(start_yyyymmdd: str, end_yyyymmdd: str) -> list[dict]:
    """直连东财大宗每日明细 API，返回统一格式 list[dict]。支持分页。"""
    import requests
    start_iso = start_yyyymmdd[:4] + "-" + start_yyyymmdd[4:6] + "-" + start_yyyymmdd[6:8]
    end_iso = end_yyyymmdd[:4] + "-" + end_yyyymmdd[4:6] + "-" + end_yyyymmdd[6:8]
    filter_str = (
        f"(SECURITY_TYPE_WEB=1)(TRADE_DATE>='{start_iso}')(TRADE_DATE<='{end_iso}')"
    )
    params = {
        "sortColumns": "SECURITY_CODE",
        "sortTypes": "1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_DATA_BLOCKTRADE",
        "columns": "TRADE_DATE,SECURITY_CODE,DEAL_PRICE,PREMIUM_RATIO,DEAL_VOLUME,DEAL_AMT,BUYER_NAME,SELLER_NAME",
        "source": "WEB",
        "client": "WEB",
        "filter": filter_str,
    }
    out: list[dict] = []
    try:
        while True:
            r = requests.get(_EM_URL, params=params, headers=_EM_HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
            res = data.get("result") or {}
            rows = res.get("data")
            if not rows:
                break
            for rec in rows:
                raw_date = rec.get("TRADE_DATE") or ""
                if isinstance(raw_date, str) and len(raw_date) >= 10:
                    trade_date_iso = raw_date[:10]
                elif isinstance(raw_date, str) and len(raw_date) >= 8:
                    trade_date_iso = raw_date[:4] + "-" + raw_date[4:6] + "-" + raw_date[6:8]
                else:
                    trade_date_iso = start_iso if start_yyyymmdd == end_yyyymmdd else ""
                symbol = (rec.get("SECURITY_CODE") or "").strip()
                if not symbol:
                    continue
                out.append({
                    "symbol": symbol,
                    "trade_date": trade_date_iso,
                    "price": _norm_float(rec.get("DEAL_PRICE")),
                    "volume": _norm_float(rec.get("DEAL_VOLUME")),
                    "turnover": _norm_float(rec.get("DEAL_AMT")),
                    "buyer": _norm_str(rec.get("BUYER_NAME")),
                    "seller": _norm_str(rec.get("SELLER_NAME")),
                    "premium": _norm_float(rec.get("PREMIUM_RATIO")),
                })
            pages = res.get("pages", 1)
            if params["pageNumber"] >= pages:
                break
            params["pageNumber"] = params["pageNumber"] + 1
        return out
    except Exception as e:
        logger.debug("大宗直连东财失败 %s~%s: %s", start_yyyymmdd, end_yyyymmdd, e)
        return []


def _fetch_block_trade_akshare(start_yyyymmdd: str, end_yyyymmdd: str) -> list[dict]:
    """兜底：akshare 大宗每日明细，返回统一格式 list[dict]。"""
    try:
        import akshare as ak
        import pandas as pd
        df = ak.stock_dzjy_mrmx(symbol="A股", start_date=start_yyyymmdd, end_date=end_yyyymmdd)
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return []
        out = []
        for _, row in df.iterrows():
            raw = str(row.get("交易日期", ""))
            trade_date_iso = (
                raw[:10]
                if len(raw) >= 10
                else (raw[:4] + "-" + raw[4:6] + "-" + raw[6:8] if len(raw) >= 8 else "")
            )
            symbol = str(row.get("证券代码", "") or "").strip()
            if not symbol:
                continue
            out.append({
                "symbol": symbol,
                "trade_date": trade_date_iso,
                "price": _safe_float(row, "成交价"),
                "volume": _safe_float(row, "成交量(股)") or _safe_float(row, "成交量"),
                "turnover": _safe_float(row, "成交额(元)") or _safe_float(row, "成交额"),
                "buyer": _norm_str(row.get("买方营业部")),
                "seller": _norm_str(row.get("卖方营业部")),
                "premium": _safe_float(row, "折溢率"),
            })
        return out
    except Exception as e:
        logger.debug("大宗 akshare 兜底失败 %s~%s: %s", start_yyyymmdd, end_yyyymmdd, e)
        return []


def _fetch_block_trade_one_date(date_yyyymmdd: str) -> list[dict]:
    """拉取某一交易日大宗数据，直连为主、akshare 备。"""
    rows = _fetch_block_trade_direct(date_yyyymmdd, date_yyyymmdd)
    if not rows:
        rows = _fetch_block_trade_akshare(date_yyyymmdd, date_yyyymmdd)
        if rows:
            logger.info("大宗直连无数据，改用 akshare 兜底 %s: %d 条", date_yyyymmdd, len(rows))
    else:
        logger.info("大宗直连 %s: %d 条", date_yyyymmdd, len(rows))
    return rows


async def _write_block_trade_rows_for_one_date(
    session,
    rows: list[dict],
    trade_date_iso: str,
    symbol_filter: str | None,
    use_mysql: bool,
    now_utc: datetime,
) -> int:
    """对单日数据：先删除该日旧数据，再写入新数据；若 symbol_filter 则只写该只。返回写入条数。"""
    from sqlalchemy import text

    if symbol_filter:
        rows = [r for r in rows if r.get("symbol") == symbol_filter]
    if not rows:
        return 0
    delete_sql = text("DELETE FROM stock_block_trade WHERE trade_date = :trade_date")
    await session.execute(delete_sql, {"trade_date": trade_date_iso})
    insert_sql = text(
        "INSERT INTO stock_block_trade (symbol, trade_date, price, volume, turnover, buyer, seller, premium, updated_at) "
        "VALUES (:symbol, :trade_date, :price, :volume, :turnover, :buyer, :seller, :premium, :updated_at)"
    )
    for r in rows:
        await session.execute(
            insert_sql,
            {
                "symbol": r.get("symbol", ""),
                "trade_date": trade_date_iso,
                "price": r.get("price"),
                "volume": r.get("volume"),
                "turnover": r.get("turnover"),
                "buyer": r.get("buyer"),
                "seller": r.get("seller"),
                "premium": r.get("premium"),
                "updated_at": now_utc,
            },
        )
    return len(rows)


async def run(dry_run: bool = False, symbol_filter: str | None = None, months: int = 0) -> None:
    wm = await get_watermark(CATEGORY_ID, "")
    trade_date = await get_expected_latest_date()
    trade_date_iso = trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:8]
    if symbol_filter:
        logger.info("单只拉取模式: %s，忽略 watermark", symbol_filter)
    if months > 0:
        logger.info("范围拉取: 过去 %d 个月交易日，忽略 watermark", months)
    log_stage_start(logger, CATEGORY_NAME, expected_date=trade_date_iso, watermark=wm, dry_run=dry_run)

    # 范围模式：过去 N 个月交易日，逐日拉取并按日全量覆盖写入
    if months > 0:
        date_list = await get_trading_dates_last_n_months(months)
        if not date_list:
            logger.warning("未获取到交易日列表，请确保 exchange_trading_dates 有数据")
            log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
            return
        logger.info("共 %d 个交易日待处理: %s ... %s", len(date_list), date_list[0], date_list[-1])

        from src.core.db import get_session

        _dsn = os.environ.get("MYSQL_DSN", "")
        use_mysql = "sqlite" not in _dsn.lower()
        now_utc = datetime.now(timezone.utc)
        total_count = 0
        failed_list: list[str] = []

        async for session in get_session():
            for i, date_yyyymmdd in enumerate(date_list):
                trade_date_iso = date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8]
                rows = await asyncio.to_thread(_fetch_block_trade_one_date, date_yyyymmdd)
                if not rows:
                    if (i + 1) % 10 == 0 or i == 0:
                        logger.info("[%d/%d] %s 无数据，跳过", i + 1, len(date_list), trade_date_iso)
                    continue
                if dry_run:
                    total_count += len(rows) if not symbol_filter else len([r for r in rows if r.get("symbol") == symbol_filter])
                else:
                    cnt = await _write_block_trade_rows_for_one_date(
                        session,
                        rows,
                        trade_date_iso,
                        symbol_filter,
                        use_mysql,
                        now_utc,
                    )
                    total_count += cnt
                if (i + 1) % 5 == 0 or i == len(date_list) - 1:
                    logger.info("[%d/%d] 已处理至 %s，累计写入 %d 条", i + 1, len(date_list), trade_date_iso, total_count)
            await session.commit()

        if not dry_run and date_list:
            await set_watermark(CATEGORY_ID, date_list[0][:4] + "-" + date_list[0][4:6] + "-" + date_list[0][6:8], "")
        logger.info("范围拉取完成，共写入 %d 条", total_count)
        log_stage_end(logger, CATEGORY_NAME, total_count, len(failed_list), 0, category_id=CATEGORY_ID)
        return

    # 单日模式：仅最近交易日，受 watermark 控制
    if not symbol_filter and wm is not None and trade_date <= (wm or "").replace("-", ""):
        logger.info("最近交易日 %s 已同步，跳过", trade_date_iso)
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    failed_list: list[str] = []
    dates_to_try = [trade_date, await get_last_trading_date_str(include_today=False)]
    all_rows: list[dict] = []
    chosen_trade_date_iso = ""
    for date_yyyymmdd in dates_to_try:
        rows = await asyncio.to_thread(_fetch_block_trade_one_date, date_yyyymmdd)
        if rows:
            all_rows = rows
            chosen_trade_date_iso = date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8]
            break
        if date_yyyymmdd == dates_to_try[0]:
            logger.warning("预期交易日 %s 无大宗数据，尝试上一交易日", date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8])
        else:
            logger.warning("上一交易日 %s 仍无数据", date_yyyymmdd[:4] + "-" + date_yyyymmdd[4:6] + "-" + date_yyyymmdd[6:8])

    if not all_rows:
        last_tried_iso = dates_to_try[-1][:4] + "-" + dates_to_try[-1][4:6] + "-" + dates_to_try[-1][6:8]
        logger.info("无数据，仍更新 watermark 为 %s", last_tried_iso)
        if not dry_run and not symbol_filter:
            await set_watermark(CATEGORY_ID, last_tried_iso, "")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    trade_date_iso = chosen_trade_date_iso
    if symbol_filter:
        all_rows = [r for r in all_rows if r.get("symbol") == symbol_filter]
        if not all_rows:
            logger.info("单只 %s 在当日大宗结果中未找到", symbol_filter)
            log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
            return

    count = 0
    if dry_run:
        count = len(all_rows)
        logger.info("dry_run: 当日大宗共 %d 条，跳过落库", count)
        log_stage_end(logger, CATEGORY_NAME, count, 0, 0, category_id=CATEGORY_ID)
        return

    from src.core.db import get_session

    _dsn = os.environ.get("MYSQL_DSN", "")
    use_mysql = "sqlite" not in _dsn.lower()
    now_utc = datetime.now(timezone.utc)

    async for session in get_session():
        count = await _write_block_trade_rows_for_one_date(
            session,
            all_rows,
            trade_date_iso,
            symbol_filter,
            use_mysql,
            now_utc,
        )
        await session.commit()

    if not dry_run and not symbol_filter:
        await set_watermark(CATEGORY_ID, trade_date_iso, "")
        logger.info("写入 %d 条，watermark 已更新", count)
    else:
        logger.info("写入 %d 条", count)
    log_stage_end(logger, CATEGORY_NAME, count, len(failed_list), 0, category_id=CATEGORY_ID)


def main() -> None:
    parser = argparse.ArgumentParser(description="大宗交易增量同步（东财直连主+akshare备）")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument(
        "--months",
        type=int,
        default=0,
        metavar="N",
        help="拉取过去 N 个月的交易日数据（逐日、按日全量覆盖）；默认 0 表示仅拉最近一日",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="",
        help="仅拉取该股票（6 位代码）；范围模式时对每个交易日只保留该只",
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
