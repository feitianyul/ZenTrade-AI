#!/usr/bin/env python3
"""
独立脚本：北向持股排行增量同步。按最近交易日拉取 stock_hsgt_hold_stock_em(北向, 今日排行)，写入 northbound_hold_stock，更新 watermark。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_northbound_hold_latest.py [--dry-run] [-j N]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

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

from scripts.check_ank_sync.sync_script_utils import (
    get_watermark,
    set_watermark,
    get_expected_latest_date,
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
CATEGORY_NAME = "北向持股排行"
CATEGORY_ID = "northbound_hold"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def run(dry_run: bool = False, concurrency: int = 3) -> None:
    wm = await get_watermark(CATEGORY_ID, "")
    trade_date_ymd = await get_expected_latest_date()
    trade_date = f"{trade_date_ymd[:4]}-{trade_date_ymd[4:6]}-{trade_date_ymd[6:8]}"
    log_stage_start(logger, CATEGORY_NAME, expected_date=trade_date, watermark=wm, dry_run=dry_run)

    if wm is not None and trade_date_ymd <= wm.replace("-", ""):
        logger.info("最近交易日 %s 已同步，跳过", trade_date)
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    try:
        import akshare as ak
    except ImportError as e:
        logger.error("akshare 未安装: %s", e)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["import_error"], logger)
        return

    to_sync = [("北向", "今日排行")]
    now_nb = datetime.now(timezone.utc)
    total_count = 0
    failed_list: list[str] = []

    for market, indicator in to_sync:
        try:
            df = await asyncio.to_thread(ak.stock_hsgt_hold_stock_em, market=market, indicator=indicator)
        except Exception as e:
            logger.exception("拉取 %s %s 失败: %s", market, indicator, e)
            failed_list.append(f"{market}_{indicator}")
            continue

        if df is None or df.empty:
            logger.info("%s %s: 无数据", market, indicator)
            continue

        rows = []
        for _, row in df.iterrows():
            rows.append({
                "trade_date": trade_date,
                "market": market,
                "indicator": indicator,
                "code": str(row.get("代码", "")),
                "name": str(row.get("名称", "")),
                "close": _safe_float(row, "今日收盘价"),
                "change_pct": _safe_float(row, "今日涨跌幅"),
                "hold_shares": _safe_float(row, "持股股数"),
                "hold_value": _safe_float(row, "持股市值"),
                "float_ratio": _safe_float(row, "持股数量占A股百分比"),
                "increase_shares": _safe_float(row, "增持股数"),
                "increase_value": _safe_float(row, "增持市值"),
                "sector": str(row.get("所属板块", "")),
                "updated_at": now_nb,
            })
        total_count += len(rows)

        if dry_run:
            logger.info("dry_run: %s %s 将写入 %d 条", market, indicator, len(rows))
            continue

        from sqlalchemy import text
        from src.core.db import get_session
        from src.models.market_sync import NorthboundHoldStock

        _dsn = os.environ.get("MYSQL_DSN", "")
        use_mysql_upsert = "sqlite" not in _dsn.lower()
        async for session in get_session():
            if use_mysql_upsert and rows:
                from sqlalchemy.dialects.mysql import insert as mysql_insert
                stmt = mysql_insert(NorthboundHoldStock).values(rows)
                stmt = stmt.on_duplicate_key_update(
                    name=stmt.inserted.name,
                    close=stmt.inserted.close,
                    change_pct=stmt.inserted.change_pct,
                    hold_shares=stmt.inserted.hold_shares,
                    hold_value=stmt.inserted.hold_value,
                    float_ratio=stmt.inserted.float_ratio,
                    increase_shares=stmt.inserted.increase_shares,
                    increase_value=stmt.inserted.increase_value,
                    sector=stmt.inserted.sector,
                    updated_at=stmt.inserted.updated_at,
                )
                await session.execute(stmt)
            elif rows:
                # SQLite：仅新增/更新，不删历史
                ins = text(
                    "INSERT INTO northbound_hold_stock (trade_date, market, indicator, code, name, close, change_pct, hold_shares, hold_value, float_ratio, increase_shares, increase_value, sector) "
                    "VALUES (:trade_date, :market, :indicator, :code, :name, :close, :change_pct, :hold_shares, :hold_value, :float_ratio, :increase_shares, :increase_value, :sector) "
                    "ON CONFLICT(trade_date, market, indicator, code) DO UPDATE SET name=excluded.name, close=excluded.close, change_pct=excluded.change_pct, "
                    "hold_shares=excluded.hold_shares, hold_value=excluded.hold_value, float_ratio=excluded.float_ratio, "
                    "increase_shares=excluded.increase_shares, increase_value=excluded.increase_value, sector=excluded.sector"
                )
                for r in rows:
                    await session.execute(ins, {k: v for k, v in r.items() if k != "updated_at"})
            await session.commit()
        logger.info("%s %s: 写入 %d 条", market, indicator, len(rows))

    if not dry_run and total_count > 0:
        await set_watermark(CATEGORY_ID, trade_date, "")
    if failed_list:
        write_failed_list(CATEGORY_ID, failed_list, logger)
    log_stage_end(logger, CATEGORY_NAME, total_count, len(failed_list), 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="北向持股排行增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument("-j", "--concurrency", type=int, default=3, help="并发数")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
