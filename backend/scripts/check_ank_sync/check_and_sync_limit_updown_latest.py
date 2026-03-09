#!/usr/bin/env python3
"""
独立脚本：涨跌停池增量同步。按最近交易日拉取 stock_zt_pool_em(date=)，仅新增/更新（不删历史），便于按日对比。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_limit_updown_latest.py [--dry-run]
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
    _safe_int,
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
CATEGORY_NAME = "涨跌停"
CATEGORY_ID = "limit_updown"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def run(dry_run: bool = False) -> None:
    wm = await get_watermark(CATEGORY_ID, "")
    trade_date = await get_expected_latest_date()
    trade_date_iso = trade_date[:4] + "-" + trade_date[4:6] + "-" + trade_date[6:8]
    log_stage_start(logger, CATEGORY_NAME, expected_date=trade_date_iso, watermark=wm, dry_run=dry_run)

    if wm is not None and trade_date <= wm.replace("-", ""):
        logger.info("最近交易日 %s 已同步，跳过", trade_date_iso)
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    try:
        import akshare as ak
    except ImportError as e:
        logger.error("akshare 未安装: %s", e)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["import_error"], logger)
        return

    try:
        df = await asyncio.to_thread(ak.stock_zt_pool_em, date=trade_date)
    except Exception as e:
        logger.exception("拉取涨跌停 %s 失败: %s", trade_date_iso, e)
        write_failed_list(CATEGORY_ID, ["api_error"], logger)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        return

    if df is None or df.empty:
        logger.info("最近交易日 %s 无涨停数据（可能休市），仍更新 watermark", trade_date_iso)
        if not dry_run:
            await set_watermark(CATEGORY_ID, trade_date, "")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    count = len(df)
    if dry_run:
        logger.info("dry_run: 将删除当日涨停并写入 %d 条，跳过落库", count)
        log_stage_end(logger, CATEGORY_NAME, count, 0, 0, category_id=CATEGORY_ID)
        return

    import os
    from sqlalchemy import text
    from src.core.db import get_session
    from src.models.market_sync import StockLimitUpDown

    _dsn = os.environ.get("MYSQL_DSN", "")
    use_mysql_upsert = "sqlite" not in _dsn.lower()
    now_utc = datetime.now(timezone.utc)

    async for session in get_session():
        for _, row in df.iterrows():
            symbol = str(row.get("代码", ""))
            symbol_name = str(row.get("名称", ""))
            close_price = _safe_float(row, "最新价")
            change_pct = _safe_float(row, "涨跌幅")
            first_limit_time = str(row.get("首次封板时间", ""))
            last_limit_time = str(row.get("最后封板时间", ""))
            open_count = _safe_int(row, "炸板次数")
            continuous_days = _safe_int(row, "连板数")
            if use_mysql_upsert:
                sql = text(
                    "INSERT INTO stock_limit_updown "
                    "(symbol, symbol_name, trade_date, limit_type, close_price, change_pct, first_limit_time, last_limit_time, open_count, continuous_days, updated_at) "
                    "VALUES (:symbol, :symbol_name, :trade_date, 'up', :close_price, :change_pct, :first_limit_time, :last_limit_time, :open_count, :continuous_days, :updated_at) "
                    "ON DUPLICATE KEY UPDATE symbol_name=VALUES(symbol_name), close_price=VALUES(close_price), change_pct=VALUES(change_pct), "
                    "first_limit_time=VALUES(first_limit_time), last_limit_time=VALUES(last_limit_time), open_count=VALUES(open_count), continuous_days=VALUES(continuous_days), updated_at=VALUES(updated_at)"
                )
                await session.execute(sql, {
                    "symbol": symbol, "symbol_name": symbol_name, "trade_date": trade_date_iso,
                    "close_price": close_price, "change_pct": change_pct,
                    "first_limit_time": first_limit_time, "last_limit_time": last_limit_time,
                    "open_count": open_count, "continuous_days": continuous_days,
                    "updated_at": now_utc,
                })
            else:
                # SQLite: INSERT OR REPLACE 依赖唯一约束 uq_limit_updown_symbol_date_type
                sql = text(
                    "INSERT INTO stock_limit_updown "
                    "(symbol, symbol_name, trade_date, limit_type, close_price, change_pct, first_limit_time, last_limit_time, open_count, continuous_days, updated_at) "
                    "VALUES (:symbol, :symbol_name, :trade_date, 'up', :close_price, :change_pct, :first_limit_time, :last_limit_time, :open_count, :continuous_days, :updated_at) "
                    "ON CONFLICT(symbol, trade_date, limit_type) DO UPDATE SET "
                    "symbol_name=excluded.symbol_name, close_price=excluded.close_price, change_pct=excluded.change_pct, "
                    "first_limit_time=excluded.first_limit_time, last_limit_time=excluded.last_limit_time, "
                    "open_count=excluded.open_count, continuous_days=excluded.continuous_days, updated_at=excluded.updated_at"
                )
                await session.execute(sql, {
                    "symbol": symbol, "symbol_name": symbol_name, "trade_date": trade_date_iso,
                    "close_price": close_price, "change_pct": change_pct,
                    "first_limit_time": first_limit_time or None, "last_limit_time": last_limit_time or None,
                    "open_count": open_count, "continuous_days": continuous_days,
                    "updated_at": now_utc,
                })
        await session.commit()
        break

    await set_watermark(CATEGORY_ID, trade_date, "")
    logger.info("涨跌停同步完成: 写入 %d 条", count)
    log_stage_end(logger, CATEGORY_NAME, count, 0, 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="涨跌停池增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
