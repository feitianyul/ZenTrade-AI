#!/usr/bin/env python3
"""
独立脚本：交易所日历增量同步。拉取 tool_trade_date_hist_sina() 全量覆盖 exchange_trading_dates，更新 watermark。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_trade_calendar_latest.py [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
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
CATEGORY_NAME = "交易所日历"
CATEGORY_ID = "trade_calendar"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def run(dry_run: bool = False) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    wm = await get_watermark(CATEGORY_ID, "")
    log_stage_start(logger, CATEGORY_NAME, expected_date=today, watermark=wm, dry_run=dry_run)

    if wm is not None and wm >= today:
        logger.info("watermark %s 已>=今日，跳过", wm)
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
        df = await asyncio.to_thread(ak.tool_trade_date_hist_sina)
    except Exception as e:
        logger.exception("拉取交易日历失败: %s", e)
        write_failed_list(CATEGORY_ID, ["api_error"], logger)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        return

    if df is None or df.empty:
        logger.warning("交易日历无数据")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 1, category_id=CATEGORY_ID)
        return

    col = "trade_date" if "trade_date" in df.columns else df.columns[0]
    dates = df[col].astype(str).str.strip()
    dates = [str(x) for x in dates[dates.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)].unique().tolist()]
    count = len(dates)

    if dry_run:
        logger.info("dry_run: 将全量覆盖 %d 个交易日，跳过落库", count)
        log_stage_end(logger, CATEGORY_NAME, count, 0, 0, category_id=CATEGORY_ID)
        return

    from sqlalchemy import text
    from src.core.db import get_session
    from src.models.market_sync import ExchangeTradingDate

    async for session in get_session():
        await session.execute(text("DELETE FROM exchange_trading_dates"))
        for d in dates:
            session.add(ExchangeTradingDate(trade_date=str(d)))
        await session.commit()
        break

    await set_watermark(CATEGORY_ID, today, "")
    logger.info("交易所日历同步完成: %d 个交易日", count)
    log_stage_end(logger, CATEGORY_NAME, count, 0, 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="交易所日历增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
