#!/usr/bin/env python3
"""
独立脚本：股东户数增量同步。拉取 stock_hold_num_cninfo(date=最近季末)，写入 stock_holder_count，更新 watermark。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_holder_count_latest.py [--dry-run] [-j N]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
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
    _safe_float,
    _safe_int,
    latest_quarter_end_date,
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
CATEGORY_NAME = "股东户数"
CATEGORY_ID = "holder_count"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def run(dry_run: bool = False, concurrency: int = 3) -> None:
    date_param = latest_quarter_end_date()
    wm = await get_watermark(CATEGORY_ID, "")
    log_stage_start(logger, CATEGORY_NAME, expected_date=date_param, watermark=wm, dry_run=dry_run)

    if wm is not None and date_param <= wm:
        logger.info("季末 %s 已同步，跳过", date_param)
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
        df = await asyncio.to_thread(ak.stock_hold_num_cninfo, date=date_param)
    except Exception as e:
        logger.exception("拉取股东户数失败: %s", e)
        write_failed_list(CATEGORY_ID, ["holder_count_api"], logger)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        return

    if df is None or df.empty:
        logger.info("无数据")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 1, category_id=CATEGORY_ID)
        return

    if dry_run:
        logger.info("dry_run: 将写入 %d 条", len(df))
        log_stage_end(logger, CATEGORY_NAME, len(df), 0, 0, category_id=CATEGORY_ID)
        return

    from src.core.db import get_session
    from src.models.market_sync import StockHolderCount

    count = 0
    async for session in get_session():
        for _, row in df.iterrows():
            symbol = str(row.get("证券代码", "")).strip()
            end_d = str(row.get("变动日期", ""))[:10]
            hc = StockHolderCount(
                symbol=symbol,
                end_date=end_d,
                holder_count=_safe_int(row, "本期股东人数"),
                holder_count_change=_safe_float(row, "股东人数增幅"),
                avg_hold_amount=_safe_float(row, "本期人均持股数量"),
            )
            session.add(hc)
            count += 1
        await session.commit()

    await set_watermark(CATEGORY_ID, date_param, "")
    logger.info("写入 %d 条，watermark 已更新", count)
    log_stage_end(logger, CATEGORY_NAME, count, 0, 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="股东户数增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument("-j", "--concurrency", type=int, default=3, help="并发数")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
