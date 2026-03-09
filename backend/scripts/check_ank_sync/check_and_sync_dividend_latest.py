#!/usr/bin/env python3
"""
独立脚本：分红配股增量同步。拉取 stock_fhps_em(date=最近报告期)，写入 stock_dividends，更新 watermark。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_dividend_latest.py [--dry-run] [-j N]
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
    latest_dividend_report_date,
    previous_dividend_report_date,
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
CATEGORY_NAME = "分红配股"
CATEGORY_ID = "dividend"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


def _safe_date_str(val) -> str:
    if val is None:
        return ""
    try:
        return val.strftime("%Y-%m-%d")
    except Exception:
        return str(val)[:10] if val else ""


async def run(dry_run: bool = False, concurrency: int = 3) -> None:
    date_param = latest_dividend_report_date()
    wm = await get_watermark(CATEGORY_ID, "")
    log_stage_start(logger, CATEGORY_NAME, expected_date=date_param, watermark=wm, dry_run=dry_run)

    if wm is not None and date_param <= wm:
        logger.info("报告期 %s 已同步，跳过", date_param)
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    try:
        import akshare as ak
    except ImportError as e:
        logger.error("akshare 未安装: %s", e)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["import_error"], logger)
        return

    dates_to_fetch = [date_param]
    total = 0
    failed_list: list[str] = []

    for one_date in dates_to_fetch:
        try:
            df = await asyncio.to_thread(ak.stock_fhps_em, date=one_date)
        except Exception as e:
            logger.exception("拉取分红 %s 失败: %s", one_date, e)
            failed_list.append(one_date)
            continue

        if df is None or df.empty:
            logger.info("报告期 %s 无数据", one_date)
            continue

        if dry_run:
            total += len(df)
            logger.info("dry_run: 报告期 %s 将写入 %d 条", one_date, len(df))
            continue

        from src.core.db import get_session
        from src.models.market_sync import StockDividend

        count = 0
        async for session in get_session():
            for _, row in df.iterrows():
                report_date = _safe_date_str(row.get("最新公告日期") or row.get("预案公告日"))
                ex_date = _safe_date_str(row.get("除权除息日"))
                record_date = _safe_date_str(row.get("股权登记日"))
                dv = StockDividend(
                    symbol=str(row.get("代码", "")),
                    report_date=report_date or f"{one_date[:4]}-{one_date[4:6]}-{one_date[6:8]}",
                    ex_date=ex_date or None,
                    record_date=record_date or None,
                    bonus_ratio=_safe_float(row, "送转股份-送转比例"),
                    convert_ratio=_safe_float(row, "送转股份-转股比例"),
                    dividend_per_share=_safe_float(row, "现金分红-现金分红比例"),
                )
                session.add(dv)
                count += 1
            await session.commit()
        total += count
        logger.info("报告期 %s 写入 %d 条", one_date, count)

    if not dry_run and total > 0:
        await set_watermark(CATEGORY_ID, date_param, "")
    if failed_list:
        write_failed_list(CATEGORY_ID, failed_list, logger)
    log_stage_end(logger, CATEGORY_NAME, total, len(failed_list), 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="分红配股增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument("-j", "--concurrency", type=int, default=3, help="并发数")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
