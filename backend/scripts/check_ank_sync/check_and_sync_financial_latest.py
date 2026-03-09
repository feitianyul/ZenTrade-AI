#!/usr/bin/env python3
"""
独立脚本：财务指标增量同步。按全标的拉取 stock_financial_analysis_indicator，只写 report_date > watermark 写入 stock_financial，更新 watermark。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_financial_latest.py [--dry-run] [-j N]
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
CATEGORY_NAME = "财务指标"
CATEGORY_ID = "financial"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def _get_all_stock_codes() -> list[str]:
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_data import StockInfo
        async for session in get_session():
            result = await session.execute(select(StockInfo.code))
            return [str(r[0]) for r in result.fetchall()]
    except Exception:
        pass
    return []


async def run(dry_run: bool = False, concurrency: int = 3) -> None:
    wm = await get_watermark(CATEGORY_ID, "")
    log_stage_start(logger, CATEGORY_NAME, expected_date="", watermark=wm, dry_run=dry_run)

    symbols = await _get_all_stock_codes()
    if not symbols:
        logger.warning("数据库无股票列表，请先同步 A股列表")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    try:
        import akshare as ak
    except ImportError as e:
        logger.error("akshare 未安装: %s", e)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["import_error"], logger)
        return

    sem = asyncio.Semaphore(concurrency)
    failed_list: list[str] = []
    new_row_count = 0
    max_report_date = wm or ""
    total = len(symbols)

    async def pull_one(symbol: str):
        async with sem:
            try:
                df = await asyncio.to_thread(ak.stock_financial_analysis_indicator, symbol=symbol)
                if df is None or df.empty:
                    return (symbol, [], "")
                mr = ""
                rows_to_add = []
                for _, row in df.iterrows():
                    report_date = str(row.iloc[0]) if len(row) > 0 else ""
                    if not report_date:
                        continue
                    if wm and report_date <= wm:
                        continue
                    if report_date > mr:
                        mr = report_date
                    raw_data = row.to_json(force_ascii=False) if hasattr(row, "to_json") else None
                    rows_to_add.append({
                        "symbol": symbol,
                        "report_date": report_date,
                        "roe": _safe_float(row, "净资产收益率(%)"),
                        "gross_margin": _safe_float(row, "销售毛利率(%)"),
                        "net_margin": _safe_float(row, "销售净利率(%)"),
                        "eps": _safe_float(row, "基本每股收益(元)"),
                        "debt_ratio": _safe_float(row, "资产负债比率(%)"),
                        "current_ratio": _safe_float(row, "流动比率"),
                        "raw_data": raw_data,
                    })
                return (symbol, rows_to_add, mr)
            except Exception as e:
                return (symbol, [], "")

    for i in range(0, total, concurrency):
        batch = symbols[i : i + concurrency]
        results = await asyncio.gather(*[pull_one(s) for s in batch], return_exceptions=True)
        for j, r in enumerate(results):
            if isinstance(r, Exception):
                failed_list.append(batch[j])
                continue
            symbol, rows_to_add, mr = r
            if mr and mr > max_report_date:
                max_report_date = mr
            if not rows_to_add:
                continue
            if dry_run:
                new_row_count += len(rows_to_add)
                continue
            try:
                from src.core.db import get_session
                from src.models.market_sync import StockFinancial
                async for session in get_session():
                    for row_dict in rows_to_add:
                        session.add(StockFinancial(**row_dict))
                    await session.commit()
                new_row_count += len(rows_to_add)
            except Exception as e:
                failed_list.append(symbol)
                logger.debug("财务 %s 写入失败: %s", symbol, e)
        if (i + concurrency) % 300 == 0 or i + concurrency >= total:
            logger.info("已处理 %d/%d，写入 %d 条，失败 %d",
                        min(i + concurrency, total), total, new_row_count, len(failed_list))

    if not dry_run and new_row_count > 0 and max_report_date:
        await set_watermark(CATEGORY_ID, max_report_date, "")
    if failed_list:
        write_failed_list(CATEGORY_ID, failed_list[:500], logger)
    log_stage_end(logger, CATEGORY_NAME, new_row_count, len(failed_list), 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="财务指标增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument("-j", "--concurrency", type=int, default=3, help="并发数")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
