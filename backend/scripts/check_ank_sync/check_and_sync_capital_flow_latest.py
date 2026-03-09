#!/usr/bin/env python3
"""
独立脚本：资金流向增量同步。按全标的拉取 stock_individual_fund_flow 最近 30 条，过滤 trade_date > watermark 写入 stock_capital_flow。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_capital_flow_latest.py [--dry-run] [-j N]
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
CATEGORY_NAME = "资金流向"
CATEGORY_ID = "capital_flow"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def _get_all_stock_codes() -> list[str]:
    """从数据库获取 A 股代码列表。"""
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


async def run(dry_run: bool = False, concurrency: int = 1) -> None:
    wm = await get_watermark(CATEGORY_ID, "")
    expected_ymd = await get_expected_latest_date()
    last_td_iso = f"{expected_ymd[:4]}-{expected_ymd[4:6]}-{expected_ymd[6:8]}"
    log_stage_start(logger, CATEGORY_NAME, expected_date=last_td_iso, watermark=wm, dry_run=dry_run)

    wm_ymd = wm.replace("-", "") if wm else None
    if wm_ymd is not None and expected_ymd <= wm_ymd:
        logger.info("最近交易日 %s 已同步，跳过", last_td_iso)
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

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
    empty_list: list[str] = []
    all_rows: list[dict] = []

    async def pull_one(symbol: str):
        async with sem:
            try:
                market = "sh" if symbol.startswith("6") else "sz"
                df = await asyncio.to_thread(ak.stock_individual_fund_flow, stock=symbol, market=market)
                if df is None or df.empty:
                    empty_list.append(symbol)
                    return
                for _, row in df.tail(30).iterrows():
                    trade_date = str(row.get("日期", str(row.iloc[0]) if len(row) > 0 else ""))[:10]
                    if not trade_date or (wm and trade_date <= wm):
                        continue
                    all_rows.append({
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "main_net_inflow": _safe_float(row, "主力净流入-净额"),
                        "small_net_inflow": _safe_float(row, "小单净流入-净额"),
                        "medium_net_inflow": _safe_float(row, "中单净流入-净额"),
                        "large_net_inflow": _safe_float(row, "大单净流入-净额"),
                        "super_large_net_inflow": _safe_float(row, "超大单净流入-净额"),
                        "updated_at": datetime.now(timezone.utc),
                    })
            except Exception as e:
                logger.debug("资金流向 %s 失败: %s", symbol, e)
                failed_list.append(symbol)

    total = len(symbols)
    for i in range(0, total, concurrency):
        batch = symbols[i : i + concurrency]
        await asyncio.gather(*[pull_one(s) for s in batch])
        if (i + concurrency) % 500 == 0 or i + concurrency >= total:
            logger.info("已处理 %d/%d，成功 %d 条待写，失败 %d，空 %d",
                        min(i + concurrency, total), total, len(all_rows), len(failed_list), len(empty_list))

    if dry_run:
        logger.info("dry_run: 将写入 %d 条（%d 只标的），失败 %d，空 %d",
                   len(all_rows), len(symbols) - len(failed_list) - len(empty_list), len(failed_list), len(empty_list))
        write_failed_list(CATEGORY_ID, failed_list + empty_list, logger)
        log_stage_end(logger, CATEGORY_NAME, len(all_rows), len(failed_list), len(empty_list), category_id=CATEGORY_ID)
        return

    if not all_rows:
        logger.info("无新数据需写入")
        write_failed_list(CATEGORY_ID, failed_list + empty_list, logger)
        log_stage_end(logger, CATEGORY_NAME, 0, len(failed_list), len(empty_list), category_id=CATEGORY_ID)
        return

    from src.core.db import get_session
    from src.models.market_sync import StockCapitalFlow

    _dsn = os.environ.get("MYSQL_DSN", "")
    use_mysql_upsert = "sqlite" not in _dsn.lower()
    batch_size = 500
    written = 0
    for start in range(0, len(all_rows), batch_size):
        chunk = all_rows[start : start + batch_size]
        async for session in get_session():
            if use_mysql_upsert and chunk:
                from sqlalchemy.dialects.mysql import insert as mysql_insert
                stmt = mysql_insert(StockCapitalFlow).values(chunk)
                stmt = stmt.on_duplicate_key_update(
                    main_net_inflow=stmt.inserted.main_net_inflow,
                    small_net_inflow=stmt.inserted.small_net_inflow,
                    medium_net_inflow=stmt.inserted.medium_net_inflow,
                    large_net_inflow=stmt.inserted.large_net_inflow,
                    super_large_net_inflow=stmt.inserted.super_large_net_inflow,
                    updated_at=stmt.inserted.updated_at,
                )
                await session.execute(stmt)
            else:
                for r in chunk:
                    session.add(StockCapitalFlow(**r))
            await session.commit()
            written += len(chunk)

    if written > 0:
        await set_watermark(CATEGORY_ID, last_td_iso, "")
    write_failed_list(CATEGORY_ID, failed_list + empty_list, logger)
    logger.info("写入 %d 条，watermark 已更新", written)
    log_stage_end(logger, CATEGORY_NAME, written, len(failed_list), len(empty_list), category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="资金流向增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument("-j", "--concurrency", type=int, default=1, help="并发数，默认 1 降低东财限流")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
