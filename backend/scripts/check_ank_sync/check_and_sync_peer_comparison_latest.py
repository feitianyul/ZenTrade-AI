#!/usr/bin/env python3
"""
独立脚本：同行比较增量同步。按全标的拉取东财 4 类接口(growth/valuation/dupont/scale)，按最近交易日 as_of_date 写入 stock_peer_comparison。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_peer_comparison_latest.py [--dry-run] [-j N]
"""
from __future__ import annotations

import argparse
import asyncio
import json
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
    get_last_trading_date_str,
    symbol_to_em,
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
CATEGORY_NAME = "同行比较"
CATEGORY_ID = "peer_comparison"
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


def _df_to_json(df) -> str:
    if df is None or df.empty:
        return ""
    return json.dumps(df.to_dict(orient="records"), ensure_ascii=False)


async def run(dry_run: bool = False, concurrency: int = 5) -> None:
    wm = await get_watermark(CATEGORY_ID, "")
    last_td = await get_last_trading_date_str(include_today=True)
    target_date_iso = last_td[:4] + "-" + last_td[4:6] + "-" + last_td[6:8]
    log_stage_start(logger, CATEGORY_NAME, expected_date=target_date_iso, watermark=wm, dry_run=dry_run)

    if wm is not None and target_date_iso <= wm:
        logger.info("最近交易日 %s 已同步，跳过", target_date_iso)
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
    success_count = 0
    apis = [
        (ak.stock_zh_growth_comparison_em, "growth"),
        (ak.stock_zh_valuation_comparison_em, "valuation"),
        (ak.stock_zh_dupont_comparison_em, "dupont"),
        (ak.stock_zh_scale_comparison_em, "scale"),
    ]

    async def pull_one(sym: str) -> tuple[str, list[tuple[str, str]]] | tuple[str, None]:
        async with sem:
            try:
                em_symbol = symbol_to_em(sym)
                rows = []
                for api_fn, sub_type in apis:
                    df = await asyncio.to_thread(api_fn, symbol=em_symbol)
                    if df is not None and not df.empty:
                        rows.append((sub_type, _df_to_json(df)))
                return (sym, rows)
            except Exception as e:
                return (sym, None)

    batch_size = 25
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        results = await asyncio.gather(*[pull_one(s) for s in batch], return_exceptions=True)
        now = datetime.now(timezone.utc)
        for j, res in enumerate(results):
            sym = batch[j]
            if isinstance(res, Exception):
                failed_list.append(sym)
                continue
            _, rows = res
            if not rows:
                continue
            success_count += 1
            if dry_run:
                continue
            try:
                from sqlalchemy import text
                from src.core.db import get_session
                _dsn = os.environ.get("MYSQL_DSN", "")
                async for session in get_session():
                    for sub_type, raw_data in rows:
                        if "sqlite" in _dsn:
                            sql = text(
                                "INSERT INTO stock_peer_comparison (symbol, sub_type, as_of_date, raw_data, updated_at) "
                                "VALUES (:symbol, :sub_type, :as_of_date, :raw_data, :now) "
                                "ON CONFLICT(symbol, sub_type, as_of_date) DO UPDATE SET raw_data=excluded.raw_data, updated_at=excluded.updated_at"
                            )
                        else:
                            sql = text(
                                "INSERT INTO stock_peer_comparison (symbol, sub_type, as_of_date, raw_data, updated_at) "
                                "VALUES (:symbol, :sub_type, :as_of_date, :raw_data, :now) AS new "
                                "ON DUPLICATE KEY UPDATE raw_data=new.raw_data, updated_at=new.updated_at"
                            )
                        await session.execute(
                            sql,
                            {
                                "symbol": sym,
                                "sub_type": sub_type,
                                "as_of_date": target_date_iso,
                                "raw_data": raw_data,
                                "now": now,
                            },
                        )
                    await session.commit()
                    break
            except Exception as exc:
                logger.warning("peer_comparison upsert %s: %s", sym, exc)
                failed_list.append(sym)
                success_count -= 1
        if (i + len(batch)) % 100 == 0 or i + len(batch) == len(symbols):
            logger.info("已处理 %d/%d 成功 %d 失败 %d", min(i + len(batch), len(symbols)), len(symbols), success_count, len(failed_list))

    if not dry_run:
        await set_watermark(CATEGORY_ID, target_date_iso, "")
    write_failed_list(CATEGORY_ID, failed_list, logger)
    log_stage_end(logger, CATEGORY_NAME, success_count, len(failed_list), 0, category_id=CATEGORY_ID)


def main() -> None:
    parser = argparse.ArgumentParser(description="同行比较增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅拉取不落库")
    parser.add_argument("-j", "--jobs", type=int, default=5, help="并发数")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.jobs))


if __name__ == "__main__":
    main()
