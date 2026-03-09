#!/usr/bin/env python3
"""
独立脚本：龙虎榜增量同步。拉取 stock_lhb_detail_em(start_date, end_date)，写入 stock_lhb，更新 watermark。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_lhb_latest.py [--dry-run] [-j N]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime, timedelta
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
CATEGORY_NAME = "龙虎榜"
CATEGORY_ID = "lhb"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def run(dry_run: bool = False, concurrency: int = 3) -> None:
    wm = await get_watermark(CATEGORY_ID, "")
    end = datetime.now().strftime("%Y%m%d")
    start = wm if wm else (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
    log_stage_start(logger, CATEGORY_NAME, expected_date=end, watermark=wm, dry_run=dry_run)

    try:
        import akshare as ak
    except ImportError as e:
        logger.error("akshare 未安装: %s", e)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["import_error"], logger)
        return

    try:
        df = await asyncio.to_thread(ak.stock_lhb_detail_em, start_date=start, end_date=end)
    except (TypeError, KeyError) as e:
        if "NoneType" in str(e) and "not subscriptable" in str(e):
            logger.info("API 返回 null，视为无数据")
            log_stage_end(logger, CATEGORY_NAME, 0, 0, 1, category_id=CATEGORY_ID)
            return
        raise
    except Exception as e:
        logger.exception("拉取龙虎榜失败: %s", e)
        write_failed_list(CATEGORY_ID, ["lhb_api"], logger)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        return

    if df is None or df.empty:
        logger.info("龙虎榜无数据")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 1, category_id=CATEGORY_ID)
        return

    if "上榜日期" not in df.columns and "上榜日" in df.columns:
        df["上榜日期"] = df["上榜日"].astype(str)
    if "成交额" not in df.columns and "龙虎榜成交额" in df.columns:
        df["成交额"] = df["龙虎榜成交额"]

    if wm:
        df = df.copy()
        df["_dt"] = df["上榜日期"].astype(str).str.replace("-", "")
        df = df[df["_dt"] > wm].drop(columns=["_dt"], errors="ignore")
    if df.empty:
        logger.info("无新数据需写入")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    if dry_run:
        logger.info("dry_run: 将写入 %d 条，跳过落库", len(df))
        log_stage_end(logger, CATEGORY_NAME, len(df), 0, 0, category_id=CATEGORY_ID)
        return

    from src.core.db import get_session
    from src.models.market_sync import StockLHB

    count = 0
    async for session in get_session():
        for _, row in df.iterrows():
            lhb = StockLHB(
                symbol=str(row.get("代码", "")),
                symbol_name=str(row.get("名称", "")),
                trade_date=str(row.get("上榜日期", "")),
                reason=str(row.get("解读", row.get("上榜原因", ""))),
                close_price=_safe_float(row, "收盘价"),
                change_pct=_safe_float(row, "涨跌幅"),
                net_buy=_safe_float(row, "龙虎榜净买额"),
                buy_amount=_safe_float(row, "龙虎榜买入额"),
                sell_amount=_safe_float(row, "龙虎榜卖出额"),
                turnover=_safe_float(row, "成交额"),
            )
            session.add(lhb)
            count += 1
        await session.commit()

    await set_watermark(CATEGORY_ID, end, "")
    logger.info("写入 %d 条，watermark 已更新", count)
    log_stage_end(logger, CATEGORY_NAME, count, 0, 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="龙虎榜增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument("-j", "--concurrency", type=int, default=3, help="并发数")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
