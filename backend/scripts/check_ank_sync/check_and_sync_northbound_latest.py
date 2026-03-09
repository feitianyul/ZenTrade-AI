#!/usr/bin/env python3
"""
独立脚本：北向资金增量同步。拉取 stock_hsgt_hist_em(北向资金) 及沪股通/深股通补分项，写入 northbound_flow，更新 watermark。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_northbound_latest.py [--dry-run] [-j N]
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
    get_last_trading_date_str,
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
CATEGORY_NAME = "北向资金"
CATEGORY_ID = "northbound"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def run(dry_run: bool = False, concurrency: int = 3) -> None:
    wm = await get_watermark(CATEGORY_ID, "")
    today_iso = datetime.now().strftime("%Y-%m-%d")
    log_stage_start(logger, CATEGORY_NAME, expected_date=today_iso, watermark=wm, dry_run=dry_run)

    try:
        import akshare as ak
    except ImportError as e:
        logger.error("akshare 未安装: %s", e)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["import_error"], logger)
        return

    fn = getattr(ak, "stock_hsgt_hist_em", None)
    if fn is None:
        logger.error("akshare 缺少 stock_hsgt_hist_em")
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["api_missing"], logger)
        return

    try:
        df = await asyncio.to_thread(fn, symbol="北向资金")
    except Exception as e:
        logger.exception("拉取北向资金失败: %s", e)
        write_failed_list(CATEGORY_ID, ["北向资金"], logger)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        return

    if df is None or df.empty:
        logger.info("北向资金无数据")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 1, category_id=CATEGORY_ID)
        return

    _date_col = "日期"
    if _date_col in df.columns and wm:
        df = df.copy()
        df["_dt"] = df[_date_col].astype(str).str[:10]
        df = df[df["_dt"] > wm].drop(columns=["_dt"], errors="ignore")
    if df.empty:
        logger.info("无新数据需写入，watermark 已是最新")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    sh_by_date: dict[str, float] = {}
    sz_by_date: dict[str, float] = {}
    try:
        df_sh = await asyncio.to_thread(fn, symbol="沪股通")
        if df_sh is not None and not df_sh.empty and "日期" in df_sh.columns:
            col = "当日成交净买额" if "当日成交净买额" in df_sh.columns else df_sh.columns[1]
            for _, row in df_sh.iterrows():
                d = str(row.get("日期", ""))[:10]
                if d:
                    sh_by_date[d] = _safe_float(row, col) or 0.0
        df_sz = await asyncio.to_thread(fn, symbol="深股通")
        if df_sz is not None and not df_sz.empty and "日期" in df_sz.columns:
            col = "当日成交净买额" if "当日成交净买额" in df_sz.columns else df_sz.columns[1]
            for _, row in df_sz.iterrows():
                d = str(row.get("日期", ""))[:10]
                if d:
                    sz_by_date[d] = _safe_float(row, col) or 0.0
    except Exception:
        pass

    total_col = "当日成交净买额" if "当日成交净买额" in df.columns else "当日净流入"
    now_nb = datetime.now(timezone.utc)
    rows = []
    for _, row in df.iterrows():
        trade_date = str(row.get("日期", str(row.iloc[0]) if len(row) > 0 else ""))[:10]
        if not trade_date:
            continue
        rows.append({
            "trade_date": trade_date,
            "direction": "north",
            "total_net_buy": _safe_float(row, total_col),
            "sh_net_buy": sh_by_date.get(trade_date) if sh_by_date else _safe_float(row, "沪股通净流入"),
            "sz_net_buy": sz_by_date.get(trade_date) if sz_by_date else _safe_float(row, "深股通净流入"),
            "updated_at": now_nb,
        })

    if dry_run:
        logger.info("dry_run: 将写入 %d 条，跳过落库", len(rows))
        log_stage_end(logger, CATEGORY_NAME, len(rows), 0, 0, category_id=CATEGORY_ID)
        return

    from src.core.db import get_session
    from src.models.market_sync import NorthboundFlow

    _dsn = os.environ.get("MYSQL_DSN", "")
    use_mysql_upsert = "sqlite" not in _dsn.lower()
    async for session in get_session():
        if use_mysql_upsert and rows:
            from sqlalchemy.dialects.mysql import insert as mysql_insert
            stmt = mysql_insert(NorthboundFlow).values(rows)
            stmt = stmt.on_duplicate_key_update(
                total_net_buy=stmt.inserted.total_net_buy,
                sh_net_buy=stmt.inserted.sh_net_buy,
                sz_net_buy=stmt.inserted.sz_net_buy,
                updated_at=stmt.inserted.updated_at,
            )
            await session.execute(stmt)
        else:
            for r in rows:
                session.add(NorthboundFlow(**r))
        await session.commit()

    await set_watermark(CATEGORY_ID, today_iso, "")
    logger.info("写入 %d 条，watermark 已更新", len(rows))
    log_stage_end(logger, CATEGORY_NAME, len(rows), 0, 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="北向资金增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument("-j", "--concurrency", type=int, default=3, help="并发数（本脚本单接口可忽略）")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
