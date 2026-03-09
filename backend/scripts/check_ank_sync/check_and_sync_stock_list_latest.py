#!/usr/bin/env python3
"""
独立脚本：A股列表增量同步。先 stock_info_sh_name_code(主板A股)，失败则 stock_info_a_code_name，UPSERT 写入 stock_info。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_stock_list_latest.py [--dry-run]
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
CATEGORY_NAME = "A股列表"
CATEGORY_ID = "stock_list"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def run(dry_run: bool = False) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    wm = await get_watermark(CATEGORY_ID, "")
    log_stage_start(logger, CATEGORY_NAME, expected_date=today, watermark=wm, dry_run=dry_run)

    try:
        import akshare as ak
    except ImportError as e:
        logger.error("akshare 未安装: %s", e)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["import_error"], logger)
        return

    df = None
    try:
        df = await asyncio.wait_for(asyncio.to_thread(ak.stock_info_sh_name_code, "主板A股"), timeout=30)
        if df is not None and not df.empty and "证券代码" in df.columns and "证券简称" in df.columns:
            df = df[["证券代码", "证券简称"]].rename(columns={"证券代码": "code", "证券简称": "name"})
        else:
            df = None
    except (asyncio.TimeoutError, Exception) as e:
        logger.warning("主板A股拉取失败: %s，尝试全市场", e)
        df = None

    if df is None or df.empty:
        try:
            df = await asyncio.wait_for(asyncio.to_thread(ak.stock_info_a_code_name), timeout=60)
            if df is not None and not df.empty and ("code" not in df.columns or "name" not in df.columns):
                df = None
        except (asyncio.TimeoutError, Exception) as e:
            logger.exception("全市场 A 股列表拉取失败: %s", e)
            write_failed_list(CATEGORY_ID, ["api_error"], logger)
            log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
            return

    if df is None or df.empty:
        logger.error("AKShare 返回空数据")
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        return

    count = len(df)
    if dry_run:
        logger.info("dry_run: 将 UPSERT %d 条，跳过落库", count)
        log_stage_end(logger, CATEGORY_NAME, count, 0, 0, category_id=CATEGORY_ID)
        return

    from sqlalchemy import text
    from src.core.db import get_session

    _dsn = os.environ.get("MYSQL_DSN", "")
    saved = 0
    async for session in get_session():
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            name = str(row.get("name", "")).strip()
            if not code:
                continue
            if "sqlite" in _dsn.lower():
                sql = text(
                    "INSERT OR REPLACE INTO stock_info (code, name, market) VALUES (:code, :name, 'A')"
                )
            else:
                sql = text(
                    "INSERT INTO stock_info (code, name, market, updated_at) "
                    "VALUES (:code, :name, 'A', :now) AS s "
                    "ON DUPLICATE KEY UPDATE name=s.name, updated_at=:now"
                )
            await session.execute(sql, {"code": code, "name": name, "now": datetime.utcnow()})
            saved += 1
        await session.commit()
        break

    await set_watermark(CATEGORY_ID, today, "")
    logger.info("A股列表同步完成: 写入 %d 条", saved)
    log_stage_end(logger, CATEGORY_NAME, saved, 0, 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="A股列表增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
