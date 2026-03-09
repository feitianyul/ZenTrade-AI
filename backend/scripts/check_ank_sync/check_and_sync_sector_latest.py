#!/usr/bin/env python3
"""
独立脚本：行业/概念板块增量同步。拉取行业+概念板块列表及成分股，UPSERT stock_sectors 与 stock_sector_members。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_sector_latest.py [--dry-run] [-j N]
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
CATEGORY_NAME = "行业/概念板块"
CATEGORY_ID = "sector"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def run(dry_run: bool = False, concurrency: int = 1) -> None:
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

    try:
        ind_df = await asyncio.to_thread(ak.stock_board_industry_name_em)
        con_df = await asyncio.to_thread(ak.stock_board_concept_name_em)
    except Exception as e:
        logger.exception("拉取板块列表失败: %s", e)
        write_failed_list(CATEGORY_ID, ["api_error"], logger)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        return

    sectors_to_fetch: list[tuple[str, str, str]] = []
    if ind_df is not None and not ind_df.empty:
        for _, r in ind_df.iterrows():
            if r.get("板块代码") and r.get("板块名称"):
                sectors_to_fetch.append((
                    "industry",
                    str(r.get("板块代码", "")),
                    str(r.get("板块名称", "")),
                ))
    if con_df is not None and not con_df.empty:
        for _, r in con_df.iterrows():
            if r.get("板块代码") and r.get("板块名称"):
                sectors_to_fetch.append((
                    "concept",
                    str(r.get("板块代码", "")),
                    str(r.get("板块名称", "")),
                ))

    if not sectors_to_fetch:
        logger.warning("无板块数据")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 1, category_id=CATEGORY_ID)
        return

    sector_count = len(sectors_to_fetch)
    if dry_run:
        logger.info("dry_run: 将写入 %d 个板块及成分股，跳过落库", sector_count)
        log_stage_end(logger, CATEGORY_NAME, sector_count, 0, 0, category_id=CATEGORY_ID)
        return

    from sqlalchemy import text
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    from src.core.db import get_session
    from src.models.market_sync import StockSector, StockSectorMember

    _dsn = os.environ.get("MYSQL_DSN", "")
    use_mysql_upsert = "sqlite" not in _dsn.lower()
    now = datetime.utcnow()

    # 1. UPSERT 板块列表
    saved_sectors = 0
    async for session in get_session():
        if use_mysql_upsert:
            if ind_df is not None and not ind_df.empty:
                rows = [
                    {
                        "sector_type": "industry",
                        "sector_code": str(r.get("板块代码", "")),
                        "sector_name": str(r.get("板块名称", "")),
                        "updated_at": now,
                    }
                    for _, r in ind_df.iterrows() if r.get("板块代码") and r.get("板块名称")
                ]
                if rows:
                    stmt = mysql_insert(StockSector).values(rows)
                    stmt = stmt.on_duplicate_key_update(
                        sector_name=stmt.inserted.sector_name,
                        updated_at=stmt.inserted.updated_at,
                    )
                    await session.execute(stmt)
                    saved_sectors += len(rows)
            if con_df is not None and not con_df.empty:
                rows = [
                    {
                        "sector_type": "concept",
                        "sector_code": str(r.get("板块代码", "")),
                        "sector_name": str(r.get("板块名称", "")),
                        "updated_at": now,
                    }
                    for _, r in con_df.iterrows() if r.get("板块代码") and r.get("板块名称")
                ]
                if rows:
                    stmt = mysql_insert(StockSector).values(rows)
                    stmt = stmt.on_duplicate_key_update(
                        sector_name=stmt.inserted.sector_name,
                        updated_at=stmt.inserted.updated_at,
                    )
                    await session.execute(stmt)
                    saved_sectors += len(rows)
        else:
            if ind_df is not None and not ind_df.empty:
                for _, row in ind_df.iterrows():
                    code = str(row.get("板块代码", ""))
                    name = str(row.get("板块名称", ""))
                    if code and name:
                        session.add(StockSector(sector_type="industry", sector_code=code, sector_name=name))
                        saved_sectors += 1
            if con_df is not None and not con_df.empty:
                for _, row in con_df.iterrows():
                    code = str(row.get("板块代码", ""))
                    name = str(row.get("板块名称", ""))
                    if code and name:
                        session.add(StockSector(sector_type="concept", sector_code=code, sector_name=name))
                        saved_sectors += 1
        await session.commit()
        break

    # 2. 按板块拉取成分股并写入
    async def pull_one(stype: str, code: str, name: str) -> int:
        try:
            if stype == "industry":
                df_cons = await asyncio.to_thread(ak.stock_board_industry_cons_em, symbol=name)
            else:
                df_cons = await asyncio.to_thread(ak.stock_board_concept_cons_em, symbol=name)
        except Exception as e:
            logger.warning("板块 %s %s 成分股拉取失败: %s", stype, name, e)
            return 0
        if df_cons is None or df_cons.empty:
            return 0
        written = 0
        async for session in get_session():
            await session.execute(
                text("DELETE FROM stock_sector_members WHERE sector_code = :code"),
                {"code": code},
            )
            if use_mysql_upsert:
                batch = []
                for _, row in df_cons.iterrows():
                    sym = str(row.get("代码", "")).strip()
                    if not sym:
                        continue
                    sym_name = row.get("名称")
                    sym_name = (str(sym_name)[:64] if sym_name is not None and str(sym_name) != "nan" else None)
                    batch.append({
                        "sector_code": code,
                        "symbol": sym,
                        "symbol_name": sym_name,
                        "updated_at": now,
                    })
                if batch:
                    stmt = mysql_insert(StockSectorMember).values(batch)
                    stmt = stmt.on_duplicate_key_update(
                        symbol_name=stmt.inserted.symbol_name,
                        updated_at=stmt.inserted.updated_at,
                    )
                    await session.execute(stmt)
                    written = len(batch)
            else:
                for _, row in df_cons.iterrows():
                    sym = str(row.get("代码", "")).strip()
                    if not sym:
                        continue
                    sym_name = row.get("名称")
                    sym_name = (str(sym_name)[:64] if sym_name is not None and str(sym_name) != "nan" else None)
                    session.add(StockSectorMember(sector_code=code, symbol=sym, symbol_name=sym_name))
                    written += 1
            await session.commit()
            break
        return written

    sem = asyncio.Semaphore(concurrency)
    member_count = 0
    failed_sectors: list[str] = []

    async def pull_with_sem(stype: str, code: str, name: str) -> int:
        async with sem:
            return await pull_one(stype, code, name)

    for i in range(0, len(sectors_to_fetch), concurrency):
        batch = sectors_to_fetch[i : i + concurrency]
        results = await asyncio.gather(
            *[pull_with_sem(stype, code, name) for stype, code, name in batch],
            return_exceptions=True,
        )
        for (stype, code, name), r in zip(batch, results):
            if isinstance(r, Exception):
                logger.warning("板块 %s %s 失败: %s", stype, name, r)
                failed_sectors.append(f"{stype}:{code}:{name}")
                continue
            member_count += r
        if (i + len(batch)) % 50 == 0 or i + len(batch) == len(sectors_to_fetch):
            logger.info("成分股进度: %d/%d 板块, 已写入 %d 条", i + len(batch), len(sectors_to_fetch), member_count)

    if failed_sectors:
        write_failed_list(CATEGORY_ID, failed_sectors, logger)

    await set_watermark(CATEGORY_ID, today, "")
    total = saved_sectors + member_count
    logger.info("行业/概念板块同步完成: 板块 %d, 成分股 %d", saved_sectors, member_count)
    log_stage_end(
        logger,
        CATEGORY_NAME,
        total,
        len(failed_sectors),
        0,
        category_id=CATEGORY_ID,
    )


def main():
    parser = argparse.ArgumentParser(description="行业/概念板块增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument("-j", "--concurrency", type=int, default=1, help="拉取成分股时的并发数，默认 1")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency))


if __name__ == "__main__":
    main()
