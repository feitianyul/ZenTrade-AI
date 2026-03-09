#!/usr/bin/env python3
"""
独立脚本：资讯/公告增量同步。按全标的拉取 stock_news_em，过滤 publish_time > watermark 写入 stock_news。
watermark 无时从 stock_news 表取 max(publish_time) 前 10 位作为 fallback。
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_news_latest.py [--dry-run] [-j N]
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
    get_watermark_fallback_news,
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
CATEGORY_NAME = "资讯/公告"
CATEGORY_ID = "news"
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
    if wm is None:
        wm = await get_watermark_fallback_news()
    today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
    log_stage_start(logger, CATEGORY_NAME, expected_date=today, watermark=wm, dry_run=dry_run)

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
    max_publish_date: str | None = None

    async def pull_one(code: str) -> tuple[str, list[dict] | None, str | None]:
        async with sem:
            try:
                df = await asyncio.to_thread(ak.stock_news_em, symbol=code)
                if df is None or df.empty:
                    return (code, None, None)
                rows = []
                for _, row in df.head(30).iterrows():
                    title = str(row.get("新闻标题", ""))[:512]
                    content = str(row.get("新闻内容", ""))[:2000] if row.get("新闻内容") else None
                    publish_time = str(row.get("发布时间", ""))[:32]
                    source = str(row.get("文章来源", ""))[:128]
                    url = str(row.get("新闻链接", ""))[:512]
                    if not url:
                        continue
                    if wm and publish_time:
                        pt_date = publish_time[:10] if len(publish_time) >= 10 else publish_time
                        if pt_date <= wm:
                            continue
                    rows.append({
                        "symbol": code,
                        "title": title or "",
                        "content": content,
                        "publish_time": publish_time or "",
                        "source": source,
                        "url": url or "",
                    })
                if not rows:
                    return (code, None, None)
                pt_max = max((r["publish_time"][:10] for r in rows if len(r.get("publish_time", "")) >= 10), default=None)
                return (code, rows, pt_max)
            except Exception as e:
                return (code, None, str(e))

    batch_size = 50
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        results = await asyncio.gather(*[pull_one(c) for c in batch], return_exceptions=True)
        for j, r in enumerate(results):
            code = batch[j]
            if isinstance(r, Exception):
                failed_list.append(code)
                continue
            _, rows, pt_max = r
            if rows is None:
                continue
            if pt_max and (max_publish_date is None or pt_max > max_publish_date):
                max_publish_date = pt_max
            if dry_run:
                success_count += 1
                continue
            try:
                from sqlalchemy import select
                from src.core.db import get_session
                from src.models.market_sync import StockNews
                async for session in get_session():
                    for rec in rows:
                        stmt = select(StockNews).where(StockNews.symbol == code, StockNews.url == rec["url"])
                        res = await session.execute(stmt)
                        existing = res.scalar_one_or_none()
                        if existing:
                            existing.title = rec["title"]
                            existing.content = rec["content"]
                            existing.publish_time = rec["publish_time"]
                            existing.source = rec["source"]
                        else:
                            session.add(StockNews(**rec))
                    await session.commit()
                    success_count += 1
                    break
            except Exception as exc:
                logger.warning("news write %s: %s", code, exc)
                failed_list.append(code)
        if (i + len(batch)) % 200 == 0 or i + len(batch) == len(symbols):
            logger.info("已处理 %d/%d 成功 %d 失败 %d", min(i + len(batch), len(symbols)), len(symbols), success_count, len(failed_list))

    if not dry_run:
        await set_watermark(CATEGORY_ID, max_publish_date or today, "")
    write_failed_list(CATEGORY_ID, failed_list, logger)
    log_stage_end(logger, CATEGORY_NAME, success_count, len(failed_list), 0, category_id=CATEGORY_ID)


def main() -> None:
    parser = argparse.ArgumentParser(description="资讯/公告增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅拉取不落库")
    parser.add_argument("-j", "--jobs", type=int, default=3, help="并发数")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.jobs))


if __name__ == "__main__":
    main()
