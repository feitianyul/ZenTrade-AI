#!/usr/bin/env python3
"""
独立脚本：互动易/上证e互动问答同步（深市 stock_irm_cninfo + 沪市 stock_sns_sseinfo），写入 stock_irm_qa。
- 周六、周日：全量拉取（沪深两市有互动数据的标的）
- 周一至周五：仅拉取库中已有记录的标的（增量）
- 不落库：问题与回答均为空的记录
用法：cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_irm_latest.py [--dry-run] [-j N]
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
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
CATEGORY_NAME = "互动易/上证e互动"
CATEGORY_ID = "irm_qa"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


def _is_shenzhen(code: str) -> bool:
    c = str(code).strip()
    return len(c) >= 6 and (c.startswith("00") or c.startswith("30"))


def _is_shanghai(code: str) -> bool:
    c = str(code).strip()
    return len(c) >= 6 and (c.startswith("60") or c.startswith("68"))


async def _get_full_irm_symbols() -> list[str]:
    """全量：深市(00/30) + 沪市(60/68) 标的。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_data import StockInfo
        async for session in get_session():
            result = await session.execute(select(StockInfo.code))
            codes = [str(r[0]) for r in result.fetchall()]
            return [c for c in codes if _is_shenzhen(c) or _is_shanghai(c)]
    except Exception:
        pass
    return []


async def _get_existing_irm_symbols() -> list[str]:
    """增量：库中 stock_irm_qa 已有记录的 symbol 去重。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import StockIrmQa
        async for session in get_session():
            result = await session.execute(select(StockIrmQa.symbol).distinct())
            return [str(r[0]) for r in result.fetchall()]
    except Exception:
        pass
    return []


def _is_weekend() -> bool:
    """周六=5、周日=6。"""
    return __import__("datetime").datetime.now().weekday() in (5, 6)


def _row_to_rec(row, code: str) -> dict | None:
    """深市 stock_irm_cninfo 行 -> 统一记录。问题与回答都为空则不落库（返回 None）。"""
    try:
        symbol = str(row.get("股票代码", code)).strip() or code
        question_id = str(row.get("问题编号", "")).strip() or str(row.get("问答ID", "")).strip()
        if not question_id:
            return None
        q_raw = row.get("问题") or row.get("问题内容") or row.get("内容")
        question_content = (str(q_raw)[:4000] if q_raw is not None else "") or ""
        a_raw = row.get("回答") or row.get("回答内容") or row.get("问答内容")
        answer_content = (str(a_raw)[:4000] if a_raw is not None else "") or ""
        if not question_content and a_raw is None:
            content_legacy = row.get("内容") or row.get("问答内容")
            question_content = (str(content_legacy)[:4000] if content_legacy is not None else "") or ""
        ask_time = str(row.get("提问时间", ""))[:32]
        answer_time = str(row.get("回答时间", ""))[:32]
        source = str(row.get("来源", ""))[:128]
        if not question_content.strip() and not answer_content.strip():
            return None
        return {
            "symbol": symbol,
            "question_id": question_id,
            "question_content": question_content,
            "answer_content": answer_content,
            "ask_time": ask_time,
            "answer_time": answer_time,
            "source": source,
        }
    except Exception:
        return None


def _row_sse_to_rec(row, code: str, idx: int) -> dict | None:
    """沪市 stock_sns_sseinfo 行 -> 统一记录。问题与回答都为空则不落库。"""
    try:
        symbol = str(row.get("股票代码", code)).strip() or code
        q_raw = row.get("问题")
        question_content = (str(q_raw)[:4000] if q_raw is not None else "") or ""
        a_raw = row.get("回答")
        answer_content = (str(a_raw)[:4000] if a_raw is not None else "") or ""
        if not question_content.strip() and not answer_content.strip():
            return None
        ask_time = str(row.get("问题时间", ""))[:32]
        answer_time = str(row.get("回答时间", ""))[:32]
        source = str(row.get("问题来源", "") or row.get("回答来源", ""))[:128]
        raw_id = f"{symbol}|{ask_time}|{question_content[:200]}"
        question_id = hashlib.md5(raw_id.encode("utf-8")).hexdigest()[:24]
        return {
            "symbol": symbol,
            "question_id": question_id,
            "question_content": question_content,
            "answer_content": answer_content,
            "ask_time": ask_time,
            "answer_time": answer_time,
            "source": source,
        }
    except Exception:
        return None


async def run(dry_run: bool = False, concurrency: int = 2) -> None:
    today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
    wm = await get_watermark(CATEGORY_ID, "")
    is_full = _is_weekend()
    if is_full:
        symbols = await _get_full_irm_symbols()
        log_stage_start(logger, CATEGORY_NAME, expected_date=today, watermark=wm, dry_run=dry_run)
        logger.info("周末全量：沪深两市标的共 %d 只", len(symbols))
    else:
        symbols = await _get_existing_irm_symbols()
        log_stage_start(logger, CATEGORY_NAME, expected_date=today, watermark=wm, dry_run=dry_run)
        logger.info("工作日增量：已有互动记录的标的共 %d 只", len(symbols))
    if not symbols:
        if is_full:
            logger.warning("数据库无沪深股票列表，请先同步 A股列表")
        else:
            logger.info("库中尚无互动记录，请先于周末运行全量")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    try:
        import akshare as ak
    except ImportError as e:
        logger.error("akshare 未安装: %s", e)
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["import_error"], logger)
        return

    fn_irm = getattr(ak, "stock_irm_cninfo", None)
    fn_sse = getattr(ak, "stock_sns_sseinfo", None)
    if fn_irm is None:
        logger.error("akshare 无 stock_irm_cninfo")
        log_stage_end(logger, CATEGORY_NAME, 0, 1, 0, category_id=CATEGORY_ID)
        write_failed_list(CATEGORY_ID, ["no_api"], logger)
        return

    sem = asyncio.Semaphore(concurrency)
    failed_list: list[str] = []
    success_count = 0

    async def pull_one(code: str) -> tuple[str, list[dict] | None, str | None]:
        async with sem:
            try:
                if _is_shenzhen(code):
                    sig = __import__("inspect").signature(fn_irm)
                    params = list(sig.parameters.keys())
                    if "symbol" in params:
                        df = await asyncio.to_thread(fn_irm, symbol=code)
                    elif "stock" in params:
                        df = await asyncio.to_thread(fn_irm, stock=code)
                    else:
                        df = await asyncio.to_thread(fn_irm, code)
                    if df is None or df.empty:
                        return (code, [], None)
                    rows = []
                    for _, row in df.iterrows():
                        rec = _row_to_rec(row, code)
                        if rec:
                            rows.append(rec)
                    return (code, rows, None)
                elif _is_shanghai(code) and fn_sse is not None:
                    df = await asyncio.to_thread(fn_sse, symbol=code)
                    if df is None or df.empty:
                        return (code, [], None)
                    rows = []
                    for idx, (_, row) in enumerate(df.iterrows()):
                        rec = _row_sse_to_rec(row, code, idx)
                        if rec:
                            rows.append(rec)
                    return (code, rows, None)
                else:
                    return (code, [], None)
            except Exception as e:
                return (code, None, str(e))

    batch_size = 30
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        results = await asyncio.gather(*[pull_one(c) for c in batch], return_exceptions=True)
        for j, r in enumerate(results):
            code = batch[j]
            if isinstance(r, Exception):
                failed_list.append(code)
                continue
            _, rows, err = r
            if err:
                failed_list.append(code)
                continue
            if not rows:
                continue
            success_count += 1
            if dry_run:
                continue
            try:
                from sqlalchemy import select
                from src.core.db import get_session
                from src.models.market_sync import StockIrmQa
                async for session in get_session():
                    for rec in rows:
                        stmt = select(StockIrmQa).where(
                            StockIrmQa.symbol == rec["symbol"],
                            StockIrmQa.question_id == rec["question_id"],
                        )
                        res = await session.execute(stmt)
                        existing = res.scalar_one_or_none()
                        if existing:
                            existing.question_content = rec.get("question_content") or ""
                            existing.answer_content = rec.get("answer_content") or ""
                            existing.ask_time = rec["ask_time"]
                            existing.answer_time = rec["answer_time"]
                            existing.source = rec["source"]
                        else:
                            session.add(StockIrmQa(
                                symbol=rec["symbol"],
                                question_id=rec["question_id"],
                                question_content=rec.get("question_content") or "",
                                answer_content=rec.get("answer_content") or "",
                                ask_time=rec["ask_time"],
                                answer_time=rec["answer_time"],
                                source=rec["source"],
                            ))
                    await session.commit()
                    break
            except Exception as exc:
                logger.warning("irm write %s: %s", code, exc)
                failed_list.append(code)
        if (i + len(batch)) % 100 == 0 or i + len(batch) == len(symbols):
            logger.info("已处理 %d/%d 成功 %d 失败 %d", min(i + len(batch), len(symbols)), len(symbols), success_count, len(failed_list))

    if not dry_run:
        await set_watermark(CATEGORY_ID, today, "")
    write_failed_list(CATEGORY_ID, failed_list, logger)
    log_stage_end(logger, CATEGORY_NAME, success_count, len(failed_list), 0, category_id=CATEGORY_ID)


def main() -> None:
    parser = argparse.ArgumentParser(description="互动易/上证e互动同步（周末全量、工作日增量）")
    parser.add_argument("--dry-run", action="store_true", help="仅拉取不落库")
    parser.add_argument("-j", "--jobs", type=int, default=2, help="并发数")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.jobs))


if __name__ == "__main__":
    main()
