#!/usr/bin/env python3
"""
独立脚本：十大股东增量同步。按最近 4 季报告期、全标的拉取东财 PageSDLTGD，仅新增/更新（不删历史）。
用法：
  cd backend && PYTHONPATH=. python scripts/check_ank_sync/check_and_sync_top_holder_latest.py [--dry-run] [-j N]
  仅拉取单只（补数/测试）：python scripts/check_ank_sync/check_and_sync_top_holder_latest.py --symbol 000630
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
# 与 debug 脚本一致：固定工作目录为 backend，避免从 scripts/check_ank_sync 运行时 DB/相对路径异常
os.chdir(_backend_dir)

from scripts.check_ank_sync.sync_script_utils import (
    set_watermark,
    last_n_semi_annual_report_dates,
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
CATEGORY_NAME = "十大股东"
CATEGORY_ID = "top_holder"
_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


def _top_holder_em_code(symbol: str) -> str:
    """6 位代码转东财十大流通股东接口 code：小写 sh/sz + 6 位。"""
    s = (symbol or "").strip()
    if not s or len(s) < 6:
        return s
    if s.upper().startswith("SH"):
        return ("sh" + (s[2:].lstrip() if len(s) > 2 else "")).lower()
    if s.upper().startswith("SZ"):
        return ("sz" + (s[2:].lstrip() if len(s) > 2 else "")).lower()
    if s[0] in "56":
        return ("sh" + s).lower()
    if s[0] in "03":
        return ("sz" + s).lower()
    return ("sz" + s).lower()


def _fetch_top_holder_em_sync(code: str, date_ymd: str) -> list:
    """同步请求东财十大流通股东 PageSDLTGD。返回与 sdltgd 元素同构的 list[dict]（英文字段）。"""
    import requests
    url = "https://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/PageSDLTGD"
    r = requests.get(url, params={"code": code, "date": date_ymd}, timeout=15)
    j = r.json()
    if not isinstance(j, dict) or "sdltgd" not in j:
        if j.get("message"):
            raise RuntimeError(j.get("message", "接口未返回 sdltgd"))
        return []
    return list(j["sdltgd"]) if j["sdltgd"] else []


def _fetch_top_holder_akshare_sync(code: str, date_ymd: str) -> list:
    """兜底：用 akshare stock_gdfx_free_top_10_em 拉取，返回与 PageSDLTGD 同构的 list[dict]（英文字段）。
    date_ymd 为 YYYY-MM-DD，内部转为 YYYYMMDD 传给 akshare。
    若 akshare 解析异常（如某报告期未披露导致列数不一致），返回 [] 不抛错，避免计为失败。
    """
    try:
        import akshare as ak
        date_compact = date_ymd.replace("-", "")
        df = ak.stock_gdfx_free_top_10_em(symbol=code, date=date_compact)
        if df is None or df.empty:
            return []
        # akshare 返回中文列名，映射为与东财接口一致的英文字段，供后续落库复用同一套解析
        col_map = {
            "名次": "HOLDER_RANK",
            "股东名称": "HOLDER_NAME",
            "股东类型": "HOLDER_TYPE",
            "持股数": "HOLD_NUM",
            "占总流通股持股比例": "FREE_HOLDNUM_RATIO",
            "增减": "HOLD_NUM_CHANGE",
            "变动比率": "CHANGE_RATIO",
        }
        out = []
        for _, row in df.iterrows():
            rec = {}
            for cn_col, en_key in col_map.items():
                if cn_col in df.columns:
                    rec[en_key] = row.get(cn_col)
            if rec:
                out.append(rec)
        return out
    except Exception:
        return []


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


async def _get_existing_symbol_report_dates(quarter_dates: list[str]) -> set[tuple[str, str]]:
    """查询 stock_top_holders 中在给定报告期里已存在的 (symbol, report_date)，用于增量只补缺失。"""
    if not quarter_dates:
        return set()
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import StockTopHolder
        async for session in get_session():
            result = await session.execute(
                select(StockTopHolder.symbol, StockTopHolder.report_date).where(
                    StockTopHolder.report_date.in_(quarter_dates)
                ).distinct()
            )
            return {(str(r[0]), str(r[1])) for r in result.fetchall()}
    except Exception:
        return set()


async def run(dry_run: bool = False, concurrency: int = 3, symbol_filter: str | None = None) -> None:
    # 报告期仅取半年报 06-30、年报 12-31；只取「报告期 <= 当前日期」的最近 4 个
    quarter_dates = last_n_semi_annual_report_dates(4)
    single_symbol = (symbol_filter or "").strip()
    if single_symbol:
        symbols = [single_symbol]
        existing_set: set[tuple[str, str]] = set()
        logger.info("单只拉取模式: %s，报告期 %s", single_symbol, quarter_dates)
    else:
        symbols = await _get_all_stock_codes()
        if not symbols:
            logger.warning("数据库无股票列表，请先同步 A股列表")
            log_stage_start(logger, CATEGORY_NAME, dry_run=dry_run)
            log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
            return
        # 增量：只补「库中已有股票 × 最近 4 季」里缺失的 (symbol, report_date)
        existing_set = await _get_existing_symbol_report_dates(quarter_dates)
        to_sync_count = sum(1 for s in symbols for d in quarter_dates if (s, d) not in existing_set)
        logger.info("增量：共 %d 只股票 × %d 个报告期，库中已有 %d 条，待补 %d 条",
                   len(symbols), len(quarter_dates), len(existing_set), to_sync_count)
        if to_sync_count == 0:
            logger.info("无缺失 (股票,报告期)，跳过")
            log_stage_start(logger, CATEGORY_NAME, dry_run=dry_run)
            log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
            return

    items: list[tuple[str, str, str]] = []
    for symbol in symbols:
        em_code = _top_holder_em_code(symbol)
        if not em_code or len(em_code) < 8:
            continue
        for date_ymd in quarter_dates:
            if (symbol, date_ymd) in existing_set:
                continue
            items.append((symbol, em_code, date_ymd))

    if not items and not single_symbol:
        log_stage_start(logger, CATEGORY_NAME, dry_run=dry_run)
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    log_stage_start(logger, CATEGORY_NAME, expected_date=quarter_dates[0] if quarter_dates else "", dry_run=dry_run)

    sem = asyncio.Semaphore(concurrency)
    failed_list: list[str] = []
    success_count = 0
    new_row_count = 0
    max_report_date = ""

    async def pull_one(symbol: str, em_code: str, date_ymd: str):
        async with sem:
            rows = []
            err = None
            try:
                rows = await asyncio.to_thread(_fetch_top_holder_em_sync, em_code, date_ymd)
            except Exception as e:
                err = e
            # 仅当直连抛错时用 akshare 兜底；直连返回空（如报告期未披露）不再调 akshare，避免 akshare 解析异常被计为失败
            if err is not None and not rows:
                rows = await asyncio.to_thread(_fetch_top_holder_akshare_sync, em_code, date_ymd)
                if rows:
                    logger.info("东财直连失败，改用 akshare 兜底: %s %s", symbol, date_ymd)
                    err = None
            if err is not None and not rows:
                return (symbol, date_ymd, [], err)
            return (symbol, date_ymd, rows, None)

    total_items = len(items)
    for i in range(0, total_items, concurrency):
        batch = items[i : i + concurrency]
        results = await asyncio.gather(*[pull_one(s, em, d) for s, em, d in batch])
        for symbol, date_ymd, rows, err in results:
            if err:
                failed_list.append(f"{symbol}_{date_ymd}")
                logger.warning("十大股东拉取失败 %s %s: %s", symbol, date_ymd, err)
                continue
            if not rows:
                if single_symbol:
                    logger.info("接口返回空（该报告期可能未披露）: %s %s", symbol, date_ymd)
                continue
            if dry_run:
                new_row_count += len(rows)
                success_count += 1
                continue
            try:
                import os
                from sqlalchemy import text
                from src.core.db import get_session
                from src.models.market_sync import StockTopHolder
                _dsn = os.environ.get("MYSQL_DSN", "")
                use_mysql_upsert = "sqlite" not in _dsn.lower()
                async for session in get_session():
                    for rec in rows:
                        rank = rec.get("HOLDER_RANK")
                        if rank is not None and isinstance(rank, (int, float)):
                            rank = int(rank)
                        hold_num = rec.get("HOLD_NUM")
                        if hold_num is not None and isinstance(hold_num, (int, float)):
                            hold_num = float(hold_num)
                        hold_ratio = rec.get("FREE_HOLDNUM_RATIO")
                        if hold_ratio is not None and isinstance(hold_ratio, (int, float)):
                            hold_ratio = float(hold_ratio)
                        change_ratio = rec.get("CHANGE_RATIO")
                        if change_ratio is not None and isinstance(change_ratio, (int, float)):
                            change_ratio = float(change_ratio)
                        else:
                            change_ratio = None
                        holder_type = str(rec.get("HOLDER_TYPE") or "top10_free")[:64]
                        holder_name = str(rec.get("HOLDER_NAME") or "")[:256]
                        change_type = str(rec.get("HOLD_NUM_CHANGE") or "")[:20] or None
                        now_utc = datetime.now(timezone.utc)
                        if use_mysql_upsert:
                            # MySQL 8.0.20+ 推荐用 AS 别名替代已弃用的 VALUES()
                            sql = text(
                                "INSERT INTO stock_top_holders (symbol, report_date, holder_type, `rank`, holder_name, hold_count, hold_ratio, change_type, change_count, change_ratio, updated_at) "
                                "VALUES (:symbol, :report_date, :holder_type, :rank, :holder_name, :hold_count, :hold_ratio, :change_type, :change_count, :change_ratio, :updated_at) AS v "
                                "ON DUPLICATE KEY UPDATE holder_type=v.holder_type, holder_name=v.holder_name, hold_count=v.hold_count, "
                                "hold_ratio=v.hold_ratio, change_type=v.change_type, change_ratio=v.change_ratio, updated_at=v.updated_at"
                            )
                            await session.execute(sql, {
                                "symbol": symbol, "report_date": date_ymd, "holder_type": holder_type, "rank": rank,
                                "holder_name": holder_name, "hold_count": hold_num, "hold_ratio": hold_ratio,
                                "change_type": change_type, "change_count": None, "change_ratio": change_ratio,
                                "updated_at": now_utc,
                            })
                        else:
                            sql = text(
                                "INSERT INTO stock_top_holders (symbol, report_date, holder_type, rank, holder_name, hold_count, hold_ratio, change_type, change_count, change_ratio, updated_at) "
                                "VALUES (:symbol, :report_date, :holder_type, :rank, :holder_name, :hold_count, :hold_ratio, :change_type, :change_count, :change_ratio, :updated_at) "
                                "ON CONFLICT(symbol, report_date, rank) DO UPDATE SET holder_type=excluded.holder_type, holder_name=excluded.holder_name, "
                                "hold_count=excluded.hold_count, hold_ratio=excluded.hold_ratio, change_type=excluded.change_type, change_ratio=excluded.change_ratio, updated_at=excluded.updated_at"
                            )
                            await session.execute(sql, {
                                "symbol": symbol, "report_date": date_ymd, "holder_type": holder_type, "rank": rank,
                                "holder_name": holder_name, "hold_count": hold_num, "hold_ratio": hold_ratio,
                                "change_type": change_type, "change_count": None, "change_ratio": change_ratio,
                                "updated_at": now_utc,
                            })
                    await session.commit()
                success_count += 1
                new_row_count += len(rows)
                if date_ymd > max_report_date:
                    max_report_date = date_ymd
            except Exception as e:
                failed_list.append(f"{symbol}_{date_ymd}")
                logger.warning("十大股东 %s %s 写入失败: %s", symbol, date_ymd, e)
        if (i + concurrency) % 200 == 0 or i + concurrency >= total_items:
            logger.info("已处理 %d/%d，成功 %d 只，写入 %d 条，失败 %d",
                        min(i + concurrency, total_items), total_items, success_count, new_row_count, len(failed_list))

    if not dry_run and new_row_count > 0 and max_report_date and not single_symbol:
        await set_watermark(CATEGORY_ID, max_report_date, "")
    if failed_list:
        write_failed_list(CATEGORY_ID, failed_list[:500], logger)
    log_stage_end(logger, CATEGORY_NAME, new_row_count, len(failed_list), 0, category_id=CATEGORY_ID)


def main():
    parser = argparse.ArgumentParser(description="十大股东增量同步")
    parser.add_argument("--dry-run", action="store_true", help="仅检查不写库")
    parser.add_argument("-j", "--concurrency", type=int, default=3, help="并发数")
    parser.add_argument("--symbol", type=str, default="", help="仅拉取该股票（6 位代码如 000630），忽略 watermark，拉最近 4 季")
    args = parser.parse_args()
    asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency, symbol_filter=args.symbol or None))


if __name__ == "__main__":
    main()
