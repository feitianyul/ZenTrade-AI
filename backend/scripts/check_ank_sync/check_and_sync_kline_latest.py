#!/usr/bin/env python3
"""
独立脚本：检查云端 ClickHouse 日 K 是否均为「最新」，落后则拉取到最新并同步 MySQL。

规则：
- 若今天为开市日且当前时间 >= 15:30（北京时间），则「最新」= 今天；
- 否则「最新」= 最近一个开市日（如 3 月 2 日 9:35 → 最新为 2 月 27 日）。

用法（需能连到 ClickHouse / MySQL，使用 backend/.env 或 ENV_FILE）：
  仅检查并列出落后股票（不拉取、不写库）：
    cd backend && PYTHONPATH=. python scripts/check_and_sync_kline_latest.py --dry-run
  检查并拉取落后股票到最新，再同步到 MySQL：
    cd backend && PYTHONPATH=. python scripts/check_and_sync_kline_latest.py
  指定并发数（默认 5）：
    python scripts/check_and_sync_kline_latest.py -j 10
  K 线数据源顺序：腾讯主东财备（默认）或东财主腾讯备：
    python scripts/check_and_sync_kline_latest.py --source tx
    python scripts/check_and_sync_kline_latest.py --source em
  云端示例（在 /opt/trading/backend）：
    PYTHONPATH=. .venv/bin/python scripts/check_and_sync_kline_latest.py --dry-run
    PYTHONPATH=. .venv/bin/python scripts/check_and_sync_kline_latest.py
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# 脚本在 check_ank_sync/ 子目录下，backend 根目录为 parent.parent.parent（计划 4/7）
_backend_dir = Path(__file__).resolve().parent.parent.parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

# 加载 backend .env
_env_file = Path(os.getenv("ENV_FILE", _backend_dir / ".env"))
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=True)
    except Exception:
        pass

# 复用公共工具（交易日、watermark、日志规范 3b）
from scripts.check_ank_sync.sync_script_utils import (
    get_expected_latest_date,
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

PERIOD = "daily"
ADJUST = ""
CATEGORY_NAME = "K线行情"
CATEGORY_ID = "kline"

_log_path = setup_script_log_file(logger, CATEGORY_ID)
if _log_path:
    logger.info("日志文件: %s", _log_path)


async def get_ch_symbol_max_dates() -> list[tuple[str, str]]:
    """查询 ClickHouse 中每只股票日 K 的最大 trade_date。返回 [(symbol, yyyymmdd), ...]。"""
    try:
        from src.core.clickhouse import execute_clickhouse
    except Exception as e:
        logger.error("ClickHouse 不可用: %s", e)
        return []

    sql = (
        "SELECT symbol, max(trade_date) AS max_date FROM market_kline "
        "WHERE period = 'daily' GROUP BY symbol"
    )
    try:
        rows = await execute_clickhouse(sql)
        out = []
        for r in rows:
            sym = (r.get("symbol") or "").strip()
            md = r.get("max_date")
            if not sym:
                continue
            if hasattr(md, "strftime"):
                md = md.strftime("%Y-%m-%d").replace("-", "")
            else:
                md = str(md).replace("-", "")[:8]
            out.append((sym, md))
        return out
    except Exception as e:
        logger.error("查询 ClickHouse 失败: %s", e)
        return []


async def get_mysql_symbol_max_dates() -> list[tuple[str, str]]:
    """查询 MySQL market_kline 中每只股票日 K 的最大 trade_date。返回 [(symbol, yyyymmdd), ...]。"""
    try:
        from sqlalchemy import text
        from src.core.db import get_session

        async for session in get_session():
            result = await session.execute(
                text(
                    "SELECT symbol, MAX(trade_date) AS max_date FROM market_kline "
                    "WHERE period = :period GROUP BY symbol"
                ),
                {"period": PERIOD},
            )
            rows = result.fetchall()
            out = []
            for r in rows:
                sym = (r[0] or "").strip()
                md = r[1]
                if not sym:
                    continue
                md_str = str(md).replace("-", "")[:8] if md else ""
                out.append((sym, md_str))
            return out
    except Exception as e:
        logger.error("查询 MySQL 失败: %s", e)
        return []


def _count_behind(symbol_max_dates: list[tuple[str, str]], expected_ymd: str) -> tuple[int, int, list[str]]:
    """给定 [(symbol, max_ymd), ...] 与预期最新日，返回 (总数, 落后数, 落后 symbol 列表前20)。"""
    total = len(symbol_max_dates)
    behind = [s for s, md in symbol_max_dates if md < expected_ymd]
    return total, len(behind), behind[:20]


async def verify_ch_and_mysql_latest(expected_ymd: str) -> None:
    """同步后核对：检查 ClickHouse 与 MySQL 中每只股票最后日期是否到预期最新日，并打印报告。"""
    expected_iso = f"{expected_ymd[:4]}-{expected_ymd[4:6]}-{expected_ymd[6:8]}"
    logger.info("========== 【核对】预期最新交易日: %s ==========", expected_iso)

    ch_data = await get_ch_symbol_max_dates()
    if ch_data:
        total_ch, behind_ch_n, behind_ch_list = _count_behind(ch_data, expected_ymd)
        ok_ch = total_ch - behind_ch_n
        logger.info(
            "ClickHouse: 共 %d 只，已到最新 %d 只，仍落后 %d 只",
            total_ch, ok_ch, behind_ch_n,
        )
        if behind_ch_list:
            logger.info("  ClickHouse 仍落后样例（前 %d 只）: %s", len(behind_ch_list), behind_ch_list)
    else:
        logger.warning("ClickHouse: 无数据或不可用，跳过核对")

    mysql_data = await get_mysql_symbol_max_dates()
    if mysql_data:
        total_mysql, behind_mysql_n, behind_mysql_list = _count_behind(mysql_data, expected_ymd)
        ok_mysql = total_mysql - behind_mysql_n
        logger.info(
            "MySQL:     共 %d 只，已到最新 %d 只，仍落后 %d 只",
            total_mysql, ok_mysql, behind_mysql_n,
        )
        if behind_mysql_list:
            logger.info("  MySQL 仍落后样例（前 %d 只）: %s", len(behind_mysql_list), behind_mysql_list)
    else:
        logger.warning("MySQL: 无数据或不可用，跳过核对")

    logger.info("========== 【核对】结束 ==========")


async def run_compare_ch_mysql() -> None:
    """独立核对：对比 ClickHouse 与 MySQL 的日 K 标的集合及每只的最大日期差异。"""
    logger.info("========== 【CH vs MySQL 差异核对】==========")

    ch_data = await get_ch_symbol_max_dates()
    mysql_data = await get_mysql_symbol_max_dates()

    ch_set = {s for s, _ in ch_data}
    mysql_set = {s for s, _ in mysql_data}
    ch_max = dict(ch_data)
    mysql_max = dict(mysql_data)

    only_ch = ch_set - mysql_set
    only_mysql = mysql_set - ch_set
    common = ch_set & mysql_set
    date_diff = [s for s in common if ch_max.get(s) != mysql_max.get(s)]
    date_same = [s for s in common if ch_max.get(s) == mysql_max.get(s)]

    logger.info("ClickHouse 总标的数: %d  |  MySQL 总标的数: %d", len(ch_set), len(mysql_set))
    logger.info("仅 ClickHouse 有（MySQL 无）: %d 只", len(only_ch))
    if only_ch:
        sample = sorted(only_ch)[:15]
        logger.info("  样例: %s", sample)
    logger.info("仅 MySQL 有（ClickHouse 无）: %d 只", len(only_mysql))
    if only_mysql:
        sample = sorted(only_mysql)[:15]
        logger.info("  样例: %s", sample)
    logger.info("两者均有且最大日期一致: %d 只", len(date_same))
    logger.info("两者均有但最大日期不一致: %d 只", len(date_diff))
    if date_diff:
        sample = date_diff[:10]
        lines = [f"{s} CH={ch_max.get(s)} MySQL={mysql_max.get(s)}" for s in sample]
        logger.info("  样例: %s", lines)
    logger.info("========== 【CH vs MySQL 差异核对】结束 ==========")


async def run_ch_to_mysql(concurrency: int = 5) -> None:
    """将 ClickHouse 中已有的日 K 数据同步到 MySQL（仅补 MySQL 缺失或更旧的区间）。"""
    from src.services.data_service.kline_storage import load_kline_range, _save_kline_to_mysql

    logger.info("========== 【ClickHouse → MySQL 同步】==========")
    ch_data = await get_ch_symbol_max_dates()
    mysql_data = await get_mysql_symbol_max_dates()
    if not ch_data:
        logger.warning("ClickHouse 无数据，退出")
        return

    mysql_max = dict(mysql_data)
    to_sync: list[tuple[str, str, str]] = []  # (symbol, start_ymd, end_ymd)
    for symbol, ch_max_ymd in ch_data:
        mysql_ymd = mysql_max.get(symbol) or ""
        if mysql_ymd and mysql_ymd >= ch_max_ymd:
            continue
        if mysql_ymd:
            start_d = datetime.strptime(mysql_ymd, "%Y%m%d") + timedelta(days=1)
            start_ymd = start_d.strftime("%Y%m%d")
        else:
            start_ymd = "19900101"
        to_sync.append((symbol, start_ymd, ch_max_ymd))

    if not to_sync:
        logger.info("MySQL 已与 ClickHouse 齐平，无需同步")
        return

    logger.info("待同步: %d 只（CH 有而 MySQL 无或 MySQL 更旧）", len(to_sync))
    sem = asyncio.Semaphore(concurrency)
    ok_count = 0
    err_count = 0

    async def sync_one(symbol: str, start_ymd: str, end_ymd: str) -> bool:
        async with sem:
            start_iso = f"{start_ymd[:4]}-{start_ymd[4:6]}-{start_ymd[6:8]}"
            end_iso = f"{end_ymd[:4]}-{end_ymd[4:6]}-{end_ymd[6:8]}"
            try:
                bars = await load_kline_range(symbol, PERIOD, start_iso, end_iso)
                if not bars:
                    return False
                n = await _save_kline_to_mysql(symbol, PERIOD, bars)
                return n > 0
            except Exception as e:
                logger.warning("CH→MySQL %s 失败: %s", symbol, e)
                return False

    total = len(to_sync)
    for i in range(0, total, concurrency):
        batch = to_sync[i : i + concurrency]
        tasks = [sync_one(sym, s, e) for sym, s, e in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if r is True:
                ok_count += 1
            elif isinstance(r, Exception):
                err_count += 1
                logger.warning("同步异常: %s", r)
        logger.info("已处理 %d/%d，成功写入 MySQL %d 只", min(i + concurrency, total), total, ok_count)

    logger.info("========== 【ClickHouse → MySQL 同步】结束: 成功 %d，失败/无数据 %d ==========", ok_count, total - ok_count)


def _df_to_bars(df, date_key="日期", open_key="开盘", high_key="最高", low_key="最低",
                close_key="收盘", vol_key="成交量", turn_key="成交额"):
    """DataFrame 转 bars 列表（东财列名）。"""
    if df is None or df.empty:
        return []
    bars = []
    for _, row in df.iterrows():
        bars.append({
            "date": str(row.get(date_key, ""))[:10],
            "open": float(row.get(open_key, 0)),
            "high": float(row.get(high_key, 0)),
            "low": float(row.get(low_key, 0)),
            "close": float(row.get(close_key, 0)),
            "volume": float(row.get(vol_key, 0)),
            "turnover": float(row.get(turn_key, 0)) if turn_key else 0.0,
        })
    return bars


def _symbol_for_akshare(symbol: str) -> str:
    """AKShare stock_zh_a_hist 需要 6 位代码，去掉 .SH/.SZ 等后缀。"""
    s = (symbol or "").strip()
    for suffix in (".SH", ".SZ", ".sh", ".sz"):
        if s.upper().endswith(suffix):
            s = s[: -len(suffix)].strip()
    if len(s) == 6 and s.isdigit():
        return s
    if len(s) > 6 and s[:6].isdigit():
        return s[:6]
    return s


def _symbol_to_tx(symbol: str) -> str:
    """6 位代码 → 腾讯接口所需前缀：上交所 5/6→sh，深交所 0/3→sz。"""
    s = (symbol or "").strip()
    if not s or len(s) < 6:
        return s
    if s.lower().startswith(("sh", "sz")):
        return s.lower()
    s = _symbol_for_akshare(symbol)
    if s and s[0] in "56":
        return "sh" + s
    if s and s[0] in "03":
        return "sz" + s
    return "sz" + s


def fetch_bars_tx(symbol: str, start_ymd: str, end_ymd: str) -> list[dict]:
    """同步调用腾讯 stock_zh_a_hist_tx 拉取日 K。返回 bars（turnover 填 0），失败返回空列表。"""
    tx_sym = _symbol_to_tx(symbol)
    try:
        import akshare as ak
        df = ak.stock_zh_a_hist_tx(symbol=tx_sym, start_date=start_ymd, end_date=end_ymd, adjust="")
        if df is None or df.empty:
            return []
        bars = []
        for _, row in df.iterrows():
            d = row.get("date")
            date_str = str(d).split(" ")[0].strip()[:10] if d is not None else ""
            if not date_str:
                continue
            bars.append({
                "date": date_str,
                "open": float(row.get("open", 0)),
                "high": float(row.get("high", 0)),
                "low": float(row.get("low", 0)),
                "close": float(row.get("close", 0)),
                "volume": float(row.get("amount", 0)),
                "turnover": 0.0,
            })
        return bars
    except Exception as e:
        logger.debug("腾讯 %s %s-%s: %s", tx_sym, start_ymd, end_ymd, e)
        return []


def fetch_bars_akshare(symbol: str, start_ymd: str, end_ymd: str) -> list[dict]:
    """同步调用 AKShare 拉取日 K（不复权）。返回 bars，失败返回空列表。"""
    symbol_ak = _symbol_for_akshare(symbol)
    try:
        import akshare as ak
        df = ak.stock_zh_a_hist(symbol=symbol_ak, period="daily", start_date=start_ymd, end_date=end_ymd, adjust="")
        return _df_to_bars(df)
    except Exception as e:
        logger.debug("akshare %s %s-%s: %s", symbol_ak, start_ymd, end_ymd, e)
        return []


async def fetch_bars_async(symbol: str, start_ymd: str, end_ymd: str) -> list[dict]:
    """异步包装：在线程池中调用 akshare（东财）。"""
    return await asyncio.to_thread(fetch_bars_akshare, symbol, start_ymd, end_ymd)


async def fetch_bars_tx_async(symbol: str, start_ymd: str, end_ymd: str) -> list[dict]:
    """异步包装：在线程池中调用腾讯日 K。"""
    return await asyncio.to_thread(fetch_bars_tx, symbol, start_ymd, end_ymd)


async def run(
    dry_run: bool = False,
    concurrency: int = 5,
    kline_source: str = "tx",
    verify_only: bool = False,
    compare_ch_mysql: bool = False,
    ch_to_mysql: bool = False,
) -> None:
    expected_ymd = await get_expected_latest_date()
    expected_iso = f"{expected_ymd[:4]}-{expected_ymd[4:6]}-{expected_ymd[6:8]}"
    wm = await get_watermark("kline", "")
    log_stage_start(logger, CATEGORY_NAME, expected_date=expected_iso, watermark=wm, dry_run=dry_run)
    if kline_source not in ("em", "tx"):
        kline_source = "tx"
    logger.info("K 线数据源: %s", "东财主腾讯备" if kline_source == "em" else "腾讯主东财备")

    if verify_only:
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        await verify_ch_and_mysql_latest(expected_ymd)
        return

    if compare_ch_mysql:
        await verify_ch_and_mysql_latest(expected_ymd)
        await run_compare_ch_mysql()
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    if ch_to_mysql:
        await run_ch_to_mysql(concurrency=concurrency)
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        return

    ch_data = await get_ch_symbol_max_dates()
    if not ch_data:
        logger.warning("ClickHouse 无数据或不可用，退出")
        return

    behind: list[tuple[str, str]] = []
    for symbol, max_ymd in ch_data:
        if max_ymd < expected_ymd:
            behind.append((symbol, max_ymd))

    if not behind:
        logger.info("所有股票均已到最新日期 %s，无需拉取。", expected_iso)
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        await verify_ch_and_mysql_latest(expected_ymd)
        return

    logger.info("落后于 %s 的股票共 %d 只", expected_iso, len(behind))
    for sym, max_d in behind[:30]:
        logger.info("  落后: %s 最后日期 %s", sym, f"{max_d[:4]}-{max_d[4:6]}-{max_d[6:8]}")
    if len(behind) > 30:
        logger.info("  ... 及其他 %d 只", len(behind) - 30)

    if dry_run:
        logger.info("dry_run=True，仅检查不拉取，退出")
        log_stage_end(logger, CATEGORY_NAME, 0, 0, 0, category_id=CATEGORY_ID)
        await verify_ch_and_mysql_latest(expected_ymd)
        return

    sem = asyncio.Semaphore(concurrency)
    saved_to_ch: list[tuple[str, list[dict]]] = []
    empty_bars_list: list[str] = []  # AKShare 返回空
    failed_list: list[str] = []      # 拉取异常

    tx_first = kline_source == "tx"

    async def pull_one(symbol: str, max_ymd: str) -> tuple[str, list[dict]] | None:
        async with sem:
            start_ymd = (datetime.strptime(max_ymd, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
            if start_ymd > expected_ymd:
                return None
            if tx_first:
                # 腾讯主、东财备
                bars = await fetch_bars_tx_async(symbol, start_ymd, expected_ymd)
                if not bars:
                    await asyncio.sleep(1.5)
                    bars = await fetch_bars_tx_async(symbol, start_ymd, expected_ymd)
                if not bars:
                    bars = await fetch_bars_async(symbol, start_ymd, expected_ymd)
                    if bars:
                        logger.info("腾讯无数据或失败，改用东财兜底: %s", symbol)
            else:
                # 东财主、腾讯备
                bars = await fetch_bars_async(symbol, start_ymd, expected_ymd)
                if not bars:
                    await asyncio.sleep(1.5)  # 重试前间隔，减轻限流
                    bars = await fetch_bars_async(symbol, start_ymd, expected_ymd)
                if not bars:
                    bars = await fetch_bars_tx_async(symbol, start_ymd, expected_ymd)
                    if bars:
                        logger.info("东财无数据或失败，改用腾讯兜底: %s", symbol)
            if not bars:
                empty_bars_list.append(symbol)
                return None
            from src.services.data_service.kline_storage import save_kline_to_ch
            try:
                n = await save_kline_to_ch(symbol, PERIOD, bars)
                if n > 0:
                    return (symbol, bars)
            except Exception as exc:
                failed_list.append(symbol)
                logger.warning("写入 CH 失败 %s: %s", symbol, exc)
            return None

    total = len(behind)
    for i in range(0, total, concurrency):
        batch = behind[i : i + concurrency]
        tasks = [pull_one(sym, max_d) for sym, max_d in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for j, r in enumerate(results):
            if isinstance(r, Exception):
                failed_list.append(batch[j][0])
                logger.warning("拉取异常: %s", r)
                continue
            if r is not None:
                saved_to_ch.append(r)
        logger.info("已处理 %d/%d，已写入 CH 的 %d 批，拉取为空 %d，失败 %d",
                    min(i + concurrency, total), total, len(saved_to_ch), len(empty_bars_list), len(failed_list))

    logger.info("ClickHouse 写入完成，共 %d 只股票有新数据；拉取为空 %d 只，异常/写入失败 %d 只",
                len(saved_to_ch), len(empty_bars_list), len(failed_list))

    # 3b：失败/空数据列表写入 backend/logs/check_ank_sync/kline_failed_<date>_<time>.txt
    still_failed = list(dict.fromkeys(empty_bars_list + failed_list))
    write_failed_list("kline", still_failed, logger)

    success_count = len(saved_to_ch)
    failed_count = len(failed_list)
    empty_count = len(empty_bars_list)

    # 同步到 MySQL
    if not saved_to_ch:
        logger.info("无新数据需同步 MySQL，结束")
        log_stage_end(logger, CATEGORY_NAME, success_count, failed_count, empty_count, category_id=CATEGORY_ID)
        await verify_ch_and_mysql_latest(expected_ymd)
        return

    from src.services.data_service.kline_storage import _save_kline_to_mysql
    mysql_ok = 0
    for symbol, bars in saved_to_ch:
        try:
            n = await _save_kline_to_mysql(symbol, PERIOD, bars)
            if n > 0:
                mysql_ok += 1
        except Exception as e:
            logger.warning("MySQL 写入 %s 失败: %s", symbol, e)
    logger.info("MySQL 同步完成: %d/%d 只股票已写入", mysql_ok, len(saved_to_ch))
    # 1c：成功同步后写 watermark（sub_key=""）以便前端「已同步」日期更新
    await set_watermark("kline", expected_ymd, "")
    log_stage_end(logger, CATEGORY_NAME, success_count, failed_count, empty_count, category_id=CATEGORY_ID)
    await verify_ch_and_mysql_latest(expected_ymd)


def main():
    parser = argparse.ArgumentParser(description="检查 ClickHouse 日 K 是否最新，落后则拉取并同步 MySQL")
    parser.add_argument("--dry-run", action="store_true", help="仅检查并列出落后股票，不拉取不写库")
    parser.add_argument("--verify-only", action="store_true", help="仅核对 ClickHouse/MySQL 最后日期，不拉取不写库")
    parser.add_argument("--compare-ch-mysql", action="store_true", help="核对与最新日期的差异，并对比 CH 与 MySQL 的标的/日期差异")
    parser.add_argument("--ch-to-mysql", action="store_true", help="将 ClickHouse 日 K 数据同步到 MySQL（补 MySQL 缺失或更旧区间）")
    parser.add_argument("-j", "--concurrency", type=int, default=5, help="拉取并发数，默认 5")
    parser.add_argument("--source", choices=("em", "tx"), default="tx",
                        help="K 线数据源顺序: tx=腾讯主东财备（默认）, em=东财主腾讯备")
    args = parser.parse_args()
    asyncio.run(run(
        dry_run=args.dry_run,
        concurrency=args.concurrency,
        kline_source=args.source,
        verify_only=args.verify_only,
        compare_ch_mysql=args.compare_ch_mysql,
        ch_to_mysql=args.ch_to_mysql,
    ))


if __name__ == "__main__":
    main()
    sys.exit(0)
