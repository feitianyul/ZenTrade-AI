#!/usr/bin/env python3
"""
公共工具：供 check_ank_sync 目录下各增量同步脚本复用。

- 交易日与收市时间：北京时间 15:30 cutoff。拉取「哪一天」或判断「是否已到最新」请用 get_expected_latest_date()；get_last_trading_date_str() 仅按日历表取日，不区分 15:30。
- watermark 读/写（与 data_sync_service 语义一致）
- _safe_float / _safe_int
- 统一日志辅助：阶段头尾、写失败列表到 backend/logs/check_ank_sync/
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

# 北京时间与收市 cutoff（与 K 线脚本、计划 1b 一致）
BEIJING = timezone(timedelta(hours=8))
CLOSE_CUTOFF_HOUR = 15
CLOSE_CUTOFF_MINUTE = 30


def _last_trading_date_str() -> str:
    """按星期回退得到最近交易日 YYYYMMDD（周末用周五）。表空或异常时回退用。"""
    d = datetime.now()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


async def get_expected_latest_date() -> str:
    """
    返回「预期最新交易日」YYYYMMDD。
    若今天为交易日且当前北京时间 >= 15:30，则返回今天；否则返回最近一个交易日。
    """
    now_bj = datetime.now(BEIJING)
    today_iso = now_bj.strftime("%Y-%m-%d")
    today_ymd = now_bj.strftime("%Y%m%d")
    after_cutoff = (now_bj.hour > CLOSE_CUTOFF_HOUR) or (
        now_bj.hour == CLOSE_CUTOFF_HOUR and now_bj.minute >= CLOSE_CUTOFF_MINUTE
    )

    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import ExchangeTradingDate

        async for session in get_session():
            stmt = (
                select(ExchangeTradingDate.trade_date)
                .where(ExchangeTradingDate.trade_date <= today_iso)
                .order_by(ExchangeTradingDate.trade_date.desc())
                .limit(3)
            )
            result = await session.execute(stmt)
            rows = [str(r[0]) for r in result.fetchall()]
            if not rows:
                break
            last_td = rows[0].replace("-", "")
            today_is_trading = (rows[0] == today_iso) or (last_td == today_ymd)
            if today_is_trading and after_cutoff:
                return today_ymd
            # 未到 15:30 时，当日 K 线未收，预期最新日应为「上一交易日」
            if today_is_trading and not after_cutoff and len(rows) >= 2:
                return rows[1].replace("-", "")
            return last_td
    except Exception:
        pass

    d = now_bj.date()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


async def get_trading_dates_between(start_iso: str, end_iso: str) -> list[str]:
    """
    从 exchange_trading_dates 查询 [start_iso, end_iso] 内的交易日，返回 YYYYMMDD 列表，新日期在前。
    start_iso / end_iso 格式 YYYY-MM-DD。
    """
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import ExchangeTradingDate

        async for session in get_session():
            stmt = (
                select(ExchangeTradingDate.trade_date)
                .where(ExchangeTradingDate.trade_date >= start_iso)
                .where(ExchangeTradingDate.trade_date <= end_iso)
                .order_by(ExchangeTradingDate.trade_date.desc())
            )
            result = await session.execute(stmt)
            rows = [str(r[0]) for r in result.fetchall()]
            return [d.replace("-", "") for d in rows]
    except Exception:
        pass
    return []


async def get_trading_dates_last_n_months(n: int = 3) -> list[str]:
    """
    过去 n 个自然月的交易日列表（YYYYMMDD），新日期在前。
    使用 exchange_trading_dates；若表空则回退为按星期生成的近似列表。
    """
    end_d = datetime.now().date()
    start_d = end_d - timedelta(days=max(n * 31, 1))
    start_iso = start_d.strftime("%Y-%m-%d")
    end_iso = end_d.strftime("%Y-%m-%d")
    dates = await get_trading_dates_between(start_iso, end_iso)
    if dates:
        return dates
    # 回退：按工作日生成近似
    out = []
    d = end_d
    for _ in range(n * 31):
        if d.weekday() < 5:
            out.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
        if d < start_d:
            break
    return out[: n * 22]


async def get_last_trading_date_str(include_today: bool = True) -> str:
    """
    从表 exchange_trading_dates 查询「最近交易日」YYYYMMDD。
    include_today=True 取 trade_date<=当前日期的最后一条；
    include_today=False 取倒数第二条（上一交易日）。
    注意：不区分 15:30，未到收市也会返回「今天」。若需「已收市的最新日」（如拉取日频数据），请用 get_expected_latest_date()。
    表空或异常时回退到按星期回退。
    """
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import ExchangeTradingDate

        today = datetime.now().strftime("%Y-%m-%d")
        async for session in get_session():
            stmt = (
                select(ExchangeTradingDate.trade_date)
                .where(ExchangeTradingDate.trade_date <= today)
                .order_by(ExchangeTradingDate.trade_date.desc())
                .limit(2)
            )
            result = await session.execute(stmt)
            rows = [r[0] for r in result.fetchall()]
            if not rows:
                break
            if include_today and len(rows) >= 1:
                return str(rows[0]).replace("-", "")
            if not include_today and len(rows) >= 2:
                return str(rows[1]).replace("-", "")
            if rows:
                return str(rows[0]).replace("-", "")
            break
    except Exception:
        pass
    return _last_trading_date_str()


async def get_watermark(category: str, sub_key: str = "") -> Optional[str]:
    """获取某分类最后同步日期（与 data_sync_service._get_watermark 语义一致）。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_sync import DataSyncWatermark

        async for session in get_session():
            stmt = select(DataSyncWatermark).where(
                DataSyncWatermark.category == category,
                DataSyncWatermark.sub_key == sub_key,
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            return row.last_sync_date if row else None
    except Exception:
        return None


async def get_watermark_fallback_news() -> Optional[str]:
    """当 news 无 watermark 时，从 stock_news 表取 max(publish_time) 前 10 位。"""
    try:
        from sqlalchemy import select, func
        from src.core.db import get_session
        from src.models.market_sync import StockNews

        async for session in get_session():
            stmt = select(func.max(StockNews.publish_time)).where(StockNews.publish_time != "")
            result = await session.execute(stmt)
            val = result.scalar()
            if val and str(val).strip():
                s = str(val).strip()
                return s[:10] if len(s) >= 10 else s
            break
    except Exception:
        pass
    return None


async def set_watermark(category: str, date_str: str, sub_key: str = "") -> None:
    """更新某分类最后同步日期（与 data_sync_service._set_watermark 语义一致）。"""
    import os
    try:
        from sqlalchemy import text
        from src.core.db import get_session

        _dsn = os.environ.get("MYSQL_DSN", "")
        async for session in get_session():
            if "sqlite" in _dsn.lower():
                sql = text(
                    "INSERT OR REPLACE INTO data_sync_watermarks (category, sub_key, last_sync_date, last_sync_at) "
                    "VALUES (:cat, :sk, :d, :now)"
                )
            else:
                sql = text(
                    "INSERT INTO data_sync_watermarks (category, sub_key, last_sync_date, last_sync_at) "
                    "VALUES (:cat, :sk, :d, :now) AS w "
                    "ON DUPLICATE KEY UPDATE last_sync_date=w.last_sync_date, last_sync_at=w.last_sync_at"
                )
            await session.execute(
                sql,
                {"cat": category, "sk": sub_key, "d": date_str, "now": datetime.utcnow()},
            )
            await session.commit()
    except Exception as exc:
        logging.getLogger(__name__).warning("Set watermark failed: %s", exc)


def _safe_float(row: Any, col: str) -> Optional[float]:
    """安全提取 float（与 data_sync_service._safe_float 一致）。"""
    try:
        v = row.get(col)
        if v is None or str(v).strip() in ("", "nan", "NaN", "--", "-"):
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


def latest_dividend_report_date() -> str:
    """最近分红报告期 XXXX0630 或 XXXX1231，供 stock_fhps_em(date=) 使用。"""
    now = datetime.now()
    y, m = now.year, now.month
    if m >= 7:
        return f"{y}0630"
    return f"{y - 1}1231"


def previous_dividend_report_date(current: str) -> str:
    """上一分红报告期。current 为 XXXX0630 或 XXXX1231。"""
    y = int(current[:4])
    if current.endswith("0630"):
        return f"{y - 1}1231"
    return f"{y}0630"


def latest_quarter_end_date() -> str:
    """最近季末日期 XXXX0331/0630/0930/1231，供 stock_hold_num_cninfo(date=) 使用。"""
    now = datetime.now()
    y, m = now.year, now.month
    if m >= 10:
        return f"{y}0930"
    if m >= 7:
        return f"{y}0630"
    if m >= 4:
        return f"{y}0331"
    return f"{y - 1}0930"


def last_n_quarter_end_dates(n: int = 4) -> list[str]:
    """最近 n 个日历季末 YYYY-MM-DD（03-31、06-30、09-30、12-31）。"""
    now = datetime.now()
    y, m = now.year, now.month
    out = []
    for _ in range(n):
        if m >= 10:
            out.append(f"{y}-09-30")
            m, y = 6, y
        elif m >= 7:
            out.append(f"{y}-06-30")
            m, y = 3, y
        elif m >= 4:
            out.append(f"{y}-03-31")
            m, y = 12, y - 1
        else:
            out.append(f"{y - 1}-12-31")
            m, y = 9, y - 1
    return out


def last_n_semi_annual_report_dates(n: int = 4) -> list[str]:
    """最近 n 个半年报/年报报告期（仅 06-30 与 12-31）YYYY-MM-DD。
    用于十大股东等接口：报告期一般为每年 6 月 30 日（半年报）、12 月 31 日（年报）。
    只返回「报告期截止日 <= 当前日期」的报告期，且按时间倒序（最近的在前）。
    例如 2026-03-06 调用返回：['2025-12-31', '2025-06-30', '2024-12-31', '2024-06-30']。
    """
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    candidates: list[str] = []
    for y in range(now.year, now.year - 4, -1):
        candidates.append(f"{y}-12-31")
        candidates.append(f"{y}-06-30")
    candidates.sort(reverse=True)
    valid = [d for d in candidates if d <= today_str]
    return valid[:n]


def _safe_int(row: Any, col: str) -> Optional[int]:
    """安全提取 int（与 data_sync_service._safe_int 一致）。"""
    try:
        v = row.get(col)
        if v is None or str(v).strip() in ("", "nan", "NaN", "--", "-"):
            return None
        return int(float(v))
    except (ValueError, TypeError):
        return None


def symbol_to_em(symbol: str) -> str:
    """将 6 位代码转为东财同行比较接口格式: 0/3→SZ，5/6→SH；若已带 sh/sz 前缀则统一为大写 SH/SZ。"""
    s = (symbol or "").strip()
    if not s or len(s) < 6:
        return s
    if s.upper().startswith("SH"):
        return "SH" + (s[2:].lstrip() if len(s) > 2 else "")
    if s.upper().startswith("SZ"):
        return "SZ" + (s[2:].lstrip() if len(s) > 2 else "")
    if s[0] in "56":
        return "SH" + s
    if s[0] in "03":
        return "SZ" + s
    return "SZ" + s


def get_check_ank_sync_log_dir() -> Path:
    """backend/logs/check_ank_sync/ 目录（相对本文件所在 backend 根目录）。"""
    # 本文件: backend/scripts/check_ank_sync/sync_script_utils.py -> parent.parent.parent = backend
    backend_root = Path(__file__).resolve().parent.parent.parent
    log_dir = backend_root / "logs" / "check_ank_sync"
    return log_dir


def setup_script_log_file(logger: logging.Logger, category_id: str) -> Optional[Path]:
    """
    为当前运行创建 backend/logs/check_ank_sync/<category_id>_<YYYYMMDD>_<HHmmss>.log，
    并为 logger 添加 FileHandler，使后续日志同时输出到该文件与控制台。
    返回日志文件路径；若创建失败返回 None。
    """
    try:
        log_dir = get_check_ank_sync_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = log_dir / f"{category_id}_{suffix}.log"
        fh = logging.FileHandler(path, mode="a", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(fh)
        return path
    except Exception:
        return None


def write_run_result(category_id: str, success: int, failed: int, empty: int) -> Optional[Path]:
    """
    将本次运行结果写入 backend/logs/check_ank_sync/<category_id>_last_result.json，
    供主控 run_incremental_sync_scripts.py 汇总生成 run_summary。
    """
    try:
        import json
        log_dir = get_check_ank_sync_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        path = log_dir / f"{category_id}_last_result.json"
        data = {
            "category": category_id,
            "success": success,
            "failed": failed,
            "empty": empty,
            "ts": datetime.now().isoformat(),
        }
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        return path
    except Exception:
        return None


def log_stage_start(
    logger: logging.Logger,
    category_name: str,
    expected_date: str = "",
    watermark: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """输出阶段开始行（3b 规范）。"""
    logger.info("========== 【%s】开始 ==========", category_name)
    if expected_date:
        logger.info("预期最新日: %s", expected_date)
    logger.info("watermark: %s", watermark if watermark is not None else "无")
    if dry_run:
        logger.info("dry_run: True，不写库")


def log_stage_end(
    logger: logging.Logger,
    category_name: str,
    success: int,
    failed: int,
    empty: int,
    category_id: str = "",
) -> None:
    """输出阶段结束行（3b 规范）。若提供 category_id 则同时写入 last_result.json 供主控汇总。"""
    logger.info(
        "========== 【%s】结束: 成功 %d, 失败 %d, 空数据 %d ==========",
        category_name,
        success,
        failed,
        empty,
    )
    if category_id:
        write_run_result(category_id, success, failed, empty)


def write_failed_list(
    category: str,
    failed_items: list[str],
    logger: logging.Logger,
) -> Optional[Path]:
    """
    将失败/空数据列表写入 backend/logs/check_ank_sync/<category>_failed_<YYYYMMDD>_<HHmmss>.txt。
    若目录不存在则创建。返回写入的文件路径；若 failed_items 为空则不写文件并返回 None。
    """
    if not failed_items:
        return None
    log_dir = get_check_ank_sync_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    suffix = now.strftime("%Y%m%d_%H%M%S")
    path = log_dir / f"{category}_failed_{suffix}.txt"
    path.write_text("\n".join(failed_items), encoding="utf-8")
    logger.info("失败/空数据列表已写入: %s (共 %d 条)", path, len(failed_items))
    return path
