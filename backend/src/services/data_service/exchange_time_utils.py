"""公共模块：A 股交易时间/交易日判断。"""

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# 默认交易时段（Phase 1 可配置，从 config_entries 读取 market_trading_*）
_DEFAULT_START_AM = (9, 15)
_DEFAULT_END_AM = (11, 30)
_DEFAULT_START_PM = (13, 0)
_DEFAULT_END_PM = (15, 0)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def _parse_hm(s: str, default: tuple[int, int]) -> tuple[int, int]:
    """解析 HH:MM 或 HH:MM:SS 为 (h, m)。"""
    if not s or not isinstance(s, str):
        return default
    s = s.strip()
    parts = s.split(":")
    if len(parts) >= 2:
        try:
            return (int(parts[0]), int(parts[1]))
        except (ValueError, TypeError):
            pass
    return default


async def _get_trading_hours() -> tuple[tuple[int, int], tuple[int, int], tuple[int, int], tuple[int, int]]:
    """从 config 读取交易时段，默认 09:15 11:30 13:00 15:00。"""
    try:
        from src.services.config_center_service import get_config
        cfg_keys = (
            "market_trading_start_am", "market_trading_end_am",
            "market_trading_start_pm", "market_trading_end_pm",
        )
        values = []
        for key in cfg_keys:
            r = await get_config("public", "default", key)
            v = (r.get("value") if isinstance(r, dict) else r) or ""
            values.append(str(v).strip() if v else "")
        return (
            _parse_hm(values[0], _DEFAULT_START_AM),
            _parse_hm(values[1], _DEFAULT_END_AM),
            _parse_hm(values[2], _DEFAULT_START_PM),
            _parse_hm(values[3], _DEFAULT_END_PM),
        )
    except Exception:
        return (_DEFAULT_START_AM, _DEFAULT_END_AM, _DEFAULT_START_PM, _DEFAULT_END_PM)


def _is_weekday_beijing() -> bool:
    """北京时区今日是否为工作日（周一至周五）。用于表空时的回退。"""
    now = datetime.now(BEIJING_TZ)
    return now.weekday() < 5  # 0=Mon .. 4=Fri, 5=Sat 6=Sun


async def is_trading_day() -> bool:
    """今日是否为 A 股交易日。依赖 exchange_trading_dates 表；表空时回退为按周末判断，并打日志。"""
    try:
        from sqlalchemy import func, select
        from src.core.db import get_session
        from src.models.market_sync import ExchangeTradingDate

        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        async for session in get_session():
            stmt = select(ExchangeTradingDate.trade_date).where(
                ExchangeTradingDate.trade_date == today
            ).limit(1)
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            if row is not None:
                return True
            # 表可能为空：检查表是否有任何数据
            count_stmt = select(func.count()).select_from(ExchangeTradingDate)
            cnt = await session.execute(count_stmt)
            total = cnt.scalar() or 0
            if total == 0:
                logger.warning(
                    "exchange_trading_dates 表为空，请同步交易日历；is_trading_day 回退为按周末判断"
                )
            break
    except Exception as e:
        logger.warning("exchange_time_utils: is_trading_day query failed: %s", e)
    return _is_weekday_beijing()


def _time_in_range(now: datetime, start_hm: tuple[int, int], end_hm: tuple[int, int]) -> bool:
    """当前时刻是否在 [start_hm, end_hm] 区间内（闭区间）。"""
    h, m = now.hour, now.minute
    s_h, s_m = start_hm
    e_h, e_m = end_hm
    start_min = s_h * 60 + s_m
    end_min = e_h * 60 + e_m
    cur_min = h * 60 + m
    return start_min <= cur_min <= end_min


async def is_trading_time() -> bool:
    """当前是否处于 A 股交易时段（含集合竞价 9:15-9:25）。时段从 config market_trading_* 读取。"""
    if not await is_trading_day():
        return False
    start_am, end_am, start_pm, end_pm = await _get_trading_hours()
    now = datetime.now(BEIJING_TZ)
    return (
        _time_in_range(now, start_am, end_am)
        or _time_in_range(now, start_pm, end_pm)
    )
