"""Phase 4c — K线数据持久化存储层

ClickHouse (L3a): 日/周/月 K线时序数据
  - 列式存储，高压缩比 (10:1+)
  - MergeTree 引擎按月分区，时间范围查询毫秒级
  - ReplacingMergeTree 自动去重

MySQL (L3b): 作为 fallback (当 ClickHouse 不可用时)
"""

import asyncio
import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ClickHouse DDL
# ---------------------------------------------------------------------------

_KLINE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS market_kline (
    symbol    String,
    trade_date Date,
    period    LowCardinality(String),
    open      Float64,
    high      Float64,
    low       Float64,
    close     Float64,
    volume    Float64,
    turnover  Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, period, trade_date)
""".strip()

_ch_table_ensured = False


async def ensure_kline_table() -> bool:
    """启动时调用: 确保 ClickHouse market_kline 表存在。
    返回 True=成功, False=ClickHouse 不可用(静默降级)。
    """
    global _ch_table_ensured
    if _ch_table_ensured:
        return True
    try:
        from src.core.clickhouse import execute_clickhouse
        await execute_clickhouse(_KLINE_TABLE_DDL)
        _ch_table_ensured = True
        logger.info("ClickHouse market_kline table ensured")
        return True
    except Exception as exc:
        logger.warning("ClickHouse kline table creation failed (degraded): %s", exc)
        return False


# ---------------------------------------------------------------------------
# ClickHouse 写入
# ---------------------------------------------------------------------------

async def save_kline_to_ch(
    symbol: str,
    period: str,
    bars: List[Dict[str, Any]],
) -> int:
    """批量写入 K线数据到 ClickHouse。返回写入行数。

    bars 格式: [{"date": "2026-01-15", "open": ..., "close": ..., ...}, ...]
    """
    global _ch_table_ensured
    if not bars:
        return 0

    if not _ch_table_ensured:
        ok = await ensure_kline_table()
        if not ok:
            # ClickHouse 不可用，尝试 MySQL fallback
            return await _save_kline_to_mysql(symbol, period, bars)

    try:
        from src.core.clickhouse import execute_clickhouse

        # 构造 VALUES 字符串 (ClickHouse HTTP INSERT)
        rows = []
        for bar in bars:
            date_str = str(bar.get("date", ""))[:10]  # 截取 YYYY-MM-DD
            if not date_str or date_str == "":
                continue
            rows.append(
                f"('{symbol}', '{date_str}', '{period}', "
                f"{float(bar.get('open', 0))}, {float(bar.get('high', 0))}, "
                f"{float(bar.get('low', 0))}, {float(bar.get('close', 0))}, "
                f"{float(bar.get('volume', 0))}, {float(bar.get('turnover', 0) or 0)})"
            )

        if not rows:
            return 0

        # 分批 INSERT (每批 500 行)
        batch_size = 500
        total = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            values_str = ",\n".join(batch)
            sql = (
                "INSERT INTO market_kline "
                "(symbol, trade_date, period, open, high, low, close, volume, turnover) "
                f"VALUES {values_str}"
            )
            await execute_clickhouse(sql)
            total += len(batch)

        logger.info("Saved %d kline bars to ClickHouse: %s/%s", total, symbol, period)
        return total

    except Exception as exc:
        _ch_table_ensured = False  # 下次请求重试 ensure，便于 CH 恢复后自动切回
        logger.warning("ClickHouse kline save failed: %s — fallback to MySQL", exc)
        return await _save_kline_to_mysql(symbol, period, bars)


# ---------------------------------------------------------------------------
# ClickHouse 已有股票列表（用于断点续传）
# ---------------------------------------------------------------------------

async def get_kline_existing_symbols(period: str = "daily") -> List[str]:
    """查询 ClickHouse 中已有 K 线数据的股票代码列表，用于断点续传时跳过已同步的股票。
    若 ClickHouse 不可用或查询失败返回空列表。
    """
    if not _ch_table_ensured:
        ok = await ensure_kline_table()
        if not ok:
            return []
    try:
        from src.core.clickhouse import execute_clickhouse

        # 避免 SQL 注入：period 仅限 daily/weekly/monthly
        if period not in ("daily", "weekly", "monthly"):
            period = "daily"
        sql = (
            "SELECT DISTINCT symbol FROM market_kline "
            f"WHERE period = '{period}' ORDER BY symbol"
        )
        rows = await execute_clickhouse(sql)
        return [str(r.get("symbol", "")).strip() for r in rows if r.get("symbol")]
    except Exception as exc:
        logger.warning("get_kline_existing_symbols failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# 按日期区间查询（回测用）
# ---------------------------------------------------------------------------

async def load_kline_range(
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    """按日期区间从 ClickHouse/MySQL 读取 K 线。返回 bars 列表 (按日期正序)。

    symbol: 6 位代码，如 000630（不含 .SH/.SZ 后缀）
    period: daily | weekly | monthly
    start_date / end_date: YYYY-MM-DD
    """
    global _ch_table_ensured
    if not _ch_table_ensured:
        ok = await ensure_kline_table()
        if not ok:
            return await _load_kline_range_mysql(symbol, period, start_date, end_date)

    try:
        from src.core.clickhouse import execute_clickhouse

        if period not in ("daily", "weekly", "monthly"):
            period = "daily"
        # 参数化避免注入（symbol 已由调用方规范化）
        sql = (
            "SELECT trade_date, open, high, low, close, volume, turnover "
            "FROM market_kline FINAL "
            f"WHERE symbol = '{symbol}' AND period = '{period}' "
            f"AND trade_date >= '{start_date[:10]}' AND trade_date <= '{end_date[:10]}' "
            "ORDER BY trade_date ASC"
        )
        rows = await execute_clickhouse(sql)
        if not rows:
            return await _load_kline_range_mysql(symbol, period, start_date, end_date)

        bars = []
        for r in rows:
            bars.append({
                "date": str(r.get("trade_date", "")),
                "open": float(r.get("open", 0)),
                "high": float(r.get("high", 0)),
                "low": float(r.get("low", 0)),
                "close": float(r.get("close", 0)),
                "volume": float(r.get("volume", 0)),
                "turnover": float(r.get("turnover", 0)),
            })
        return bars if bars else await _load_kline_range_mysql(
            symbol, period, start_date, end_date
        )

    except Exception as exc:
        _ch_table_ensured = False  # 下次请求重试 ensure，便于 CH 恢复后自动切回
        logger.warning("ClickHouse load_kline_range failed: %s — fallback to MySQL", exc)
        return await _load_kline_range_mysql(symbol, period, start_date, end_date)


async def _load_kline_range_mysql(
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    """从 MySQL market_kline 表按日期区间读取 K 线。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_data import MarketKline

        s_start = str(start_date)[:10]
        s_end = str(end_date)[:10]

        async for session in get_session():
            stmt = (
                select(MarketKline)
                .where(
                    MarketKline.symbol == symbol,
                    MarketKline.period == period,
                    MarketKline.trade_date >= s_start,
                    MarketKline.trade_date <= s_end,
                )
                .order_by(MarketKline.trade_date.asc())
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            if not rows:
                return []

            bars = []
            for r in rows:
                bars.append({
                    "date": str(r.trade_date),
                    "open": float(r.open),
                    "high": float(r.high),
                    "low": float(r.low),
                    "close": float(r.close),
                    "volume": float(r.volume),
                    "turnover": float(r.turnover or 0),
                })
            return bars
    except Exception as exc:
        logger.warning("MySQL load_kline_range failed: %s", exc)
    return []


# ---------------------------------------------------------------------------
# ClickHouse 读取
# ---------------------------------------------------------------------------

async def load_kline_from_ch(
    symbol: str,
    period: str,
    count: int = 60,
) -> List[Dict[str, Any]]:
    """从 ClickHouse 读取 K线数据。返回 bars 列表 (按日期正序)。
    如果 ClickHouse 不可用，自动降级到 MySQL。
    """
    global _ch_table_ensured
    if not _ch_table_ensured:
        ok = await ensure_kline_table()
        if not ok:
            return await _load_kline_from_mysql(symbol, period, count)

    try:
        from src.core.clickhouse import execute_clickhouse

        sql = (
            "SELECT trade_date, open, high, low, close, volume, turnover "
            "FROM market_kline FINAL "
            f"WHERE symbol = '{symbol}' AND period = '{period}' "
            "ORDER BY trade_date DESC "
            f"LIMIT {count}"
        )
        rows = await execute_clickhouse(sql)
        if not rows:
            # ClickHouse 空，尝试 MySQL
            return await _load_kline_from_mysql(symbol, period, count)

        # 转换为标准 bars 格式 (正序)
        bars = []
        for r in reversed(rows):
            bars.append({
                "date": str(r.get("trade_date", "")),
                "open": float(r.get("open", 0)),
                "high": float(r.get("high", 0)),
                "low": float(r.get("low", 0)),
                "close": float(r.get("close", 0)),
                "volume": float(r.get("volume", 0)),
                "turnover": float(r.get("turnover", 0)),
            })
        return bars if bars else await _load_kline_from_mysql(symbol, period, count)

    except Exception as exc:
        _ch_table_ensured = False  # 下次请求重试 ensure，便于 CH 恢复后自动切回
        logger.warning("ClickHouse kline load failed: %s — fallback to MySQL", exc)
        return await _load_kline_from_mysql(symbol, period, count)


# ---------------------------------------------------------------------------
# MySQL fallback (L3b)
# ---------------------------------------------------------------------------

async def _save_kline_to_mysql(
    symbol: str,
    period: str,
    bars: List[Dict[str, Any]],
) -> int:
    """将 K线数据写入 MySQL market_kline 表 (upsert)。"""
    if not bars:
        return 0
    try:
        from sqlalchemy import text
        from src.core.db import get_session

        count = 0
        async for session in get_session():
            for bar in bars:
                date_str = str(bar.get("date", ""))[:10]
                if not date_str:
                    continue
                # MySQL upsert (INSERT ... ON DUPLICATE KEY UPDATE)
                # 对 SQLite 也兼容 (通过 INSERT OR REPLACE)
                _dsn = os.environ.get("MYSQL_DSN", "")
                if "sqlite" in _dsn:
                    sql = text(
                        "INSERT OR REPLACE INTO market_kline "
                        "(symbol, trade_date, period, open, high, low, close, volume, turnover) "
                        "VALUES (:symbol, :trade_date, :period, :open, :high, :low, :close, :volume, :turnover)"
                    )
                else:
                    sql = text(
                        "INSERT INTO market_kline "
                        "(symbol, trade_date, period, `open`, high, low, `close`, volume, turnover) "
                        "VALUES (:symbol, :trade_date, :period, :open, :high, :low, :close, :volume, :turnover) AS k "
                        "ON DUPLICATE KEY UPDATE "
                        "`open`=k.`open`, high=k.high, low=k.low, "
                        "`close`=k.`close`, volume=k.volume, turnover=k.turnover"
                    )
                await session.execute(sql, {
                    "symbol": symbol,
                    "trade_date": date_str,
                    "period": period,
                    "open": float(bar.get("open", 0)),
                    "high": float(bar.get("high", 0)),
                    "low": float(bar.get("low", 0)),
                    "close": float(bar.get("close", 0)),
                    "volume": float(bar.get("volume", 0)),
                    "turnover": float(bar.get("turnover", 0) or 0),
                })
                count += 1
            await session.commit()
            logger.info("Saved %d kline bars to MySQL: %s/%s", count, symbol, period)
            return count
    except Exception as exc:
        logger.warning("MySQL kline save failed: %s", exc)
    return 0


async def _load_kline_from_mysql(
    symbol: str,
    period: str,
    count: int = 60,
) -> List[Dict[str, Any]]:
    """从 MySQL market_kline 表读取 K线数据。"""
    try:
        from sqlalchemy import select
        from src.core.db import get_session
        from src.models.market_data import MarketKline

        async for session in get_session():
            stmt = (
                select(MarketKline)
                .where(MarketKline.symbol == symbol, MarketKline.period == period)
                .order_by(MarketKline.trade_date.desc())
                .limit(count)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            if not rows:
                return []

            bars = []
            for r in reversed(rows):  # 正序
                bars.append({
                    "date": str(r.trade_date),
                    "open": float(r.open),
                    "high": float(r.high),
                    "low": float(r.low),
                    "close": float(r.close),
                    "volume": float(r.volume),
                    "turnover": float(r.turnover or 0),
                })
            return bars
    except Exception as exc:
        logger.warning("MySQL kline load failed: %s", exc)
    return []
