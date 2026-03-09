"""Phase 4 — 行情持久化模型: 股票列表 + 日K线"""

from datetime import datetime

from sqlalchemy import DateTime, Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class StockInfo(Base):
    """全量 A 股代码/名称 — L3 持久层, 避免每次冷启动 30s 查询。"""
    __tablename__ = "stock_info"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(10), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    market: Mapped[str] = mapped_column(String(10), default="A", comment="A / HK / US")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )


class MarketKline(Base):
    """日/周/月 K线持久层 — 加速历史数据二次访问。"""
    __tablename__ = "market_kline"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    trade_date: Mapped[str] = mapped_column(String(20), nullable=False)
    period: Mapped[str] = mapped_column(String(10), default="daily", comment="daily/weekly/monthly")
    open: Mapped[float] = mapped_column(Float, default=0.0)
    high: Mapped[float] = mapped_column(Float, default=0.0)
    low: Mapped[float] = mapped_column(Float, default=0.0)
    close: Mapped[float] = mapped_column(Float, default=0.0)
    volume: Mapped[float] = mapped_column(Float, default=0.0)
    turnover: Mapped[float] = mapped_column(Float, default=0.0, nullable=True)

    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", "period", name="uq_kline_symbol_date_period"),
        Index("ix_kline_symbol_period", "symbol", "period"),
    )


class MarketSpotSnapshot(Base):
    """预热行情快照 — 热门/排行拉取后写入，Redis 未命中时读库展示。按日覆盖。"""
    __tablename__ = "market_spot_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="YYYY-MM-DD")
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(64), default="")
    price: Mapped[float] = mapped_column(Float, default=0.0)
    change_pct: Mapped[float] = mapped_column(Float, default=0.0)
    change_amt: Mapped[float] = mapped_column(Float, default=0.0)
    volume: Mapped[float] = mapped_column(Float, default=0.0)
    turnover: Mapped[float] = mapped_column(Float, default=0.0)
    turnover_rate: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("snapshot_date", "symbol", name="uq_spot_snapshot_date_symbol"),
        Index("ix_spot_snapshot_date", "snapshot_date"),
    )


class MarketIndicesSnapshot(Base):
    """预热大盘指数快照 — 按日保留历史。"""
    __tablename__ = "market_indices_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="YYYY-MM-DD")
    code: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(32), default="")
    price: Mapped[float] = mapped_column(Float, default=0.0)
    change_pct: Mapped[float] = mapped_column(Float, default=0.0)
    change_amt: Mapped[float] = mapped_column(Float, default=0.0)
    volume: Mapped[float] = mapped_column(Float, default=0.0)
    turnover: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("snapshot_date", "code", name="uq_indices_snapshot_date_code"),
        Index("ix_indices_snapshot_date", "snapshot_date"),
    )


class MarketSectorsSnapshot(Base):
    """预热板块快照 — 按日保留历史。"""
    __tablename__ = "market_sectors_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="YYYY-MM-DD")
    sector_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True, comment="industry / concept")
    code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(64), default="")
    change_pct: Mapped[float] = mapped_column(Float, default=0.0)
    turnover: Mapped[float] = mapped_column(Float, default=0.0)
    leader: Mapped[str] = mapped_column(String(32), default="", nullable=True)
    leader_pct: Mapped[float] = mapped_column(Float, default=0.0, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("snapshot_date", "sector_type", "code", name="uq_sectors_snapshot_date_type_code"),
        Index("ix_sectors_snapshot_date", "snapshot_date"),
    )


class MarketMinuteSnapshot(Base):
    """分时前 N 快照 — 预热写入，读路径 L3 读库。唯一约束 (snapshot_date, symbol)，增量 upsert。"""
    __tablename__ = "market_minute_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="YYYY-MM-DD")
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    pre_close: Mapped[float] = mapped_column(Float, default=0.0)
    bars: Mapped[str] = mapped_column(Text, nullable=False, default="[]", comment="JSON array of {time,price,avg_price,volume,change_pct}")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("snapshot_date", "symbol", name="uq_minute_snapshot_date_symbol"),
        Index("ix_minute_snapshot_date", "snapshot_date"),
    )


class MarketMinuteKlineSnapshot(Base):
    """分钟 K 线快照 — 1/5/15/30/60 分钟周期，按日存储，读路径 L3 使用。唯一约束 (symbol, period_min, snapshot_date)。"""
    __tablename__ = "market_minute_kline_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="如 000630")
    period_min: Mapped[str] = mapped_column(String(6), nullable=False, index=True, comment="1/5/15/30/60")
    snapshot_date: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="YYYY-MM-DD")
    bars: Mapped[str] = mapped_column(Text, nullable=False, default="[]", comment="JSON array of K-line bars")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("symbol", "period_min", "snapshot_date", name="uq_minute_kline_symbol_period_date"),
        Index("ix_minute_kline_snapshot_date", "snapshot_date"),
    )
