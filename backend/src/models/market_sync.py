"""市场数据同步模型 — 对齐 AKShare 官方数据分类

参考: https://akshare.akfamily.xyz/data/stock/stock.html

数据分层:
  - ClickHouse (L3a): 时序数据 (K线、资金流向日线)
  - MySQL (L3b): 业务维度数据 (财务、股东、分红、板块等)
  - Redis (L2): 实时快照缓存
"""

from datetime import date, datetime

from sqlalchemy import (
    BigInteger, Boolean, Date, DateTime, Float, Index, Integer, String, Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


# ─────────────────────────────────────────────────────────────────
# 1. 数据同步任务记录
# ─────────────────────────────────────────────────────────────────

class DataSyncTask(Base):
    """数据拉取任务 — 跟踪每次同步的进度与结果"""
    __tablename__ = "data_sync_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category: Mapped[str] = mapped_column(String(50), nullable=False, index=True,
                                          comment="数据分类: stock_list/kline/financial/...")
    sync_type: Mapped[str] = mapped_column(String(20), default="full",
                                           comment="full=全量, incremental=增量")
    status: Mapped[str] = mapped_column(String(20), default="pending",
                                        comment="pending/running/success/failed/cancelled")
    total_count: Mapped[int] = mapped_column(Integer, default=0, comment="总记录数")
    success_count: Mapped[int] = mapped_column(Integer, default=0, comment="成功数")
    error_count: Mapped[int] = mapped_column(Integer, default=0, comment="失败数")
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True, comment="错误详情")
    failed_symbols: Mapped[str | None] = mapped_column(Text, nullable=True, comment="K线同步失败代码列表 JSON 如 [\"600519\",\"000001\"]")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True,
                                                        comment="最后活动时间，用于长时间未更新(stale)判定")

    __table_args__ = (
        Index("ix_sync_task_cat_status", "category", "status"),
    )


class DataSyncTaskLog(Base):
    """数据同步任务日志 — 结构化分级日志，供前端分页展示与运维排查"""
    __tablename__ = "data_sync_task_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True, comment="关联 data_sync_tasks.id")
    level: Mapped[str] = mapped_column(String(20), nullable=False, default="INFO", comment="INFO/WARNING/ERROR")
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_sync_task_log_task_created", "task_id", "created_at"),
    )


# ─────────────────────────────────────────────────────────────────
# 2. 增量同步水位线 — 记录每个分类最后同步时间
# ─────────────────────────────────────────────────────────────────

class DataSyncWatermark(Base):
    """增量同步水位线"""
    __tablename__ = "data_sync_watermarks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category: Mapped[str] = mapped_column(String(50), nullable=False, comment="数据分类")
    sub_key: Mapped[str] = mapped_column(String(50), default="", comment="子分类: symbol/板块代码等")
    last_sync_date: Mapped[str] = mapped_column(String(20), nullable=False, comment="最后同步日期 YYYY-MM-DD")
    last_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("category", "sub_key", name="uq_watermark_cat_key"),
    )


# ─────────────────────────────────────────────────────────────────
# 2b. 交易所交易日历 — 供「最近交易日」及开市/休市展示
# ─────────────────────────────────────────────────────────────────

class ExchangeTradingDate(Base):
    """交易所交易日历 — 来源: ak.tool_trade_date_hist_sina()"""
    __tablename__ = "exchange_trading_dates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_date: Mapped[str] = mapped_column(String(10), nullable=False, unique=True, index=True,
                                            comment="交易日 YYYY-MM-DD")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("trade_date", name="uq_exchange_trading_date"),
    )


# ─────────────────────────────────────────────────────────────────
# 3. 财务数据 — 财务指标 + 财务摘要
# ─────────────────────────────────────────────────────────────────

class StockFinancial(Base):
    """个股财务指标 — 来源: stock_financial_analysis_indicator"""
    __tablename__ = "stock_financial"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    report_date: Mapped[str] = mapped_column(String(20), nullable=False, comment="报告期")
    # 盈利能力
    roe: Mapped[float | None] = mapped_column(Float, nullable=True, comment="净资产收益率(%)")
    roa: Mapped[float | None] = mapped_column(Float, nullable=True, comment="总资产收益率(%)")
    gross_margin: Mapped[float | None] = mapped_column(Float, nullable=True, comment="毛利率(%)")
    net_margin: Mapped[float | None] = mapped_column(Float, nullable=True, comment="净利率(%)")
    # 成长能力
    revenue_yoy: Mapped[float | None] = mapped_column(Float, nullable=True, comment="营收同比(%)")
    profit_yoy: Mapped[float | None] = mapped_column(Float, nullable=True, comment="净利润同比(%)")
    # 偿债能力
    debt_ratio: Mapped[float | None] = mapped_column(Float, nullable=True, comment="资产负债率(%)")
    current_ratio: Mapped[float | None] = mapped_column(Float, nullable=True, comment="流动比率")
    # 每股指标
    eps: Mapped[float | None] = mapped_column(Float, nullable=True, comment="每股收益")
    bps: Mapped[float | None] = mapped_column(Float, nullable=True, comment="每股净资产")
    # 原始JSON
    raw_data: Mapped[str | None] = mapped_column(Text, nullable=True, comment="完整原始数据JSON")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "report_date", name="uq_financial_symbol_date"),
        Index("ix_financial_symbol", "symbol"),
    )


# ─────────────────────────────────────────────────────────────────
# 4. 融资融券 — Margin Trading
# ─────────────────────────────────────────────────────────────────

class StockMarginTrading(Base):
    """融资融券数据"""
    __tablename__ = "stock_margin_trading"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    trade_date: Mapped[str] = mapped_column(String(20), nullable=False)
    # 融资
    rz_balance: Mapped[float | None] = mapped_column(Float, nullable=True, comment="融资余额(元)")
    rz_buy: Mapped[float | None] = mapped_column(Float, nullable=True, comment="融资买入额(元)")
    rz_repay: Mapped[float | None] = mapped_column(Float, nullable=True, comment="融资偿还额(元)")
    # 融券
    rq_balance: Mapped[float | None] = mapped_column(Float, nullable=True, comment="融券余额(元)")
    rq_sell: Mapped[float | None] = mapped_column(Float, nullable=True, comment="融券卖出量(股)")
    rq_repay: Mapped[float | None] = mapped_column(Float, nullable=True, comment="融券偿还量(股)")
    # 合计
    rz_rq_balance: Mapped[float | None] = mapped_column(Float, nullable=True, comment="融资融券余额(元)")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", name="uq_margin_symbol_date"),
    )


# ─────────────────────────────────────────────────────────────────
# 5. 大宗交易
# ─────────────────────────────────────────────────────────────────

class StockBlockTrade(Base):
    """大宗交易数据"""
    __tablename__ = "stock_block_trade"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    trade_date: Mapped[str] = mapped_column(String(20), nullable=False)
    price: Mapped[float | None] = mapped_column(Float, nullable=True, comment="成交价")
    volume: Mapped[float | None] = mapped_column(Float, nullable=True, comment="成交量(股)")
    turnover: Mapped[float | None] = mapped_column(Float, nullable=True, comment="成交额(元)")
    buyer: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="买方营业部")
    seller: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="卖方营业部")
    premium: Mapped[float | None] = mapped_column(Float, nullable=True, comment="溢折价率(%)")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_block_trade_date", "trade_date"),
    )


# ─────────────────────────────────────────────────────────────────
# 6. 资金流向
# ─────────────────────────────────────────────────────────────────

class StockCapitalFlow(Base):
    """个股资金流向"""
    __tablename__ = "stock_capital_flow"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    trade_date: Mapped[str] = mapped_column(String(20), nullable=False)
    main_net_inflow: Mapped[float | None] = mapped_column(Float, nullable=True, comment="主力净流入(元)")
    small_net_inflow: Mapped[float | None] = mapped_column(Float, nullable=True, comment="小单净流入(元)")
    medium_net_inflow: Mapped[float | None] = mapped_column(Float, nullable=True, comment="中单净流入(元)")
    large_net_inflow: Mapped[float | None] = mapped_column(Float, nullable=True, comment="大单净流入(元)")
    super_large_net_inflow: Mapped[float | None] = mapped_column(Float, nullable=True, comment="超大单净流入(元)")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", name="uq_capital_flow_symbol_date"),
    )


# ─────────────────────────────────────────────────────────────────
# 7. 十大股东 / 十大流通股东
# ─────────────────────────────────────────────────────────────────

class StockTopHolder(Base):
    """十大流通股东 — 与东财接口 PageSDLTGD 返回字段一致。"""
    __tablename__ = "stock_top_holders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    report_date: Mapped[str] = mapped_column(String(20), nullable=False, comment="报告期 END_DATE")
    holder_type: Mapped[str] = mapped_column(String(64), default="top10_free",
                                             comment="股东性质 HOLDER_TYPE 如 保险公司")
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="名次 HOLDER_RANK")
    holder_name: Mapped[str | None] = mapped_column(String(256), nullable=True, comment="股东名称 HOLDER_NAME")
    hold_count: Mapped[float | None] = mapped_column(Float, nullable=True, comment="持股数(股) HOLD_NUM")
    hold_ratio: Mapped[float | None] = mapped_column(Float, nullable=True, comment="占总流通股本比例(%) FREE_HOLDNUM_RATIO")
    change_type: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="增减 HOLD_NUM_CHANGE 如 新进/增加/减少/不变")
    change_count: Mapped[float | None] = mapped_column(Float, nullable=True, comment="变动数量(股)，接口无则空")
    change_ratio: Mapped[float | None] = mapped_column(Float, nullable=True, comment="变动比率 CHANGE_RATIO")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "report_date", "rank", name="uq_top_holder_symbol_date_rank"),
        Index("ix_holder_symbol_date", "symbol", "report_date"),
    )


# ─────────────────────────────────────────────────────────────────
# 8. 分红配股
# ─────────────────────────────────────────────────────────────────

class StockDividend(Base):
    """分红配股历史"""
    __tablename__ = "stock_dividends"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    report_date: Mapped[str] = mapped_column(String(20), nullable=False, comment="报告期/公告日")
    ex_date: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="除权除息日")
    record_date: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="股权登记日")
    # 方案
    bonus_ratio: Mapped[float | None] = mapped_column(Float, nullable=True, comment="送股(每10股)")
    convert_ratio: Mapped[float | None] = mapped_column(Float, nullable=True, comment="转增(每10股)")
    dividend_per_share: Mapped[float | None] = mapped_column(Float, nullable=True, comment="派息(元/每10股)")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_dividend_symbol", "symbol"),
    )


# ─────────────────────────────────────────────────────────────────
# 9. 行业/概念板块成分
# ─────────────────────────────────────────────────────────────────

class StockSector(Base):
    """行业/概念板块"""
    __tablename__ = "stock_sectors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sector_type: Mapped[str] = mapped_column(String(20), nullable=False,
                                             comment="industry=行业, concept=概念, area=地域")
    sector_code: Mapped[str] = mapped_column(String(20), nullable=False)
    sector_name: Mapped[str] = mapped_column(String(64), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("sector_type", "sector_code", name="uq_sector_type_code"),
    )


class StockSectorMember(Base):
    """板块成分股"""
    __tablename__ = "stock_sector_members"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sector_code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    symbol_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("sector_code", "symbol", name="uq_sector_member"),
    )


# ─────────────────────────────────────────────────────────────────
# 10. 龙虎榜
# ─────────────────────────────────────────────────────────────────

class StockLHB(Base):
    """龙虎榜数据"""
    __tablename__ = "stock_lhb"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    symbol_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    trade_date: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    reason: Mapped[str | None] = mapped_column(String(256), nullable=True, comment="上榜原因")
    close_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    change_pct: Mapped[float | None] = mapped_column(Float, nullable=True, comment="涨跌幅(%)")
    net_buy: Mapped[float | None] = mapped_column(Float, nullable=True, comment="龙虎榜净买入(元)")
    buy_amount: Mapped[float | None] = mapped_column(Float, nullable=True, comment="买入总额(元)")
    sell_amount: Mapped[float | None] = mapped_column(Float, nullable=True, comment="卖出总额(元)")
    turnover: Mapped[float | None] = mapped_column(Float, nullable=True, comment="当日成交额(元)")
    raw_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_lhb_date_symbol", "trade_date", "symbol"),
    )


# ─────────────────────────────────────────────────────────────────
# 11. 北向资金 / 南向资金
# ─────────────────────────────────────────────────────────────────

class NorthboundFlow(Base):
    """北向资金 / 南向资金 日度数据"""
    __tablename__ = "northbound_flow"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_date: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    direction: Mapped[str] = mapped_column(String(10), default="north",
                                           comment="north=北向, south=南向")
    sh_net_buy: Mapped[float | None] = mapped_column(Float, nullable=True, comment="沪股通净买入(亿)")
    sz_net_buy: Mapped[float | None] = mapped_column(Float, nullable=True, comment="深股通净买入(亿)")
    total_net_buy: Mapped[float | None] = mapped_column(Float, nullable=True, comment="合计净买入(亿)")
    sh_buy: Mapped[float | None] = mapped_column(Float, nullable=True, comment="沪股通买入(亿)")
    sz_buy: Mapped[float | None] = mapped_column(Float, nullable=True, comment="深股通买入(亿)")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("trade_date", "direction", name="uq_northbound_date_dir"),
    )


# ─────────────────────────────────────────────────────────────────
# 11b. 北向持股排行（个股截面：谁被买、买多少）
# ─────────────────────────────────────────────────────────────────

class NorthboundHoldStock(Base):
    """北向资金持股排行 — 个股截面，供页面读库+Redis 加速"""
    __tablename__ = "northbound_hold_stock"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_date: Mapped[str] = mapped_column(String(20), nullable=False, index=True, comment="快照日期 YYYY-MM-DD")
    market: Mapped[str] = mapped_column(String(20), nullable=False, index=True, comment="北向/沪股通/深股通")
    indicator: Mapped[str] = mapped_column(String(20), nullable=False, index=True, comment="今日排行/5日排行/10日排行")
    code: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    close: Mapped[float | None] = mapped_column(Float, nullable=True, comment="今日收盘价")
    change_pct: Mapped[float | None] = mapped_column(Float, nullable=True, comment="今日涨跌幅(%)")
    hold_shares: Mapped[float | None] = mapped_column(Float, nullable=True, comment="持股股数")
    hold_value: Mapped[float | None] = mapped_column(Float, nullable=True, comment="持股市值")
    float_ratio: Mapped[float | None] = mapped_column(Float, nullable=True, comment="持股数量占A股百分比")
    increase_shares: Mapped[float | None] = mapped_column(Float, nullable=True, comment="增持股数")
    increase_value: Mapped[float | None] = mapped_column(Float, nullable=True, comment="增持市值")
    sector: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="所属板块")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("trade_date", "market", "indicator", "code", name="uq_nb_hold_date_mkt_ind_code"),
        Index("ix_nb_hold_mkt_ind", "market", "indicator"),
    )


# ─────────────────────────────────────────────────────────────────
# 12. 涨跌停统计
# ─────────────────────────────────────────────────────────────────

class StockLimitUpDown(Base):
    """涨跌停数据"""
    __tablename__ = "stock_limit_updown"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    symbol_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    trade_date: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    limit_type: Mapped[str] = mapped_column(String(10), default="up", comment="up=涨停, down=跌停")
    close_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    change_pct: Mapped[float | None] = mapped_column(Float, nullable=True, comment="涨跌幅(%)")
    first_limit_time: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="首次封板时间")
    last_limit_time: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="最后封板时间")
    open_count: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="打开次数")
    continuous_days: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="连板天数")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", "limit_type", name="uq_limit_updown_symbol_date_type"),
        Index("ix_limit_date_type", "trade_date", "limit_type"),
    )


# ─────────────────────────────────────────────────────────────────
# 13. 股东户数
# ─────────────────────────────────────────────────────────────────

class StockHolderCount(Base):
    """股东户数变动"""
    __tablename__ = "stock_holder_count"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    end_date: Mapped[str] = mapped_column(String(20), nullable=False, comment="截止日期")
    holder_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True, comment="股东户数")
    holder_count_change: Mapped[float | None] = mapped_column(Float, nullable=True, comment="较上期变化(%)")
    avg_hold_amount: Mapped[float | None] = mapped_column(Float, nullable=True, comment="户均持股金额(元)")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "end_date", name="uq_holder_count_symbol_date"),
    )


# ─────────────────────────────────────────────────────────────────
# 14. 同行比较（东方财富：成长性/估值/杜邦/规模）
# ─────────────────────────────────────────────────────────────────

class StockPeerComparison(Base):
    """同行比较 — 按 symbol 请求东财接口，每只股票当日 4 个子类型各一条"""
    __tablename__ = "stock_peer_comparison"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, comment="6 位代码")
    sub_type: Mapped[str] = mapped_column(String(20), nullable=False, comment="growth|valuation|dupont|scale")
    as_of_date: Mapped[str] = mapped_column(String(10), nullable=False, comment="同步日 YYYY-MM-DD")
    raw_data: Mapped[str | None] = mapped_column(Text, nullable=True, comment="整表 JSON")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "sub_type", "as_of_date", name="uq_peer_comparison_symbol_sub_date"),
        Index("ix_peer_comparison_symbol", "symbol"),
        Index("ix_peer_comparison_as_of_date", "as_of_date"),
    )


# ─────────────────────────────────────────────────────────────────
# 15. 个股资讯/公告
# ─────────────────────────────────────────────────────────────────

class StockNews(Base):
    """个股资讯/公告 — 来源: AKShare stock_news_em"""
    __tablename__ = "stock_news"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    content: Mapped[str | None] = mapped_column(Text, nullable=True)
    publish_time: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    source: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    url: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (Index("ix_stock_news_symbol_time", "symbol", "publish_time"),)


# ─────────────────────────────────────────────────────────────────
# 16. 互动易问答
# ─────────────────────────────────────────────────────────────────

class StockIrmQa(Base):
    """互动易/上证e互动问答 — 来源: AKShare stock_irm_cninfo（深市）+ stock_sns_sseinfo（沪市）"""
    __tablename__ = "stock_irm_qa"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    question_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    content: Mapped[str | None] = mapped_column(Text, nullable=True)  # 兼容旧数据，优先使用 question_content/answer_content
    question_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    answer_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    ask_time: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    answer_time: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    source: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("ix_stock_irm_qa_symbol_time", "symbol", "answer_time"),
        UniqueConstraint("symbol", "question_id", name="uk_stock_irm_qa_symbol_question"),
    )
