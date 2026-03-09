from typing import Any, Optional

from pydantic import BaseModel, Field


class MarketQuote(BaseModel):
    symbol: str
    price: float
    change: float  # 涨跌幅 %
    volume: float
    name: Optional[str] = None  # 股票名称
    open: Optional[float] = None  # 今开
    high: Optional[float] = None  # 最高
    low: Optional[float] = None  # 最低
    pre_close: Optional[float] = None  # 昨收
    amount: Optional[float] = None  # 成交额


# ---------------------------------------------------------------------------
# K-Line (candlestick) data
# ---------------------------------------------------------------------------

class KlineBar(BaseModel):
    """Single K-line bar (OHLCV)."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: Optional[float] = None  # 成交额


class KlineResponse(BaseModel):
    symbol: str
    period: str  # daily / weekly / monthly
    bars: list[KlineBar]


# ---------------------------------------------------------------------------
# Minute / time-sharing data
# ---------------------------------------------------------------------------

class MinuteBar(BaseModel):
    """Single minute-level bar for time-sharing chart."""
    time: str  # HH:MM or full datetime
    price: float
    avg_price: Optional[float] = None  # 均价线
    volume: float
    change_pct: Optional[float] = None


class MinuteResponse(BaseModel):
    symbol: str
    pre_close: float  # 昨收价 (for time-sharing baseline)
    bars: list[MinuteBar]


# ---------------------------------------------------------------------------
# Fundamental / F10 data
# ---------------------------------------------------------------------------

class FundamentalItem(BaseModel):
    """Single fundamental data item."""
    item: str
    value: str


class FundamentalResponse(BaseModel):
    symbol: str
    name: Optional[str] = None
    items: list[FundamentalItem]
    # Phase 2 扩展：F10 十大股东、分红配股、股东户数（方案 A）
    top_holders: Optional[list[dict[str, Any]]] = None
    dividends: Optional[list[dict[str, Any]]] = None
    holder_count: Optional[list[dict[str, Any]]] = None


class MarketDepth(BaseModel):
    symbol: str
    bids: list[list[float]]
    asks: list[list[float]]


class MarketAlertCreate(BaseModel):
    symbol: str
    condition: str
    threshold: float
    level: str = Field(default="info", max_length=16)


class MarketAlertOut(BaseModel):
    alert_id: str
    symbol: str
    condition: str
    threshold: float
    level: str
    status: str


class MarketSchemaMap(BaseModel):
    source: str
    mapping: dict[str, Any]


class MarketSubscription(BaseModel):
    symbols: list[str]
    channel: str = "quote"
