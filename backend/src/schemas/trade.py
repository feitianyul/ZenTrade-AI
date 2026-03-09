from typing import Literal

from pydantic import BaseModel, Field


class OrderRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=32)
    direction: Literal["BUY", "SELL"]
    price: float = Field(..., gt=0)
    volume: int = Field(..., gt=0)
    env: Literal["sim", "real"] = "sim"


class OrderOut(BaseModel):
    order_id: str
    symbol: str
    status: str
    direction: str
    price: float
    volume: int
    env: str


class PositionOut(BaseModel):
    symbol: str
    volume: int
    pnl: float
    avg_price: float
    frozen_volume: int
    env: str
