from typing import Any

from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class TradeAnalysis(BaseModel):
    __tablename__ = "trade_analysis"

    trade_id: Mapped[str] = mapped_column(String(36), index=True)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    summary: Mapped[str] = mapped_column(String(256), default="")
