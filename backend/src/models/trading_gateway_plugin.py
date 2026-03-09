from typing import Any

from sqlalchemy import JSON, Boolean, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class TradingGatewayPlugin(BaseModel):
    __tablename__ = "trading_gateway_plugins"

    name: Mapped[str] = mapped_column(String(64), index=True)
    provider: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="active")
    supported_markets: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
