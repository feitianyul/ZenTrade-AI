from sqlalchemy import Boolean, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class Position(BaseModel):
    __tablename__ = "positions"

    user_id: Mapped[str] = mapped_column(String(64), index=True)
    env: Mapped[str] = mapped_column(String(16), default="sim", index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    volume: Mapped[int] = mapped_column(Integer, default=0)
    avg_price: Mapped[float] = mapped_column(Float, default=0.0)
    pnl: Mapped[float] = mapped_column(Float, default=0.0)
    frozen_volume: Mapped[int] = mapped_column(Integer, default=0)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
