from sqlalchemy import Boolean, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class Order(BaseModel):
    __tablename__ = "orders"

    user_id: Mapped[str] = mapped_column(String(64), index=True)
    env: Mapped[str] = mapped_column(String(16), default="sim", index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    direction: Mapped[str] = mapped_column(String(8))
    price: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32), default="submitted")
    gateway_ref: Mapped[str] = mapped_column(String(128))
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
