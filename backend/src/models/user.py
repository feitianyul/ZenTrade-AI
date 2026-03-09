from sqlalchemy import Boolean, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class User(BaseModel):
    __tablename__ = "users"

    phone: Mapped[str] = mapped_column(String(256), unique=True, index=True)
    email: Mapped[str | None] = mapped_column(String(256), unique=True, index=True, nullable=True)
    nickname: Mapped[str | None] = mapped_column(String(64), nullable=True)
    password_hash: Mapped[str] = mapped_column(String(256))
    level: Mapped[str] = mapped_column(String(32), default="basic")
    risk_level: Mapped[str] = mapped_column(String(32), default="unassessed")
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)
    ai_calls_limit_override: Mapped[int | None] = mapped_column(Integer, nullable=True)
