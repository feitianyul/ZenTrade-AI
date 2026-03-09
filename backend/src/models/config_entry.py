"""T255 - 配置中心模型"""

from sqlalchemy import Boolean, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class ConfigEntry(BaseModel):
    __tablename__ = "config_entries"

    namespace: Mapped[str] = mapped_column(String(64), index=True)
    key: Mapped[str] = mapped_column(String(128), index=True)
    value: Mapped[str] = mapped_column(Text, default="")
    value_type: Mapped[str] = mapped_column(String(16), default="string")  # string|int|float|json|bool
    version: Mapped[int] = mapped_column(Integer, default=1)
    description: Mapped[str] = mapped_column(String(256), default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
