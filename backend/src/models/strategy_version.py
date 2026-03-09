from typing import Any

from sqlalchemy import JSON, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class StrategyVersion(BaseModel):
    __tablename__ = "strategy_versions"

    strategy_id: Mapped[str] = mapped_column(String(36), index=True)
    version_no: Mapped[int] = mapped_column(Integer)
    content_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_by: Mapped[str] = mapped_column(String(64))
