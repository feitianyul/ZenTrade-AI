from typing import Any

from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class ReplayReport(BaseModel):
    __tablename__ = "replay_reports"

    strategy_id: Mapped[str] = mapped_column(String(36), index=True)
    report_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(32), default="draft")
