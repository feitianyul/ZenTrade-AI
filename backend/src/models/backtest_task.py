from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class BacktestTask(BaseModel):
    __tablename__ = "backtest_tasks"

    strategy_id: Mapped[str] = mapped_column(String(36), index=True)
    start_date: Mapped[str] = mapped_column(String(32))
    end_date: Mapped[str] = mapped_column(String(32))
    result_metrics_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(
        String(32), default="pending", index=True
    )  # pending | running | completed | failed
    request_params_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    error_detail: Mapped[str | None] = mapped_column(String(512), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
