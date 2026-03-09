from typing import Any, Optional

from sqlalchemy import JSON, Boolean, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class Strategy(BaseModel):
    __tablename__ = "strategies"

    name: Mapped[str] = mapped_column(String(128))
    active_version_no: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=None)
    logic_desc: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    logic_code: Mapped[str] = mapped_column(Text)
    params_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(32), default="draft")
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    # --- 回测摘要 (最近一次) ---
    last_backtest_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, default=None)
    last_backtest_grade: Mapped[Optional[str]] = mapped_column(String(4), nullable=True, default=None)
    last_backtest_metrics: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True, default=None)
    source_report_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, default=None)
