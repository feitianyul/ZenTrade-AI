"""AI 分析报告版本：按标的存储分析快照，支持列表/查看/删除。"""
from typing import Any

from sqlalchemy import JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class AiAnalysisReport(BaseModel):
    __tablename__ = "ai_analysis_reports"

    symbol: Mapped[str] = mapped_column(String(20), index=True)
    report_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    user_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="draft")  # draft | adopted
    created_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
