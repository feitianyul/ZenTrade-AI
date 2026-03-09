from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class AgentTask(BaseModel):
    __tablename__ = "agent_tasks"

    agent_id: Mapped[str] = mapped_column(String(36), ForeignKey("agents.id"), nullable=True)
    task_type: Mapped[str] = mapped_column(String(50))
    payload: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(
        String(20),
        default="pending",
    )  # pending, running, completed, failed
    result: Mapped[dict] = mapped_column(JSON, nullable=True)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    error_msg: Mapped[str] = mapped_column(String(500), nullable=True)
