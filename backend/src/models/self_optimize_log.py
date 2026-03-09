from datetime import datetime

from sqlalchemy import JSON, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class SelfOptimizeLog(BaseModel):
    __tablename__ = "self_optimize_logs"

    trigger_type: Mapped[str] = mapped_column(
        String(50),
    )  # e.g., 'user_feedback', 'perf_degradation'
    action_taken: Mapped[str] = mapped_column(String(100))
    details: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(20))  # success, failed
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
    )
