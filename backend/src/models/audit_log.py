from datetime import datetime

from sqlalchemy import DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class AuditLog(BaseModel):
    __tablename__ = "audit_logs"

    actor_id: Mapped[str] = mapped_column(String(64), index=True)
    action: Mapped[str] = mapped_column(String(128), index=True)
    resource_type: Mapped[str] = mapped_column(String(128))
    resource_id: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), default="success")
    ip_address: Mapped[str] = mapped_column(String(64))
    user_agent: Mapped[str] = mapped_column(String(256))
    detail: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
