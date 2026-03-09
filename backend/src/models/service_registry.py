from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import BaseModel


class ServiceRegistry(BaseModel):
    __tablename__ = "service_registry"

    service_name: Mapped[str] = mapped_column(String(128), index=True)
    version: Mapped[str] = mapped_column(String(32))
    endpoint: Mapped[str] = mapped_column(String(256))
    protocol: Mapped[str] = mapped_column(String(32))
    health_status: Mapped[str] = mapped_column(String(32), default="unknown")
    meta: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict)
    last_heartbeat: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
    )
