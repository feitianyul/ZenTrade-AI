"""Migration version tracking for database migrations."""
from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class MigrationVersion(Base):
    """Tracks applied database migrations."""

    __tablename__ = "migration_versions"

    version: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(256))
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
