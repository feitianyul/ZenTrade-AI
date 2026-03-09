import uuid
from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, String
from sqlalchemy import Enum as SAEnum

from src.models.base import Base
from src.schemas.alert import AlertLevel, AlertStatus


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id = Column(String(36), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    message = Column(String(1024), nullable=False)
    level = Column(SAEnum(AlertLevel), default=AlertLevel.INFO)
    status = Column(SAEnum(AlertStatus), default=AlertStatus.ACTIVE)
    source = Column(String(64), nullable=False)
    metadata_info = Column(
        JSON,
        nullable=True,
    )  # 'metadata' is reserved in SQLAlchemy Base sometimes
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)
