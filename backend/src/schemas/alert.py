from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(str, Enum):
    ACTIVE = "active"
    RESOLVED = "resolved"
    ACKNOWLEDGED = "acknowledged"
    PENDING = "pending"  # 前端用于“未处理”筛选，查询时按 active 处理


class AlertBase(BaseModel):
    title: str = Field(..., description="Alert title")
    message: str = Field(..., description="Alert message")
    level: AlertLevel = Field(default=AlertLevel.INFO, description="Alert level")
    source: str = Field(..., description="Alert source (e.g. system, strategy, trade)")
    metadata: Optional[dict] = Field(default=None, description="Additional metadata")


class AlertCreate(AlertBase):
    pass


class AlertUpdate(BaseModel):
    status: Optional[AlertStatus] = None
    resolved_at: Optional[datetime] = None


class AlertOut(AlertBase):
    id: str
    status: AlertStatus
    created_at: datetime
    updated_at: datetime
    resolved_at: Optional[datetime] = None

    class Config:
        from_attributes = True
