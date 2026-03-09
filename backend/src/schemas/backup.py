import json
from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator

from src.models.backup import BackupStatus

# 备份内容枚举（与规格 5.2 一致）
CONTENT_OPTIONS = ["mysql", "qdrant", "ai_config", "redis", "system_config", "clickhouse"]
# 备份目标：一期仅 local；二期 aliyundrive / baidupan
DESTINATION_OPTIONS = ["local", "aliyundrive", "baidupan"]


class BackupCreate(BaseModel):
    name: str
    type: str = "full"  # full | incremental
    content: List[str] = Field(default_factory=lambda: ["mysql", "ai_config", "system_config"], description="备份内容")
    destination: str = "local"  # 一期仅 local


class BackupRestoreBody(BaseModel):
    """恢复时可选的配置（配置备份时生效）"""
    restore_config: Optional[str] = None  # db_only | file_only | both


class BackupPolicyOut(BaseModel):
    """GET /backup-policy 响应"""
    schedule_cron: str = "0 2 * * *"  # 保留兼容
    full_interval_days: int = 1  # 1=每日, 7=每7天, 30=每30天
    schedule_time: str = "02:00"  # HH:mm
    incremental_enabled: bool = False
    retention_days: int = 90
    enabled: bool = True


class BackupPolicyPut(BaseModel):
    """PUT /backup-policy 请求体"""
    schedule_cron: Optional[str] = None
    full_interval_days: Optional[int] = None
    schedule_time: Optional[str] = None
    incremental_enabled: Optional[bool] = None
    retention_days: Optional[int] = None
    enabled: Optional[bool] = None


class BackupOut(BaseModel):
    id: str
    tenant_id: str
    parent_id: Optional[str] = None
    name: str
    type: str
    status: BackupStatus
    content_summary: Optional[str] = None
    destination: Optional[str] = None
    size_bytes: Optional[int] = None
    location: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    error_detail: Optional[str] = None
    log_url: Optional[str] = None
    progress_percent: Optional[int] = None
    log_entries: Optional[List[dict[str, Any]]] = None

    @field_validator("log_entries", mode="before")
    @classmethod
    def parse_log_entries(cls, v: Any) -> Optional[List[dict[str, Any]]]:
        if v is None:
            return None
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                data = json.loads(v)
                return data if isinstance(data, list) else None
            except (json.JSONDecodeError, TypeError):
                return None
        return None

    class Config:
        from_attributes = True


class BackupListData(BaseModel):
    """GET /backups 分页响应 data 结构"""
    items: List[BackupOut]
    total: int
    page: int
    page_size: int
