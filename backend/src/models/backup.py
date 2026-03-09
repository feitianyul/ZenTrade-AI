import enum
import uuid
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy import Enum as SAEnum

from src.models.base import Base


class BackupStatus(str, enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"

class Backup(Base):
    __tablename__ = "backups"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id = Column(String(36), nullable=False, index=True)
    parent_id = Column(String(36), nullable=True, index=True)  # 增量备份依赖的全量备份 ID
    name = Column(String(255), nullable=False)
    type = Column(String(32), default="full")  # full, incremental
    status = Column(SAEnum(BackupStatus), default=BackupStatus.PENDING)
    # 备份内容枚举: mysql, qdrant, ai_config, redis, system_config, clickhouse
    content = Column(String(512), nullable=True)  # JSON 数组序列化，如 ["mysql","ai_config"]
    content_summary = Column(String(255), nullable=True)  # 列表展示用摘要
    destination = Column(String(32), default="local")  # local | aliyundrive(二期) | baidupan(二期)
    size_bytes = Column(Integer, nullable=True)
    location = Column(String(1024), nullable=True)  # 本地路径或网盘 path/key
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    error_detail = Column(String(1024), nullable=True)  # 失败时错误信息
    log_url = Column(String(512), nullable=True)  # 失败时日志入口（可选）
    progress_percent = Column(Integer, nullable=True)  # 0-100，当前步骤进度
    log_entries = Column(String(65535), nullable=True)  # JSON 数组，步骤日志 [{step, message, status, ts}]
