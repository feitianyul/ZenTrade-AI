"""T168 - 全量与增量备份调度服务"""

import os
from datetime import datetime, timedelta
from typing import Any, Optional

FULL_BACKUP_INTERVAL_HOURS = int(os.getenv("FULL_BACKUP_INTERVAL_HOURS", "24"))
INCREMENTAL_BACKUP_INTERVAL_HOURS = int(os.getenv("INCR_BACKUP_INTERVAL_HOURS", "6"))


class BackupScheduleConfig:
    """备份调度配置"""

    def __init__(
        self,
        tenant_id: str,
        full_interval_hours: int = FULL_BACKUP_INTERVAL_HOURS,
        incremental_interval_hours: int = INCREMENTAL_BACKUP_INTERVAL_HOURS,
        enabled: bool = True,
        retention_count: int = 7,
    ):
        self.tenant_id = tenant_id
        self.full_interval_hours = full_interval_hours
        self.incremental_interval_hours = incremental_interval_hours
        self.enabled = enabled
        self.retention_count = retention_count


# 内存中调度配置（生产应持久化到数据库）
_schedules: dict[str, BackupScheduleConfig] = {}


async def get_schedule(tenant_id: str) -> dict[str, Any]:
    """获取备份调度配置"""
    config = _schedules.get(tenant_id)
    if not config:
        config = BackupScheduleConfig(tenant_id=tenant_id)
        _schedules[tenant_id] = config
    return {
        "tenant_id": config.tenant_id,
        "full_interval_hours": config.full_interval_hours,
        "incremental_interval_hours": config.incremental_interval_hours,
        "enabled": config.enabled,
        "retention_count": config.retention_count,
    }


async def update_schedule(
    tenant_id: str,
    full_interval_hours: Optional[int] = None,
    incremental_interval_hours: Optional[int] = None,
    enabled: Optional[bool] = None,
    retention_count: Optional[int] = None,
) -> dict[str, Any]:
    """更新备份调度配置"""
    config = _schedules.get(tenant_id, BackupScheduleConfig(tenant_id=tenant_id))
    if full_interval_hours is not None:
        config.full_interval_hours = max(1, full_interval_hours)
    if incremental_interval_hours is not None:
        config.incremental_interval_hours = max(1, incremental_interval_hours)
    if enabled is not None:
        config.enabled = enabled
    if retention_count is not None:
        config.retention_count = max(1, retention_count)
    _schedules[tenant_id] = config
    return await get_schedule(tenant_id)


async def trigger_full_backup(tenant_id: str) -> dict[str, Any]:
    """触发全量备份"""
    return {
        "tenant_id": tenant_id,
        "type": "full",
        "status": "triggered",
        "triggered_at": datetime.utcnow().isoformat(),
    }


async def trigger_incremental_backup(tenant_id: str) -> dict[str, Any]:
    """触发增量备份"""
    return {
        "tenant_id": tenant_id,
        "type": "incremental",
        "status": "triggered",
        "triggered_at": datetime.utcnow().isoformat(),
    }


async def get_next_backup_time(tenant_id: str) -> dict[str, Any]:
    """获取下次备份时间"""
    config = _schedules.get(tenant_id, BackupScheduleConfig(tenant_id=tenant_id))
    now = datetime.utcnow()
    return {
        "tenant_id": tenant_id,
        "next_full": (now + timedelta(hours=config.full_interval_hours)).isoformat(),
        "next_incremental": (
            now + timedelta(hours=config.incremental_interval_hours)
        ).isoformat(),
    }


async def list_backup_history(
    tenant_id: str, limit: int = 20
) -> list[dict[str, Any]]:
    """列出备份历史（占位实现）"""
    return []
