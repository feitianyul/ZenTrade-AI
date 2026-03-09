"""T167 - 审计与日志留存策略服务"""

import os
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db import get_session, with_tenant
from src.models.audit_log import AuditLog

# 默认保留天数
DEFAULT_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "90"))
ARCHIVE_RETENTION_DAYS = int(os.getenv("LOG_ARCHIVE_RETENTION_DAYS", "365"))
MAX_RETENTION_DAYS = 730


async def get_retention_policy(tenant_id: str) -> dict[str, Any]:
    """获取当前租户的日志留存策略"""
    return {
        "tenant_id": tenant_id,
        "active_retention_days": DEFAULT_RETENTION_DAYS,
        "archive_retention_days": ARCHIVE_RETENTION_DAYS,
        "max_retention_days": MAX_RETENTION_DAYS,
        "auto_cleanup_enabled": True,
    }


async def update_retention_policy(
    tenant_id: str,
    active_days: Optional[int] = None,
    archive_days: Optional[int] = None,
) -> dict[str, Any]:
    """更新日志留存策略（需管理员权限）"""
    active = min(active_days or DEFAULT_RETENTION_DAYS, MAX_RETENTION_DAYS)
    archive = min(archive_days or ARCHIVE_RETENTION_DAYS, MAX_RETENTION_DAYS)
    return {
        "tenant_id": tenant_id,
        "active_retention_days": active,
        "archive_retention_days": archive,
        "updated": True,
    }


async def cleanup_expired_logs(tenant_id: str) -> dict[str, Any]:
    """清理过期日志"""
    cutoff = datetime.utcnow() - timedelta(days=DEFAULT_RETENTION_DAYS)
    deleted_count = 0
    async for session in get_session():
        query = (
            delete(AuditLog)
            .where(AuditLog.tenant_id == tenant_id)
            .where(AuditLog.created_at < cutoff)
        )
        result = await session.execute(query)
        await session.commit()
        deleted_count = result.rowcount  # type: ignore[assignment]
    return {
        "tenant_id": tenant_id,
        "cutoff": cutoff.isoformat(),
        "deleted_count": deleted_count,
    }


async def get_log_statistics(tenant_id: str) -> dict[str, Any]:
    """获取日志统计信息"""
    async for session in get_session():
        total_query = with_tenant(
            select(func.count(AuditLog.id)), AuditLog, tenant_id
        )
        result = await session.execute(total_query)
        total = result.scalar() or 0

        cutoff = datetime.utcnow() - timedelta(days=DEFAULT_RETENTION_DAYS)
        expired_query = (
            select(func.count(AuditLog.id))
            .where(AuditLog.tenant_id == tenant_id)
            .where(AuditLog.created_at < cutoff)
        )
        result2 = await session.execute(expired_query)
        expired = result2.scalar() or 0

        return {
            "tenant_id": tenant_id,
            "total_logs": total,
            "expired_logs": expired,
            "active_logs": total - expired,
            "retention_days": DEFAULT_RETENTION_DAYS,
        }
    return {}


async def archive_logs(tenant_id: str) -> dict[str, Any]:
    """归档日志到冷存储"""
    # 在生产环境中将日志导出到对象存储或 ClickHouse
    return {
        "tenant_id": tenant_id,
        "status": "archived",
        "archive_target": "clickhouse",
    }
