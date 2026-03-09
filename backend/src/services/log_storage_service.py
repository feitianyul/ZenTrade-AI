"""T173 - 日志隔离存储服务"""

import os
from typing import Any

LOG_STORAGE_BACKEND = os.getenv("LOG_STORAGE_BACKEND", "local")  # local | clickhouse | s3


async def store_log_batch(
    tenant_id: str,
    log_type: str,
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    """批量存储日志条目"""
    if LOG_STORAGE_BACKEND == "clickhouse":
        return await _store_to_clickhouse(tenant_id, log_type, entries)
    return await _store_local(tenant_id, log_type, entries)


async def query_logs(
    tenant_id: str,
    log_type: str,
    filters: dict[str, Any] | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """查询隔离存储的日志"""
    # 占位实现 - 按租户隔离查询
    return []


async def get_storage_usage(tenant_id: str) -> dict[str, Any]:
    """获取租户日志存储使用量"""
    return {
        "tenant_id": tenant_id,
        "backend": LOG_STORAGE_BACKEND,
        "total_entries": 0,
        "storage_size_mb": 0.0,
    }


async def _store_local(
    tenant_id: str, log_type: str, entries: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "backend": "local",
        "tenant_id": tenant_id,
        "log_type": log_type,
        "stored_count": len(entries),
    }


async def _store_to_clickhouse(
    tenant_id: str, log_type: str, entries: list[dict[str, Any]]
) -> dict[str, Any]:
    from src.core.clickhouse import execute_clickhouse

    # 占位 - 生产中构造 INSERT 语句
    return {
        "backend": "clickhouse",
        "tenant_id": tenant_id,
        "log_type": log_type,
        "stored_count": len(entries),
    }
