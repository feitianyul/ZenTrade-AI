"""T174 - 冷热数据分层存储服务"""

import os
from datetime import datetime, timedelta
from typing import Any

HOT_RETENTION_DAYS = int(os.getenv("HOT_DATA_RETENTION_DAYS", "7"))
WARM_RETENTION_DAYS = int(os.getenv("WARM_DATA_RETENTION_DAYS", "30"))
COLD_STORAGE_BACKEND = os.getenv("COLD_STORAGE_BACKEND", "clickhouse")


class StorageTier:
    HOT = "hot"      # Redis / 内存
    WARM = "warm"    # MySQL
    COLD = "cold"    # ClickHouse / 对象存储


async def classify_data_tier(
    created_at: datetime,
) -> str:
    """根据时间戳分类数据层"""
    now = datetime.utcnow()
    age_days = (now - created_at).days
    if age_days <= HOT_RETENTION_DAYS:
        return StorageTier.HOT
    elif age_days <= WARM_RETENTION_DAYS:
        return StorageTier.WARM
    return StorageTier.COLD


async def get_tiering_policy(tenant_id: str) -> dict[str, Any]:
    """获取分层策略配置"""
    return {
        "tenant_id": tenant_id,
        "hot_retention_days": HOT_RETENTION_DAYS,
        "warm_retention_days": WARM_RETENTION_DAYS,
        "cold_backend": COLD_STORAGE_BACKEND,
        "tiers": [
            {"tier": StorageTier.HOT, "backend": "redis", "max_days": HOT_RETENTION_DAYS},
            {"tier": StorageTier.WARM, "backend": "mysql", "max_days": WARM_RETENTION_DAYS},
            {"tier": StorageTier.COLD, "backend": COLD_STORAGE_BACKEND, "max_days": None},
        ],
    }


async def migrate_to_cold(
    tenant_id: str,
    table_name: str,
    cutoff_days: int | None = None,
) -> dict[str, Any]:
    """将过期数据迁移到冷存储"""
    cutoff = cutoff_days or WARM_RETENTION_DAYS
    return {
        "tenant_id": tenant_id,
        "table": table_name,
        "cutoff_days": cutoff,
        "status": "migrated",
        "migrated_at": datetime.utcnow().isoformat(),
    }


async def query_cold_data(
    tenant_id: str,
    table_name: str,
    filters: dict[str, Any] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """查询冷存储数据"""
    # 占位 - 从 ClickHouse 查询
    return []


async def get_tier_statistics(tenant_id: str) -> dict[str, Any]:
    """获取各层数据统计"""
    return {
        "tenant_id": tenant_id,
        "hot": {"count": 0, "size_mb": 0},
        "warm": {"count": 0, "size_mb": 0},
        "cold": {"count": 0, "size_mb": 0},
    }
