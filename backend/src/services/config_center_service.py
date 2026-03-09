"""T255 - 配置中心服务与版本发布

行情数据源五键（market_source, market_api_key, market_backup_source, market_backup_api_key,
market_refresh_interval）存什么返什么，不做脱敏或占位替换，与《配置中心-行情数据源需求规格说明书》2.3.1 一致。
"""

import json
from typing import Any, Optional

from sqlalchemy import func, select

from src.core.db import get_session, with_tenant
from src.models.config_entry import ConfigEntry


async def set_config(
    tenant_id: str,
    namespace: str,
    key: str,
    value: str,
    value_type: str = "string",
    description: str = "",
) -> dict[str, Any]:
    """设置配置项（自动去重：保留最新一条，软删除其余）"""
    async for session in get_session():
        # 查询所有活跃记录（可能存在重复）
        query = (
            with_tenant(select(ConfigEntry), ConfigEntry, tenant_id)
            .where(ConfigEntry.namespace == namespace)
            .where(ConfigEntry.key == key)
            .where(ConfigEntry.is_active.is_(True))
            .order_by(ConfigEntry.id.desc())
        )
        result = await session.execute(query)
        rows = result.scalars().all()

        if rows:
            # 保留第一条（最新），软删除其余重复记录
            existing = rows[0]
            for dup in rows[1:]:
                dup.is_active = False

            existing.value = value
            existing.value_type = value_type
            existing.version += 1
            if description:
                existing.description = description
            await session.commit()
            return {
                "namespace": namespace,
                "key": key,
                "version": existing.version,
                "status": "updated",
            }

        entry = ConfigEntry(
            tenant_id=tenant_id,
            namespace=namespace,
            key=key,
            value=value,
            value_type=value_type,
            description=description,
            version=1,
        )
        session.add(entry)
        await session.commit()
        return {
            "namespace": namespace,
            "key": key,
            "version": 1,
            "status": "created",
        }
    raise RuntimeError("database session unavailable")


async def get_config(
    tenant_id: str, namespace: str, key: str
) -> Optional[dict[str, Any]]:
    """获取配置项（取最新一条活跃记录）"""
    async for session in get_session():
        query = (
            with_tenant(select(ConfigEntry), ConfigEntry, tenant_id)
            .where(ConfigEntry.namespace == namespace)
            .where(ConfigEntry.key == key)
            .where(ConfigEntry.is_active.is_(True))
            .order_by(ConfigEntry.id.desc())
        )
        result = await session.execute(query)
        entry = result.scalars().first()
        if entry:
            return {
                "namespace": entry.namespace,
                "key": entry.key,
                "value": _parse_value(entry.value, entry.value_type),
                "value_type": entry.value_type,
                "version": entry.version,
                "description": entry.description,
            }
    return None


async def list_configs(
    tenant_id: str, namespace: Optional[str] = None, limit: int = 100
) -> list[dict[str, Any]]:
    """列出配置项"""
    async for session in get_session():
        query = (
            with_tenant(select(ConfigEntry), ConfigEntry, tenant_id)
            .where(ConfigEntry.is_active.is_(True))
        )
        if namespace:
            query = query.where(ConfigEntry.namespace == namespace)
        query = query.order_by(ConfigEntry.namespace, ConfigEntry.key).limit(limit)
        result = await session.execute(query)
        entries = result.scalars().all()
        return [
            {
                "namespace": e.namespace,
                "key": e.key,
                "value": _parse_value(e.value, e.value_type),
                "value_type": e.value_type,
                "version": e.version,
                "description": e.description,
            }
            for e in entries
        ]
    return []


async def list_tenant_ids_for_key(namespace: str, key: str) -> list[str]:
    """列出拥有该 namespace+key 配置的 tenant_id（用于定时任务等）。"""
    async for session in get_session():
        query = (
            select(ConfigEntry.tenant_id)
            .where(ConfigEntry.namespace == namespace)
            .where(ConfigEntry.key == key)
            .where(ConfigEntry.is_active.is_(True))
            .distinct()
        )
        result = await session.execute(query)
        return [row[0] for row in result.fetchall()]
    return []


async def delete_config(
    tenant_id: str, namespace: str, key: str
) -> dict[str, Any]:
    """删除（软删除）配置项 — 处理可能的重复记录"""
    async for session in get_session():
        query = (
            with_tenant(select(ConfigEntry), ConfigEntry, tenant_id)
            .where(ConfigEntry.namespace == namespace)
            .where(ConfigEntry.key == key)
        )
        result = await session.execute(query)
        entries = result.scalars().all()
        if entries:
            for entry in entries:
                entry.is_active = False
            await session.commit()
            return {"namespace": namespace, "key": key, "deleted": True}
    return {"deleted": False}


def _parse_value(value: str, value_type: str) -> Any:
    """解析配置值"""
    if value_type == "int":
        return int(value)
    if value_type == "float":
        return float(value)
    if value_type == "bool":
        return value.lower() in ("true", "1", "yes")
    if value_type == "json":
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value
