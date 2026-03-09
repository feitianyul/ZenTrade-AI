"""T169 - 数据库迁移与回滚管理"""

import os
from datetime import datetime
from typing import Any, Optional

from src.core.db import get_engine


class MigrationRecord:
    """迁移记录"""

    def __init__(
        self,
        version: str,
        description: str,
        applied_at: Optional[str] = None,
        rolled_back: bool = False,
    ):
        self.version = version
        self.description = description
        self.applied_at = applied_at or datetime.utcnow().isoformat()
        self.rolled_back = rolled_back


# 内存中迁移注册表（生产用 alembic）
_migrations: list[MigrationRecord] = []
_applied: list[str] = []


async def register_migration(version: str, description: str) -> dict[str, Any]:
    """注册新迁移"""
    record = MigrationRecord(version=version, description=description)
    _migrations.append(record)
    return {"version": version, "description": description, "registered": True}


async def apply_migration(version: str) -> dict[str, Any]:
    """应用指定版本迁移"""
    for m in _migrations:
        if m.version == version:
            if version in _applied:
                return {"version": version, "status": "already_applied"}
            _applied.append(version)
            m.applied_at = datetime.utcnow().isoformat()
            return {"version": version, "status": "applied", "applied_at": m.applied_at}
    return {"version": version, "status": "not_found"}


async def rollback_migration(version: str) -> dict[str, Any]:
    """回滚指定版本迁移"""
    if version in _applied:
        _applied.remove(version)
        for m in _migrations:
            if m.version == version:
                m.rolled_back = True
        return {"version": version, "status": "rolled_back"}
    return {"version": version, "status": "not_applied"}


async def get_migration_status() -> dict[str, Any]:
    """获取迁移状态"""
    return {
        "total_registered": len(_migrations),
        "applied": list(_applied),
        "pending": [m.version for m in _migrations if m.version not in _applied],
    }


async def get_current_version() -> Optional[str]:
    """获取当前数据库版本"""
    if _applied:
        return _applied[-1]
    return None


async def auto_migrate() -> dict[str, Any]:
    """自动应用所有未执行的迁移"""
    results = []
    for m in _migrations:
        if m.version not in _applied:
            result = await apply_migration(m.version)
            results.append(result)
    return {"migrated": results, "current_version": await get_current_version()}
