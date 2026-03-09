"""T225 - 配置版本管理工具"""

from datetime import datetime
from typing import Any, Optional

_version_store: dict[str, list[dict[str, Any]]] = {}


async def save_version(
    namespace: str,
    key: str,
    data: dict[str, Any],
    author: str = "",
) -> dict[str, Any]:
    """保存配置版本"""
    store_key = f"{namespace}:{key}"
    versions = _version_store.setdefault(store_key, [])
    version_no = len(versions) + 1
    record = {
        "version": version_no,
        "data": data,
        "author": author,
        "created_at": datetime.utcnow().isoformat(),
    }
    versions.append(record)
    return record


async def get_version(
    namespace: str, key: str, version: Optional[int] = None
) -> Optional[dict[str, Any]]:
    """获取配置版本"""
    store_key = f"{namespace}:{key}"
    versions = _version_store.get(store_key, [])
    if not versions:
        return None
    if version:
        for v in versions:
            if v["version"] == version:
                return v
        return None
    return versions[-1]


async def list_versions(namespace: str, key: str) -> list[dict[str, Any]]:
    """列出版本历史"""
    store_key = f"{namespace}:{key}"
    return _version_store.get(store_key, [])


async def rollback(namespace: str, key: str, target_version: int) -> Optional[dict[str, Any]]:
    """回滚到指定版本"""
    target = await get_version(namespace, key, target_version)
    if not target:
        return None
    return await save_version(namespace, key, target["data"], "rollback")
