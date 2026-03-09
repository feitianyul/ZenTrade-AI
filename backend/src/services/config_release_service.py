"""T257 - 配置发布审计与回滚"""

from datetime import datetime
from typing import Any, Optional

_releases: list[dict[str, Any]] = []


async def create_release(
    tenant_id: str,
    namespace: str,
    changes: list[dict[str, Any]],
    author: str,
    description: str = "",
) -> dict[str, Any]:
    """创建配置发布"""
    release = {
        "id": f"rel-{len(_releases) + 1:04d}",
        "tenant_id": tenant_id,
        "namespace": namespace,
        "changes": changes,
        "author": author,
        "description": description,
        "status": "published",
        "published_at": datetime.utcnow().isoformat(),
    }
    _releases.append(release)
    return release


async def rollback_release(
    tenant_id: str, release_id: str
) -> dict[str, Any]:
    """回滚配置发布"""
    for r in _releases:
        if r["id"] == release_id and r["tenant_id"] == tenant_id:
            r["status"] = "rolled_back"
            r["rolled_back_at"] = datetime.utcnow().isoformat()
            return r
    return {"error": "release not found"}


async def list_releases(
    tenant_id: str, namespace: Optional[str] = None, limit: int = 20
) -> list[dict[str, Any]]:
    """列出发布历史"""
    results = [r for r in _releases if r["tenant_id"] == tenant_id]
    if namespace:
        results = [r for r in results if r["namespace"] == namespace]
    return results[:limit]
