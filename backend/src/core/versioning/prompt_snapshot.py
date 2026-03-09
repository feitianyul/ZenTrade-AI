"""T234 - 自优化回滚与版本快照"""

from datetime import datetime
from typing import Any, Optional

_snapshots: list[dict[str, Any]] = []


async def create_snapshot(
    tenant_id: str,
    snapshot_type: str,
    data: dict[str, Any],
    trigger: str = "manual",
) -> dict[str, Any]:
    """创建快照"""
    record = {
        "id": f"snap-{len(_snapshots) + 1:04d}",
        "tenant_id": tenant_id,
        "snapshot_type": snapshot_type,
        "data": data,
        "trigger": trigger,
        "created_at": datetime.utcnow().isoformat(),
    }
    _snapshots.append(record)
    return record


async def restore_snapshot(
    tenant_id: str, snapshot_id: str
) -> Optional[dict[str, Any]]:
    """恢复快照"""
    for s in _snapshots:
        if s["id"] == snapshot_id and s["tenant_id"] == tenant_id:
            return {"status": "restored", "snapshot": s}
    return None


async def list_snapshots(
    tenant_id: str, snapshot_type: Optional[str] = None, limit: int = 20
) -> list[dict[str, Any]]:
    """列出快照"""
    results = [s for s in _snapshots if s["tenant_id"] == tenant_id]
    if snapshot_type:
        results = [s for s in results if s["snapshot_type"] == snapshot_type]
    return results[:limit]
