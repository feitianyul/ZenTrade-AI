"""T254 - 状态快照存储"""

from datetime import datetime
from typing import Any, Optional

_snapshots: dict[str, list[dict[str, Any]]] = {}


async def save(
    entity_key: str, state: str, data: dict[str, Any]
) -> dict[str, Any]:
    """保存状态快照"""
    record = {
        "state": state,
        "data": data,
        "timestamp": datetime.utcnow().isoformat(),
    }
    _snapshots.setdefault(entity_key, []).append(record)
    return record


async def get_latest(entity_key: str) -> Optional[dict[str, Any]]:
    """获取最新快照"""
    entries = _snapshots.get(entity_key, [])
    return entries[-1] if entries else None


async def get_history(entity_key: str, limit: int = 10) -> list[dict[str, Any]]:
    """获取快照历史"""
    return _snapshots.get(entity_key, [])[-limit:]


async def rollback(entity_key: str) -> Optional[dict[str, Any]]:
    """回滚到前一个快照"""
    entries = _snapshots.get(entity_key, [])
    if len(entries) >= 2:
        entries.pop()  # remove current
        return entries[-1]
    return None
