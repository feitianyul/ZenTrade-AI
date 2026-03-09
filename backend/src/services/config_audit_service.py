"""T257 - 配置审计服务"""

from datetime import datetime
from typing import Any, Optional

_audit_entries: list[dict[str, Any]] = []


async def log_config_operation(
    tenant_id: str,
    actor_id: str,
    namespace: str,
    key: str,
    action: str,
    old_value: Optional[str] = None,
    new_value: Optional[str] = None,
) -> dict[str, Any]:
    """记录配置操作"""
    entry = {
        "id": f"ca-{len(_audit_entries) + 1:04d}",
        "tenant_id": tenant_id,
        "actor_id": actor_id,
        "namespace": namespace,
        "key": key,
        "action": action,
        "old_value": old_value,
        "new_value": new_value,
        "timestamp": datetime.utcnow().isoformat(),
    }
    _audit_entries.append(entry)
    return entry


async def get_config_audit_trail(
    tenant_id: str,
    namespace: Optional[str] = None,
    key: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """获取配置审计记录"""
    results = [e for e in _audit_entries if e["tenant_id"] == tenant_id]
    if namespace:
        results = [e for e in results if e["namespace"] == namespace]
    if key:
        results = [e for e in results if e["key"] == key]
    return results[:limit]
