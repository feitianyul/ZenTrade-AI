"""T226 - AI 配置备份恢复服务"""

from datetime import datetime
from typing import Any, Optional

_config_backups: list[dict[str, Any]] = []


async def backup_config(
    tenant_id: str, config_type: str, config_data: dict[str, Any], created_by: str = ""
) -> dict[str, Any]:
    """备份 AI 配置"""
    record = {
        "id": f"cb-{len(_config_backups) + 1:04d}",
        "tenant_id": tenant_id,
        "config_type": config_type,
        "config_data": config_data,
        "created_by": created_by,
        "created_at": datetime.utcnow().isoformat(),
    }
    _config_backups.append(record)
    return record


async def restore_config(tenant_id: str, backup_id: str) -> dict[str, Any]:
    """从备份恢复配置"""
    for b in _config_backups:
        if b["id"] == backup_id and b["tenant_id"] == tenant_id:
            return {"status": "restored", "backup": b}
    return {"status": "error", "message": "backup not found"}


async def list_config_backups(tenant_id: str, config_type: Optional[str] = None) -> list[dict[str, Any]]:
    """列出配置备份"""
    results = [b for b in _config_backups if b["tenant_id"] == tenant_id]
    if config_type:
        results = [b for b in results if b["config_type"] == config_type]
    return results
