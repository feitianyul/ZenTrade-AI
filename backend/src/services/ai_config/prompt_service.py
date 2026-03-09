"""T225 - 提示词配置服务与版本管理"""

from datetime import datetime
from typing import Any, Optional

# 内存存储（生产使用数据库）
_prompt_configs: dict[str, list[dict[str, Any]]] = {}


async def create_prompt(
    tenant_id: str,
    name: str,
    template: str,
    variables: list[str] | None = None,
    created_by: str = "",
) -> dict[str, Any]:
    """创建提示词配置"""
    key = f"{tenant_id}:{name}"
    version = len(_prompt_configs.get(key, [])) + 1
    record = {
        "name": name,
        "template": template,
        "variables": variables or [],
        "version": version,
        "created_by": created_by,
        "created_at": datetime.utcnow().isoformat(),
        "is_active": True,
    }
    _prompt_configs.setdefault(key, []).append(record)
    return record


async def get_prompt(
    tenant_id: str, name: str, version: Optional[int] = None
) -> Optional[dict[str, Any]]:
    """获取提示词配置"""
    key = f"{tenant_id}:{name}"
    versions = _prompt_configs.get(key, [])
    if not versions:
        return None
    if version:
        for v in versions:
            if v["version"] == version:
                return v
        return None
    return versions[-1]  # latest


async def list_prompts(tenant_id: str) -> list[dict[str, Any]]:
    """列出所有提示词"""
    results = []
    for key, versions in _prompt_configs.items():
        if key.startswith(f"{tenant_id}:"):
            if versions:
                results.append(versions[-1])
    return results


async def rollback_prompt(
    tenant_id: str, name: str, target_version: int
) -> dict[str, Any]:
    """回滚到指定版本"""
    target = await get_prompt(tenant_id, name, target_version)
    if not target:
        return {"status": "error", "message": "version not found"}
    # 创建新版本（内容来自旧版本）
    return await create_prompt(
        tenant_id, name, target["template"], target.get("variables"), "rollback"
    )
