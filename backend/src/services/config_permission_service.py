"""T256 - 配置权限控制服务"""

from typing import Any

# 命名空间权限矩阵
NAMESPACE_PERMISSIONS: dict[str, dict[str, list[str]]] = {
    "system": {"read": ["admin", "ops"], "write": ["admin"]},
    "trading": {"read": ["admin", "ops", "trader"], "write": ["admin"]},
    "ai": {"read": ["admin", "ops"], "write": ["admin"]},
    "user": {"read": ["admin", "ops", "user"], "write": ["admin", "user"]},
    "market": {"read": ["admin", "ops", "trader", "user"], "write": ["admin"]},
}


async def check_config_access(
    namespace: str,
    action: str,
    user_role: str,
) -> dict[str, Any]:
    """检查配置访问权限"""
    perms = NAMESPACE_PERMISSIONS.get(namespace, {"read": ["admin"], "write": ["admin"]})
    allowed_roles = perms.get(action, [])
    allowed = user_role in allowed_roles
    return {
        "namespace": namespace,
        "action": action,
        "user_role": user_role,
        "allowed": allowed,
    }


async def list_accessible_namespaces(user_role: str) -> list[str]:
    """列出用户可访问的命名空间"""
    accessible = []
    for ns, perms in NAMESPACE_PERMISSIONS.items():
        if user_role in perms.get("read", []):
            accessible.append(ns)
    return accessible
