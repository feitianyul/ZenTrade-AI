"""T253 - 敏感操作 MFA 触发矩阵"""

from typing import Any

# MFA 触发矩阵：操作 -> 是否需要 MFA
MFA_TRIGGER_MATRIX: dict[str, dict[str, Any]] = {
    "login": {"require_mfa": False, "condition": None},
    "password_change": {"require_mfa": True, "condition": "always"},
    "env_switch_real": {"require_mfa": True, "condition": "always"},
    "full_auto_enable": {"require_mfa": True, "condition": "always"},
    "strategy_delete": {"require_mfa": True, "condition": "always"},
    "backup_restore": {"require_mfa": True, "condition": "always"},
    "data_export": {"require_mfa": True, "condition": "sensitive_data"},
    "role_change": {"require_mfa": True, "condition": "admin_action"},
    "config_update": {"require_mfa": True, "condition": "production_env"},
    "order_submit": {"require_mfa": False, "condition": None},
    "order_cancel": {"require_mfa": False, "condition": None},
    "profile_update": {"require_mfa": False, "condition": None},
}


async def should_require_mfa(
    operation: str,
    env: str = "sim",
    user_role: str = "user",
) -> dict[str, Any]:
    """判断操作是否需要 MFA"""
    config = MFA_TRIGGER_MATRIX.get(operation)
    if not config:
        return {"require_mfa": False, "operation": operation}

    require = config["require_mfa"]
    condition = config.get("condition")

    # 条件细化
    if condition == "production_env" and env != "real":
        require = False
    if condition == "admin_action" and user_role != "admin":
        require = True  # 非管理员更需要验证
    if condition == "sensitive_data" and env == "sim":
        require = False

    return {
        "require_mfa": require,
        "operation": operation,
        "condition": condition,
        "env": env,
    }


async def get_mfa_matrix() -> dict[str, dict[str, Any]]:
    """获取完整触发矩阵"""
    return MFA_TRIGGER_MATRIX


async def update_mfa_trigger(
    operation: str, require_mfa: bool, condition: str | None = None
) -> dict[str, Any]:
    """更新触发规则"""
    MFA_TRIGGER_MATRIX[operation] = {
        "require_mfa": require_mfa,
        "condition": condition,
    }
    return {"operation": operation, "updated": True}
