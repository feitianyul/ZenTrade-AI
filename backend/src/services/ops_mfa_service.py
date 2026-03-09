"""T172 - 高危操作二次验证服务"""

import hashlib
import os
import secrets
import time
from typing import Any, Optional

MFA_SECRET = os.getenv("OPS_MFA_SECRET", "ops-mfa-dev-secret")
MFA_CODE_TTL = int(os.getenv("OPS_MFA_CODE_TTL", "300"))  # 5 minutes

# 高危操作列表
HIGH_RISK_OPERATIONS = [
    "backup_restore",
    "data_delete",
    "config_change",
    "user_role_change",
    "env_switch_to_real",
    "strategy_delete",
    "full_auto_enable",
    "export_sensitive",
]

# 内存 MFA 验证码存储（生产使用 Redis）
_mfa_codes: dict[str, dict[str, Any]] = {}


def is_high_risk(operation: str) -> bool:
    """判断操作是否为高危"""
    return operation in HIGH_RISK_OPERATIONS


async def generate_mfa_code(user_id: str, operation: str) -> dict[str, Any]:
    """为高危操作生成 MFA 验证码"""
    code = f"{secrets.randbelow(1000000):06d}"
    expires_at = time.time() + MFA_CODE_TTL
    key = f"{user_id}:{operation}"
    _mfa_codes[key] = {
        "code": hashlib.sha256(f"{MFA_SECRET}:{code}".encode()).hexdigest(),
        "expires_at": expires_at,
        "operation": operation,
    }
    return {
        "user_id": user_id,
        "operation": operation,
        "code": code,  # 生产中通过短信/邮件发送
        "expires_in": MFA_CODE_TTL,
    }


async def verify_mfa_code(
    user_id: str, operation: str, code: str
) -> dict[str, Any]:
    """验证 MFA 码"""
    key = f"{user_id}:{operation}"
    stored = _mfa_codes.get(key)
    if not stored:
        return {"verified": False, "reason": "no_pending_code"}

    if time.time() > stored["expires_at"]:
        _mfa_codes.pop(key, None)
        return {"verified": False, "reason": "code_expired"}

    code_hash = hashlib.sha256(f"{MFA_SECRET}:{code}".encode()).hexdigest()
    if code_hash != stored["code"]:
        return {"verified": False, "reason": "invalid_code"}

    _mfa_codes.pop(key, None)
    return {"verified": True, "operation": operation}


async def require_mfa_for_operation(
    user_id: str, operation: str, mfa_code: Optional[str] = None
) -> dict[str, Any]:
    """高危操作 MFA 网关"""
    if not is_high_risk(operation):
        return {"allowed": True, "mfa_required": False}

    if not mfa_code:
        result = await generate_mfa_code(user_id, operation)
        return {
            "allowed": False,
            "mfa_required": True,
            "message": "请输入发送到您手机的验证码",
            "expires_in": result["expires_in"],
        }

    verification = await verify_mfa_code(user_id, operation, mfa_code)
    if verification["verified"]:
        return {"allowed": True, "mfa_required": True, "mfa_verified": True}
    return {
        "allowed": False,
        "mfa_required": True,
        "reason": verification.get("reason", "unknown"),
    }


async def list_high_risk_operations() -> list[str]:
    """列出所有高危操作"""
    return HIGH_RISK_OPERATIONS
